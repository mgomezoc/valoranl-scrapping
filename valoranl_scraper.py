# valoranl_scraper.py
"""
Scraper de portales inmobiliarios para ValoraNL.

Portales soportados:
- CASAS365
- Century 21 Taurus
- Herrera & Palacios

Base de datos:
- MySQL, esquema `valoranl_market` con tablas:
    - portals
    - properties
    - property_sources
"""

import logging
import os
import random
import re
import time
from contextlib import contextmanager
from datetime import datetime
from threading import Lock
from urllib.parse import urljoin

import pymysql
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ══════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ══════════════════════════════════════════════════════════════

DB_CONFIG = {
    "host": os.getenv("VALORANL_DB_HOST", "127.0.0.1"),
    "user": os.getenv("VALORANL_DB_USER", "mgomez"),
    "password": os.getenv("VALORANL_DB_PASSWORD", "Rrasec13!"),
    "database": os.getenv("VALORANL_DB_NAME", "valoranl_market"),
    "port": int(os.getenv("VALORANL_DB_PORT", "3306")),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": False,
}

HTTP_TIMEOUT = 15

USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Safari/605.1.15"
    ),
]

DEFAULT_HEADERS = {
    "User-Agent": random.choice(USER_AGENTS),
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
}

PORTAL_CONFIGS = {
    "CASAS365": {"max_pages": 5},
    "C21_TAURUS": {"max_pages": 3},
    "HERRERA_PALACIOS": {"max_pages": 2},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("valoranl_scraper")

_PORTAL_ID_CACHE: dict[str, int] = {}

# ══════════════════════════════════════════════════════════════
# RATE LIMIT, PERF, RETRIES
# ══════════════════════════════════════════════════════════════


class RateLimiter:
    """Rate limiter simple: máximo N llamadas por segundo."""

    def __init__(self, calls_per_second: float = 0.5):
        self.calls_per_second = calls_per_second
        self.last_call = 0.0
        self.lock = Lock()

    def wait(self):
        with self.lock:
            elapsed = time.time() - self.last_call
            min_interval = 1.0 / self.calls_per_second
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            self.last_call = time.time()


RATE_LIMITER = RateLimiter(calls_per_second=0.5)


def random_sleep(min_sec: float = 2, max_sec: float = 5) -> None:
    """Pausa pseudo-aleatoria para evitar bloqueo."""
    time.sleep(random.uniform(min_sec, max_sec))


@contextmanager
def measure_performance(label: str):
    start = time.time()
    try:
        yield
    finally:
        elapsed = time.time() - start
        logger.info("%s took %.2f seconds", label, elapsed)


def create_resilient_session() -> requests.Session:
    """Sesión HTTP con reintentos y backoff."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(DEFAULT_HEADERS)
    return session


# ══════════════════════════════════════════════════════════════
# UTILIDADES DB
# ══════════════════════════════════════════════════════════════


def get_db_connection():
    return pymysql.connect(**DB_CONFIG)


def get_portal_id(conn, portal_code: str) -> int:
    """
    Obtiene el ID del portal por su código (ej. 'CASAS365').
    Usa caché en memoria.
    """
    if portal_code in _PORTAL_ID_CACHE:
        return _PORTAL_ID_CACHE[portal_code]

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM portals WHERE code = %s", (portal_code,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                f"No se encontró portal con code='{portal_code}' en la tabla portals."
            )
        portal_id = row["id"]
        _PORTAL_ID_CACHE[portal_code] = portal_id
        return portal_id


# ══════════════════════════════════════════════════════════════
# PARSERS Y NORMALIZADORES
# ══════════════════════════════════════════════════════════════


def parse_price(text: str):
    """
    Extrae precio y moneda de un texto.
    Soporta cosas como:
    - "$ 2,500,000"
    - "MXN2,890,000"
    - "USD 5,000"
    - "2.5 MDP"
    - "Precio a consultar" -> (None, None)
    """
    if not text:
        return None, None

    t = text.upper().strip()
    currency = "MXN"  # default

    if "USD" in t or "DÓLAR" in t or "DOLARES" in t:
        currency = "USD"

    # MDP = millones de pesos
    if "MDP" in t:
        match = re.search(r"(\d+[.,]?\d*)", t)
        if match:
            try:
                val = float(match.group(1).replace(",", ""))
                return val * 1_000_000, currency
            except ValueError:
                pass

    digits = re.findall(r"\d+", t)
    if not digits:
        return None, None

    clean = "".join(digits)
    try:
        val = float(clean)
        return val, currency
    except ValueError:
        return None, None


def normalize_operation_type(text: str | None):
    if not text:
        return None
    t = text.lower()
    if "venta" in t:
        return "venta"
    if "renta" in t or "alquiler" in t:
        return "renta"
    return t


def normalize_property_type(text: str | None):
    if not text:
        return None
    t = text.lower()
    if "casa" in t:
        return "casa"
    if "departamento" in t or "depto" in t:
        return "departamento"
    return t


def safe_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ══════════════════════════════════════════════════════════════
# UPSERT DE PROPIEDADES
# ══════════════════════════════════════════════════════════════


def upsert_property(
    conn,
    *,
    portal_id: int,
    external_id: str,
    external_url: str,
    source_page_url: str | None,
    source_page_number: int | None,
    source_position: int | None,
    # Datos normalizados
    title: str,
    description: str | None = None,
    price: float | None = None,
    currency: str | None = None,
    operation_type: str | None = None,
    property_type: str | None = None,
    country: str | None = "México",
    state: str | None = None,
    municipality: str | None = None,
    neighborhood: str | None = None,
    city: str | None = None,
    postal_code: str | None = None,
    address_text: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    bedrooms: int | None = None,
    bathrooms: float | None = None,
    parking_spaces: int | None = None,
    land_area_m2: float | None = None,
    construction_area_m2: float | None = None,
    year_built: int | None = None,
    main_url: str | None = None,
    thumbnail_url: str | None = None,
    # Datos crudos
    raw_title: str | None = None,
    raw_price_text: str | None = None,
    raw_location_text: str | None = None,
    scraped_at: datetime | None = None,
):
    """
    Upsert robusto de una propiedad y su vínculo en property_sources.
    Clave externa: (portal_id, external_id).
    """
    if scraped_at is None:
        scraped_at = datetime.now()

    if not main_url:
        main_url = external_url

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id AS property_id
            FROM property_sources ps
            JOIN properties p ON p.id = ps.property_id
            WHERE ps.portal_id = %s AND ps.external_id = %s
            """,
            (portal_id, external_id),
        )
        row = cur.fetchone()

        if row:
            property_id = row["property_id"]

            fields = {
                "title": title,
                "description": description,
                "price": price,
                "currency": currency,
                "operation_type": operation_type,
                "property_type": property_type,
                "country": country,
                "state": state,
                "municipality": municipality,
                "neighborhood": neighborhood,
                "city": city,
                "postal_code": postal_code,
                "address_text": address_text,
                "latitude": latitude,
                "longitude": longitude,
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "parking_spaces": parking_spaces,
                "land_area_m2": land_area_m2,
                "construction_area_m2": construction_area_m2,
                "year_built": year_built,
                "main_url": main_url,
                "thumbnail_url": thumbnail_url,
                "last_seen_at": scraped_at,
                "is_active": 1,
            }

            set_parts = []
            params: list = []
            for col, val in fields.items():
                if val is not None:
                    set_parts.append(f"{col} = %s")
                    params.append(val)

            if set_parts:
                set_sql = ", ".join(set_parts)
                params.append(property_id)
                cur.execute(
                    f"UPDATE properties SET {set_sql} WHERE id = %s",
                    params,
                )

            cur.execute(
                """
                UPDATE property_sources
                SET external_url = %s,
                    source_page_url = %s,
                    source_page_number = %s,
                    source_position = %s,
                    scraped_at = %s,
                    raw_title = %s,
                    raw_price_text = %s,
                    raw_location_text = %s
                WHERE portal_id = %s AND external_id = %s
                """,
                (
                    external_url,
                    source_page_url,
                    source_page_number,
                    source_position,
                    scraped_at,
                    raw_title,
                    raw_price_text,
                    raw_location_text,
                    portal_id,
                    external_id,
                ),
            )

        else:
            cur.execute(
                """
                INSERT INTO properties (
                    portal_id,
                    title,
                    description,
                    price,
                    currency,
                    operation_type,
                    property_type,
                    country,
                    state,
                    municipality,
                    neighborhood,
                    city,
                    postal_code,
                    address_text,
                    latitude,
                    longitude,
                    bedrooms,
                    bathrooms,
                    parking_spaces,
                    land_area_m2,
                    construction_area_m2,
                    year_built,
                    main_url,
                    thumbnail_url,
                    is_active,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    1, %s, %s
                )
                """,
                (
                    portal_id,
                    title,
                    description,
                    price,
                    currency,
                    operation_type,
                    property_type,
                    country,
                    state,
                    municipality,
                    neighborhood,
                    city,
                    postal_code,
                    address_text,
                    latitude,
                    longitude,
                    bedrooms,
                    bathrooms,
                    parking_spaces,
                    land_area_m2,
                    construction_area_m2,
                    year_built,
                    main_url,
                    thumbnail_url,
                    scraped_at,
                    scraped_at,
                ),
            )
            property_id = cur.lastrowid

            cur.execute(
                """
                INSERT INTO property_sources (
                    portal_id,
                    property_id,
                    external_id,
                    external_url,
                    source_page_url,
                    source_page_number,
                    source_position,
                    scraped_at,
                    raw_title,
                    raw_price_text,
                    raw_location_text
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s
                )
                """,
                (
                    portal_id,
                    property_id,
                    external_id,
                    external_url,
                    source_page_url,
                    source_page_number,
                    source_position,
                    scraped_at,
                    raw_title,
                    raw_price_text,
                    raw_location_text,
                ),
            )

    conn.commit()


# ══════════════════════════════════════════════════════════════
# BASE SCRAPER (OOP)
# ══════════════════════════════════════════════════════════════


class BasePortalScraper:
    """
    Clase base que encapsula:
    - Sesión HTTP
    - Paginación
    - Bucle de cards
    - Manejo de errores básicos
    """

    def __init__(self, conn, portal_code: str, base_url: str, max_pages: int = 1):
        self.conn = conn
        self.portal_code = portal_code
        self.portal_id = get_portal_id(conn, portal_code)
        self.base_url = base_url
        self.max_pages = max_pages
        self.session = create_resilient_session()

    def get_listing_url(self, page_num: int) -> str:
        """Sobrescribir si la paginación es especial."""
        if page_num == 1:
            return self.base_url
        return f"{self.base_url}?page={page_num}"

    def get_cards_selector(self) -> str:
        """Sobrescribir con el selector correcto de cards."""
        return "div.property-card"

    def parse_card(self, card: BeautifulSoup, page_url: str) -> dict | None:
        """
        Método ABSTRACTO: cada portal tiene su propia lógica.
        Debe devolver un dict con al menos:
            - external_id
            - external_url
            - title
            - raw_title
            - raw_price_text
        y opcionalmente el resto de campos.
        """
        raise NotImplementedError

    def run(self):
        logger.info("=== Iniciando scraping %s ===", self.portal_code)
        total_processed = 0

        for page in range(1, self.max_pages + 1):
            url = self.get_listing_url(page)
            RATE_LIMITER.wait()
            logger.info("%s GET %s", self.portal_code, url)

            try:
                resp = self.session.get(url, timeout=HTTP_TIMEOUT)
                if resp.status_code != 200:
                    logger.error(
                        "%s: HTTP %s en %s", self.portal_code, resp.status_code, url
                    )
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                cards = soup.select(self.get_cards_selector())
                logger.info(
                    "%s página %s: encontrados %s anuncios",
                    self.portal_code,
                    page,
                    len(cards),
                )

                if not cards:
                    break

                for idx, card in enumerate(cards, start=1):
                    try:
                        data = self.parse_card(card, url)
                        if not data:
                            continue

                        scraped_at = datetime.now()

                        upsert_property(
                            self.conn,
                            portal_id=self.portal_id,
                            external_id=data["external_id"],
                            external_url=data["external_url"],
                            source_page_url=url,
                            source_page_number=page,
                            source_position=idx,
                            title=data.get(
                                "title", data.get("raw_title", "Sin título")
                            ),
                            description=data.get("description"),
                            price=data.get("price"),
                            currency=data.get("currency"),
                            operation_type=data.get("operation_type"),
                            property_type=data.get("property_type"),
                            country=data.get("country", "México"),
                            state=data.get("state"),
                            municipality=data.get("municipality"),
                            neighborhood=data.get("neighborhood"),
                            city=data.get("city"),
                            postal_code=data.get("postal_code"),
                            address_text=data.get("address_text"),
                            latitude=data.get("latitude"),
                            longitude=data.get("longitude"),
                            bedrooms=data.get("bedrooms"),
                            bathrooms=data.get("bathrooms"),
                            parking_spaces=data.get("parking_spaces"),
                            land_area_m2=data.get("land_area_m2"),
                            construction_area_m2=data.get(
                                "construction_area_m2"
                            ),
                            year_built=data.get("year_built"),
                            main_url=data.get("main_url", data["external_url"]),
                            thumbnail_url=data.get("thumbnail_url"),
                            raw_title=data.get("raw_title"),
                            raw_price_text=data.get("raw_price_text"),
                            raw_location_text=data.get("raw_location_text"),
                            scraped_at=scraped_at,
                        )

                        total_processed += 1

                    except Exception as e:
                        logger.error(
                            "%s: Error procesando card página %s idx %s: %s",
                            self.portal_code,
                            page,
                            idx,
                            e,
                            exc_info=False,
                        )

                random_sleep(1.5, 4.0)

            except Exception as e:
                logger.error(
                    "%s: Error fatal en página %s: %s", self.portal_code, page, e
                )
                break

        logger.info(
            "=== Fin scraping %s: %s propiedades procesadas ===",
            self.portal_code,
            total_processed,
        )


# ══════════════════════════════════════════════════════════════
# SCRAPERS ESPECÍFICOS
# ══════════════════════════════════════════════════════════════


class Casas365Scraper(BasePortalScraper):
    """
    Scraper para CASAS365.
    Usamos IDs propios del card si existen (data-id, etc.) y
    fallback al URL completo si no.
    """

    def __init__(self, conn):
        cfg = PORTAL_CONFIGS.get("CASAS365", {"max_pages": 1})
        super().__init__(
            conn,
            portal_code="CASAS365",
            base_url="https://casas365.mx/busqueda-avanzada/",
            max_pages=cfg["max_pages"],
        )

    def get_listing_url(self, page_num: int) -> str:
        if page_num == 1:
            return "https://casas365.mx/busqueda-avanzada/"
        return f"https://casas365.mx/busqueda-avanzada/?pagina={page_num}"

    def get_cards_selector(self) -> str:
        # Ajustado a la grilla de resultados (cards tipo inmueble)
        return (
            "div.property-card, "
            "div.listing-item, "
            "div.inmueble-card, "
            "div.resultado, "
            "div.col-md-4 div.item"
        )

    def parse_card(self, card, page_url: str) -> dict | None:
        link = card.select_one("a[href]")
        if not link:
            return None

        external_url = urljoin("https://casas365.mx", link.get("href", "").strip())

        # ID propio del portal si existe
        external_id = (
            card.get("data-id")
            or card.get("id")
            or link.get("data-id")
            or external_url
        )

        # Título
        title_el = (
            card.select_one("h2, h3, .title, .titulo, .property-title")
            or link.select_one("h2, h3")
        )
        raw_title = title_el.get_text(strip=True) if title_el else "Sin título"

        # Precio
        price_el = card.select_one(
            ".price, .precio, .property-price, .prop-valor-monto, .precio span"
        )
        raw_price_text = price_el.get_text(strip=True) if price_el else ""
        price, currency = parse_price(raw_price_text)

        # Ubicación (colonia / zona)
        loc_el = card.select_one(".location, .ubicacion, .property-location, .direccion")
        raw_location_text = loc_el.get_text(strip=True) if loc_el else None

        # M2 si se ven en el card (ej. "387 m²")
        m2 = None
        m2_match = re.search(r"(\d+[.,]?\d*)\s*m²", card.get_text(" ", strip=True))
        if m2_match:
            try:
                m2 = float(m2_match.group(1).replace(",", ""))
            except ValueError:
                m2 = None

        operation_type = "venta"
        property_type = normalize_property_type(raw_title)

        # Thumbnail
        img_el = card.select_one("img")
        thumbnail_url = (
            urljoin("https://casas365.mx", img_el["src"])
            if img_el and img_el.get("src")
            else None
        )

        return {
            "external_id": external_id,
            "external_url": external_url,
            "title": raw_title,
            "raw_title": raw_title,
            "price": price,
            "currency": currency,
            "operation_type": operation_type,
            "property_type": property_type,
            "country": "México",
            "state": "Nuevo León",
            "municipality": None,
            "neighborhood": raw_location_text,
            "city": None,
            "postal_code": None,
            "address_text": raw_location_text,
            "latitude": None,
            "longitude": None,
            "bedrooms": None,
            "bathrooms": None,
            "parking_spaces": None,
            "land_area_m2": None,
            "construction_area_m2": m2,
            "year_built": None,
            "main_url": external_url,
            "thumbnail_url": thumbnail_url,
            "raw_price_text": raw_price_text,
            "raw_location_text": raw_location_text,
        }


class C21TaurusScraper(BasePortalScraper):
    """
    Scraper para Century 21 Taurus.
    """

    def __init__(self, conn):
        cfg = PORTAL_CONFIGS.get("C21_TAURUS", {"max_pages": 1})
        base_url = (
            "https://century21taurus.com/"
            "v/resultados/tipo_casa-o-casa-duplex-o-casa-en-condominio-o-town-house/"
            "operacion_venta/en-pais_mexico/en-estado_nuevo-leon/"
            "oficina_176-century-21-taurus_local"
        )
        super().__init__(
            conn,
            portal_code="C21_TAURUS",
            base_url=base_url,
            max_pages=cfg["max_pages"],
        )

    def get_listing_url(self, page_num: int) -> str:
        if page_num == 1:
            return self.base_url
        return f"{self.base_url}/pagina_{page_num}"

    def get_cards_selector(self) -> str:
        # Cards típicos del sitio (grilla de propiedades)
        return (
            "div.resultado-listado, "
            "div.inmueble, "
            "div.property-card, "
            "div.col-md-4 div.item"
        )

    def parse_card(self, card, page_url: str) -> dict | None:
        link = card.select_one("a[href]")
        if not link:
            return None

        external_url = urljoin(
            "https://century21taurus.com", link.get("href", "").strip()
        )

        # ID portal: data-id, data-idpropiedad, etc. Fallback al URL.
        external_id = (
            card.get("data-idpropiedad")
            or card.get("data-id")
            or link.get("data-id")
            or external_url
        )

        # Título
        title_el = (
            card.select_one("h2, h3, .title, .titulo, .property-title")
            or link.select_one("h2, h3")
        )
        raw_title = title_el.get_text(strip=True) if title_el else "Sin título"

        # Precio
        price_el = card.select_one(".price, .precio, .property-price, .precio span")
        raw_price_text = price_el.get_text(strip=True) if price_el else ""
        price, currency = parse_price(raw_price_text)

        # Ubicación (colonia / fracc.)
        loc_el = card.select_one(".location, .ubicacion, .property-location, .direccion")
        raw_location_text = loc_el.get_text(strip=True) if loc_el else None

        # Área y recámaras si aparecen como "387 m²", "3 recámaras"
        text_all = card.get_text(" ", strip=True)
        m2 = None
        m2_match = re.search(r"(\d+[.,]?\d*)\s*m²", text_all)
        if m2_match:
            try:
                m2 = float(m2_match.group(1).replace(",", ""))
            except ValueError:
                m2 = None

        bed_match = re.search(
            r"(\d+)\s*(RECÁMARAS|RECAMARAS|DORMITORIOS|HABITACIONES)", text_all, re.I
        )
        bedrooms = safe_int(bed_match.group(1)) if bed_match else None

        operation_type = "venta"
        property_type = normalize_property_type(raw_title)

        # Thumbnail
        img_el = card.select_one("img")
        thumbnail_url = (
            urljoin("https://century21taurus.com", img_el["src"])
            if img_el and img_el.get("src")
            else None
        )

        return {
            "external_id": external_id,
            "external_url": external_url,
            "title": raw_title,
            "raw_title": raw_title,
            "price": price,
            "currency": currency,
            "operation_type": operation_type,
            "property_type": property_type,
            "country": "México",
            "state": "Nuevo León",
            "municipality": None,
            "neighborhood": raw_location_text,
            "city": None,
            "postal_code": None,
            "address_text": raw_location_text,
            "latitude": None,
            "longitude": None,
            "bedrooms": bedrooms,
            "bathrooms": None,
            "parking_spaces": None,
            "land_area_m2": None,
            "construction_area_m2": m2,
            "year_built": None,
            "main_url": external_url,
            "thumbnail_url": thumbnail_url,
            "raw_price_text": raw_price_text,
            "raw_location_text": raw_location_text,
        }


class HerreraPalaciosScraper(BasePortalScraper):
    """
    Scraper para Herrera & Palacios.

    Basado en el HTML:

    <li prop-id="7511393">
      <a href="/p/7511393-Casa-en-Venta-en-Valle-de-las-Cumbres-...">
        <div class="prop-img"> ... </div>
        <div class="prop-desc">
          <div class="prop-desc-tipo-ub">Casa en Venta en Valle de las Cumbres, Monterrey</div>
          <div class="prop-desc-dir">Valle de las Cumbres</div>
        </div>
      </a>
      <div class="prop-valor-nro">
        MXN2,890,000
        <div class="codref detalleColorText">24-CV-7448</div>
        ...
      </div>
    </li>
    """

    def __init__(self, conn):
        cfg = PORTAL_CONFIGS.get("HERRERA_PALACIOS", {"max_pages": 1})
        super().__init__(
            conn,
            portal_code="HERRERA_PALACIOS",
            base_url="https://www.herrerapalacios.mx/Buscar-Casa-en-Venta",
            max_pages=cfg["max_pages"],
        )

    def get_listing_url(self, page_num: int) -> str:
        # Actualmente la búsqueda usa querystring, pero para venta de casas
        # la URL base ya es correcta (min/max price vacíos y currency=MXN).
        return (
            "https://www.herrerapalacios.mx/"
            "Buscar-Casa-en-Venta?min-price=&max-price=&currency=MXN"
        )

    def get_cards_selector(self) -> str:
        # Cada resultado de propiedad viene en un <li prop-id="...">
        return "li[prop-id]"

    def parse_card(self, card, page_url: str) -> dict | None:
        link = card.select_one("a[href]")
        if not link:
            return None

        href = link.get("href", "").strip()
        external_url = urljoin("https://www.herrerapalacios.mx", href)

        # ID de la propiedad: atributo prop-id del <li>
        external_id = card.get("prop-id") or external_url

        # Título general (incluye operación + zona + ciudad)
        title_el = card.select_one(".prop-desc-tipo-ub")
        raw_title = title_el.get_text(strip=True) if title_el else "Sin título"

        # Dir / colonia / fracc.
        dir_el = card.select_one(".prop-desc-dir")
        neighborhood = dir_el.get_text(strip=True) if dir_el else None

        # Precio:
        #   <div class="prop-valor-nro">
        #       MXN2,890,000
        #       <div class="codref ...">24-CV-7448</div>
        #       ...
        #   </div>
        price_el = card.select_one(".prop-valor-nro")
        raw_price_text = ""
        if price_el:
            # Solo los textos directos, sin incluir hijos (para no mezclar codref)
            direct_texts = [
                t.strip()
                for t in price_el.find_all(string=True, recursive=False)
                if t.strip()
            ]
            if direct_texts:
                raw_price_text = direct_texts[0]
            else:
                raw_price_text = price_el.get_text(" ", strip=True)

        price, currency = parse_price(raw_price_text)

        # Área y recámaras:
        #   <div class="prop-data"><div>216 m²</div>...</div>
        #   <div class="prop-data2"><div>3</div>...</div>
        m2 = None
        m2_el = card.select_one(".prop-data div")
        if m2_el:
            m2_match = re.search(r"(\d+[.,]?\d*)", m2_el.get_text(strip=True))
            if m2_match:
                try:
                    m2 = float(m2_match.group(1).replace(",", ""))
                except ValueError:
                    m2 = None

        bedrooms = None
        beds_el = card.select_one(".prop-data2 div")
        if beds_el:
            bedrooms = safe_int(re.sub(r"[^\d]", "", beds_el.get_text(strip=True)))

        # Operación y tipo a partir del título "Casa en Venta ..."
        operation_type = normalize_operation_type(raw_title)
        property_type = normalize_property_type(raw_title)

        # Ciudad / municipio: el título suele terminar con ", Monterrey"
        city = None
        city_match = re.search(r",\s*([A-Za-zÁÉÍÓÚÜÑñ\s]+)$", raw_title)
        if city_match:
            city = city_match.group(1).strip()

        # Thumbnail
        img_el = card.select_one("img.dest-img")
        thumbnail_url = (
            urljoin("https://www.herrerapalacios.mx", img_el["src"])
            if img_el and img_el.get("src")
            else None
        )

        # Dirección textual base
        address_text = neighborhood
        if city:
            address_text = f"{neighborhood}, {city}" if neighborhood else city

        return {
            "external_id": external_id,
            "external_url": external_url,
            "title": raw_title,
            "raw_title": raw_title,
            "price": price,
            "currency": currency,
            "operation_type": operation_type,
            "property_type": property_type,
            "country": "México",
            "state": "Nuevo León",
            "municipality": city,  # en estos casos municipio = ciudad
            "neighborhood": neighborhood,
            "city": city,
            "postal_code": None,
            "address_text": address_text,
            "latitude": None,
            "longitude": None,
            "bedrooms": bedrooms,
            "bathrooms": None,
            "parking_spaces": None,
            "land_area_m2": None,
            "construction_area_m2": m2,  # 216 m² del card
            "year_built": None,
            "main_url": external_url,
            "thumbnail_url": thumbnail_url,
            "raw_price_text": raw_price_text,
            "raw_location_text": address_text,
        }


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════


def main():
    logger.info("=== Iniciando proceso completo de scraping ===")
    total_start = time.time()
    success_count = 0

    conn = None
    try:
        conn = get_db_connection()

        scrapers = [
            Casas365Scraper(conn),
            C21TaurusScraper(conn),
            HerreraPalaciosScraper(conn),
        ]

        for scraper in scrapers:
            try:
                with measure_performance(scraper.portal_code):
                    scraper.run()
                success_count += 1
            except Exception as e:
                logger.error(
                    "Error crítico en %s: %s", scraper.portal_code, e, exc_info=True
                )

    except Exception as e:
        logger.exception("Error global de conexión o inicialización: %s", e)
    finally:
        if conn:
            try:
                conn.close()
                logger.info("Conexión a BD cerrada")
            except Exception:
                pass

    total_time = time.time() - total_start
    logger.info(
        "=== Proceso completado en %.2fs. Scrapers exitosos: %s/%s ===",
        total_time,
        success_count,
        len(scrapers),
    )


if __name__ == "__main__":
    main()
