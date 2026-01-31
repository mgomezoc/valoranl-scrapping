# valoranl_scraper.py
"""
Scraper de portales inmobiliarios para ValoraNL.

Estrategia profesional (recomendada):
- UPSERT por (portal_id, external_id) para construir histórico.
- Soft-delete por portal (is_active=0) solo si el scrape del portal tuvo resultados.
- last_seen_at se actualiza cada corrida, first_seen_at se conserva.

Portales soportados:
- CASAS365
- C21_TAURUS (AJAX/JSON en mx.omnimls.com)
- HERRERA_PALACIOS

BD: MySQL esquema `valoranl_market`
Tablas esperadas:
- portals (code)
- properties
- property_sources
"""

import logging
import random
import re
import time
from contextlib import contextmanager
from datetime import datetime
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import pymysql
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ══════════════════════════════════════════════════════════════
# CONFIGURACIÓN (SIN ENV VARS)
# ══════════════════════════════════════════════════════════════

DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "mgomez",
    "password": "Rrasec13!",
    "database": "valoranl_market",
    "port": 3306,
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": False,
}

HTTP_TIMEOUT = 20

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
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

PORTAL_CONFIGS = {
    "CASAS365": {"max_pages": 8},
    "C21_TAURUS": {"max_pages": 5},
    "HERRERA_PALACIOS": {"max_pages": 5},
}


# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("valoranl.scraper")


# ══════════════════════════════════════════════════════════════
# RATE LIMIT
# ══════════════════════════════════════════════════════════════

class RateLimiter:
    """Rate limiter simple: máximo N llamadas por segundo."""
    def __init__(self, calls_per_second: float = 0.6):
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


RATE_LIMITER = RateLimiter(calls_per_second=0.6)


def random_sleep(min_sec: float = 1.2, max_sec: float = 3.2) -> None:
    time.sleep(random.uniform(min_sec, max_sec))


@contextmanager
def measure(label: str):
    start = time.time()
    try:
        yield
    finally:
        elapsed = time.time() - start
        logger.info("%s | tiempo=%.2fs", label, elapsed)


def create_resilient_session() -> requests.Session:
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
# DB
# ══════════════════════════════════════════════════════════════

_PORTAL_ID_CACHE: Dict[str, int] = {}


def get_db_connection():
    return pymysql.connect(**DB_CONFIG)


def get_portal_id(conn, portal_code: str) -> int:
    if portal_code in _PORTAL_ID_CACHE:
        return _PORTAL_ID_CACHE[portal_code]

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM portals WHERE code = %s", (portal_code,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                f"No se encontró portal con code='{portal_code}' en portals."
            )
        _PORTAL_ID_CACHE[portal_code] = int(row["id"])
        return _PORTAL_ID_CACHE[portal_code]


def mark_portal_properties_inactive(conn, portal_id: int, run_started_at: datetime) -> int:
    """
    Soft-delete por portal:
    Si una propiedad NO fue vista en esta corrida (last_seen_at < run_started_at),
    la marcamos inactive, pero NO la borramos (histórico).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE properties
            SET is_active = 0
            WHERE portal_id = %s
              AND is_active = 1
              AND last_seen_at < %s
            """,
            (portal_id, run_started_at),
        )
        affected = cur.rowcount
    conn.commit()
    return affected


# ══════════════════════════════════════════════════════════════
# NORMALIZACIÓN
# ══════════════════════════════════════════════════════════════

def safe_int(v) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def normalize_operation_type(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    t = text.lower()
    if "venta" in t:
        return "venta"
    if "renta" in t or "alquiler" in t:
        return "renta"
    return t


def normalize_property_type(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    t = text.lower()
    if "casa" in t:
        return "casa"
    if "departamento" in t or "depto" in t:
        return "departamento"
    if "terreno" in t:
        return "terreno"
    return t


def parse_price(text: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Soporta:
    - "$ 2,500,000 MXN"
    - "MXN2,890,000"
    - "USD 5,000"
    - "2.5 MDP"
    - "Precio a consultar"
    """
    if not text:
        return None, None

    t = text.upper().strip()
    if "CONSULT" in t:
        return None, None

    currency = "MXN"
    if "USD" in t or "DÓLAR" in t or "DOLARES" in t:
        currency = "USD"

    if "MDP" in t:
        m = re.search(r"(\d+[.,]?\d*)", t)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
                return val * 1_000_000, currency
            except Exception:
                return None, currency

    digits = re.findall(r"\d+", t)
    if not digits:
        return None, None

    clean = "".join(digits)
    try:
        return float(clean), currency
    except Exception:
        return None, currency


def clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


# ══════════════════════════════════════════════════════════════
# UPSERT
# ══════════════════════════════════════════════════════════════

def upsert_property(
    conn,
    *,
    portal_id: int,
    external_id: str,
    external_url: str,
    source_page_url: Optional[str],
    source_page_number: Optional[int],
    source_position: Optional[int],
    # normalizados
    title: str,
    description: Optional[str] = None,
    price: Optional[float] = None,
    currency: Optional[str] = None,
    operation_type: Optional[str] = None,
    property_type: Optional[str] = None,
    country: Optional[str] = "México",
    state: Optional[str] = None,
    municipality: Optional[str] = None,
    neighborhood: Optional[str] = None,
    city: Optional[str] = None,
    postal_code: Optional[str] = None,
    address_text: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    bedrooms: Optional[int] = None,
    bathrooms: Optional[float] = None,
    parking_spaces: Optional[int] = None,
    land_area_m2: Optional[float] = None,
    construction_area_m2: Optional[float] = None,
    year_built: Optional[int] = None,
    main_url: Optional[str] = None,
    thumbnail_url: Optional[str] = None,
    # crudos
    raw_title: Optional[str] = None,
    raw_price_text: Optional[str] = None,
    raw_location_text: Optional[str] = None,
    scraped_at: Optional[datetime] = None,
) -> str:
    """
    Upsert robusto. Retorna:
    - "insert"
    - "update"
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
            property_id = int(row["property_id"])

            # actualiza properties (solo columnas con valor)
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
            params: List[Any] = []
            for col, val in fields.items():
                if val is not None:
                    set_parts.append(f"{col} = %s")
                    params.append(val)

            if set_parts:
                params.append(property_id)
                cur.execute(
                    f"UPDATE properties SET {', '.join(set_parts)} WHERE id = %s",
                    params,
                )

            # actualiza property_sources
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

            conn.commit()
            return "update"

        # INSERT
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
    return "insert"


# ══════════════════════════════════════════════════════════════
# BASE SCRAPER
# ══════════════════════════════════════════════════════════════

class BasePortalScraper:
    def __init__(self, conn, portal_code: str, max_pages: int):
        self.conn = conn
        self.portal_code = portal_code
        self.portal_id = get_portal_id(conn, portal_code)
        self.max_pages = max_pages
        self.session = create_resilient_session()

        self.run_started_at = datetime.now()
        self.pages_with_results = 0
        self.total_cards_seen = 0
        self.total_inserts = 0
        self.total_updates = 0

        self._seen_external_ids: set[str] = set()

    def finalize(self):
        """
        Soft-delete por portal, solo si el portal devolvió resultados.
        """
        if self.pages_with_results <= 0:
            logger.warning(
                "%s | 0 páginas con resultados. No se aplica soft-delete (protección).",
                self.portal_code,
            )
            return

        deactivated = mark_portal_properties_inactive(
            self.conn, self.portal_id, self.run_started_at
        )
        logger.info(
            "%s | soft-delete aplicado: %s propiedades marcadas inactivas",
            self.portal_code,
            deactivated,
        )

    def log_summary(self):
        logger.info(
            "%s | resumen: pages_ok=%s cards=%s inserts=%s updates=%s",
            self.portal_code,
            self.pages_with_results,
            self.total_cards_seen,
            self.total_inserts,
            self.total_updates,
        )


# ══════════════════════════════════════════════════════════════
# CASAS365
# ══════════════════════════════════════════════════════════════

class Casas365Scraper(BasePortalScraper):
    BASE = "https://casas365.mx/busqueda-avanzada/"
    CARD_SELECTOR = "div.listing_wrapper[data-listid]"

    def __init__(self, conn):
        super().__init__(conn, "CASAS365", PORTAL_CONFIGS["CASAS365"]["max_pages"])

    def _build_url(self, page: int, mode: str = "pagina") -> str:
        # mode: pagina / paged
        if page <= 1:
            return self.BASE
        parsed = urlparse(self.BASE)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        qs.pop("pagina", None)
        qs.pop("paged", None)
        qs[mode] = [str(page)]
        q = urlencode(qs, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, q, parsed.fragment))

    def _fetch_cards(self, url: str) -> List[Any]:
        RATE_LIMITER.wait()
        resp = self.session.get(url, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            logger.error("CASAS365 | HTTP %s | %s", resp.status_code, url)
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        return soup.select(self.CARD_SELECTOR)

    def _parse_card(self, card: Any) -> Optional[Dict[str, Any]]:
        external_id = clean_ws(card.get("data-listid") or "")
        if not external_id:
            return None

        # URL
        link_url = None
        inner = card.select_one("div.property_listing[data-link]")
        if inner and inner.get("data-link"):
            link_url = clean_ws(inner.get("data-link") or "")
        if not link_url:
            a = card.select_one("h4 a[href]")
            if a and a.get("href"):
                link_url = clean_ws(a.get("href") or "")
        if not link_url:
            return None

        external_url = urljoin("https://casas365.mx", link_url)

        # title
        title_el = card.select_one("h4 a")
        raw_title = clean_ws(title_el.get_text(" ", strip=True) if title_el else "") or "Sin título"

        # price
        price_wrap = card.select_one("div.listing_unit_price_wrapper")
        raw_price_text = clean_ws(price_wrap.get_text(" ", strip=True) if price_wrap else "")
        price, currency = parse_price(raw_price_text)

        # image
        thumbnail_url = None
        img = card.select_one("div.listing-unit-img-wrapper img")
        if img:
            thumbnail_url = clean_ws(img.get("data-original") or img.get("src") or "") or None
            if thumbnail_url:
                thumbnail_url = urljoin("https://casas365.mx", thumbnail_url)

        # tooltips
        def _get_int(tt: str) -> Optional[int]:
            node = card.select_one(f'div.property_listing_details_v7_item[data-bs-original-title="{tt}"]')
            if not node:
                return None
            txt = clean_ws(node.get_text(" ", strip=True))
            m = re.search(r"(\d+)\s*$", txt)
            return safe_int(m.group(1)) if m else None

        def _get_m2(tt: str) -> Optional[float]:
            node = card.select_one(f'div.property_listing_details_v7_item[data-bs-original-title="{tt}"] span')
            if not node:
                return None
            txt = clean_ws(node.get_text(" ", strip=True))
            m = re.search(r"([\d.]+)", txt)
            return safe_float(m.group(1)) if m else None

        bedrooms = _get_int("Recámaras")
        bathrooms = _get_int("Baños")
        built_m2 = _get_m2("Construcción")
        lot_m2 = _get_m2("Lot Size")

        # location best-effort
        city = None
        neighborhood = None
        raw_location_text = None
        if "," in raw_title:
            left, right = raw_title.rsplit(",", 1)
            city = clean_ws(right) or None
            left = re.sub(
                r"^(Casa|Departamento|Terreno|Bodega|Oficina)\s+en\s+(Venta|Renta)\s+",
                "",
                clean_ws(left),
                flags=re.I,
            ).strip()
            neighborhood = left or None

        raw_location_text = ", ".join([x for x in [neighborhood, city] if x]) or None

        return {
            "external_id": external_id,
            "external_url": external_url,
            "raw_title": raw_title,
            "raw_price_text": raw_price_text,
            "raw_location_text": raw_location_text,
            "title": raw_title,
            "price": price,
            "currency": currency,
            "operation_type": "venta",
            "property_type": normalize_property_type(raw_title) or "casa",
            "country": "México",
            "state": "Nuevo León",
            "municipality": city,
            "neighborhood": neighborhood,
            "city": city,
            "address_text": raw_location_text,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "construction_area_m2": built_m2,
            "land_area_m2": lot_m2,
            "thumbnail_url": thumbnail_url,
            "main_url": external_url,
        }

    def run(self):
        logger.info("=== %s | inicio ===", self.portal_code)

        for page in range(1, self.max_pages + 1):
            # intenta pagina, si no trae nuevos IDs intenta paged
            url_a = self._build_url(page, "pagina")
            url_b = self._build_url(page, "paged")

            cards_a = self._fetch_cards(url_a)
            ids_a = {clean_ws(c.get("data-listid") or "") for c in cards_a if clean_ws(c.get("data-listid") or "")}

            new_a = ids_a - self._seen_external_ids

            used_url = url_a
            cards = cards_a
            new_ids = new_a

            if page > 1 and (not cards_a or len(new_a) == 0):
                cards_b = self._fetch_cards(url_b)
                ids_b = {clean_ws(c.get("data-listid") or "") for c in cards_b if clean_ws(c.get("data-listid") or "")}
                new_b = ids_b - self._seen_external_ids
                if len(new_b) > len(new_a):
                    used_url = url_b
                    cards = cards_b
                    new_ids = new_b

            logger.info(
                "%s | page=%s | cards=%s | nuevos=%s | url=%s",
                self.portal_code,
                page,
                len(cards),
                len(new_ids),
                used_url,
            )

            if not cards:
                break

            # si ya no hay nuevos, es señal de paginación repetida
            if page > 1 and len(new_ids) == 0:
                logger.warning("%s | page=%s repetida (0 nuevos). Cortando paginación.", self.portal_code, page)
                break

            self.pages_with_results += 1

            for idx, card in enumerate(cards, start=1):
                data = self._parse_card(card)
                if not data:
                    continue

                external_id = data["external_id"]
                self._seen_external_ids.add(external_id)
                self.total_cards_seen += 1

                action = upsert_property(
                    self.conn,
                    portal_id=self.portal_id,
                    external_id=external_id,
                    external_url=data["external_url"],
                    source_page_url=used_url,
                    source_page_number=page,
                    source_position=idx,
                    title=data["title"],
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
                    construction_area_m2=data.get("construction_area_m2"),
                    year_built=data.get("year_built"),
                    main_url=data.get("main_url"),
                    thumbnail_url=data.get("thumbnail_url"),
                    raw_title=data.get("raw_title"),
                    raw_price_text=data.get("raw_price_text"),
                    raw_location_text=data.get("raw_location_text"),
                    scraped_at=datetime.now(),
                )

                if action == "insert":
                    self.total_inserts += 1
                else:
                    self.total_updates += 1

            random_sleep()

        self.finalize()
        self.log_summary()
        logger.info("=== %s | fin ===", self.portal_code)


# ══════════════════════════════════════════════════════════════
# HERRERA & PALACIOS
# ══════════════════════════════════════════════════════════════

class HerreraPalaciosScraper(BasePortalScraper):
    BASE = "https://www.herrerapalacios.mx/Buscar-Casa-en-Venta?min-price=&max-price=&currency=MXN"
    CARD_SELECTOR = "li[prop-id]"

    def __init__(self, conn):
        super().__init__(conn, "HERRERA_PALACIOS", PORTAL_CONFIGS["HERRERA_PALACIOS"]["max_pages"])

    def _build_url(self, page: int, mode: str) -> str:
        if page <= 1:
            return self.BASE
        parsed = urlparse(self.BASE)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        qs[mode] = [str(page)]
        q = urlencode(qs, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, q, parsed.fragment))

    def _fetch_cards(self, url: str) -> List[Any]:
        RATE_LIMITER.wait()
        resp = self.session.get(url, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            logger.error("%s | HTTP %s | %s", self.portal_code, resp.status_code, url)
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        return soup.select(self.CARD_SELECTOR)

    def _parse_card(self, card: Any) -> Optional[Dict[str, Any]]:
        external_id = clean_ws(card.get("prop-id") or "")
        if not external_id:
            return None

        a = card.select_one("a[href]")
        if not a or not a.get("href"):
            return None

        external_url = urljoin("https://www.herrerapalacios.mx", clean_ws(a.get("href") or ""))

        title_el = card.select_one(".prop-desc-tipo-ub")
        raw_title = clean_ws(title_el.get_text(" ", strip=True) if title_el else "") or "Sin título"

        dir_el = card.select_one(".prop-desc-dir")
        neighborhood = clean_ws(dir_el.get_text(" ", strip=True) if dir_el else "") or None

        price_el = card.select_one(".prop-valor-nro")
        raw_price_text = ""
        if price_el:
            direct_texts = [t.strip() for t in price_el.find_all(string=True, recursive=False) if t.strip()]
            raw_price_text = direct_texts[0] if direct_texts else clean_ws(price_el.get_text(" ", strip=True))
        price, currency = parse_price(raw_price_text)

        m2 = None
        m2_el = card.select_one(".prop-data div")
        if m2_el:
            m = re.search(r"(\d+[.,]?\d*)", m2_el.get_text(strip=True))
            if m:
                try:
                    m2 = float(m.group(1).replace(",", ""))
                except Exception:
                    m2 = None

        bedrooms = None
        beds_el = card.select_one(".prop-data2 div")
        if beds_el:
            bedrooms = safe_int(re.sub(r"[^\d]", "", beds_el.get_text(strip=True)))

        city = None
        mcity = re.search(r",\s*([A-Za-zÁÉÍÓÚÜÑñ\s]+)$", raw_title)
        if mcity:
            city = clean_ws(mcity.group(1)) or None

        address_text = neighborhood
        if city:
            address_text = f"{neighborhood}, {city}" if neighborhood else city

        img_el = card.select_one("img.dest-img")
        thumbnail_url = urljoin("https://www.herrerapalacios.mx", img_el["src"]) if img_el and img_el.get("src") else None

        return {
            "external_id": external_id,
            "external_url": external_url,
            "raw_title": raw_title,
            "raw_price_text": raw_price_text,
            "raw_location_text": address_text,
            "title": raw_title,
            "price": price,
            "currency": currency,
            "operation_type": normalize_operation_type(raw_title) or "venta",
            "property_type": normalize_property_type(raw_title) or "casa",
            "country": "México",
            "state": "Nuevo León",
            "municipality": city,
            "neighborhood": neighborhood,
            "city": city,
            "address_text": address_text,
            "bedrooms": bedrooms,
            "construction_area_m2": m2,
            "thumbnail_url": thumbnail_url,
            "main_url": external_url,
        }

    def run(self):
        logger.info("=== %s | inicio ===", self.portal_code)

        for page in range(1, self.max_pages + 1):
            url_a = self._build_url(page, "page")
            url_b = self._build_url(page, "pagina")

            cards_a = self._fetch_cards(url_a)
            ids_a = {clean_ws(c.get("prop-id") or "") for c in cards_a if clean_ws(c.get("prop-id") or "")}
            new_a = ids_a - self._seen_external_ids

            used_url = url_a
            cards = cards_a
            new_ids = new_a

            if page > 1 and (not cards_a or len(new_a) == 0):
                cards_b = self._fetch_cards(url_b)
                ids_b = {clean_ws(c.get("prop-id") or "") for c in cards_b if clean_ws(c.get("prop-id") or "")}
                new_b = ids_b - self._seen_external_ids
                if len(new_b) > len(new_a):
                    used_url = url_b
                    cards = cards_b
                    new_ids = new_b

            logger.info(
                "%s | page=%s | cards=%s | nuevos=%s | url=%s",
                self.portal_code,
                page,
                len(cards),
                len(new_ids),
                used_url,
            )

            if not cards:
                break

            if page > 1 and len(new_ids) == 0:
                logger.warning("%s | page=%s repetida (0 nuevos). Cortando paginación.", self.portal_code, page)
                break

            self.pages_with_results += 1

            for idx, card in enumerate(cards, start=1):
                data = self._parse_card(card)
                if not data:
                    continue

                external_id = data["external_id"]
                self._seen_external_ids.add(external_id)
                self.total_cards_seen += 1

                action = upsert_property(
                    self.conn,
                    portal_id=self.portal_id,
                    external_id=external_id,
                    external_url=data["external_url"],
                    source_page_url=used_url,
                    source_page_number=page,
                    source_position=idx,
                    title=data["title"],
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
                    construction_area_m2=data.get("construction_area_m2"),
                    year_built=data.get("year_built"),
                    main_url=data.get("main_url"),
                    thumbnail_url=data.get("thumbnail_url"),
                    raw_title=data.get("raw_title"),
                    raw_price_text=data.get("raw_price_text"),
                    raw_location_text=data.get("raw_location_text"),
                    scraped_at=datetime.now(),
                )

                if action == "insert":
                    self.total_inserts += 1
                else:
                    self.total_updates += 1

            random_sleep()

        self.finalize()
        self.log_summary()
        logger.info("=== %s | fin ===", self.portal_code)


# ══════════════════════════════════════════════════════════════
# C21 TAURUS (AJAX / JSON)
# ══════════════════════════════════════════════════════════════

class C21TaurusScraper(BasePortalScraper):
    """
    Century21 Taurus:
    - El listado visible es renderizado por AJAX.
    - Endpoint JSON real:
      https://mx.omnimls.com/v/resultados/tipo_casa/operacion_venta/en-pais_mexico/en-estado_nuevo-leon/oficina_176-century-21-taurus_local?json=true
      página 2:
      https://mx.omnimls.com/v/resultados/tipo_casa/operacion_venta/en-pais_mexico/en-estado_nuevo-leon/pagina_2/oficina_176-century-21-taurus_local?json=true

    Nota:
    - La respuesta puede traer HTML embebido (cards) o estructuras con resultados.
    - Este scraper intenta extraer de forma resiliente.
    """
    JSON_BASE = (
        "https://mx.omnimls.com/v/resultados/tipo_casa/operacion_venta/"
        "en-pais_mexico/en-estado_nuevo-leon/oficina_176-century-21-taurus_local?json=true"
    )
    JSON_PAGE_TMPL = (
        "https://mx.omnimls.com/v/resultados/tipo_casa/operacion_venta/"
        "en-pais_mexico/en-estado_nuevo-leon/pagina_{page}/oficina_176-century-21-taurus_local?json=true"
    )

    def __init__(self, conn):
        super().__init__(conn, "C21_TAURUS", PORTAL_CONFIGS["C21_TAURUS"]["max_pages"])

    def _get_json_url(self, page: int) -> str:
        if page <= 1:
            return self.JSON_BASE
        return self.JSON_PAGE_TMPL.format(page=page)

    def _safe_json(self, text: str) -> Optional[Dict[str, Any]]:
        # por si llega con BOM o contenido raro, intentamos limpiar
        try:
            return requests.models.complexjson.loads(text)
        except Exception:
            # fallback: extrae primer {...} si viene envuelto
            m = re.search(r"(\{.*\})", text, re.S)
            if not m:
                return None
            try:
                return requests.models.complexjson.loads(m.group(1))
            except Exception:
                return None

    def _extract_html_candidates(self, payload: Any) -> List[str]:
        """
        Busca strings grandes con HTML dentro del JSON.
        """
        htmls: List[str] = []

        def walk(o: Any):
            if isinstance(o, dict):
                for k, v in o.items():
                    if isinstance(v, str) and ("<div" in v or "<article" in v or "<li" in v) and len(v) > 300:
                        htmls.append(v)
                    else:
                        walk(v)
            elif isinstance(o, list):
                for it in o:
                    walk(it)

        walk(payload)
        return htmls

    def _parse_cards_from_html(self, html: str) -> List[Dict[str, Any]]:
        """
        Parse de cards desde HTML embebido (basado en tu ejemplo.html y el sitio real).
        """
        soup = BeautifulSoup(html, "html.parser")

        # Selector flexible: cada tarjeta suele ser un contenedor con link "Ver Detalle" o link a /propiedad/
        anchors = soup.select('a[href*="/propiedad/"]')
        cards: List[Dict[str, Any]] = []

        seen: set[str] = set()

        for a in anchors:
            href = clean_ws(a.get("href") or "")
            if "/propiedad/" not in href:
                continue

            # external_id: suele iniciar como /propiedad/1079303_...
            m = re.search(r"/propiedad/(\d+)", href)
            if not m:
                continue
            external_id = m.group(1)

            if external_id in seen:
                continue
            seen.add(external_id)

            external_url = urljoin("https://century21taurus.com", href)

            # title: intenta buscar el texto más “headline” cercano
            title = clean_ws(a.get_text(" ", strip=True))
            if not title or len(title) < 5:
                # fallback: busca h3/h4 cercano
                h = a.find_parent().find(["h3", "h4"]) if a.find_parent() else None
                title = clean_ws(h.get_text(" ", strip=True) if h else "") or "Sin título"

            # price: busca patrón $X,XXX,XXX MXN
            parent_text = clean_ws(a.find_parent().get_text(" ", strip=True) if a.find_parent() else "")
            price, currency = parse_price(parent_text)

            # location: líneas tipo "Mitras Poniente Bicentenario, García, Nuevo León"
            location = None
            # heurística: busca algo con comas
            mm = re.search(r"([A-Za-zÁÉÍÓÚÜÑñ0-9\s]+,\s*[A-Za-zÁÉÍÓÚÜÑñ\s]+(?:,\s*[A-Za-zÁÉÍÓÚÜÑñ\s]+)?)", parent_text)
            if mm:
                location = clean_ws(mm.group(1))

            # image
            img = a.find_parent().find("img") if a.find_parent() else None
            thumbnail_url = None
            if img and img.get("src"):
                thumbnail_url = clean_ws(img.get("src") or "")
                if thumbnail_url:
                    thumbnail_url = urljoin("https://century21taurus.com", thumbnail_url)

            # extras: intenta detectar recámaras/baños/estac y m2
            bedrooms = None
            bathrooms = None
            parking = None
            built_m2 = None
            lot_m2 = None

            mbed = re.search(r"(\d+)\s*(REC\.|RECÁMARAS|RECAMARAS)", parent_text, re.I)
            if mbed:
                bedrooms = safe_int(mbed.group(1))

            mbath = re.search(r"(\d+)\s*(BAÑOS|BANOS)", parent_text, re.I)
            if mbath:
                bathrooms = safe_int(mbath.group(1))

            mpark = re.search(r"(\d+)\s*(ESTAC\.|ESTACIONAMIENTOS?)", parent_text, re.I)
            if mpark:
                parking = safe_int(mpark.group(1))

            # m2: "315 m² Construcción" / "295 m² Terreno"
            mbuilt = re.search(r"(\d+(?:\.\d+)?)\s*m²\s*CONSTRUCCI", parent_text, re.I)
            if mbuilt:
                built_m2 = safe_float(mbuilt.group(1))

            mlot = re.search(r"(\d+(?:\.\d+)?)\s*m²\s*TERRENO", parent_text, re.I)
            if mlot:
                lot_m2 = safe_float(mlot.group(1))

            # split de municipio/ciudad (best effort)
            city = None
            neighborhood = None
            if location and "," in location:
                parts = [clean_ws(p) for p in location.split(",") if clean_ws(p)]
                if len(parts) >= 2:
                    neighborhood = parts[0]
                    city = parts[1]

            cards.append(
                {
                    "external_id": external_id,
                    "external_url": external_url,
                    "raw_title": title,
                    "raw_price_text": parent_text,
                    "raw_location_text": location,
                    "title": title,
                    "price": price,
                    "currency": currency or "MXN",
                    "operation_type": "venta",
                    "property_type": normalize_property_type(title) or "casa",
                    "country": "México",
                    "state": "Nuevo León",
                    "municipality": city,
                    "neighborhood": neighborhood,
                    "city": city,
                    "address_text": location,
                    "bedrooms": bedrooms,
                    "bathrooms": bathrooms,
                    "parking_spaces": parking,
                    "construction_area_m2": built_m2,
                    "land_area_m2": lot_m2,
                    "thumbnail_url": thumbnail_url,
                    "main_url": external_url,
                }
            )

        return cards

    def _extract_items_from_payload(self, payload: Any) -> List[Dict[str, Any]]:
        """
        Estrategia:
        1) Si hay HTML embebido en el JSON -> parse HTML -> cards
        2) Si no, intenta encontrar listas de dicts con url/id/titulo/precio
        """
        htmls = self._extract_html_candidates(payload)
        for html in htmls:
            cards = self._parse_cards_from_html(html)
            if cards:
                return cards

        # fallback: escaneo de listas tipo results/items
        candidates: List[Dict[str, Any]] = []

        def walk(o: Any):
            if isinstance(o, dict):
                for v in o.values():
                    walk(v)
            elif isinstance(o, list):
                # lista de dicts con algo de url/id
                if o and isinstance(o[0], dict):
                    for it in o:
                        keys = set(k.lower() for k in it.keys())
                        if any(k in keys for k in ["id", "external_id", "url", "link", "titulo", "title"]):
                            candidates.append(it)
                for it in o:
                    walk(it)

        walk(payload)

        # Mapea candidatos si se puede
        mapped: List[Dict[str, Any]] = []
        for it in candidates:
            # intenta id
            raw_id = it.get("id") or it.get("external_id") or it.get("property_id") or it.get("Id")
            external_id = str(raw_id) if raw_id else None

            # url
            raw_url = it.get("url") or it.get("link") or it.get("href") or it.get("Url") or it.get("Link")
            if not raw_url and isinstance(it.get("detalle"), str):
                raw_url = it.get("detalle")

            if not external_id and isinstance(raw_url, str):
                m = re.search(r"/propiedad/(\d+)", raw_url)
                if m:
                    external_id = m.group(1)

            if not external_id:
                continue

            if isinstance(raw_url, str) and raw_url:
                external_url = urljoin("https://century21taurus.com", raw_url)
            else:
                external_url = None

            title = it.get("title") or it.get("titulo") or it.get("nombre") or it.get("Title") or it.get("Titulo") or "Sin título"
            title = clean_ws(str(title))

            price_text = str(it.get("price") or it.get("precio") or it.get("Price") or "")
            price, currency = parse_price(price_text)

            mapped.append(
                {
                    "external_id": external_id,
                    "external_url": external_url or f"https://century21taurus.com/propiedad/{external_id}",
                    "raw_title": title,
                    "raw_price_text": price_text,
                    "raw_location_text": None,
                    "title": title,
                    "price": price,
                    "currency": currency or "MXN",
                    "operation_type": "venta",
                    "property_type": normalize_property_type(title) or "casa",
                    "country": "México",
                    "state": "Nuevo León",
                    "municipality": None,
                    "neighborhood": None,
                    "city": None,
                    "address_text": None,
                    "thumbnail_url": None,
                    "main_url": external_url or f"https://century21taurus.com/propiedad/{external_id}",
                }
            )

        # dedupe por external_id
        uniq: Dict[str, Dict[str, Any]] = {}
        for x in mapped:
            uniq[x["external_id"]] = x
        return list(uniq.values())

    def run(self):
        logger.info("=== %s | inicio ===", self.portal_code)

        for page in range(1, self.max_pages + 1):
            url = self._get_json_url(page)
            RATE_LIMITER.wait()

            resp = self.session.get(url, timeout=HTTP_TIMEOUT)
            if resp.status_code != 200:
                logger.error("%s | HTTP %s | %s", self.portal_code, resp.status_code, url)
                break

            payload = self._safe_json(resp.text)
            if not payload:
                logger.error("%s | JSON inválido en page=%s | url=%s", self.portal_code, page, url)
                break

            items = self._extract_items_from_payload(payload)
            ids = {it["external_id"] for it in items if it.get("external_id")}
            new_ids = ids - self._seen_external_ids

            logger.info(
                "%s | page=%s | items=%s | nuevos=%s | url=%s",
                self.portal_code,
                page,
                len(items),
                len(new_ids),
                url,
            )

            if not items:
                break

            if page > 1 and len(new_ids) == 0:
                logger.warning("%s | page=%s repetida (0 nuevos). Cortando paginación.", self.portal_code, page)
                break

            self.pages_with_results += 1

            for idx, data in enumerate(items, start=1):
                external_id = data["external_id"]
                self._seen_external_ids.add(external_id)
                self.total_cards_seen += 1

                action = upsert_property(
                    self.conn,
                    portal_id=self.portal_id,
                    external_id=external_id,
                    external_url=data["external_url"],
                    source_page_url=url,
                    source_page_number=page,
                    source_position=idx,
                    title=data["title"],
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
                    construction_area_m2=data.get("construction_area_m2"),
                    year_built=data.get("year_built"),
                    main_url=data.get("main_url"),
                    thumbnail_url=data.get("thumbnail_url"),
                    raw_title=data.get("raw_title"),
                    raw_price_text=data.get("raw_price_text"),
                    raw_location_text=data.get("raw_location_text"),
                    scraped_at=datetime.now(),
                )

                if action == "insert":
                    self.total_inserts += 1
                else:
                    self.total_updates += 1

            random_sleep()

        self.finalize()
        self.log_summary()
        logger.info("=== %s | fin ===", self.portal_code)


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    logger.info("=== ValoraNL | inicio corrida ===")
    started = time.time()

    conn = None
    ok = 0
    total = 0

    try:
        conn = get_db_connection()

        scrapers = [
            Casas365Scraper(conn),
            C21TaurusScraper(conn),
            HerreraPalaciosScraper(conn),
        ]
        total = len(scrapers)

        for s in scrapers:
            with measure(f"{s.portal_code}"):
                try:
                    s.run()
                    ok += 1
                except Exception as e:
                    logger.exception("%s | error crítico: %s", s.portal_code, e)

    except Exception as e:
        logger.exception("Error global: %s", e)

    finally:
        if conn:
            try:
                conn.close()
                logger.info("BD | conexión cerrada")
            except Exception:
                pass

    elapsed = time.time() - started
    logger.info("=== ValoraNL | fin corrida | ok=%s/%s | tiempo=%.2fs ===", ok, total, elapsed)


if __name__ == "__main__":
    main()
