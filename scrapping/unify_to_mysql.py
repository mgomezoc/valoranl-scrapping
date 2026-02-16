#!/usr/bin/env python3
"""Unifica bases SQLite de scrapers inmobiliarios hacia esquema canónico MySQL."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit, urlunsplit

import importlib


LOGGER = logging.getLogger("valoranl_unify")


def setup_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def truncate_text(value: str | None, max_len: int, field_name: str, metrics: "Metrics") -> str | None:
    if value is None:
        return None
    if len(value) <= max_len:
        return value
    metrics.warnings += 1
    LOGGER.warning(
        "Campo truncado %s (len=%s > %s)",
        field_name,
        len(value),
        max_len,
    )
    return value[:max_len]


def parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("m²", "").replace("m2", "").replace("mts", "")
    text = text.replace("$", "").replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def parse_int(value: Any) -> int | None:
    as_float = parse_float(value)
    if as_float is None:
        return None
    return int(round(as_float))


def parse_bathrooms(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("½", ".5")
    match = re.search(r"\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def normalize_url(url: str | None) -> str | None:
    if not url:
        return None
    url = url.strip()
    if not url:
        return None
    parts = urlsplit(url)
    clean_path = re.sub(r"/+", "/", parts.path).rstrip("/")
    normalized = urlunsplit((parts.scheme.lower(), parts.netloc.lower(), clean_path, parts.query, ""))
    return normalized


def sha256(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonical_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def normalize_status(raw_status: str | None) -> str:
    text = (raw_status or "").strip().lower()
    if any(word in text for word in ["vend", "sold"]):
        return "sold"
    if any(word in text for word in ["inactiv", "baja", "no disponible"]):
        return "inactive"
    if text:
        return "active"
    return "active"


def normalize_price_type(*texts: str | None) -> str:
    joined = " ".join((t or "") for t in texts).lower()
    if "renta" in joined or "rent" in joined:
        return "rent"
    if "venta" in joined or "sale" in joined:
        return "sale"
    return "unknown"


# ---------------------------------------------------------------------------
# Mejora 2: Normalización canónica de property_type
# ---------------------------------------------------------------------------
PROPERTY_TYPE_MAP: dict[str, str] = {
    "casa": "casa",
    "casas": "casa",
    "house": "casa",
    "residencia": "casa",
    "departamento": "departamento",
    "depto": "departamento",
    "depto.": "departamento",
    "departamentos": "departamento",
    "apartment": "departamento",
    "terreno": "terreno",
    "terrenos": "terreno",
    "lote": "terreno",
    "land": "terreno",
    "local": "local",
    "local comercial": "local",
    "oficina": "oficina",
    "bodega": "bodega",
    "rancho": "rancho",
}


def normalize_property_type(raw: str | None) -> str | None:
    if not raw:
        return None
    key = raw.strip().lower()
    return PROPERTY_TYPE_MAP.get(key, key)


# ---------------------------------------------------------------------------
# Mejora 3: Normalización de municipio y colonia para NL
# ---------------------------------------------------------------------------
MUNICIPALITY_ALIASES: dict[str, str] = {
    "sta. catarina": "Santa Catarina",
    "sta catarina": "Santa Catarina",
    "santa catarina, n.l.": "Santa Catarina",
    "mty": "Monterrey",
    "mty.": "Monterrey",
    "monterrey, n.l.": "Monterrey",
    "san pedro": "San Pedro Garza García",
    "san pedro garza garcia": "San Pedro Garza García",
    "san pedro garza garcía, n.l.": "San Pedro Garza García",
    "spgg": "San Pedro Garza García",
    "apodaca": "Apodaca",
    "gral. escobedo": "General Escobedo",
    "general escobedo": "General Escobedo",
    "gral escobedo": "General Escobedo",
    "guadalupe, n.l.": "Guadalupe",
    "garcia": "García",
    "garcía": "García",
    "juarez": "Juárez",
    "juárez": "Juárez",
    "cadereyta jimenez": "Cadereyta Jiménez",
    "cadereyta jiménez": "Cadereyta Jiménez",
    "cienega de flores": "Ciénega de Flores",
    "ciénega de flores": "Ciénega de Flores",
    "santiago, n.l.": "Santiago",
}


def normalize_municipality(raw: str | None) -> str | None:
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None
    lookup = text.lower()
    if lookup in MUNICIPALITY_ALIASES:
        return MUNICIPALITY_ALIASES[lookup]
    return text.title()


def normalize_colony(raw: str | None) -> str | None:
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None
    # Eliminar sufijos ruidosos comunes
    text = re.sub(r",?\s*Nuevo León$", "", text, flags=re.IGNORECASE)
    text = re.sub(r",?\s*N\.?L\.?$", "", text, flags=re.IGNORECASE)
    text = text.strip().strip(",").strip()
    if not text:
        return None
    return text.title()


# ---------------------------------------------------------------------------
# Mejora 1: Inferir age_years desde año de construcción o descripción
# ---------------------------------------------------------------------------
_AGE_FROM_YEAR_RE = re.compile(
    r"(?:construi(?:da|do)\s+en|año\s+(?:de\s+)?construcci[oó]n[:\s]*|built\s+in)\s*(\d{4})",
    re.IGNORECASE,
)
_AGE_FROM_YEARS_RE = re.compile(
    r"(\d{1,3})\s*años?\s+de\s+antig[uü]edad",
    re.IGNORECASE,
)


def infer_age_years(
    ano_construccion: int | None = None,
    description: str | None = None,
    title: str | None = None,
) -> int | None:
    """Intenta inferir la edad del inmueble en años."""
    current_year = datetime.now().year

    # Prioridad 1: campo directo año de construcción
    if ano_construccion is not None and 1900 < ano_construccion <= current_year:
        return current_year - ano_construccion

    # Prioridad 2: regex en descripción/título
    for text in (description, title):
        if not text:
            continue
        # "construida en 2018" / "año de construcción: 2015"
        match = _AGE_FROM_YEAR_RE.search(text)
        if match:
            year = int(match.group(1))
            if 1900 < year <= current_year:
                return current_year - year
        # "15 años de antigüedad"
        match = _AGE_FROM_YEARS_RE.search(text)
        if match:
            years = int(match.group(1))
            if 0 <= years <= 120:
                return years

    return None


# ---------------------------------------------------------------------------
# Mejora 5: Validación de precios razonables para NL
# ---------------------------------------------------------------------------
MIN_SALE_PRICE = 100_000        # $100K MXN mínimo para venta
MAX_SALE_PRICE = 100_000_000    # $100M MXN máximo
MIN_PPU_M2 = 3_000              # $3,000/m² mínimo razonable en NL
MAX_PPU_M2 = 80_000             # $80,000/m² máximo razonable en NL


def validate_listing_price(
    price: float | None,
    area_construction_m2: float | None,
    price_type: str,
) -> tuple[bool, str | None]:
    """Valida si el precio es razonable. Retorna (is_valid, reason)."""
    if price is None:
        return True, None
    if price_type != "sale":
        return True, None

    if price < MIN_SALE_PRICE:
        return False, f"precio_venta={price:.0f} < mín {MIN_SALE_PRICE}"
    if price > MAX_SALE_PRICE:
        return False, f"precio_venta={price:.0f} > máx {MAX_SALE_PRICE}"

    if area_construction_m2 and area_construction_m2 > 0:
        ppu = price / area_construction_m2
        if ppu < MIN_PPU_M2:
            return False, f"PPU={ppu:.0f} < mín {MIN_PPU_M2}"
        if ppu > MAX_PPU_M2:
            return False, f"PPU={ppu:.0f} > máx {MAX_PPU_M2}"

    return True, None


def resolve_sqlite_path(file_name: str) -> Path:
    here = Path(__file__).resolve().parent
    candidates = [
        here / file_name,
        here.parent / file_name,
        here / "scrapping" / file_name,
        here.parent / "scrapping" / file_name,
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(f"No se encontró la base SQLite: {file_name}")


@dataclass
class Metrics:
    read: int = 0
    inserted: int = 0
    updated: int = 0
    duplicates: int = 0
    skipped_price: int = 0
    stale_deactivated: int = 0
    warnings: int = 0
    errors: int = 0


@dataclass
class CanonicalListing:
    source_code: str
    source_listing_id: str | None
    parse_version: str
    url: str | None
    url_normalized: str | None
    url_hash: str | None
    fingerprint_hash: str
    dedupe_hash: str
    status: str
    price_type: str
    price_amount: float | None
    currency: str
    maintenance_fee: float | None
    property_type: str | None
    area_construction_m2: float | None
    area_land_m2: float | None
    bedrooms: int | None
    bathrooms: float | None
    half_bathrooms: float | None
    parking: int | None
    floors: int | None
    age_years: int | None
    title: str | None
    description: str | None
    street: str | None
    colony: str | None
    municipality: str | None
    state: str | None
    country: str | None
    postal_code: str | None
    lat: float | None
    lng: float | None
    geo_precision: str
    images_json: str | None
    contact_json: str | None
    amenities_json: str | None
    details_json: str | None
    raw_json: str | None
    source_first_seen_at: datetime | None
    source_last_seen_at: datetime | None


class SQLiteSourceMapper:
    source_code = ""
    source_name = ""
    db_file = ""
    parse_version = "unify_v1"

    def __init__(self) -> None:
        self.db_path = resolve_sqlite_path(self.db_file)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def discover_table(self, conn: sqlite3.Connection) -> str:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables = [r[0] for r in cursor.fetchall()]
        if not tables:
            raise RuntimeError(f"Sin tablas en {self.db_path}")
        if "propiedades" in tables:
            return "propiedades"
        return tables[0]

    def iter_rows(self) -> Iterable[sqlite3.Row]:
        with self.connect() as conn:
            table = self.discover_table(conn)
            LOGGER.info("Fuente %s: tabla detectada %s", self.source_code, table)
            for row in conn.execute(f"SELECT * FROM {table}"):
                yield row

    def map_row(self, row: sqlite3.Row, metrics: Metrics) -> CanonicalListing:
        raise NotImplementedError

    @staticmethod
    def build_fingerprint(
        municipality: str | None,
        colony: str | None,
        area_construction_m2: float | None,
        price_amount: float | None,
        bedrooms: int | None,
    ) -> str:
        chunks = [
            (municipality or "").strip().lower(),
            (colony or "").strip().lower(),
            str(round(area_construction_m2 or 0, 1)),
            str(round(price_amount or 0, 0)),
            str(bedrooms or 0),
        ]
        return sha256("|".join(chunks)) or hashlib.sha256(b"").hexdigest()


class Casas365Mapper(SQLiteSourceMapper):
    source_code = "casas365"
    source_name = "Casas 365"
    db_file = "casas365_propiedades.db"

    def map_row(self, row: sqlite3.Row, metrics: Metrics) -> CanonicalListing:
        url = clean_text(row["url"])
        url_norm = normalize_url(url)
        price = parse_float(row["precio"])
        bathrooms = parse_bathrooms(row["banos"])
        municipality = normalize_municipality(clean_text(row["ciudad"]))
        colony = normalize_colony(clean_text(row["colonia"]))
        title = clean_text(row["titulo"])
        description = clean_text(row["descripcion"])
        status = normalize_status(clean_text(row["estado"]))
        if price is None:
            metrics.warnings += 1
            LOGGER.warning("casas365 sin precio | url=%s", url)

        images = [img.strip() for img in (row["imagenes"] or "").split(",") if img.strip()]
        contact = {
            "agent_name": clean_text(row["agente_nombre"]),
            "agent_phone": clean_text(row["agente_telefono"]),
            "agent_whatsapp": clean_text(row["agente_whatsapp"]),
            "agent_email": clean_text(row["agente_email"]),
        }
        details = {
            "accion": clean_text(row["accion"]),
            "habitaciones": parse_int(row["habitaciones"]),
            "clase_energetica": clean_text(row["clase_energetica"]),
        }
        raw = dict(row)

        area_const = parse_float(row["construccion_m2"])
        area_land = parse_float(row["terreno_m2"])
        bedrooms = parse_int(row["recamaras"])
        fingerprint = self.build_fingerprint(municipality, colony, area_const, price, bedrooms)
        url_hash = sha256(url_norm)
        dedupe = url_hash or fingerprint

        street = clean_text(row["calle"])
        # En Casas365 el campo calle a veces contiene descripciones completas; evitamos error Data too long.
        street = truncate_text(street, 255, "street", metrics)

        age_years = infer_age_years(description=description, title=title)

        return CanonicalListing(
            source_code=self.source_code,
            source_listing_id=None,
            parse_version=self.parse_version,
            url=url,
            url_normalized=url_norm,
            url_hash=url_hash,
            fingerprint_hash=fingerprint,
            dedupe_hash=dedupe,
            status=status,
            price_type=normalize_price_type(clean_text(row["accion"]), title),
            price_amount=price,
            currency=(clean_text(row["moneda"]) or "MXN")[:3].upper(),
            maintenance_fee=None,
            property_type=normalize_property_type(clean_text(row["tipo"])),
            area_construction_m2=area_const,
            area_land_m2=area_land,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            half_bathrooms=None,
            parking=parse_int(row["estacionamientos"]),
            floors=parse_int(row["plantas"]),
            age_years=age_years,
            title=truncate_text(title, 500, "title", metrics),
            description=description,
            street=street,
            colony=truncate_text(colony, 180, "colony", metrics),
            municipality=truncate_text(municipality, 180, "municipality", metrics),
            state=truncate_text(clean_text(row["estado_geo"]) or "Nuevo León", 120, "state", metrics),
            country="México",
            postal_code=None,
            lat=parse_float(row["latitud"]),
            lng=parse_float(row["longitud"]),
            geo_precision="exact" if row["latitud"] and row["longitud"] else "unknown",
            images_json=canonical_json(images),
            contact_json=canonical_json(contact),
            amenities_json=None,
            details_json=canonical_json(details),
            raw_json=canonical_json(raw),
            source_first_seen_at=parse_datetime(row["fecha_scraping"]),
            source_last_seen_at=parse_datetime(row["fecha_scraping"]),
        )


class GPViviendaMapper(SQLiteSourceMapper):
    source_code = "gpvivienda"
    source_name = "GP Vivienda"
    db_file = "gpvivienda_nuevoleon.db"

    def map_row(self, row: sqlite3.Row, metrics: Metrics) -> CanonicalListing:
        url = clean_text(row["url"])
        url_norm = normalize_url(url)
        price = parse_float(row["precio"])
        municipality = normalize_municipality(clean_text(row["ciudad"]))
        colony = normalize_colony(clean_text(row["fraccionamiento"]))
        bedrooms = parse_int(row["recamaras"])
        area_const = parse_float(row["m2_construidos"])
        area_land = parse_float(row["m2_terreno"])

        if area_const and area_const < 20:
            metrics.warnings += 1
            LOGGER.warning("gpvivienda m2 construcción sospechoso=%s | url=%s", area_const, url)

        fingerprint = self.build_fingerprint(municipality, colony, area_const, price, bedrooms)
        url_hash = sha256(url_norm)
        dedupe = url_hash or fingerprint

        amenities = clean_text(row["amenidades"])
        amenities_list = [x.strip() for x in amenities.split(",")] if amenities else []
        details = {
            "modelo": clean_text(row["modelo"]),
            "es_promocion": bool(row["es_promocion"]),
            "es_preventa": bool(row["es_preventa"]),
            "precio_texto": clean_text(row["precio_texto"]),
            "plano_url": clean_text(row["plano_url"]),
            "imagen_url": clean_text(row["imagen_url"]),
        }

        title = clean_text(row["titulo"]) or clean_text(row["modelo"])
        description = clean_text(row["descripcion"])
        age_years = infer_age_years(description=description, title=title)

        return CanonicalListing(
            source_code=self.source_code,
            source_listing_id=None,
            parse_version=self.parse_version,
            url=url,
            url_normalized=url_norm,
            url_hash=url_hash,
            fingerprint_hash=fingerprint,
            dedupe_hash=dedupe,
            status="active",
            price_type="sale",
            price_amount=price,
            currency="MXN",
            maintenance_fee=None,
            property_type=normalize_property_type("casa"),
            area_construction_m2=area_const,
            area_land_m2=area_land,
            bedrooms=bedrooms,
            bathrooms=parse_bathrooms(row["banos"]),
            half_bathrooms=None,
            parking=None,
            floors=None,
            age_years=age_years,
            title=truncate_text(title, 500, "title", metrics),
            description=description,
            street=None,
            colony=truncate_text(colony, 180, "colony", metrics),
            municipality=truncate_text(municipality, 180, "municipality", metrics),
            state=truncate_text(clean_text(row["estado"]) or "Nuevo León", 120, "state", metrics),
            country="México",
            postal_code=None,
            lat=None,
            lng=None,
            geo_precision="unknown",
            images_json=canonical_json([clean_text(row["imagen_url"])] if row["imagen_url"] else []),
            contact_json=None,
            amenities_json=canonical_json(amenities_list),
            details_json=canonical_json(details),
            raw_json=canonical_json(dict(row)),
            source_first_seen_at=parse_datetime(row["fecha_scraping"]),
            source_last_seen_at=parse_datetime(row["fecha_actualizacion"] or row["fecha_scraping"]),
        )


class RealtyWorldMapper(SQLiteSourceMapper):
    source_code = "realtyworld"
    source_name = "Realty World"
    db_file = "realtyworld_propiedades.db"

    def map_row(self, row: sqlite3.Row, metrics: Metrics) -> CanonicalListing:
        url = clean_text(row["url"])
        url_norm = normalize_url(url)
        price = parse_float(row["precio"])
        municipality = normalize_municipality(clean_text(row["ciudad"]))
        colony = normalize_colony(clean_text(row["colonia"]))
        bedrooms = parse_int(row["recamaras"])
        area_const = parse_float(row["construccion_m2"])

        if municipality and "renta" in municipality.lower():
            metrics.warnings += 1
            LOGGER.warning("realtyworld ciudad aparentemente ruidosa=%s | url=%s", municipality, url)

        fingerprint = self.build_fingerprint(municipality, colony, area_const, price, bedrooms)
        url_hash = sha256(url_norm)
        dedupe = url_hash or fingerprint

        half_baths = parse_float(row["medios_banos"])
        ano_construccion = parse_int(row["ano_construccion"])
        details = {
            "property_id": clean_text(row["property_id"]),
            "frente_m": parse_float(row["frente_m"]),
            "fondo_m": parse_float(row["fondo_m"]),
            "ano_construccion": ano_construccion,
            "fecha_publicacion": clean_text(row["fecha_publicacion"]),
            "precio_texto": clean_text(row["precio_texto"]),
        }

        title = clean_text(row["titulo"])
        description = clean_text(row["descripcion"])
        age_years = infer_age_years(
            ano_construccion=ano_construccion,
            description=description,
            title=title,
        )

        return CanonicalListing(
            source_code=self.source_code,
            source_listing_id=clean_text(row["property_id"]),
            parse_version=self.parse_version,
            url=url,
            url_normalized=url_norm,
            url_hash=url_hash,
            fingerprint_hash=fingerprint,
            dedupe_hash=dedupe,
            status="active",
            price_type=normalize_price_type(title),
            price_amount=price,
            currency="MXN",
            maintenance_fee=None,
            property_type=normalize_property_type("casa"),
            area_construction_m2=area_const,
            area_land_m2=parse_float(row["terreno_m2"]),
            bedrooms=bedrooms,
            bathrooms=parse_bathrooms(row["banos"]),
            half_bathrooms=half_baths,
            parking=parse_int(row["estacionamientos"]),
            floors=parse_int(row["plantas"]),
            age_years=age_years,
            title=truncate_text(title, 500, "title", metrics),
            description=description,
            street=None,
            colony=truncate_text(colony, 180, "colony", metrics),
            municipality=truncate_text(municipality, 180, "municipality", metrics),
            state=truncate_text(clean_text(row["estado"]), 120, "state", metrics),
            country="México",
            postal_code=None,
            lat=None,
            lng=None,
            geo_precision="unknown",
            images_json=canonical_json([x.strip() for x in (row["imagenes"] or "").split(",") if x.strip()]),
            contact_json=None,
            amenities_json=canonical_json([x.strip() for x in (row["amenidades"] or "").split(",") if x.strip()]),
            details_json=canonical_json(details),
            raw_json=canonical_json(dict(row)),
            source_first_seen_at=parse_datetime(row["fecha_scraping"]),
            source_last_seen_at=parse_datetime(row["fecha_scraping"]),
        )


class MySQLMigrator:
    def __init__(self) -> None:
        self.host = os.getenv("MYSQL_HOST", "127.0.0.1")
        self.port = int(os.getenv("MYSQL_PORT", "3306"))
        self.user = os.getenv("MYSQL_USER", "root")
        self.password = os.getenv("MYSQL_PASSWORD", "")
        self.database = os.getenv("MYSQL_DATABASE", "valoranl")

    def connect(self, with_database: bool = True):
        pymysql = importlib.import_module("pymysql")
        dict_cursor = importlib.import_module("pymysql.cursors").DictCursor
        params = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "charset": "utf8mb4",
            "cursorclass": dict_cursor,
            "autocommit": False,
        }
        if with_database:
            params["database"] = self.database
        return pymysql.connect(**params)

    def execute_sql_file(self, sql_file: Path) -> None:
        script = sql_file.read_text(encoding="utf-8")
        statements = self._split_sql_statements(script)
        with self.connect(with_database=False) as conn:
            with conn.cursor() as cursor:
                for stmt in statements:
                    if stmt.strip():
                        cursor.execute(stmt)
            conn.commit()
        LOGGER.info("Esquema inicializado desde %s", sql_file)

    @staticmethod
    def _split_sql_statements(script: str) -> list[str]:
        statements: list[str] = []
        current: list[str] = []
        in_single = False
        in_double = False
        escape = False
        for char in script:
            if char == "\\" and not escape:
                escape = True
                current.append(char)
                continue
            if char == "'" and not in_double and not escape:
                in_single = not in_single
            elif char == '"' and not in_single and not escape:
                in_double = not in_double
            if char == ";" and not in_single and not in_double:
                statement = "".join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
            else:
                current.append(char)
            escape = False
        tail = "".join(current).strip()
        if tail:
            statements.append(tail)
        return statements

    def get_or_create_source_id(self, cursor, mapper: SQLiteSourceMapper) -> int:
        sql = (
            "INSERT INTO sources (source_code, source_name, base_url) VALUES (%s, %s, %s) "
            "ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id), source_name=VALUES(source_name), base_url=VALUES(base_url)"
        )
        base_url = {
            "casas365": "https://casas365.mx",
            "gpvivienda": "https://gpvivienda.com",
            "realtyworld": "https://www.realtyworld.com.mx",
        }.get(mapper.source_code)
        cursor.execute(sql, (mapper.source_code, mapper.source_name, base_url))
        return int(cursor.lastrowid)

    def migrate_mapper(self, mapper: SQLiteSourceMapper) -> Metrics:
        metrics = Metrics()
        batch_size = 500
        with self.connect(with_database=True) as conn:
            with conn.cursor() as cursor:
                source_id = self.get_or_create_source_id(cursor, mapper)
                for row in mapper.iter_rows():
                    metrics.read += 1
                    try:
                        canonical = mapper.map_row(row, metrics)

                        # Mejora 5: validar precio antes de insertar
                        price_ok, price_reason = validate_listing_price(
                            canonical.price_amount,
                            canonical.area_construction_m2,
                            canonical.price_type,
                        )
                        if not price_ok:
                            metrics.skipped_price += 1
                            LOGGER.warning(
                                "%s precio inválido (%s) | url=%s",
                                mapper.source_code,
                                price_reason,
                                canonical.url,
                            )
                            continue

                        self._upsert_listing(cursor, source_id, canonical, metrics)
                    except Exception as exc:
                        metrics.errors += 1
                        LOGGER.exception("Error al migrar %s fila id=%s: %s", mapper.source_code, row["id"], exc)

                    if metrics.read % batch_size == 0:
                        conn.commit()

                conn.commit()
        return metrics

    def deactivate_stale_listings(self, days: int = 30) -> int:
        """Mejora 4: Marca como inactive los listings no vistos en N días."""
        with self.connect(with_database=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE listings
                    SET status = 'inactive', updated_at = NOW()
                    WHERE status = 'active'
                      AND seen_last_at < DATE_SUB(NOW(), INTERVAL %s DAY)
                    """,
                    (days,),
                )
                count = cursor.rowcount
                if count > 0:
                    LOGGER.info("Desactivados %d listings no vistos en %d días.", count, days)
                conn.commit()
        return count

    def _upsert_listing(self, cursor, source_id: int, listing: CanonicalListing, metrics: Metrics) -> None:
        cursor.execute(
            "SELECT id, price_amount, status FROM listings WHERE dedupe_hash = %s",
            (listing.dedupe_hash,),
        )
        existing = cursor.fetchone()

        sql = """
            INSERT INTO listings (
                source_id, source_listing_id, parse_version,
                url, url_normalized, url_hash, fingerprint_hash, dedupe_hash,
                status, price_type, price_amount, currency, maintenance_fee,
                property_type, area_construction_m2, area_land_m2, bedrooms, bathrooms, half_bathrooms,
                parking, floors, age_years,
                title, description,
                street, colony, municipality, state, country, postal_code,
                lat, lng, geo_precision,
                images_json, contact_json, amenities_json, details_json, raw_json,
                source_first_seen_at, source_last_seen_at, seen_first_at, seen_last_at
            ) VALUES (
                %(source_id)s, %(source_listing_id)s, %(parse_version)s,
                %(url)s, %(url_normalized)s, %(url_hash)s, %(fingerprint_hash)s, %(dedupe_hash)s,
                %(status)s, %(price_type)s, %(price_amount)s, %(currency)s, %(maintenance_fee)s,
                %(property_type)s, %(area_construction_m2)s, %(area_land_m2)s, %(bedrooms)s, %(bathrooms)s, %(half_bathrooms)s,
                %(parking)s, %(floors)s, %(age_years)s,
                %(title)s, %(description)s,
                %(street)s, %(colony)s, %(municipality)s, %(state)s, %(country)s, %(postal_code)s,
                %(lat)s, %(lng)s, %(geo_precision)s,
                CAST(%(images_json)s AS JSON), CAST(%(contact_json)s AS JSON), CAST(%(amenities_json)s AS JSON),
                CAST(%(details_json)s AS JSON), CAST(%(raw_json)s AS JSON),
                %(source_first_seen_at)s, %(source_last_seen_at)s, NOW(), NOW()
            )
            ON DUPLICATE KEY UPDATE
                id=LAST_INSERT_ID(id),
                source_id=VALUES(source_id),
                source_listing_id=VALUES(source_listing_id),
                parse_version=VALUES(parse_version),
                url=VALUES(url),
                url_normalized=VALUES(url_normalized),
                url_hash=VALUES(url_hash),
                fingerprint_hash=VALUES(fingerprint_hash),
                status=VALUES(status),
                price_type=VALUES(price_type),
                price_amount=VALUES(price_amount),
                currency=VALUES(currency),
                maintenance_fee=VALUES(maintenance_fee),
                property_type=VALUES(property_type),
                area_construction_m2=VALUES(area_construction_m2),
                area_land_m2=VALUES(area_land_m2),
                bedrooms=VALUES(bedrooms),
                bathrooms=VALUES(bathrooms),
                half_bathrooms=VALUES(half_bathrooms),
                parking=VALUES(parking),
                floors=VALUES(floors),
                age_years=VALUES(age_years),
                title=VALUES(title),
                description=VALUES(description),
                street=VALUES(street),
                colony=VALUES(colony),
                municipality=VALUES(municipality),
                state=VALUES(state),
                country=VALUES(country),
                postal_code=VALUES(postal_code),
                lat=VALUES(lat),
                lng=VALUES(lng),
                geo_precision=VALUES(geo_precision),
                images_json=VALUES(images_json),
                contact_json=VALUES(contact_json),
                amenities_json=VALUES(amenities_json),
                details_json=VALUES(details_json),
                raw_json=VALUES(raw_json),
                source_last_seen_at=VALUES(source_last_seen_at),
                seen_last_at=NOW(),
                updated_at=NOW()
        """

        params = listing.__dict__.copy()
        params["source_id"] = source_id
        cursor.execute(sql, params)
        listing_id = int(cursor.lastrowid)

        if existing is None:
            metrics.inserted += 1
            self._insert_price_history(cursor, listing_id, listing.status, listing.price_amount, listing.currency)
            self._insert_status_history(cursor, listing_id, None, listing.status)
        else:
            metrics.updated += 1
            metrics.duplicates += 1
            old_price = existing["price_amount"]
            old_status = existing["status"]
            if (old_price != listing.price_amount) or (old_status != listing.status):
                self._insert_price_history(cursor, listing_id, listing.status, listing.price_amount, listing.currency)
            if old_status != listing.status:
                self._insert_status_history(cursor, listing_id, old_status, listing.status)

    @staticmethod
    def _insert_price_history(cursor, listing_id: int, status: str, price: float | None, currency: str) -> None:
        cursor.execute(
            """
            INSERT INTO listing_price_history (listing_id, status, price_amount, currency, captured_at)
            VALUES (%s, %s, %s, %s, NOW())
            """,
            (listing_id, status, price, currency),
        )

    @staticmethod
    def _insert_status_history(cursor, listing_id: int, old_status: str | None, new_status: str) -> None:
        cursor.execute(
            """
            INSERT INTO listing_status_history (listing_id, old_status, new_status, changed_at)
            VALUES (%s, %s, %s, NOW())
            """,
            (listing_id, old_status, new_status),
        )


def print_summary(summary: dict[str, Metrics], stale_count: int = 0) -> None:
    print("\n=== RESUMEN DE MIGRACIÓN ===")
    totals = Metrics()
    for source, metric in summary.items():
        print(
            f"{source:<12} leídos={metric.read:<5} insertados={metric.inserted:<5} "
            f"actualizados={metric.updated:<5} duplicados={metric.duplicates:<5} "
            f"precio_inv={metric.skipped_price:<4} "
            f"warnings={metric.warnings:<4} errores={metric.errors:<4}"
        )
        totals.read += metric.read
        totals.inserted += metric.inserted
        totals.updated += metric.updated
        totals.duplicates += metric.duplicates
        totals.skipped_price += metric.skipped_price
        totals.warnings += metric.warnings
        totals.errors += metric.errors

    print("-" * 90)
    print(
        f"TOTAL        leídos={totals.read:<5} insertados={totals.inserted:<5} "
        f"actualizados={totals.updated:<5} duplicados={totals.duplicates:<5} "
        f"precio_inv={totals.skipped_price:<4} "
        f"warnings={totals.warnings:<4} errores={totals.errors:<4}"
    )
    if stale_count > 0:
        print(f"\nListings desactivados por inactividad (>30 días sin verse): {stale_count}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unifica fuentes SQLite de ValoraNL a MySQL")
    parser.add_argument("--init-schema", type=Path, help="Ruta al script SQL para inicializar esquema")
    parser.add_argument("--migrate", action="store_true", help="Ejecuta migración a MySQL")
    parser.add_argument(
        "--stale-days",
        type=int,
        default=30,
        help="Días sin ver un listing antes de marcarlo como inactive (default: 30). Usa 0 para desactivar.",
    )
    return parser


def main() -> int:
    setup_logging()
    args = build_arg_parser().parse_args()

    if not args.init_schema and not args.migrate:
        LOGGER.error("Debes indicar --init-schema y/o --migrate")
        return 1

    migrator = MySQLMigrator()

    if args.init_schema:
        migrator.execute_sql_file(args.init_schema)

    if args.migrate:
        mappers: list[SQLiteSourceMapper] = [
            Casas365Mapper(),
            GPViviendaMapper(),
            RealtyWorldMapper(),
        ]
        summary: dict[str, Metrics] = {}
        for mapper in mappers:
            LOGGER.info("Iniciando migración para %s (%s)", mapper.source_code, mapper.db_path)
            summary[mapper.source_code] = migrator.migrate_mapper(mapper)

        # Mejora 4: desactivar listings no vistos recientemente
        stale_count = 0
        if args.stale_days > 0:
            stale_count = migrator.deactivate_stale_listings(days=args.stale_days)

        print_summary(summary, stale_count=stale_count)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
