#!/usr/bin/env python3
"""
ValoraNL - Sistema Autónomo de Scraping Unificado
Ejecuta: python valora_autonomous.py

Características:
- Auto-descubrimiento de scrapers disponibles
- Checkpointing (reanuda si falla)
- Historial completo de cambios en todos los campos
- Deduplicación inteligente entre fuentes
- Reintentos automáticos con backoff exponencial
- Métricas y telemetría
"""

import os
import sys
import json
import time
import logging
import sqlite3
import asyncio
import argparse
import hashlib
import re
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Any, Optional, List, Dict, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
import importlib.util

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    handlers=[
        logging.FileHandler('valora_autonomous.log'),
        logging.StreamHandler()
    ]
)
LOGGER = logging.getLogger("valora_autonomous")

# ============================================================================
# CONFIGURACIÓN GLOBAL
# ============================================================================

CONFIG = {
    'mysql': {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'port': int(os.getenv('MYSQL_PORT', '3306')),
        'user': os.getenv('MYSQL_USER', 'root'),
        'password': os.getenv('MYSQL_PASSWORD', ''),
        'database': 'valoranl'
    },
    'checkpoint_file': 'valora_checkpoint.json',
    'max_retries': 3,
    'retry_delay_base': 2,  # segundos
    'batch_size': 100,
    'stale_days': 30,
    'parallel_workers': 2,
}

# ============================================================================
# MODELOS DE DATOS
# ============================================================================

class ExecutionStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"

@dataclass
class ExecutionCheckpoint:
    """Estado de ejecución para reanudación"""
    execution_id: str
    started_at: str
    completed_sources: List[str] = field(default_factory=list)
    failed_sources: Dict[str, str] = field(default_factory=dict)
    current_source: Optional[str] = None
    current_batch: int = 0
    total_processed: int = 0

    def save(self):
        with open(CONFIG['checkpoint_file'], 'w') as f:
            json.dump(asdict(self), f, indent=2)

    @classmethod
    def load(cls) -> Optional['ExecutionCheckpoint']:
        try:
            with open(CONFIG['checkpoint_file'], 'r') as f:
                data = json.load(f)
                return cls(**data)
        except FileNotFoundError:
            return None

    def is_source_completed(self, source: str) -> bool:
        return source in self.completed_sources

@dataclass
class FieldChange:
    """Registro de cambio en un campo específico"""
    field_name: str
    old_value: Any
    new_value: Any
    change_type: str  # 'content', 'price', 'status', 'location', 'metadata'
    changed_at: datetime = field(default_factory=datetime.now)

@dataclass
class ScrapedListing:
    """Formato canónico unificado de cualquier fuente"""
    # Identidad
    source_code: str
    source_listing_id: Optional[str]
    url: str
    url_hash: str

    # Estado comercial
    status: str = "active"
    price_type: str = "sale"
    price_amount: Optional[float] = None
    currency: str = "MXN"

    # Características físicas
    property_type: Optional[str] = None
    area_construction_m2: Optional[float] = None
    area_land_m2: Optional[float] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    half_bathrooms: Optional[float] = None
    parking: Optional[int] = None
    floors: Optional[int] = None
    age_years: Optional[int] = None

    # Ubicación
    title: Optional[str] = None
    description: Optional[str] = None
    street: Optional[str] = None
    colony: Optional[str] = None
    municipality: Optional[str] = None
    state: str = "Nuevo León"
    country: str = "México"
    postal_code: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None

    # Metadatos
    images: List[str] = field(default_factory=list)
    amenities: List[str] = field(default_factory=list)
    contact_info: Dict = field(default_factory=dict)
    raw_data: Dict = field(default_factory=dict)

    # Trazabilidad
    scraped_at: datetime = field(default_factory=datetime.now)

    def compute_fingerprint(self) -> str:
        """Hash para deduplicación"""
        key = f"{self.municipality or ''}|{self.colony or ''}|{self.area_construction_m2 or 0}|{self.price_amount or 0}|{self.bedrooms or 0}"
        return hashlib.sha256(key.encode()).hexdigest()[:32]

    def compute_dedupe_hash(self) -> str:
        """Hash principal de deduplicación"""
        url_hash = hashlib.sha256(self.url.encode()).hexdigest()[:32]
        return url_hash

# ============================================================================
# ADAPTADORES DE FUENTES (Auto-descubrimiento)
# ============================================================================

class BaseAdapter:
    """Clase base para adaptadores de fuentes"""
    source_code: str = ""
    source_name: str = ""

    def __init__(self):
        self.metrics = {
            'read': 0,
            'inserted': 0,
            'updated': 0,
            'errors': 0,
            'skipped': 0
        }

    def can_execute(self) -> bool:
        """Verifica si la fuente está disponible"""
        raise NotImplementedError

    def scrape(self, checkpoint: ExecutionCheckpoint) -> List[ScrapedListing]:
        """Extrae listings de la fuente"""
        raise NotImplementedError

    def normalize_to_canonical(self, raw_data: Dict) -> ScrapedListing:
        """Convierte datos crudos al formato canónico"""
        raise NotImplementedError


class Casas365Adapter(BaseAdapter):
    """Adaptador para Casas365 (MySQL)"""
    source_code = "casas365"
    source_name = "Casas 365"

    def __init__(self):
        super().__init__()
        self.mysql_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': '',
            'database': 'casas365'
        }

    def can_execute(self) -> bool:
        try:
            import pymysql
            conn = pymysql.connect(**self.mysql_config)
            conn.close()
            return True
        except Exception as e:
            LOGGER.warning(f"Casas365 no disponible: {e}")
            return False

    def scrape(self, checkpoint: ExecutionCheckpoint) -> List[ScrapedListing]:
        """Lee desde MySQL de casas365"""
        import pymysql
        listings = []

        try:
            conn = pymysql.connect(**self.mysql_config)
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute("SELECT * FROM propiedades ORDER BY id")
                rows = cursor.fetchall()

                LOGGER.info(f"Casas365: {len(rows)} registros en MySQL")

                for row in rows:
                    try:
                        listing = self._normalize_row(row)
                        listings.append(listing)
                        self.metrics['read'] += 1
                    except Exception as e:
                        LOGGER.error(f"Error normalizando fila {row.get('id')}: {e}")
                        self.metrics['errors'] += 1

        except Exception as e:
            LOGGER.error(f"Error leyendo Casas365: {e}")
            raise
        finally:
            conn.close()

        return listings

    def _normalize_row(self, row: Dict) -> ScrapedListing:
        """Normaliza fila de Casas365 a canónico"""

        def parse_float(val):
            if not val:
                return None
            try:
                return float(str(val).replace(',', '').replace('m²', '').replace('m2', ''))
            except:
                return None

        def parse_int(val):
            f = parse_float(val)
            return int(f) if f else None

        # Normalizar ubicación
        municipality = self._normalize_municipality(row.get('ciudad', ''))
        colony = self._normalize_colony(row.get('colonia', ''))

        # Extraer imágenes
        images = []
        if row.get('imagenes'):
            images = [img.strip() for img in row['imagenes'].split(',') if img.strip()]

        # Contacto
        contact = {
            'agent_name': row.get('agente_nombre'),
            'agent_phone': row.get('agente_telefono'),
            'agent_whatsapp': row.get('agente_whatsapp'),
            'agent_email': row.get('agente_email')
        }

        listing = ScrapedListing(
            source_code=self.source_code,
            source_listing_id=None,
            url=row.get('url', ''),
            url_hash=hashlib.sha256(row.get('url', '').encode()).hexdigest(),
            status=self._normalize_status(row.get('estado')),
            price_type=self._normalize_price_type(row.get('accion'), row.get('titulo')),
            price_amount=parse_float(row.get('precio')),
            currency=(row.get('moneda') or 'MXN')[:3].upper(),
            property_type=self._normalize_property_type(row.get('tipo')),
            area_construction_m2=parse_float(row.get('construccion_m2')),
            area_land_m2=parse_float(row.get('terreno_m2')),
            bedrooms=parse_int(row.get('recamaras')),
            bathrooms=parse_float(row.get('banos')),
            parking=parse_int(row.get('estacionamientos')),
            floors=parse_int(row.get('plantas')),
            title=row.get('titulo'),
            description=row.get('descripcion'),
            street=row.get('calle'),
            colony=colony,
            municipality=municipality,
            state=row.get('estado_geo') or 'Nuevo León',
            lat=parse_float(row.get('latitud')),
            lng=parse_float(row.get('longitud')),
            images=images[:10],
            amenities=[row.get('clase_energetica')] if row.get('clase_energetica') else [],
            contact_info=contact,
            raw_data=dict(row)
        )

        return listing

    def _normalize_municipality(self, raw: str) -> Optional[str]:
        if not raw:
            return None
        aliases = {
            'mty': 'Monterrey', 'mty.': 'Monterrey', 'monterrey, n.l.': 'Monterrey',
            'san pedro': 'San Pedro Garza García', 'spgg': 'San Pedro Garza García',
            'sta. catarina': 'Santa Catarina', 'sta catarina': 'Santa Catarina',
            'apodaca': 'Apodaca', 'gral. escobedo': 'General Escobedo',
            'guadalupe, n.l.': 'Guadalupe', 'garcia': 'García', 'juarez': 'Juárez'
        }
        return aliases.get(raw.lower().strip(), raw.title())

    def _normalize_colony(self, raw: str) -> Optional[str]:
        if not raw:
            return None
        text = re.sub(r',?\s*Nuevo León$', '', raw, flags=re.I)
        text = re.sub(r',?\s*N\.?L\.?$', '', text, flags=re.I)
        return text.strip().title()

    def _normalize_status(self, raw: str) -> str:
        text = (raw or '').lower()
        if any(w in text for w in ['vend', 'sold']):
            return 'sold'
        if any(w in text for w in ['inactiv', 'baja']):
            return 'inactive'
        return 'active'

    def _normalize_price_type(self, accion: str, titulo: str) -> str:
        joined = f"{accion or ''} {titulo or ''}".lower()
        return 'rent' if 'renta' in joined else 'sale'

    def _normalize_property_type(self, raw: str) -> Optional[str]:
        if not raw:
            return None
        types = {'casa': 'casa', 'departamento': 'departamento', 'terreno': 'terreno', 'local': 'local'}
        return types.get(raw.lower().strip(), raw.lower().strip())


class SQLiteAdapter(BaseAdapter):
    """Adaptador genérico para fuentes SQLite"""

    def __init__(self, db_path: str, source_code: str, source_name: str):
        super().__init__()
        self.db_path = db_path
        self.source_code = source_code
        self.source_name = source_name

    def can_execute(self) -> bool:
        return Path(self.db_path).exists()

    def scrape(self, checkpoint: ExecutionCheckpoint) -> List[ScrapedListing]:
        """Lee desde SQLite"""
        listings = []

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Detectar tabla
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='propiedades'")
            if not cursor.fetchone():
                LOGGER.warning(f"{self.source_name}: tabla 'propiedades' no encontrada")
                return []

            cursor.execute("SELECT * FROM propiedades ORDER BY id")
            rows = cursor.fetchall()

            LOGGER.info(f"{self.source_name}: {len(rows)} registros en SQLite")

            for row in rows:
                try:
                    row_dict = dict(row)
                    listing = self._normalize_row(row_dict)
                    listings.append(listing)
                    self.metrics['read'] += 1
                except Exception as e:
                    LOGGER.error(f"Error normalizando fila: {e}")
                    self.metrics['errors'] += 1

        except Exception as e:
            LOGGER.error(f"Error leyendo {self.source_name}: {e}")
            raise
        finally:
            conn.close()

        return listings

    def _normalize_row(self, row: Dict) -> ScrapedListing:
        """Método a sobrescribir por adaptadores específicos"""
        raise NotImplementedError


class RealtyWorldAdapter(SQLiteAdapter):
    """Adaptador para Realty World"""

    def __init__(self):
        super().__init__('realtyworld_propiedades.db', 'realtyworld', 'Realty World')

    def _normalize_row(self, row: Dict) -> ScrapedListing:
        def parse_float(val):
            try:
                return float(val) if val else None
            except:
                return None

        def parse_int(val):
            try:
                return int(float(val)) if val else None
            except:
                return None

        # Extraer imágenes
        images = []
        if row.get('imagenes'):
            images = [img.strip() for img in row['imagenes'].split(',') if img.strip()]

        # Amenidades
        amenities = []
        if row.get('amenidades'):
            amenities = [a.strip() for a in row['amenidades'].split(',') if a.strip()]

        listing = ScrapedListing(
            source_code=self.source_code,
            source_listing_id=row.get('property_id'),
            url=row.get('url', ''),
            url_hash=hashlib.sha256(row.get('url', '').encode()).hexdigest(),
            status='active',
            price_type='sale',
            price_amount=parse_float(row.get('precio')),
            currency='MXN',
            property_type='casa',
            area_construction_m2=parse_float(row.get('construccion_m2')),
            area_land_m2=parse_float(row.get('terreno_m2')),
            bedrooms=parse_int(row.get('recamaras')),
            bathrooms=parse_float(row.get('banos')),
            half_bathrooms=parse_float(row.get('medios_banos')),
            parking=parse_int(row.get('estacionamientos')),
            floors=parse_int(row.get('plantas')),
            age_years=self._infer_age(row.get('ano_construccion'), row.get('descripcion')),
            title=row.get('titulo'),
            description=row.get('descripcion'),
            colony=self._normalize_colony(row.get('colonia')),
            municipality=self._normalize_municipality(row.get('ciudad')),
            state=row.get('estado') or 'Nuevo León',
            images=images[:10],
            amenities=amenities,
            raw_data=row
        )

        return listing

    def _infer_age(self, year: Any, description: Any) -> Optional[int]:
        current_year = datetime.now().year
        if year:
            try:
                y = int(year)
                if 1900 < y <= current_year:
                    return current_year - y
            except:
                pass
        return None

    def _normalize_municipality(self, raw: str) -> Optional[str]:
        if not raw:
            return None
        aliases = {
            'monterrey': 'Monterrey', 'san pedro': 'San Pedro Garza García',
            'garcia': 'García', 'guadalupe': 'Guadalupe', 'apodaca': 'Apodaca'
        }
        return aliases.get(raw.lower().strip(), raw.title() if raw else None)

    def _normalize_colony(self, raw: str) -> Optional[str]:
        return raw.title() if raw else None


class GPViviendaAdapter(SQLiteAdapter):
    """Adaptador para GP Vivienda"""

    def __init__(self):
        super().__init__('gpvivienda_nuevoleon.db', 'gpvivienda', 'GP Vivienda')

    def _normalize_row(self, row: Dict) -> ScrapedListing:
        def parse_float(val):
            try:
                return float(val) if val else None
            except:
                return None

        def parse_int(val):
            try:
                return int(float(val)) if val else None
            except:
                return None

        # GP Vivienda no tiene lat/lng, imágenes múltiples, ni contacto detallado
        images = [row.get('imagen_url')] if row.get('imagen_url') else []

        # Amenidades desde campo específico
        amenities = []
        if row.get('amenidades'):
            amenities = [a.strip() for a in row['amenidades'].split(',') if a.strip()]

        listing = ScrapedListing(
            source_code=self.source_code,
            source_listing_id=None,
            url=row.get('url', ''),
            url_hash=hashlib.sha256(row.get('url', '').encode()).hexdigest(),
            status='active',
            price_type='sale',
            price_amount=parse_float(row.get('precio')),
            currency='MXN',
            property_type='casa',
            area_construction_m2=parse_float(row.get('m2_construidos')),
            area_land_m2=parse_float(row.get('m2_terreno')),
            bedrooms=parse_int(row.get('recamaras')),
            bathrooms=self._parse_bathrooms(row.get('banos')),
            title=row.get('titulo') or row.get('modelo'),
            description=row.get('descripcion'),
            colony=self._normalize_colony(row.get('fraccionamiento')),
            municipality=self._normalize_municipality(row.get('ciudad')),
            state='Nuevo León',
            images=images,
            amenities=amenities,
            raw_data=row
        )

        return listing

    def _parse_bathrooms(self, raw: Any) -> Optional[float]:
        if not raw:
            return None
        text = str(raw).replace('½', '.5')
        match = re.search(r'\d+(?:\.\d+)?', text)
        return float(match.group(0)) if match else None

    def _normalize_municipality(self, raw: str) -> Optional[str]:
        if not raw:
            return None
        return raw.title()

    def _normalize_colony(self, raw: str) -> Optional[str]:
        if not raw:
            return None
        # GP Vivienda usa fraccionamiento como colonia
        return raw.title()


# ============================================================================
# MOTOR DE UNIFICACIÓN A MYSQL
# ============================================================================

class UnificationEngine:
    """Motor que unifica listings al esquema canónico MySQL"""

    def __init__(self):
        self.connection = None
        self._init_mysql()

    def _init_mysql(self):
        """Inicializa conexión MySQL y esquema si no existe"""
        import pymysql

        # Conectar sin base de datos primero
        temp_config = CONFIG['mysql'].copy()
        db_name = temp_config.pop('database')

        conn = pymysql.connect(**temp_config)
        with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE DATABASE IF NOT EXISTS {db_name}
                CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """)
        conn.commit()
        conn.close()

        # Conectar a la base de datos
        self.connection = pymysql.connect(
            **CONFIG['mysql'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False
        )

        self._ensure_schema()

    def _ensure_schema(self):
        """Crea tablas si no existen"""
        schema_sql = """
        -- Fuentes
        CREATE TABLE IF NOT EXISTS sources (
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            source_code VARCHAR(50) UNIQUE NOT NULL,
            source_name VARCHAR(120) NOT NULL,
            base_url VARCHAR(500),
            is_active TINYINT(1) DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        -- Listings canónicos
        CREATE TABLE IF NOT EXISTS listings (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            source_id INT UNSIGNED NOT NULL,
            source_listing_id VARCHAR(200),
            parse_version VARCHAR(40) DEFAULT 'auto_v1',

            url VARCHAR(1000),
            url_normalized VARCHAR(1000),
            url_hash CHAR(64),
            fingerprint_hash CHAR(64),
            dedupe_hash CHAR(64) UNIQUE NOT NULL,

            status ENUM('active','inactive','sold','unknown') DEFAULT 'active',
            price_type ENUM('sale','rent','unknown') DEFAULT 'sale',
            price_amount DECIMAL(16,2),
            currency CHAR(3) DEFAULT 'MXN',
            maintenance_fee DECIMAL(12,2),

            property_type VARCHAR(80),
            area_construction_m2 DECIMAL(10,2),
            area_land_m2 DECIMAL(10,2),
            bedrooms INT,
            bathrooms DECIMAL(4,1),
            half_bathrooms DECIMAL(4,1),
            parking INT,
            floors INT,
            age_years INT,

            title VARCHAR(500),
            description MEDIUMTEXT,

            street VARCHAR(255),
            colony VARCHAR(180),
            municipality VARCHAR(180),
            state VARCHAR(120) DEFAULT 'Nuevo León',
            country VARCHAR(120) DEFAULT 'México',
            postal_code VARCHAR(20),

            lat DECIMAL(10,7),
            lng DECIMAL(10,7),
            geo_precision ENUM('exact','approx','colony','unknown') DEFAULT 'unknown',

            images_json JSON,
            contact_json JSON,
            amenities_json JSON,
            details_json JSON,
            raw_json JSON,

            source_first_seen_at DATETIME,
            source_last_seen_at DATETIME,
            seen_first_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            seen_last_at DATETIME DEFAULT CURRENT_TIMESTAMP,

            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

            INDEX ix_valuation (status, price_type, property_type, municipality, colony),
            INDEX ix_geo (municipality, colony),
            INDEX ix_price (price_amount),
            INDEX ix_status_seen (status, seen_last_at),

            FOREIGN KEY (source_id) REFERENCES sources(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        -- Historial de precios
        CREATE TABLE IF NOT EXISTS listing_price_history (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            listing_id BIGINT UNSIGNED NOT NULL,
            status VARCHAR(20),
            price_amount DECIMAL(16,2),
            currency CHAR(3),
            captured_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (listing_id) REFERENCES listings(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        -- Historial de cambios de status
        CREATE TABLE IF NOT EXISTS listing_status_history (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            listing_id BIGINT UNSIGNED NOT NULL,
            old_status VARCHAR(20),
            new_status VARCHAR(20),
            changed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (listing_id) REFERENCES listings(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        -- NUEVO: Historial de cambios en TODOS los campos
        CREATE TABLE IF NOT EXISTS listing_field_history (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            listing_id BIGINT UNSIGNED NOT NULL,
            field_name VARCHAR(50) NOT NULL,
            old_value TEXT,
            new_value TEXT,
            change_category ENUM('content','price','status','location','metadata') NOT NULL,
            changed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX ix_field_lookup (listing_id, field_name, changed_at),
            FOREIGN KEY (listing_id) REFERENCES listings(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        -- Log de ejecuciones
        CREATE TABLE IF NOT EXISTS execution_log (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            execution_id VARCHAR(64) UNIQUE NOT NULL,
            started_at DATETIME,
            completed_at DATETIME,
            status VARCHAR(20),
            sources_processed INT,
            total_listings INT,
            new_listings INT,
            updated_listings INT,
            failed_sources TEXT,
            metrics_json JSON,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        with self.connection.cursor() as cursor:
            for statement in schema_sql.split(';'):
                stmt = statement.strip()
                if stmt:
                    cursor.execute(stmt)
        self.connection.commit()
        LOGGER.info("Esquema MySQL verificado/creado")

    def get_or_create_source(self, source_code: str, source_name: str) -> int:
        """Obtiene o crea ID de fuente"""
        with self.connection.cursor() as cursor:
            # Intentar insertar
            cursor.execute("""
                INSERT INTO sources (source_code, source_name)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                    id=LAST_INSERT_ID(id),
                    source_name=VALUES(source_name),
                    updated_at=CURRENT_TIMESTAMP
            """, (source_code, source_name))
            self.connection.commit()
            return cursor.lastrowid

    def upsert_listing(self, source_id: int, listing: ScrapedListing) -> tuple[bool, List[FieldChange]]:
        """
        Inserta o actualiza listing.
        Retorna: (was_inserted, list_of_changes)
        """
        import json

        dedupe_hash = listing.compute_dedupe_hash()
        fingerprint = listing.compute_fingerprint()

        with self.connection.cursor() as cursor:
            # Buscar existente
            cursor.execute(
                "SELECT * FROM listings WHERE dedupe_hash = %s",
                (dedupe_hash,)
            )
            existing = cursor.fetchone()

            changes = []

            if existing:
                # Detectar cambios en todos los campos
                changes = self._detect_changes(existing, listing)

                # Actualizar
                cursor.execute("""
                    UPDATE listings SET
                        source_id = %s,
                        source_listing_id = %s,
                        url = %s,
                        status = %s,
                        price_type = %s,
                        price_amount = %s,
                        currency = %s,
                        property_type = %s,
                        area_construction_m2 = %s,
                        area_land_m2 = %s,
                        bedrooms = %s,
                        bathrooms = %s,
                        half_bathrooms = %s,
                        parking = %s,
                        floors = %s,
                        age_years = %s,
                        title = %s,
                        description = %s,
                        street = %s,
                        colony = %s,
                        municipality = %s,
                        state = %s,
                        lat = %s,
                        lng = %s,
                        images_json = %s,
                        contact_json = %s,
                        amenities_json = %s,
                        raw_json = %s,
                        source_last_seen_at = %s,
                        seen_last_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (
                    source_id, listing.source_listing_id, listing.url,
                    listing.status, listing.price_type, listing.price_amount, listing.currency,
                    listing.property_type, listing.area_construction_m2, listing.area_land_m2,
                    listing.bedrooms, listing.bathrooms, listing.half_bathrooms,
                    listing.parking, listing.floors, listing.age_years,
                    listing.title, listing.description, listing.street,
                    listing.colony, listing.municipality, listing.state,
                    listing.lat, listing.lng,
                    json.dumps(listing.images, ensure_ascii=False),
                    json.dumps(listing.contact_info, ensure_ascii=False),
                    json.dumps(listing.amenities, ensure_ascii=False),
                    json.dumps(listing.raw_data, ensure_ascii=False, default=str),
                    listing.scraped_at,
                    existing['id']
                ))

                listing_id = existing['id']
                was_inserted = False

                # Guardar historial de cambios
                self._save_field_history(cursor, listing_id, changes)

                # Si cambió precio, guardar en price_history
                if any(c.field_name == 'price_amount' for c in changes):
                    cursor.execute("""
                        INSERT INTO listing_price_history 
                        (listing_id, status, price_amount, currency, captured_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (listing_id, listing.status, listing.price_amount, 
                          listing.currency, datetime.now()))

                # Si cambió status, guardar en status_history
                old_status = existing['status']
                if old_status != listing.status:
                    cursor.execute("""
                        INSERT INTO listing_status_history
                        (listing_id, old_status, new_status, changed_at)
                        VALUES (%s, %s, %s, %s)
                    """, (listing_id, old_status, listing.status, datetime.now()))

            else:
                # Insertar nuevo
                cursor.execute("""
                    INSERT INTO listings (
                        source_id, source_listing_id, url, url_hash, fingerprint_hash, dedupe_hash,
                        status, price_type, price_amount, currency,
                        property_type, area_construction_m2, area_land_m2,
                        bedrooms, bathrooms, half_bathrooms, parking, floors, age_years,
                        title, description, street, colony, municipality, state,
                        lat, lng, geo_precision,
                        images_json, contact_json, amenities_json, raw_json,
                        source_first_seen_at, source_last_seen_at, seen_first_at, seen_last_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                """, (
                    source_id, listing.source_listing_id, listing.url, 
                    listing.url_hash, fingerprint, dedupe_hash,
                    listing.status, listing.price_type, listing.price_amount, listing.currency,
                    listing.property_type, listing.area_construction_m2, listing.area_land_m2,
                    listing.bedrooms, listing.bathrooms, listing.half_bathrooms,
                    listing.parking, listing.floors, listing.age_years,
                    listing.title, listing.description, listing.street,
                    listing.colony, listing.municipality, listing.state,
                    listing.lat, listing.lng,
                    'exact' if listing.lat and listing.lng else 'unknown',
                    json.dumps(listing.images, ensure_ascii=False),
                    json.dumps(listing.contact_info, ensure_ascii=False),
                    json.dumps(listing.amenities, ensure_ascii=False),
                    json.dumps(listing.raw_data, ensure_ascii=False, default=str),
                    listing.scraped_at, listing.scraped_at
                ))

                listing_id = cursor.lastrowid
                was_inserted = True

                # Guardar precio inicial en historial
                if listing.price_amount:
                    cursor.execute("""
                        INSERT INTO listing_price_history 
                        (listing_id, status, price_amount, currency, captured_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (listing_id, listing.status, listing.price_amount,
                          listing.currency, datetime.now()))

            self.connection.commit()
            return was_inserted, changes

    def _detect_changes(self, existing: Dict, new: ScrapedListing) -> List[FieldChange]:
        """Detecta todos los cambios entre versión existente y nueva"""
        changes = []

        field_mapping = {
            'price_amount': ('price', new.price_amount, existing.get('price_amount')),
            'status': ('status', new.status, existing.get('status')),
            'title': ('content', new.title, existing.get('title')),
            'description': ('content', new.description, existing.get('description')),
            'bedrooms': ('content', new.bedrooms, existing.get('bedrooms')),
            'bathrooms': ('content', new.bathrooms, existing.get('bathrooms')),
            'area_construction_m2': ('content', new.area_construction_m2, existing.get('area_construction_m2')),
            'colony': ('location', new.colony, existing.get('colony')),
            'municipality': ('location', new.municipality, existing.get('municipality')),
            'lat': ('location', new.lat, existing.get('lat')),
            'lng': ('location', new.lng, existing.get('lng')),
            'images_json': ('metadata', json.dumps(new.images), existing.get('images_json')),
        }

        for field_name, (category, new_val, old_val) in field_mapping.items():
            # Normalizar valores para comparación
            old_normalized = self._normalize_for_compare(old_val)
            new_normalized = self._normalize_for_compare(new_val)

            if old_normalized != new_normalized:
                changes.append(FieldChange(
                    field_name=field_name,
                    old_value=old_val,
                    new_value=new_val,
                    change_type=category
                ))

        return changes

    def _normalize_for_compare(self, val):
        """Normaliza valor para comparación"""
        if val is None:
            return None
        if isinstance(val, float):
            return round(val, 2)
        if isinstance(val, str):
            return val.strip() or None
        return val

    def _save_field_history(self, cursor, listing_id: int, changes: List[FieldChange]):
        """Guarda historial de cambios de campos"""
        import json

        for change in changes:
            cursor.execute("""
                INSERT INTO listing_field_history
                (listing_id, field_name, old_value, new_value, change_category, changed_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                listing_id, change.field_name,
                json.dumps(change.old_value, ensure_ascii=False, default=str) if change.old_value is not None else None,
                json.dumps(change.new_value, ensure_ascii=False, default=str) if change.new_value is not None else None,
                change.change_type,
                change.changed_at
            ))

    def deactivate_stale_listings(self, days: int = 30) -> int:
        """Marca como inactivos los listings no vistos recientemente"""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                UPDATE listings 
                SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
                WHERE status = 'active' 
                AND seen_last_at < DATE_SUB(NOW(), INTERVAL %s DAY)
            """, (days,))
            count = cursor.rowcount
            self.connection.commit()
            return count

    def log_execution(self, execution_id: str, status: str, metrics: Dict):
        """Registra ejecución en log"""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO execution_log (
                    execution_id, started_at, completed_at, status,
                    sources_processed, total_listings, new_listings, 
                    updated_listings, failed_sources, metrics_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    completed_at = VALUES(completed_at),
                    status = VALUES(status),
                    sources_processed = VALUES(sources_processed),
                    total_listings = VALUES(total_listings),
                    new_listings = VALUES(new_listings),
                    updated_listings = VALUES(updated_listings),
                    failed_sources = VALUES(failed_sources),
                    metrics_json = VALUES(metrics_json)
            """, (
                execution_id, metrics.get('started_at'), metrics.get('completed_at'),
                status, metrics.get('sources_processed'), metrics.get('total_listings'),
                metrics.get('new_listings'), metrics.get('updated_listings'),
                json.dumps(metrics.get('failed_sources', {})),
                json.dumps(metrics, default=str)
            ))
            self.connection.commit()

    def close(self):
        if self.connection:
            self.connection.close()


# ============================================================================
# ORQUESTADOR PRINCIPAL
# ============================================================================

class ValoraAutonomous:
    """Orquestador autónomo principal"""

    def __init__(self):
        self.engine = UnificationEngine()
        self.adapters: List[BaseAdapter] = []
        self.checkpoint: Optional[ExecutionCheckpoint] = None
        self.execution_id = datetime.now().strftime('%Y%m%d_%H%M%S')

    def discover_adapters(self):
        """Auto-descubre adaptadores disponibles"""
        LOGGER.info("Descubriendo adaptadores...")

        adapters_to_check = [
            Casas365Adapter(),
            RealtyWorldAdapter(),
            GPViviendaAdapter(),
        ]

        for adapter in adapters_to_check:
            if adapter.can_execute():
                LOGGER.info(f"✓ {adapter.source_name} disponible")
                self.adapters.append(adapter)
            else:
                LOGGER.warning(f"✗ {adapter.source_name} no disponible")

        if not self.adapters:
            raise RuntimeError("No hay adaptadores disponibles. Verifica las bases de datos.")

        LOGGER.info(f"Total adaptadores activos: {len(self.adapters)}")

    def load_checkpoint(self):
        """Carga checkpoint previo si existe"""
        self.checkpoint = ExecutionCheckpoint.load()
        if self.checkpoint:
            LOGGER.info(f"Checkpoint encontrado: {self.checkpoint.execution_id}")
            LOGGER.info(f"Completados: {self.checkpoint.completed_sources}")
            LOGGER.info(f"Fallidos: {list(self.checkpoint.failed_sources.keys())}")
        else:
            self.checkpoint = ExecutionCheckpoint(
                execution_id=self.execution_id,
                started_at=datetime.now().isoformat()
            )
            LOGGER.info(f"Nueva ejecución: {self.execution_id}")

    def run(self, resume: bool = True, stale_days: int = 30):
        """Ejecuta el pipeline completo"""
        LOGGER.info("=" * 70)
        LOGGER.info("INICIANDO VALORA AUTONOMOUS")
        LOGGER.info("=" * 70)

        metrics = {
            'execution_id': self.execution_id,
            'started_at': datetime.now().isoformat(),
            'sources_processed': 0,
            'total_listings': 0,
            'new_listings': 0,
            'updated_listings': 0,
            'failed_sources': {},
            'by_source': {}
        }

        try:
            self.discover_adapters()
            self.load_checkpoint()

            # Procesar cada adaptador
            for adapter in self.adapters:
                if resume and self.checkpoint.is_source_completed(adapter.source_code):
                    LOGGER.info(f"Saltando {adapter.source_name} (ya completado)")
                    continue

                self.checkpoint.current_source = adapter.source_code
                self.checkpoint.save()

                try:
                    source_metrics = self._process_adapter(adapter)

                    metrics['sources_processed'] += 1
                    metrics['total_listings'] += source_metrics['read']
                    metrics['new_listings'] += source_metrics['inserted']
                    metrics['updated_listings'] += source_metrics['updated']
                    metrics['by_source'][adapter.source_code] = source_metrics

                    self.checkpoint.completed_sources.append(adapter.source_code)
                    self.checkpoint.save()

                except Exception as e:
                    LOGGER.error(f"Error procesando {adapter.source_name}: {e}")
                    metrics['failed_sources'][adapter.source_code] = str(e)
                    self.checkpoint.failed_sources[adapter.source_code] = str(e)
                    self.checkpoint.save()

                    if not resume:  # Modo strict: fallar inmediatamente
                        raise

            # Desactivar listings antiguos
            if stale_days > 0:
                LOGGER.info(f"Desactivando listings no vistos en {stale_days} días...")
                deactivated = self.engine.deactivate_stale_listings(stale_days)
                LOGGER.info(f"Desactivados: {deactivated}")
                metrics['deactivated'] = deactivated

            # Finalizar
            metrics['completed_at'] = datetime.now().isoformat()
            self.engine.log_execution(self.execution_id, 'success', metrics)

            # Limpiar checkpoint si todo exitoso
            if not metrics['failed_sources']:
                Path(CONFIG['checkpoint_file']).unlink(missing_ok=True)

            self._print_summary(metrics)
            return metrics

        except Exception as e:
            LOGGER.error(f"Error fatal: {e}")
            metrics['completed_at'] = datetime.now().isoformat()
            self.engine.log_execution(self.execution_id, 'failed', metrics)
            raise
        finally:
            self.engine.close()

    def _process_adapter(self, adapter: BaseAdapter) -> Dict:
        """Procesa un adaptador individual con reintentos"""
        LOGGER.info(f"Procesando: {adapter.source_name}")

        for attempt in range(CONFIG['max_retries']):
            try:
                # Scrapear
                listings = adapter.scrape(self.checkpoint)

                # Unificar
                source_id = self.engine.get_or_create_source(
                    adapter.source_code,
                    adapter.source_name
                )

                inserted = 0
                updated = 0

                for i, listing in enumerate(listings):
                    try:
                        was_inserted, changes = self.engine.upsert_listing(source_id, listing)
                        if was_inserted:
                            inserted += 1
                        else:
                            updated += 1

                        if (i + 1) % 50 == 0:
                            LOGGER.info(f"  Procesados: {i+1}/{len(listings)}")

                    except Exception as e:
                        LOGGER.error(f"Error unificando listing {i}: {e}")
                        adapter.metrics['errors'] += 1

                adapter.metrics['inserted'] = inserted
                adapter.metrics['updated'] = updated

                LOGGER.info(
                    f"  Insertados: {inserted}, Actualizados: {updated}, Errores: {adapter.metrics['errors']}"
                )

                return adapter.metrics

            except Exception as e:
                LOGGER.warning(f"Intento {attempt + 1} fallido: {e}")
                if attempt < CONFIG['max_retries'] - 1:
                    delay = CONFIG['retry_delay_base'] ** (attempt + 1)
                    LOGGER.info(f"Reintentando en {delay} segundos...")
                    time.sleep(delay)
                else:
                    raise

    def _print_summary(self, metrics: Dict):
        """Imprime resumen de ejecución"""
        print("\n" + "=" * 70)
        print("RESUMEN DE EJECUCIÓN")
        print("=" * 70)
        print(f"ID: {metrics['execution_id']}")
        print(f"Estado: {'ÉXITO' if not metrics['failed_sources'] else 'PARCIAL'}")
        print(f"\nFuentes procesadas: {metrics['sources_processed']}")
        print(f"Total listings: {metrics['total_listings']}")
        print(f"Nuevos: {metrics['new_listings']}")
        print(f"Actualizados: {metrics['updated_listings']}")

        if metrics.get('deactivated'):
            print(f"Desactivados: {metrics['deactivated']}")

        print("\nPor fuente:")
        for source, m in metrics['by_source'].items():
            print(f"  • {source}: {m['read']} leídos, {m['inserted']} nuevos, {m['updated']} actualizados")

        if metrics['failed_sources']:
            print("\nFuentes fallidas:")
            for source, error in metrics['failed_sources'].items():
                print(f"  ✗ {source}: {error}")

        print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description='ValoraNL - Sistema Autónomo')
    parser.add_argument('--no-resume', action='store_true', 
                       help='No reanudar desde checkpoint, empezar de cero')
    parser.add_argument('--stale-days', type=int, default=30,
                       help='Días para considerar listing como stale (default: 30)')
    parser.add_argument('--reset', action='store_true',
                       help='Borra checkpoint y empieza fresco')

    args = parser.parse_args()

    if args.reset:
        Path(CONFIG['checkpoint_file']).unlink(missing_ok=True)
        LOGGER.info("Checkpoint reseteado")

    try:
        valora = ValoraAutonomous()
        metrics = valora.run(
            resume=not args.no_resume,
            stale_days=args.stale_days
        )
        return 0 if not metrics.get('failed_sources') else 1
    except Exception as e:
        LOGGER.error(f"Error fatal: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
