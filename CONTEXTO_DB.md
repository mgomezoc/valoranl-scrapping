# CONTEXTO_DB.md — ValoraNL (Guía para IAs)

## 1) Propósito del proyecto
ValoraNL consolida publicaciones inmobiliarias de múltiples fuentes en una base canónica MySQL para:

- búsqueda y análisis de mercado,
- deduplicación entre portales,
- historial de precio/estatus,
- preparación para cálculo de valuaciones por comparables.

La BD canónica está definida en `db/valoranl_schema.sql` y se llena desde SQLite usando `scrapping/unify_to_mysql.py`.

---

## 2) Flujo de datos (alto nivel)

1. **Scrapers por fuente** generan SQLite local:
   - `casas365_propiedades.db`
   - `gpvivienda_nuevoleon.db`
   - `realtyworld_propiedades.db`
2. **Unificador** (`scrapping/unify_to_mysql.py`) transforma cada registro al modelo canónico.
3. Inserta/actualiza en MySQL con deduplicación por hash.
4. Registra historial de precio y estatus cuando detecta cambios.

> Estrategia recomendada: **ingesta incremental diaria** (no truncar tablas).

---

## 3) Esquema canónico MySQL (resumen)

### 3.1 Tabla `sources`
Catálogo de orígenes de scraping.

Campos clave:
- `source_code` (único, ej. `casas365`, `gpvivienda`, `realtyworld`)
- `source_name`
- `base_url`
- `is_active`

Uso:
- Identificar de qué portal proviene cada listing.
- Controlar fuentes activas/inactivas.

### 3.2 Tabla `listings`
Entidad principal normalizada (una publicación canónica).

#### Identidad y deduplicación
- `source_id`
- `source_listing_id`
- `url`, `url_normalized`
- `url_hash` (SHA-256 de URL normalizada)
- `fingerprint_hash` (fallback cuando no hay URL confiable)
- `dedupe_hash` (**UNIQUE**): hash efectivo para upsert

#### Estado comercial y precio
- `status` = `active|inactive|sold|unknown`
- `price_type` = `sale|rent|unknown`
- `price_amount`
- `currency`
- `maintenance_fee`

#### Datos físicos del inmueble
- `property_type`
- `area_construction_m2`
- `area_land_m2`
- `bedrooms`
- `bathrooms`
- `half_bathrooms`
- `parking`
- `floors`
- `age_years`

#### Ubicación
- `street`
- `colony`
- `municipality`
- `state`
- `country`
- `postal_code`
- `lat`, `lng`
- `geo_precision` = `exact|approx|colony|unknown`

#### Campos de contenido y preservación de fuente
- `title`
- `description`
- `images_json`
- `contact_json`
- `amenities_json`
- `details_json` (metadata parseada por fuente)
- `raw_json` (payload original para no perder información)

#### Trazabilidad temporal
- `source_first_seen_at`
- `source_last_seen_at`
- `seen_first_at`
- `seen_last_at`
- `created_at`
- `updated_at`

### 3.3 Tabla `listing_price_history`
Historial de cambios de precio/estatus por listing.

Campos:
- `listing_id`
- `status`
- `price_amount`
- `currency`
- `captured_at`

### 3.4 Tabla `listing_status_history`
Historial de transiciones de estado.

Campos:
- `listing_id`
- `old_status`
- `new_status`
- `changed_at`

---

## 4) Reglas de deduplicación y upsert

El unificador calcula:

1. `url_hash` si hay URL normalizada.
2. `fingerprint_hash` con combinación aproximada: municipio + colonia + m2 + precio + recámaras.
3. `dedupe_hash = url_hash` o `fingerprint_hash` (fallback).

El `INSERT ... ON DUPLICATE KEY UPDATE` sobre `dedupe_hash` permite:
- insertar nuevos listings,
- actualizar existentes sin duplicar,
- refrescar `seen_last_at`.

---

## 5) Mapeo por fuente

## 5.1 Casas365 (`Casas365Mapper`)
Convierte campos como:
- `titulo -> title`
- `precio -> price_amount`
- `construccion_m2 -> area_construction_m2`
- `terreno_m2 -> area_land_m2`
- `recamaras -> bedrooms`
- `banos -> bathrooms`
- `ciudad -> municipality`
- `colonia -> colony`

Notas:
- algunas cadenas largas se truncan para respetar longitudes del esquema,
- el registro original se conserva en `raw_json`.

## 5.2 GPVivienda (`GPViviendaMapper`)
Convierte campos como:
- `modelo/titulo -> title`
- `precio -> price_amount`
- `m2_construidos -> area_construction_m2`
- `m2_terreno -> area_land_m2`
- `fraccionamiento -> colony`
- `ciudad -> municipality`

## 5.3 RealtyWorld (`RealtyWorldMapper`)
Convierte campos como:
- `property_id -> source_listing_id`
- `titulo -> title`
- `precio -> price_amount`
- `construccion_m2 -> area_construction_m2`
- `terreno_m2 -> area_land_m2`
- `medios_banos -> half_bathrooms`

---

## 6) Ejecución operativa

## 6.1 Variables de entorno MySQL
- `MYSQL_HOST`
- `MYSQL_PORT`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE`

## 6.2 Inicializar esquema
```bash
python scrapping/unify_to_mysql.py --init-schema db/valoranl_schema.sql
```

## 6.3 Migrar datos
```bash
python scrapping/unify_to_mysql.py --migrate
```

## 6.4 Flujo diario recomendado
1. Ejecutar scrapers (idealmente versión completa por fuente).
2. Ejecutar `--migrate`.
3. Registrar logs y resumen del run.
4. (Recomendado) aplicar política de desactivación por no visto (ej. >10 días).

---

## 7) Decisiones de diseño para valuación futura

- **No borrar históricos**: la señal temporal de precio y permanencia en mercado es crítica.
- Usar `listing_price_history` para curvas de precio por zona/tipo.
- Usar `seen_first_at`/`seen_last_at` para medir “tiempo en mercado”.
- Mantener `raw_json` para enriquecer features futuras sin re-scrapear todo.

---

## 8) Glosario rápido para IA

- **Listing**: publicación individual en un portal.
- **Source**: portal/origen de datos.
- **Dedupe hash**: identificador para evitar duplicados entre corridas/fuentes.
- **Price history**: eventos de precio en el tiempo.
- **Status history**: cambios de estado (activo/inactivo/vendido).
- **Seen timestamps**: primera y última vez observado por el pipeline.

---

## 9) Limitaciones actuales conocidas

- El volumen final depende del scraper de origen (p.ej. versiones simples pueden traer pocos registros).
- Si una fuente cambia HTML/selectores, puede bajar calidad de extracción.
- Datos de ubicación pueden venir incompletos o ruidosos según portal.

---

## 10) Checklist para cualquier IA que trabaje este proyecto

1. No proponer truncar/borrar tablas diariamente.
2. Mantener enfoque incremental con historial.
3. Preservar campos no normalizados en JSON (`details_json`, `raw_json`).
4. No romper la deduplicación por `dedupe_hash`.
5. Si se agregan columnas/tablas, documentar aquí y en SQL canónico.
6. Siempre validar impacto en valuación y trazabilidad temporal.

