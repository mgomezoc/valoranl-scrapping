# ValoraNL — Contexto Maestro del Proyecto (para IA)
**Proyecto:** ValoraNL  
**Enfoque geográfico:** Nuevo León, México (NL)  
**Objetivo:** Plataforma de **inteligencia de datos inmobiliarios** para **concentrar listings**, **normalizar información**, **geolocalizar** propiedades y **estimar valor de mercado** (valuación) con un rango y una medida de confianza.

> Nota de honestidad: En conversaciones previas no se definió una “fórmula final” única. Abajo dejo un **método MVP** (heurístico + comparables) y una evolución **V2** (modelo estadístico). Todo lo que esté marcado como *[asunción]* se puede ajustar cuando tengamos más datos.

---

## 1) Visión, propuesta de valor y usuarios
### 1.1 Propuesta de valor
- Un “**mapa de valor**” para NL: precio estimado por zona/colonia, tendencias, outliers y comparables.
- **Valuación rápida** basada en datos reales de mercado (listings actuales + histórico).
- **Transparencia**: mostrar por qué sale un valor (comparables, ajustes y supuestos).

### 1.2 Usuarios / perfiles
- **Compradores**: “¿Estoy pagando caro o barato?”
- **Vendedores**: “¿En cuánto conviene publicar?”
- **Asesores inmobiliarios**: análisis de mercado y comparables imprimibles.
- **Inversionistas**: detectar oportunidades (subvaluadas vs mercado).
- **Administradores internos**: control de fuentes, calidad de data, deduplicación y auditoría.

---

## 2) Alcance del MVP (recomendación)
### 2.1 MVP (lo mínimo útil)
1. **Ingesta de listings** desde fuentes públicas/permitidas (portales, páginas públicas, feeds, etc.).
2. **Normalización**: estructura única (precio, m², recámaras, baños, estacionamientos, colonia, municipio, coordenadas si existen).
3. **Geocodificación** (si hay dirección aproximada) y/o **asignación por colonia**.
4. **Motor de comparables**:
   - selección de comparables cercanos (radio, colonia y similitud),
   - cálculo de $/m² robusto,
   - estimación de valor y rango.
5. **Panel admin** para:
   - ver listings,
   - marcar duplicados,
   - corregir datos,
   - activar/desactivar fuentes.
6. **Vista pública**:
   - buscador + filtros,
   - mapa básico,
   - ficha de valuación.

### 2.2 No-MVP (posponer)
- Scraping de fuentes **cerradas** (login / grupos / marketplace) donde el acceso y/o términos lo prohíban.
- Modelos avanzados (XGBoost, redes, etc.) sin dataset suficiente y limpio.
- Predicción por micro-segmentos hiper específicos si aún no hay densidad de datos.

---

## 3) Stack preferido (alineado a tu stack)
> Basado en tu stack recurrente en otros proyectos y tus lineamientos.

### 3.1 Backend
- **CodeIgniter 4** (sin migrations; **SQL crudo** para MySQL/phpMyAdmin).
- API REST interna: endpoints para listings, catálogos, valuación y admin.

### 3.2 Frontend (admin + público)
- **Bootstrap 5**
- **jQuery** como base
- **Select2** (filtros)
- **Flatpickr** (rangos de fecha)
- **Bootstrap-Table** (tablas admin)
- **FontAwesome** (iconos)
- Mapa: *[asunción]* **Leaflet** (ligero y suficiente), o Google Maps si ya tienes licencias.

### 3.3 Scraping / Data pipeline
- **Python 3**:
  - `requests` + `BeautifulSoup` para HTML server-rendered,
  - `playwright` solo si es inevitable (sitios fuertemente JS).
- Manejo serio de **headers**, **retries**, **backoff**, **rate-limit**, **proxies** (si aplica y permitido), y **logging**.
- Jobs programados: cron / scheduler (Linux) o task scheduler (Windows) según hosting.

---

## 4) Fuentes de datos y consideraciones (muy importante)
### 4.1 Fuentes típicas
- Portales inmobiliarios con páginas públicas.
- Sitios de desarrollos (landing pages) y brokers con publicaciones públicas.
- Agregadores/feeds permitidos.
- Aportes manuales (leads) / carga CSV.

### 4.2 Redes sociales (Facebook Marketplace / grupos)
- **Riesgo alto** por términos, login, anti-bot y posible bloqueo.
- Alternativas recomendadas:
  1. **Carga manual asistida**: el usuario pega URL/post y se extrae lo que sea público.
  2. **Canal de “Leads”**: formulario para que usuarios envíen info y fotos.
  3. Integración con APIs oficiales donde aplique (páginas públicas, no grupos cerrados).
- Si se decide intentar algo aquí, primero definir **alcance legal** y **riesgo tolerado**.

---

## 5) Dominio de datos (modelo conceptual)
### 5.1 Entidades principales
- **Source**: origen (portal/sitio), reglas de crawling.
- **ListingRaw**: HTML/JSON crudo (opcional para auditoría).
- **Listing**: registro normalizado (lo que usa el negocio).
- **Property**: entidad “inmueble” deduplicada (unifica varios listings del mismo inmueble).
- **Location**: colonia, municipio, estado, lat/lng, bounding box, etc.
- **PriceHistory**: cambios de precio y estatus (venta/renta, activo/inactivo).
- **Valuation**: resultado de estimación con rango, confianza y comparables usados.
- **ComparableLink**: tabla puente (valuation ↔ listing comparable) con score y ajustes.

### 5.2 Campos recomendados en un Listing normalizado
- Identificadores:
  - `source_id`, `source_listing_id`, `url`, `url_hash`
- Estado:
  - `status` (active/inactive/sold/unknown), `seen_first_at`, `seen_last_at`
- Precio:
  - `price_amount`, `currency` (MXN), `price_type` (sale/rent), `maintenance_fee` (si existe)
- Superficies:
  - `area_construction_m2`, `area_land_m2`
- Características:
  - `bedrooms`, `bathrooms`, `parking`, `age_years` (si hay),
  - `property_type` (casa/depa/terreno/oficina…),
  - `amenities` (json)
- Ubicación:
  - `state`, `municipality`, `colony`, `postal_code`,
  - `street` (si existe), `lat`, `lng`, `geo_precision` (exact/approx/colony)
- Texto:
  - `title`, `description`, `images` (json), `contact` (json)
- Calidad:
  - `data_quality_score`, `parse_version`

---

## 6) SQL base (MVP) — MySQL (sin migrations)
> Esto es una propuesta inicial *[asunción]*: puedes recortar o extender según tu diseño final.

```sql
CREATE TABLE sources (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(120) NOT NULL,
  base_url VARCHAR(500) NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  rate_limit_per_min INT NOT NULL DEFAULT 30,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE listings (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  source_id INT NOT NULL,
  source_listing_id VARCHAR(200) NULL,
  url VARCHAR(800) NOT NULL,
  url_hash CHAR(64) NOT NULL,
  status ENUM('active','inactive','unknown') NOT NULL DEFAULT 'active',
  price_type ENUM('sale','rent') NOT NULL DEFAULT 'sale',
  price_amount DECIMAL(14,2) NULL,
  currency CHAR(3) NOT NULL DEFAULT 'MXN',
  area_construction_m2 DECIMAL(10,2) NULL,
  area_land_m2 DECIMAL(10,2) NULL,
  bedrooms INT NULL,
  bathrooms DECIMAL(4,1) NULL,
  parking INT NULL,
  age_years INT NULL,
  property_type VARCHAR(50) NULL,
  state VARCHAR(80) NULL,
  municipality VARCHAR(120) NULL,
  colony VARCHAR(160) NULL,
  postal_code VARCHAR(10) NULL,
  street VARCHAR(200) NULL,
  lat DECIMAL(10,7) NULL,
  lng DECIMAL(10,7) NULL,
  geo_precision ENUM('exact','approx','colony','unknown') NOT NULL DEFAULT 'unknown',
  title VARCHAR(300) NULL,
  description MEDIUMTEXT NULL,
  images_json JSON NULL,
  contact_json JSON NULL,
  data_quality_score DECIMAL(5,2) NOT NULL DEFAULT 0,
  parse_version VARCHAR(30) NOT NULL DEFAULT 'v1',
  seen_first_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  seen_last_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NULL ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY ux_listings_urlhash (url_hash),
  KEY ix_listings_source (source_id),
  KEY ix_listings_geo (municipality, colony),
  CONSTRAINT fk_listings_sources FOREIGN KEY (source_id) REFERENCES sources(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE listing_price_history (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  listing_id BIGINT NOT NULL,
  price_amount DECIMAL(14,2) NULL,
  status ENUM('active','inactive','unknown') NOT NULL,
  captured_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY ix_lph_listing (listing_id, captured_at),
  CONSTRAINT fk_lph_listings FOREIGN KEY (listing_id) REFERENCES listings(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE valuations (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  subject_listing_id BIGINT NULL,
  municipality VARCHAR(120) NULL,
  colony VARCHAR(160) NULL,
  lat DECIMAL(10,7) NULL,
  lng DECIMAL(10,7) NULL,
  property_type VARCHAR(50) NULL,
  area_construction_m2 DECIMAL(10,2) NULL,
  area_land_m2 DECIMAL(10,2) NULL,
  bedrooms INT NULL,
  bathrooms DECIMAL(4,1) NULL,
  parking INT NULL,
  estimated_value DECIMAL(14,2) NOT NULL,
  estimated_low DECIMAL(14,2) NOT NULL,
  estimated_high DECIMAL(14,2) NOT NULL,
  confidence_score DECIMAL(5,2) NOT NULL,
  method VARCHAR(50) NOT NULL DEFAULT 'comparables_v1',
  details_json JSON NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY ix_val_subject (subject_listing_id),
  KEY ix_val_geo (municipality, colony)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE valuation_comparables (
  valuation_id BIGINT NOT NULL,
  comparable_listing_id BIGINT NOT NULL,
  similarity_score DECIMAL(6,3) NOT NULL,
  distance_m INT NULL,
  ppu_m2 DECIMAL(14,2) NULL,
  adjustments_json JSON NULL,
  PRIMARY KEY (valuation_id, comparable_listing_id),
  CONSTRAINT fk_vc_val FOREIGN KEY (valuation_id) REFERENCES valuations(id),
  CONSTRAINT fk_vc_listing FOREIGN KEY (comparable_listing_id) REFERENCES listings(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

---

## 7) Metodología de valuación (cómo se calcula “cuánto vale una casa”)
### 7.1 Principio central
El valor estimado se obtiene de **comparables** (propiedades similares) y un **precio por m²** robusto (mediana o media truncada), con ajustes por diferencias relevantes.

### 7.2 MVP (Comparables + Ajustes simples)
#### Paso A — Selección de comparables
Filtrar listings con:
- mismo `property_type` (o compatible: casa vs townhouse, etc.),
- misma `municipality`,
- misma `colony` (preferente) o radio geográfico si hay coordenadas,
- rango de superficies (por ejemplo, 0.7× a 1.3× del sujeto),
- y datos mínimos presentes: precio y m² construcción (o terreno para terrenos).

> Regla de oro: si no hay suficientes comparables en colonia, ampliar a colonias vecinas o radio mayor, pero bajar confianza.

#### Paso B — Puntaje de similitud (ejemplo)
*Esto es un ejemplo operativo; puedes cambiar pesos.*

- Distancia (si hay lat/lng): 0–40%
- Diferencia de construcción m²: 0–25%
- Recámaras/baños/parking: 0–20%
- Antigüedad (si existe): 0–15%

`similarity_score = 1 - (w_dist*dist_norm + w_area*area_norm + w_rooms*rooms_norm + w_age*age_norm)`

#### Paso C — Precio por m² robusto
Para cada comparable:
- `ppu_m2 = comparable.price_amount / comparable.area_construction_m2`
- eliminar outliers (p. ej. por percentiles 10–90 o IQR)
- usar **mediana** ponderada por `similarity_score`

`ppu_base = weighted_median(ppu_m2, weight=similarity_score)`

#### Paso D — Ajustes
Ajustes simples por diferencias:
- `Δm2`: si sujeto es mucho más grande, aplicar descuento marginal (economías de escala).
- `bedrooms/bathrooms/parking`: ajuste fijo por unidad *[asunción]* (ej. +X MXN por recámara extra, o %).
- `amenities` (alberca, cochera techada, seguridad): ajuste por % (si hay señal).
- `estado/condición`: sólo si hay dato confiable (si no, omitir).

**Ejemplo de ajuste por tamaño (m² construcción):**
- si `subject_m2 > median_comp_m2`, entonces `ppu_adjusted = ppu_base * (1 - k*(subject_m2/median_comp_m2 - 1))`
- con `k` pequeño (p. ej. 0.05) para no exagerar.

#### Paso E — Valor estimado y rango
- `estimated_value = ppu_adjusted * subject_m2`
- Rango por dispersión:
  - `estimated_low = percentile_25(ppu_m2)*subject_m2`
  - `estimated_high = percentile_75(ppu_m2)*subject_m2`

#### Paso F — Confianza
Se calcula con:
- número de comparables útiles,
- cercanía promedio,
- coherencia (dispersión ppu_m2),
- completitud del sujeto.

Ejemplo:
- base por cantidad: `min(1, n/20)`
- penalización por dispersión: `1 - clamp(std_ppu/mean_ppu)`
- penalización por geoc precisión (colony/exact)

`confidence_score = 100 * clamp( base_n * base_geo * base_disp * base_fields )`

### 7.3 V2 (modelo estadístico)
Cuando tengas histórico y densidad:
- Modelo hedónico (regresión) o Gradient Boosting (XGBoost/LightGBM) con:
  - m² construcción, m² terreno,
  - recámaras, baños, parking, antigüedad,
  - variables de ubicación (colonia/lat/lng o embeddings geográficos),
  - cercanía a vialidades/servicios *[asunción]*.
- El modelo produce:
  - `pred_value` + intervalo (quantile regression o bootstrap),
  - feature importance para explicabilidad.

---

## 8) Normalización, deduplicación y calidad
### 8.1 Normalización
- Unificar moneda a MXN.
- Convertir textos a formato consistente (trim, espacios, acentos opcional).
- Parseo de m² desde textos (“120 m2”, “120m²”, etc.).
- Catálogos:
  - `property_type` normalizado (casa/depa/terreno/oficina).
  - `municipality` normalizado (Monterrey, San Pedro, Santa Catarina…).

### 8.2 Dedupe (Listings → Property)
Estrategias:
- Hash de URL (ya en DB).
- Fingerprint aproximado:
  - (municipio, colonia, m², recámaras, precio +/- tolerancia) + similitud de título/descr.
- Si hay coordenadas:
  - agrupar por radio (p. ej. 50–100 m) con tolerancias de m² y precio.

### 8.3 Data Quality Score (0–100)
Puntos por:
- precio presente,
- m² construcción presente,
- colonia/municipio presente,
- coordenadas,
- fotos,
- descripción.

Este score impacta:
- qué listings se muestran,
- si se permiten como comparables.

---

## 9) Arquitectura de alto nivel (recomendada)
### 9.1 Flujo
1. **Scraper** (Python) descarga listings → normaliza → upsert en MySQL.
2. **Backend** (CI4):
   - expone API admin y pública,
   - corre valuaciones on-demand o batch.
3. **Frontend**:
   - Admin: gestión de fuentes/listings/duplicados/errores.
   - Público: búsqueda, mapa, valuación.

### 9.2 Reglas para el scraper (estándar)
- Respeta `robots.txt` y TOS donde aplique.
- Rate-limit por dominio (p. ej. 1 req/2–5s).
- Reintentos con backoff exponencial.
- Headers realistas (User-Agent), compresión, keep-alive.
- Logging estructurado por:
  - source, url, status_code, ms, parse_ok, items_found.
- Guardar “parse_version” para trazabilidad.

---

## 10) Endpoints sugeridos (CI4) *[asunción]*
### Público
- `GET /api/listings` (filtros)
- `GET /api/listings/{id}`
- `POST /api/valuation` (payload: ubicación + features) → devuelve estimación + comparables
- `GET /api/market/heatmap` (agregado por colonia)

### Admin
- `GET /admin/sources`
- `POST /admin/sources/{id}/toggle`
- `GET /admin/listings` (con filtros avanzados)
- `POST /admin/listings/{id}/mark-duplicate`
- `POST /admin/listings/{id}/edit` (correcciones)
- `GET /admin/valuations/{id}`

---

## 11) KPIs y métricas
- Cobertura: #listings activos por municipio/colonia.
- Frescura: % listings vistos en últimos 7 días.
- Calidad: promedio data_quality_score.
- Valuaciones: #valuaciones/día y latencia promedio.
- Precisión (cuando tengas ground truth): error vs cierre o valuación real.

---

## 12) Roadmap sugerido
### Fase 1 (MVP)
- Ingesta + normalización + panel admin + valuación comparables.
### Fase 2
- Mejoras geográficas (mapa, clusters, heatmap por colonia).
- Ajustes por amenities / estado.
### Fase 3
- Modelo estadístico V2, intervalos de predicción más robustos.
- “Alertas de oportunidad” (subvaluadas vs mercado).
### Fase 4
- Producto comercial: planes, límites, exportables PDF/CSV, API pública.

---

## 13) Qué necesita saber una IA para ayudarte bien (reglas prácticas)
1. **No inventar selectores**: si pides scraping, comparte HTML o URL y confirmamos selectores reales.
2. Para CI4: entregar siempre **código completo** por archivo + **SQL crudo** (sin migrations).
3. Frontend: jQuery + Select2 + Flatpickr + Bootstrap-Table (nada de React/Vue).
4. Siempre incluir:
   - validación,
   - manejo de errores,
   - logging.
5. Aclara la fuente de datos y si es pública/permitida.

---

## 14) Información faltante (para cerrar “fórmula final”)
Si quieres que la IA deje el motor de valuación “cerrado” (no sólo MVP), define:
- ¿Qué pesa más: **colonia** vs **radio geográfico**?
- ¿Qué tipo(s) de inmuebles entran primero? (casa, depa, terreno)
- ¿Cuál será el “valor objetivo”? (precio de publicación vs precio de cierre estimado)
- ¿Cómo tratar nuevos fraccionamientos sin histórico?
- ¿Habrá segmentación por rango (económico/medio/alto) con reglas distintas?

---

## 15) Disclaimer recomendado (para el sitio)
ValoraNL ofrece **estimaciones** basadas en datos públicos y modelos estadísticos/heurísticos. No sustituye un avalúo profesional certificado. El valor real puede variar por condición, negociación, documentación y dinámica del mercado.
