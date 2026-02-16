-- Migración incremental: agrega índices de valuación y stale detection
-- Ejecutar SOLO si ya tienes la BD con el schema anterior.
-- Si estás creando desde cero, usa valoranl_schema.sql directamente.
--
-- Uso: mysql -u root valoranl < db/migrate_add_indexes.sql

USE valoranl;

-- Índice compuesto para queries de valuación (getComparables)
-- Cubre: WHERE status='active' AND price_type='sale' AND property_type='casa' AND municipality=X AND colony=X
ALTER TABLE listings
  ADD KEY ix_listings_valuation_main (status, price_type, property_type, municipality, colony);

-- Índice para filtro por área de construcción en valuación
ALTER TABLE listings
  ADD KEY ix_listings_valuation_area (status, price_type, property_type, municipality, area_construction_m2);

-- Índice para fallback estatal
ALTER TABLE listings
  ADD KEY ix_listings_state_type (status, price_type, property_type, state);

-- Índice para desactivación de listings stale (status + seen_last_at)
ALTER TABLE listings
  ADD KEY ix_listings_status_seen (status, seen_last_at);

-- Índice para ordenamiento por updated_at
ALTER TABLE listings
  ADD KEY ix_listings_updated (updated_at);

-- Tablas de valuación (si no existen)
CREATE TABLE IF NOT EXISTS valuations (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  request_json JSON NOT NULL,
  result_json JSON NOT NULL,
  estimated_value DECIMAL(16,2) NULL,
  estimated_low DECIMAL(16,2) NULL,
  estimated_high DECIMAL(16,2) NULL,
  ppu_aplicado DECIMAL(12,2) NULL,
  confidence_score INT NULL,
  location_scope VARCHAR(40) NULL,
  comparables_count INT NULL,
  municipality VARCHAR(180) NULL,
  colony VARCHAR(180) NULL,
  area_construction_m2 DECIMAL(10,2) NULL,
  age_years INT NULL,
  conservation_level INT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY ix_valuations_municipality (municipality, colony),
  KEY ix_valuations_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS valuation_comparables (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  valuation_id BIGINT UNSIGNED NOT NULL,
  listing_id BIGINT UNSIGNED NULL,
  ppu_bruto DECIMAL(12,2) NULL,
  ppu_homologado DECIMAL(12,2) NULL,
  fre DECIMAL(8,4) NULL,
  similarity_score DECIMAL(6,4) NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY ix_valcomp_valuation (valuation_id),
  KEY ix_valcomp_listing (listing_id),
  CONSTRAINT fk_valcomp_valuation FOREIGN KEY (valuation_id) REFERENCES valuations(id),
  CONSTRAINT fk_valcomp_listing FOREIGN KEY (listing_id) REFERENCES listings(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
