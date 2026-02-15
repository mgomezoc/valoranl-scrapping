-- ValoraNL canonical schema for MySQL 8.0.30+
-- Charset/Collation: utf8mb4 / utf8mb4_unicode_ci

CREATE DATABASE IF NOT EXISTS valoranl
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE valoranl;

CREATE TABLE IF NOT EXISTS sources (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  source_code VARCHAR(50) NOT NULL,
  source_name VARCHAR(120) NOT NULL,
  base_url VARCHAR(500) NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  notes VARCHAR(500) NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY ux_sources_code (source_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS listings (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  source_id INT UNSIGNED NOT NULL,
  source_listing_id VARCHAR(200) NULL,
  parse_version VARCHAR(40) NOT NULL DEFAULT 'unify_v1',

  url VARCHAR(1000) NULL,
  url_normalized VARCHAR(1000) NULL,
  url_hash CHAR(64) NULL,
  fingerprint_hash CHAR(64) NULL,
  dedupe_hash CHAR(64) NOT NULL,

  status ENUM('active','inactive','sold','unknown') NOT NULL DEFAULT 'active',
  price_type ENUM('sale','rent','unknown') NOT NULL DEFAULT 'sale',
  price_amount DECIMAL(16,2) NULL,
  currency CHAR(3) NOT NULL DEFAULT 'MXN',
  maintenance_fee DECIMAL(12,2) NULL,

  property_type VARCHAR(80) NULL,
  area_construction_m2 DECIMAL(10,2) NULL,
  area_land_m2 DECIMAL(10,2) NULL,
  bedrooms INT NULL,
  bathrooms DECIMAL(4,1) NULL,
  half_bathrooms DECIMAL(4,1) NULL,
  parking INT NULL,
  floors INT NULL,
  age_years INT NULL,

  title VARCHAR(500) NULL,
  description MEDIUMTEXT NULL,

  street VARCHAR(255) NULL,
  colony VARCHAR(180) NULL,
  municipality VARCHAR(180) NULL,
  state VARCHAR(120) NULL,
  country VARCHAR(120) NULL DEFAULT 'México',
  postal_code VARCHAR(20) NULL,

  lat DECIMAL(10,7) NULL,
  lng DECIMAL(10,7) NULL,
  geo_precision ENUM('exact','approx','colony','unknown') NOT NULL DEFAULT 'unknown',

  images_json JSON NULL,
  contact_json JSON NULL,
  amenities_json JSON NULL,
  details_json JSON NULL,
  raw_json JSON NULL,

  source_first_seen_at DATETIME NULL,
  source_last_seen_at DATETIME NULL,
  seen_first_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  seen_last_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY ux_listings_dedupe_hash (dedupe_hash),
  KEY ix_listings_source (source_id),
  KEY ix_listings_source_listing (source_id, source_listing_id),
  KEY ix_listings_url_hash (url_hash),
  KEY ix_listings_fingerprint_hash (fingerprint_hash),
  KEY ix_listings_municipality_colony (municipality, colony),
  KEY ix_listings_geo_lat_lng (lat, lng),
  KEY ix_listings_price (price_amount),
  KEY ix_listings_area (area_construction_m2, area_land_m2),
  KEY ix_listings_seen_last (seen_last_at),
  CONSTRAINT fk_listings_source FOREIGN KEY (source_id) REFERENCES sources(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS listing_price_history (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  listing_id BIGINT UNSIGNED NOT NULL,
  status ENUM('active','inactive','sold','unknown') NOT NULL DEFAULT 'unknown',
  price_amount DECIMAL(16,2) NULL,
  currency CHAR(3) NOT NULL DEFAULT 'MXN',
  captured_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY ix_price_history_listing_date (listing_id, captured_at),
  CONSTRAINT fk_price_history_listing FOREIGN KEY (listing_id) REFERENCES listings(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS listing_status_history (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  listing_id BIGINT UNSIGNED NOT NULL,
  old_status ENUM('active','inactive','sold','unknown') NULL,
  new_status ENUM('active','inactive','sold','unknown') NOT NULL,
  changed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY ix_status_history_listing_date (listing_id, changed_at),
  CONSTRAINT fk_status_history_listing FOREIGN KEY (listing_id) REFERENCES listings(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO sources (source_code, source_name, base_url)
VALUES
  ('casas365', 'Casas 365', 'https://casas365.mx'),
  ('gpvivienda', 'GP Vivienda', 'https://gpvivienda.com'),
  ('realtyworld', 'Realty World', 'https://www.realtyworld.com.mx')
ON DUPLICATE KEY UPDATE
  source_name = VALUES(source_name),
  base_url = VALUES(base_url),
  updated_at = CURRENT_TIMESTAMP;
