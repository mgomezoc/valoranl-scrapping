# Casas 365 Scraper - MySQL Edition

Script de scraping para extraer propiedades de [Casas 365](https://casas365.mx/) con soporte para MySQL (Laragon).

## Características

- Extrae datos completos de propiedades (precio, recámaras, baños, m², clase energética, etc.)
- Guarda en **MySQL** (compatible con Laragon)
- Exporta a Excel para análisis
- Soporta actualización incremental (INSERT ... ON DUPLICATE KEY UPDATE)
- Índices optimizados para búsquedas

## Instalación

```bash
pip install requests beautifulsoup4 pymysql pandas openpyxl lxml
```

## Configuración MySQL (Laragon)

Por defecto, el scraper usa estas credenciales:

```python
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '',  # Cambiar si tienes contraseña
    'database': 'casas365',  # Se crea automáticamente
}
```

### Para cambiar las credenciales:

```bash
# Usar credenciales personalizadas
python casas365_scraper.py --user mi_usuario --password mi_password --database mi_base
```

## Uso

```bash
# Scrapear todas las propiedades
python casas365_scraper.py

# Limitar a 10 propiedades
python casas365_scraper.py --limit 10

# Ver estadísticas
python casas365_scraper.py --stats

# Exportar a Excel
python casas365_scraper.py --export

# Ver tabla de propiedades
python casas365_scraper.py --table
```

## Estructura de la tabla MySQL

```sql
CREATE TABLE propiedades (
    id INT AUTO_INCREMENT PRIMARY KEY,
    url VARCHAR(500) UNIQUE NOT NULL,
    titulo VARCHAR(500),
    tipo VARCHAR(100),
    accion VARCHAR(100),
    estado VARCHAR(50),
    precio DECIMAL(15, 2),
    moneda VARCHAR(10) DEFAULT 'MXN',
    calle VARCHAR(500),
    colonia VARCHAR(200),
    ciudad VARCHAR(200),
    estado_geo VARCHAR(100),
    pais VARCHAR(100) DEFAULT 'México',
    recamaras INT,
    banos DECIMAL(3, 1),
    habitaciones INT,
    terreno_m2 DECIMAL(10, 2),
    construccion_m2 DECIMAL(10, 2),
    plantas INT,
    estacionamientos INT,
    clase_energetica VARCHAR(10),
    descripcion TEXT,
    imagenes TEXT,
    latitud DECIMAL(10, 6),
    longitud DECIMAL(10, 6),
    agente_nombre VARCHAR(200),
    agente_telefono VARCHAR(50),
    agente_whatsapp VARCHAR(50),
    agente_email VARCHAR(200),
    fecha_publicacion DATE,
    fecha_scraping TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_precio (precio),
    INDEX idx_ciudad (ciudad),
    INDEX idx_colonia (colonia),
    INDEX idx_recamaras (recamaras),
    FULLTEXT INDEX idx_descripcion (descripcion)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## Datos extraídos

| Campo | Descripción |
|-------|-------------|
| url | URL de la propiedad |
| titulo | Título de la propiedad |
| tipo | Tipo (Casa, Departamento, etc.) |
| accion | Acción (Casas en Venta, etc.) |
| estado | Estado (Usada, Nueva, Vendida) |
| precio | Precio en pesos mexicanos |
| moneda | Moneda (MXN/USD) |
| calle | Dirección de la calle |
| colonia | Colonia/Fraccionamiento |
| ciudad | Ciudad |
| estado_geo | Estado geográfico (Nuevo León) |
| recamaras | Número de recámaras |
| banos | Número de baños (puede ser 3.5, 2.5, etc.) |
| habitaciones | Número de habitaciones |
| terreno_m2 | Metros cuadrados de terreno |
| construccion_m2 | Metros cuadrados construidos |
| plantas | Número de plantas/niveles |
| estacionamientos | Número de estacionamientos |
| clase_energetica | Clase energética (A-G) |
| descripcion | Descripción completa |
| imagenes | URLs de imágenes (separadas por coma) |
| latitud | Latitud del mapa |
| longitud | Longitud del mapa |
| agente_nombre | Nombre del agente |
| agente_telefono | Teléfono del agente |
| agente_whatsapp | WhatsApp del agente |
| agente_email | Email del agente |

## Ejemplo de salida

```
🏠 PROPIEDADES - CASAS 365
==========================================================================================
Colonia                   Ciudad       Precio         m²       Rec  Baños  Pl  Título
------------------------------------------------------------------------------------------
Espacio Cumbres Sector    Monterrey    $3,300,000     157      3    2.5    2   Casa en Venta Espacio Cumbres Sec
Cumbres del Sol           Monterrey    $3,400,000     140      3    2.5    2   Casa en Venta Cumbres del Sol Mon
Arbado Monarca            Apodaca      $5,300,000     259      5    3.5    3   Casa en Venta Arbado Monarca en A
Balcones del Cercado      Santiago     $13,800,000    400      3    3.5    2   Casa en Venta El Cercado, Santiag
Sierra Alta               Monterrey    $41,500,000    1030     5    7.0    -   Casa en Preventa Sierra Alta 9o S
==========================================================================================
```

## Consultas SQL útiles

```sql
-- Propiedades por rango de precio
SELECT * FROM propiedades WHERE precio BETWEEN 5000000 AND 10000000;

-- Propiedades por ciudad
SELECT ciudad, COUNT(*), AVG(precio) FROM propiedades GROUP BY ciudad;

-- Propiedades con 3+ recámaras
SELECT * FROM propiedades WHERE recamaras >= 3 ORDER BY precio;

-- Búsqueda por texto en descripción
SELECT * FROM propiedades WHERE MATCH(descripcion) AGAINST('alberca terraza');

-- Propiedades con clase energética A
SELECT * FROM propiedades WHERE clase_energetica = 'A';
```

## Notas

- El scraper respeta rate limits (1 segundo entre requests)
- Las propiedades se actualizan automáticamente si cambian (ON DUPLICATE KEY UPDATE)
- Se crea automáticamente la base de datos si no existe
- Compatible con Laragon, XAMPP, WAMP o cualquier servidor MySQL

## Licencia

Uso personal con permiso del dueño del sitio web.
