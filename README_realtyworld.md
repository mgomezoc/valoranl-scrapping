# Realty World / Realty Experts Scraper

Script de scraping para extraer información de casas en venta desde [Realty World México](https://www.realtyworld.com.mx/).

## Características

- Extrae datos completos de propiedades (precio, recámaras, baños, m², etc.)
- Guarda en base de datos SQLite local
- Exporta a Excel para análisis
- Soporta múltiples ciudades
- Maneja paginación mediante scroll infinito (versión Playwright)

## Archivos

| Archivo | Descripción |
|---------|-------------|
| `realtyworld_scraper_simple.py` | Versión con Requests (rápida, ~15 propiedades) |
| `realtyworld_scraper.py` | Versión con Playwright (completa, todas las propiedades) |
| `realtyworld_propiedades.db` | Base de datos SQLite |
| `realtyworld_propiedades.xlsx` | Exportación a Excel |

## Instalación

### Versión Simple (Requests)
```bash
pip install requests beautifulsoup4 pandas openpyxl lxml
```

### Versión Completa (Playwright)
```bash
pip install playwright pandas openpyxl
playwright install chromium
```

## Uso - Versión Simple

```bash
# Scrapear Monterrey (obtiene ~15 propiedades del HTML inicial)
python realtyworld_scraper_simple.py --city monterrey

# Limitar a 10 propiedades
python realtyworld_scraper_simple.py --city monterrey --limit 10

# Ver estadísticas
python realtyworld_scraper_simple.py --stats

# Exportar a Excel
python realtyworld_scraper_simple.py --export

# Ver tabla
python realtyworld_scraper_simple.py --table
```

## Ciudades disponibles

- `monterrey` - Casas en Monterrey
- `nuevo_leon` - Casas en Nuevo León
- `san_pedro` - Casas en San Pedro Garza García
- `mexico` - Casas en todo México
- `custom` - Usa la URL personalizada

## Estructura de datos

| Campo | Descripción |
|-------|-------------|
| ID | ID de la propiedad (ej: 30-CV-3471) |
| Título | Título de la propiedad |
| Colonia | Colonia/Fraccionamiento |
| Ciudad | Ciudad |
| Estado | Estado |
| Precio | Precio en pesos mexicanos |
| m² Terreno | Metros cuadrados de terreno |
| m² Construcción | Metros cuadrados construidos |
| Frente (m) | Metros de frente |
| Recámaras | Número de recámaras |
| Baños | Número de baños completos |
| ½ Baños | Número de medios baños |
| Plantas | Número de plantas/niveles |
| Año | Año de construcción |
| URL | Enlace a la propiedad |

## Notas importantes

- **Versión Simple**: Obtiene solo las propiedades que aparecen en el HTML inicial (~15-20). Es más rápida pero incompleta.
- **Versión Playwright**: Obtiene todas las propiedades haciendo scroll, pero es más lenta y requiere más recursos.
- El scraping respeta rate limits (1 segundo entre requests)
- Las propiedades se actualizan automáticamente si cambian

## Diferencia entre versiones

| Característica | Simple | Playwright |
|----------------|--------|------------|
| Velocidad | Rápida | Lenta |
| Propiedades | ~15-20 | Todas (puede ser 100+) |
| Requisitos | requests + bs4 | Playwright + Chromium |
| JavaScript | No | Sí |

## Ejemplo de salida

```
🏠 PROPIEDADES - REALTY WORLD
==========================================================================================
Colonia                        Precio          m²       Rec  Baños  ID             
------------------------------------------------------------------------------------------
Venta en Barrio del Prado      $1,650,000      100      3    1      69-CV-2893     
Venta en Balcones de las Mit   $2,700,000      111      2    2      24-CV-7291     
Venta en Reserva Cumbres       $4,290,000      148      2    2      01-CV-12694    
...
```

## Licencia

Uso personal con permiso del dueño del sitio web.
