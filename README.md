# GP Vivienda Scraper - Nuevo León

Script de scraping para extraer información de casas en venta en Nuevo León desde [GP Vivienda](https://gpvivienda.com/casas-venta-nuevo-leon/).

## Características

- Extrae datos de propiedades (precio, recámaras, baños, m², etc.)
- Guarda en base de datos SQLite local
- Exporta a Excel para análisis
- Actualización incremental (solo propiedades nuevas)
- Estadísticas y reportes

## Instalación

```bash
# Clonar o descargar el script
pip install requests beautifulsoup4 pandas openpyxl lxml
```

## Uso

### Scraping completo
```bash
python gpvivienda_scraper.py
```

### Solo actualizar propiedades nuevas
```bash
python gpvivienda_scraper.py --update
```

### Exportar a Excel
```bash
python gpvivienda_scraper.py --export
```

### Ver estadísticas
```bash
python gpvivienda_scraper.py --stats
```

### Ver tabla de propiedades
```bash
python gpvivienda_scraper.py --table
```

## Archivos generados

- `gpvivienda_nuevoleon.db` - Base de datos SQLite
- `gpvivienda_nuevoleon.xlsx` - Exportación a Excel

## Estructura de datos

| Campo | Descripción |
|-------|-------------|
| Ciudad | Municipio donde está la propiedad |
| Fraccionamiento | Nombre del desarrollo residencial |
| Modelo | Modelo de la casa |
| Precio | Precio en pesos mexicanos |
| Recámaras | Número de recámaras |
| Baños | Número de baños |
| m² Construidos | Metros cuadrados construidos |
| m² Terreno | Metros cuadrados de terreno |
| Promoción | ¿Tiene promoción especial? |
| Preventa | ¿Está en preventa? |
| URL | Enlace a la propiedad |

## Notas

- El scraping respeta rate limits (2 segundos entre requests)
- Las propiedades se actualizan automáticamente si cambian
- Se mantiene historial de scraping en la base de datos

## Licencia

Uso personal con permiso del dueño del sitio web.
