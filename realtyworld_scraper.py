#!/usr/bin/env python3
"""
Realty World / Realty Experts Scraper - Casas en Venta
Script para extraer propiedades de realtyworld.com.mx y guardar en base de datos local SQLite.

INSTALACIÓN:
    pip install playwright pandas openpyxl
    playwright install chromium

USO:
    python realtyworld_scraper.py                    # Scrapear todas las propiedades
    python realtyworld_scraper.py --city monterrey   # Solo Monterrey
    python realtyworld_scraper.py --limit 50         # Limitar a 50 propiedades
    python realtyworld_scraper.py --export           # Solo exportar a Excel
    python realtyworld_scraper.py --stats            # Ver estadísticas

AUTOR: Generado para uso personal
FECHA: 2026-02-16
"""

import sqlite3
import asyncio
import re
import argparse
from datetime import datetime
from urllib.parse import urljoin, urlparse
from pathlib import Path

# Configuración
BASE_URL = "https://www.realtyworld.com.mx"
DB_PATH = "realtyworld_propiedades.db"
EXCEL_PATH = "realtyworld_propiedades.xlsx"

# URLs de búsqueda por ciudad
SEARCH_URLS = {
    'monterrey': 'https://www.realtyworld.com.mx/search/casas-en-venta-en-monterrey-nuevo-leon-mexico',
    'nuevo_leon': 'https://www.realtyworld.com.mx/search/casas-en-venta-en-nuevo-leon-mexico',
    'mexico': 'https://www.realtyworld.com.mx/search/casas-en-venta-en-mexico',
    'custom': 'https://www.realtyworld.com.mx/search?ot=1&pt=1&desc=&vp=25.429306559861335%2C-100.57727238863407%2C25.93610980166219%2C-99.92083928316532'
}


class RealtyWorldScraper:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Inicializa la base de datos SQLite."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS propiedades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                property_id TEXT,
                titulo TEXT,
                colonia TEXT,
                ciudad TEXT,
                estado TEXT,
                precio REAL,
                precio_texto TEXT,
                moneda TEXT DEFAULT 'MXN',
                terreno_m2 REAL,
                construccion_m2 REAL,
                frente_m REAL,
                fondo_m REAL,
                recamaras INTEGER,
                banos INTEGER,
                medios_banos INTEGER,
                plantas INTEGER,
                ano_construccion INTEGER,
                estacionamientos INTEGER,
                descripcion TEXT,
                amenidades TEXT,
                equipamiento TEXT,
                imagenes TEXT,
                latitud REAL,
                longitud REAL,
                fecha_publicacion TEXT,
                fecha_scraping TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha_inicio TIMESTAMP,
                fecha_fin TIMESTAMP,
                ciudad TEXT,
                propiedades_encontradas INTEGER,
                propiedades_nuevas INTEGER,
                propiedades_actualizadas INTEGER,
                errores INTEGER DEFAULT 0
            )
        ''')
        
        conn.commit()
        conn.close()
        print(f"✓ Base de datos lista: {self.db_path}")
    
    def extraer_precio(self, texto):
        """Extrae el precio numérico de un texto."""
        if not texto:
            return None
        # Buscar números con comas y puntos
        numeros = re.findall(r'[\d,\.]+', texto.replace('$', '').replace(',', ''))
        if numeros:
            try:
                return float(numeros[0])
            except:
                return None
        return None
    
    async def scrape_listado(self, page, url, max_scrolls=20):
        """Scrapea el listado de propiedades con scroll infinito."""
        propiedades_urls = []
        
        print(f"\n📄 Cargando listado...")
        await page.goto(url, wait_until='networkidle', timeout=60000)
        await page.wait_for_timeout(3000)
        
        # Cerrar modal si existe
        try:
            close_btn = await page.locator('button:has-text("×"), button[aria-label="close"]').first
            if await close_btn.is_visible(timeout=2000):
                await close_btn.click()
                await page.wait_for_timeout(500)
        except:
            pass
        
        # Hacer scroll para cargar más propiedades
        print(f"  Scrolleando para cargar propiedades...")
        for i in range(max_scrolls):
            # Obtener URLs actuales
            urls = await page.eval_on_selector_all(
                'a[href*="/property/"]',
                'elements => [...new Set(elements.map(e => e.href))]'
            )
            
            nuevas_urls = [u for u in urls if u not in propiedades_urls]
            propiedades_urls.extend(nuevas_urls)
            
            if i % 5 == 0:
                print(f"    Scroll {i+1}/{max_scrolls} - Total URLs: {len(propiedades_urls)}")
            
            # Scroll hacia abajo
            await page.evaluate('window.scrollBy(0, 800)')
            await page.wait_for_timeout(1500)
            
            # Verificar si hay botón "Ver Más Resultados"
            try:
                mas_btn = await page.locator('text=Ver Más Resultados').first
                if await mas_btn.is_visible(timeout=1000):
                    await mas_btn.click()
                    await page.wait_for_timeout(2000)
            except:
                pass
        
        print(f"✓ {len(propiedades_urls)} propiedades encontradas")
        return propiedades_urls
    
    async def scrape_propiedad(self, page, url):
        """Scrapea los detalles de una propiedad."""
        datos = {
            'url': url,
            'property_id': '',
            'titulo': '',
            'colonia': '',
            'ciudad': '',
            'estado': '',
            'precio': None,
            'precio_texto': '',
            'terreno_m2': None,
            'construccion_m2': None,
            'frente_m': None,
            'fondo_m': None,
            'recamaras': None,
            'banos': None,
            'medios_banos': None,
            'plantas': None,
            'ano_construccion': None,
            'estacionamientos': None,
            'descripcion': '',
            'amenidades': '',
            'equipamiento': '',
            'imagenes': '',
            'latitud': None,
            'longitud': None,
            'fecha_publicacion': ''
        }
        
        try:
            await page.goto(url, wait_until='networkidle', timeout=30000)
            await page.wait_for_timeout(2000)
            
            # Extraer datos con JavaScript
            page_data = await page.evaluate('''() => {
                const data = {};
                
                // Título
                const h1 = document.querySelector('h1');
                data.titulo = h1 ? h1.textContent.trim() : '';
                
                // Property ID
                const idLabel = document.querySelector('label, span[class*="id"]');
                if (idLabel) {
                    const match = idLabel.textContent.match(/(\d+-[A-Z]+-\d+)/);
                    if (match) data.property_id = match[1];
                }
                
                // Precio
                const precioElem = Array.from(document.querySelectorAll('div, span, p')).find(
                    el => el.textContent.includes('$') && el.textContent.includes(',')
                );
                if (precioElem) {
                    data.precio_texto = precioElem.textContent.trim();
                }
                
                // Descripción
                const descSection = document.querySelector('[class*="description"], [class*="descripcion"]');
                if (descSection) {
                    data.descripcion = descSection.textContent.trim();
                }
                
                // Coordenadas del mapa
                const mapLink = document.querySelector('a[href*="google.com/maps"]');
                if (mapLink) {
                    const match = mapLink.href.match(/@(-?\d+\.\d+),(-?\d+\.\d+)/);
                    if (match) {
                        data.latitud = parseFloat(match[1]);
                        data.longitud = parseFloat(match[2]);
                    }
                }
                
                return data;
            }''')
            
            datos.update(page_data)
            
            # Extraer precio numérico
            if datos['precio_texto']:
                datos['precio'] = self.extraer_precio(datos['precio_texto'])
            
            # Extraer datos de tablas
            html = await page.content()
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, 'lxml')
            
            # Buscar en tablas de características
            for tr in soup.find_all('tr'):
                tds = tr.find_all(['td', 'th'])
                if len(tds) >= 2:
                    label = tds[0].get_text(strip=True).lower()
                    value = tds[1].get_text(strip=True)
                    
                    if 'terreno' in label:
                        datos['terreno_m2'] = self.extraer_precio(value)
                    elif 'construcción' in label or 'construccion' in label:
                        datos['construccion_m2'] = self.extraer_precio(value)
                    elif 'frente' in label:
                        datos['frente_m'] = self.extraer_precio(value)
                    elif 'fondo' in label:
                        datos['fondo_m'] = self.extraer_precio(value)
                    elif 'recámara' in label or 'recamara' in label:
                        nums = re.findall(r'\d+', value)
                        if nums:
                            datos['recamaras'] = int(nums[0])
                    elif 'baño' in label and 'medio' not in label:
                        nums = re.findall(r'\d+', value)
                        if nums:
                            datos['banos'] = int(nums[0])
                    elif 'medio' in label and 'baño' in label:
                        nums = re.findall(r'\d+', value)
                        if nums:
                            datos['medios_banos'] = int(nums[0])
                    elif 'planta' in label:
                        nums = re.findall(r'\d+', value)
                        if nums:
                            datos['plantas'] = int(nums[0])
                    elif 'año' in label or 'construcción' in label:
                        nums = re.findall(r'\d{4}', value)
                        if nums:
                            datos['ano_construccion'] = int(nums[0])
                    elif 'estacionamiento' in label:
                        nums = re.findall(r'\d+', value)
                        if nums:
                            datos['estacionamientos'] = int(nums[0])
            
            # Extraer de divs con clase específica
            for div in soup.find_all('div', class_=re.compile(r'property|dato', re.I)):
                text = div.get_text(strip=True)
                
                if 'Terreno' in text and 'm²' in text:
                    match = re.search(r'(\d+(?:\.\d+)?)\s*m²', text)
                    if match and not datos['terreno_m2']:
                        datos['terreno_m2'] = float(match.group(1))
                
                if 'Construcción' in text and 'm²' in text:
                    match = re.search(r'(\d+(?:\.\d+)?)\s*m²', text)
                    if match and not datos['construccion_m2']:
                        datos['construccion_m2'] = float(match.group(1))
            
            # Extraer ubicación del breadcrumb
            breadcrumbs = soup.find_all('a', href=re.compile(r'/search/|/Casas/'))
            for i, bc in enumerate(breadcrumbs):
                text = bc.get_text(strip=True)
                if 'Venta' in text or 'Casas' in text:
                    continue
                if not datos['estado'] and len(text) > 2:
                    datos['estado'] = text
                elif not datos['ciudad'] and len(text) > 2:
                    datos['ciudad'] = text
            
            # Extraer colonia del título
            if datos['titulo']:
                match = re.search(r'en\s+([^\$]+?)(?:\s*\||\s*\$|$)', datos['titulo'])
                if match:
                    datos['colonia'] = match.group(1).strip()
            
            # Extraer amenidades
            amenidades = []
            for elem in soup.find_all(text=re.compile(r'Sala|Comedor|Cocina|Jardín|Patio|Cochera|Balcón|Estancia|Lavandería', re.I)):
                text = elem.strip()
                if text and len(text) < 50 and text not in amenidades:
                    amenidades.append(text)
            datos['amenidades'] = ', '.join(amENIDADES[:20])  # Limitar
            
            # Extraer imágenes
            imagenes = []
            for img in soup.find_all('img', src=re.compile(r'\.(jpg|jpeg|png|webp)', re.I)):
                src = img.get('src', '')
                if src and 'logo' not in src.lower():
                    imagenes.append(urljoin(BASE_URL, src))
            datos['imagenes'] = ', '.join(imagenes[:10])  # Limitar a 10
            
            # Fecha de publicación
            pub_elem = soup.find(text=re.compile(r'Publicado:', re.I))
            if pub_elem:
                match = re.search(r'(\d{4}-\d{2}-\d{2})', pub_elem)
                if match:
                    datos['fecha_publicacion'] = match.group(1)
            
        except Exception as e:
            print(f"    ⚠ Error: {str(e)[:80]}")
        
        return datos
    
    def guardar_propiedad(self, datos):
        """Guarda una propiedad en la base de datos."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO propiedades 
                (url, property_id, titulo, colonia, ciudad, estado, precio, precio_texto,
                 terreno_m2, construccion_m2, frente_m, fondo_m, recamaras, banos, medios_banos,
                 plantas, ano_construccion, estacionamientos, descripcion, amenidades,
                 equipamiento, imagenes, latitud, longitud, fecha_publicacion)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    property_id=excluded.property_id, titulo=excluded.titulo,
                    colonia=excluded.colonia, ciudad=excluded.ciudad, estado=excluded.estado,
                    precio=excluded.precio, precio_texto=excluded.precio_texto,
                    terreno_m2=excluded.terreno_m2, construccion_m2=excluded.construccion_m2,
                    frente_m=excluded.frente_m, fondo_m=excluded.fondo_m,
                    recamaras=excluded.recamaras, banos=excluded.banos, medios_banos=excluded.medios_banos,
                    plantas=excluded.plantas, ano_construccion=excluded.ano_construccion,
                    estacionamientos=excluded.estacionamientos, descripcion=excluded.descripcion,
                    amenidades=excluded.amenidades, equipamiento=excluded.equipamiento,
                    imagenes=excluded.imagenes, latitud=excluded.latitud, longitud=excluded.longitud,
                    fecha_publicacion=excluded.fecha_publicacion,
                    fecha_actualizacion=CURRENT_TIMESTAMP
            ''', tuple(datos.values()))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"  ⚠ Error BD: {e}")
            return False
        finally:
            conn.close()
    
    async def scrape(self, city='custom', limit=None, max_scrolls=20):
        """Ejecuta el scraping completo."""
        from playwright.async_api import async_playwright
        
        print("=" * 70)
        print("🏠 Realty World / Realty Experts Scraper")
        print("=" * 70)
        
        fecha_inicio = datetime.now()
        propiedades_nuevas = 0
        propiedades_actualizadas = 0
        errores = 0
        
        url = SEARCH_URLS.get(city, SEARCH_URLS['custom'])
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            # Obtener URLs del listado
            urls = await self.scrape_listado(page, url, max_scrolls)
            
            if limit:
                urls = urls[:limit]
            
            # Procesar cada propiedad
            print(f"\n🔍 Procesando {len(urls)} propiedades...")
            for i, prop_url in enumerate(urls, 1):
                print(f"\n  [{i}/{len(urls)}] {prop_url.split('/')[-1]}")
                
                datos = await self.scrape_propiedad(page, prop_url)
                
                # Mostrar resumen
                print(f"    📍 {datos['colonia'] or 'N/A'} - {datos['ciudad'] or 'N/A'}")
                print(f"    🏠 {datos['titulo'][:50] if datos['titulo'] else 'N/A'}")
                if datos['precio']:
                    print(f"    💰 ${datos['precio']:,.2f}")
                print(f"    📐 {datos['construccion_m2'] or '?'} m² constr | 🛏 {datos['recamaras'] or '?'} rec | 🚿 {datos['banos'] or '?'} baños")
                
                if self.guardar_propiedad(datos):
                    propiedades_nuevas += 1
            
            await browser.close()
        
        # Registrar log
        fecha_fin = datetime.now()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO scraping_log 
            (fecha_inicio, fecha_fin, ciudad, propiedades_encontradas, propiedades_nuevas, propiedades_actualizadas, errores)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (fecha_inicio, fecha_fin, city, len(urls), propiedades_nuevas, propiedades_actualizadas, errores))
        conn.commit()
        conn.close()
        
        # Resumen
        print("\n" + "=" * 70)
        print("📊 RESUMEN")
        print("=" * 70)
        print(f"⏱ Duración: {fecha_fin - fecha_inicio}")
        print(f"🔍 Propiedades: {len(urls)}")
        print(f"✨ Guardadas: {propiedades_nuevas}")
        print(f"⚠ Errores: {errores}")
        print("=" * 70)
    
    def exportar_excel(self, output_path=EXCEL_PATH):
        """Exporta los datos a Excel."""
        try:
            import pandas as pd
        except ImportError:
            print("⚠ Instala pandas: pip install pandas openpyxl")
            return
        
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query('''
            SELECT 
                property_id as 'ID Propiedad',
                titulo as 'Título',
                colonia as 'Colonia',
                ciudad as 'Ciudad',
                estado as 'Estado',
                precio as 'Precio',
                terreno_m2 as 'm² Terreno',
                construccion_m2 as 'm² Construcción',
                frente_m as 'Frente (m)',
                fondo_m as 'Fondo (m)',
                recamaras as 'Recámaras',
                banos as 'Baños',
                medios_banos as 'Medios Baños',
                plantas as 'Plantas',
                ano_construccion as 'Año Construcción',
                estacionamientos as 'Estacionamientos',
                fecha_publicacion as 'Fecha Publicación',
                url as 'URL'
            FROM propiedades
            ORDER BY precio ASC
        ''', conn)
        conn.close()
        
        if df.empty:
            print("⚠ No hay datos para exportar")
            return
        
        # Formatear precio
        df['Precio'] = df['Precio'].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else '')
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Propiedades', index=False)
            
            # Ajustar anchos
            worksheet = writer.sheets['Propiedades']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if cell.value and len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                worksheet.column_dimensions[column_letter].width = min(max_length + 2, 60)
        
        print(f"✅ Excel exportado: {output_path}")
        print(f"  Total: {len(df)} propiedades")
    
    def mostrar_estadisticas(self):
        """Muestra estadísticas de la base de datos."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM propiedades')
        total = cursor.fetchone()[0]
        
        if total == 0:
            print("⚠ No hay propiedades")
            conn.close()
            return
        
        cursor.execute('''
            SELECT AVG(precio), MIN(precio), MAX(precio),
                   AVG(construccion_m2), AVG(terreno_m2)
            FROM propiedades 
            WHERE precio IS NOT NULL
        ''')
        stats = cursor.fetchone()
        
        cursor.execute('SELECT ciudad, COUNT(*), AVG(precio) FROM propiedades GROUP BY ciudad ORDER BY COUNT(*) DESC')
        por_ciudad = cursor.fetchall()
        
        print("\n" + "=" * 70)
        print("📈 ESTADÍSTICAS")
        print("=" * 70)
        print(f"🏠 Total: {total} propiedades")
        print(f"\n💰 Precios:")
        print(f"   Promedio: ${stats[0]:,.2f}" if stats[0] else "   Promedio: N/A")
        print(f"   Mínimo: ${stats[1]:,.2f}" if stats[1] else "   Mínimo: N/A")
        print(f"   Máximo: ${stats[2]:,.2f}" if stats[2] else "   Máximo: N/A")
        print(f"\n📐 Metraje promedio:")
        print(f"   Construcción: {stats[3]:.1f} m²" if stats[3] else "   Construcción: N/A")
        print(f"   Terreno: {stats[4]:.1f} m²" if stats[4] else "   Terreno: N/A")
        print(f"\n📍 Por ciudad:")
        for ciudad, count, precio in por_ciudad[:10]:
            precio_str = f"${precio:,.0f}" if precio else "N/A"
            print(f"   • {ciudad or 'N/A'}: {count} propiedades ({precio_str} prom)")
        print("=" * 70)
        
        conn.close()
    
    def mostrar_tabla(self, limit=20):
        """Muestra propiedades en formato tabla."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT colonia, ciudad, precio, construccion_m2, recamaras, banos, property_id
            FROM propiedades 
            ORDER BY precio
            LIMIT ?
        ''', (limit,))
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            print("⚠ No hay propiedades")
            return
        
        print("\n" + "=" * 110)
        print("🏠 PROPIEDADES - REALTY WORLD")
        print("=" * 110)
        print(f"{'Colonia':<30} {'Ciudad':<20} {'Precio':<15} {'m²':<8} {'Rec':<4} {'Baños':<6} {'ID':<15}")
        print("-" * 110)
        
        for row in rows:
            colonia = (row[0] or 'N/A')[:28]
            ciudad = (row[1] or 'N/A')[:18]
            precio = f"${row[2]:,.0f}" if row[2] else 'N/A'
            m2 = f"{row[3]:.0f}" if row[3] else '-'
            rec = row[4] if row[4] else '-'
            banos = row[5] if row[5] else '-'
            pid = row[6] or 'N/A'
            print(f"{colonia:<30} {ciudad:<20} {precio:<15} {m2:<8} {rec:<4} {banos:<6} {pid:<15}")
        
        print("=" * 110)


def main():
    parser = argparse.ArgumentParser(description='Realty World Scraper')
    parser.add_argument('--city', choices=['monterrey', 'nuevo_leon', 'mexico', 'custom'], 
                        default='custom', help='Ciudad a scrapear')
    parser.add_argument('--limit', type=int, help='Limitar número de propiedades')
    parser.add_argument('--scrolls', type=int, default=20, help='Número de scrolls (default: 20)')
    parser.add_argument('--export', action='store_true', help='Solo exportar a Excel')
    parser.add_argument('--stats', action='store_true', help='Mostrar estadísticas')
    parser.add_argument('--table', action='store_true', help='Mostrar tabla')
    
    args = parser.parse_args()
    
    scraper = RealtyWorldScraper()
    
    if args.stats:
        scraper.mostrar_estadisticas()
    elif args.table:
        scraper.mostrar_tabla()
    elif args.export:
        scraper.exportar_excel()
    else:
        asyncio.run(scraper.scrape(city=args.city, limit=args.limit, max_scrolls=args.scrolls))
        scraper.exportar_excel()
        scraper.mostrar_estadisticas()


if __name__ == '__main__':
    main()
