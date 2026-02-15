#!/usr/bin/env python3
"""
Realty World Scraper - Versión Simple (Requests)
Más rápido pero obtiene menos propiedades (las que están en el HTML inicial)

INSTALACIÓN:
    pip install requests beautifulsoup4 pandas openpyxl lxml

USO:
    python realtyworld_scraper_simple.py --city monterrey    # Scrapear Monterrey
    python realtyworld_scraper_simple.py --limit 20          # Limitar a 20
    python realtyworld_scraper_simple.py --export            # Solo exportar
    python realtyworld_scraper_simple.py --stats             # Estadísticas
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import argparse
from datetime import datetime
from urllib.parse import urljoin

# Configuración
BASE_URL = "https://www.realtyworld.com.mx"
DB_PATH = "realtyworld_propiedades.db"
EXCEL_PATH = "realtyworld_propiedades.xlsx"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8',
}

# URLs de búsqueda
SEARCH_URLS = {
    'monterrey': 'https://www.realtyworld.com.mx/search/casas-en-venta-en-monterrey-nuevo-leon-mexico',
    'nuevo_leon': 'https://www.realtyworld.com.mx/search/casas-en-venta-en-nuevo-leon-mexico',
    'mexico': 'https://www.realtyworld.com.mx/search/casas-en-venta-en-mexico',
    'san_pedro': 'https://www.realtyworld.com.mx/search/casas-en-venta-en-san-pedro-garza-garcia-nuevo-leon-mexico',
    'custom': 'https://www.realtyworld.com.mx/search?ot=1&pt=1&desc=&vp=25.429306559861335%2C-100.57727238863407%2C25.93610980166219%2C-99.92083928316532'
}


class RealtyWorldScraper:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
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
                fecha_publicacion TEXT,
                fecha_scraping TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()
    
    def extraer_numero(self, texto):
        """Extrae el primer número de un texto."""
        if not texto:
            return None
        nums = re.findall(r'[\d\.]+', texto.replace(',', ''))
        if nums:
            try:
                return float(nums[0])
            except:
                return None
        return None
    
    def obtener_pagina(self, url, retries=3):
        """Obtiene el contenido HTML de una URL."""
        for i in range(retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.text
            except Exception as e:
                print(f"  ⚠ Error (intento {i+1}): {e}")
                time.sleep(2)
        return None
    
    def parsear_listado(self, html):
        """Extrae URLs de propiedades del listado."""
        soup = BeautifulSoup(html, 'lxml')
        urls = []
        
        for link in soup.find_all('a', href=re.compile(r'/property/\d+')):
            href = link.get('href', '')
            if href:
                full_url = urljoin(BASE_URL, href)
                if full_url not in urls:
                    urls.append(full_url)
        
        return urls
    
    def parsear_propiedad(self, html, url):
        """Extrae datos de una propiedad."""
        soup = BeautifulSoup(html, 'lxml')
        
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
            'fecha_publicacion': ''
        }
        
        try:
            # Título
            h1 = soup.find('h1')
            if h1:
                datos['titulo'] = h1.get_text(strip=True)
            
            # Property ID
            id_label = soup.find('label')
            if id_label:
                datos['property_id'] = id_label.get_text(strip=True)
            
            # Precio
            for div in soup.find_all('div'):
                text = div.get_text(strip=True)
                if '$' in text and ',' in text and len(text) < 100:
                    datos['precio_texto'] = text
                    match = re.search(r'\$([\d,\.]+)', text)
                    if match:
                        datos['precio'] = float(match.group(1).replace(',', ''))
                    break
            
            # Buscar en todo el texto el formato "Etiqueta:Valor"
            page_text = soup.get_text()
            
            # Recámaras
            match = re.search(r'Rec[áa]maras?\s*[:\-]?\s*(\d+)', page_text, re.I)
            if match:
                datos['recamaras'] = int(match.group(1))
            
            # Baños
            match = re.search(r'Baños?\s*[:\-]?\s*(\d+)', page_text, re.I)
            if match:
                datos['banos'] = int(match.group(1))
            
            # Medios Baños
            match = re.search(r'Medios?\s*Baños?\s*[:\-]?\s*(\d+)', page_text, re.I)
            if match:
                datos['medios_banos'] = int(match.group(1))
            
            # Plantas
            match = re.search(r'Plantas?\s*[:\-]?\s*(\d+)', page_text, re.I)
            if match:
                datos['plantas'] = int(match.group(1))
            
            # Año de construcción
            match = re.search(r'Año\s+de\s+construcción\s*[:\-]?\s*(\d{4})', page_text, re.I)
            if match:
                datos['ano_construccion'] = int(match.group(1))
            
            # Características de tablas
            for tr in soup.find_all('tr'):
                tds = tr.find_all(['td', 'th'])
                if len(tds) >= 2:
                    label = tds[0].get_text(strip=True).lower()
                    value = tds[1].get_text(strip=True)
                    
                    if 'terreno' in label and 'constr' not in label:
                        datos['terreno_m2'] = self.extraer_numero(value)
                    elif 'construcción' in label or 'construccion' in label:
                        datos['construccion_m2'] = self.extraer_numero(value)
                    elif 'frente' in label:
                        datos['frente_m'] = self.extraer_numero(value)
                    elif 'fondo' in label:
                        datos['fondo_m'] = self.extraer_numero(value)
                    elif 'estacionamiento' in label:
                        datos['estacionamientos'] = self.extraer_numero(value)
            
            # Colonia del título (mejorado)
            if datos['titulo']:
                titulo_limpio = datos['titulo'].replace(datos['property_id'], '').strip()
                match = re.search(r'en\s+([A-Za-z\s]+?)(?:\s*$)', titulo_limpio)
                if match:
                    datos['colonia'] = match.group(1).strip()
            
            # Ubicación del breadcrumb
            breadcrumbs = soup.find_all('a', href=re.compile(r'/search/|/Casas/'))
            bc_texts = [bc.get_text(strip=True) for bc in breadcrumbs]
            bc_texts = [t for t in bc_texts if t and t not in ['Venta', 'Casas', '']]
            
            if len(bc_texts) >= 2:
                datos['estado'] = bc_texts[-2] if len(bc_texts) >= 2 else ''
                datos['ciudad'] = bc_texts[-1] if len(bc_texts) >= 1 else ''
            
            # Descripción
            desc_header = soup.find(string=re.compile(r'Descripción', re.I))
            if desc_header:
                parent = desc_header.parent
                if parent:
                    next_elem = parent.find_next_sibling()
                    if next_elem:
                        datos['descripcion'] = next_elem.get_text(strip=True)[:500]
            
            # Fecha de publicación
            pub = soup.find(string=re.compile(r'Publicado:', re.I))
            if pub:
                match = re.search(r'(\d{4}-\d{2}-\d{2})', pub)
                if match:
                    datos['fecha_publicacion'] = match.group(1)
            
        except Exception as e:
            print(f"  ⚠ Error parseando: {e}")
        
        return datos
    
    def guardar_propiedad(self, datos):
        """Guarda una propiedad en la base de datos."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO propiedades 
                (url, property_id, titulo, colonia, ciudad, estado, precio, precio_texto,
                 terreno_m2, construccion_m2, frente_m, fondo_m, recamaras, banos, medios_banos,
                 plantas, ano_construccion, estacionamientos, descripcion, fecha_publicacion)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', tuple(datos.values()))
            conn.commit()
            return True
        except Exception as e:
            print(f"  ⚠ Error BD: {e}")
            return False
        finally:
            conn.close()
    
    def scrape(self, city='custom', limit=None):
        """Ejecuta el scraping."""
        import time
        
        print("=" * 70)
        print("🏠 Realty World Scraper - Versión Simple")
        print("=" * 70)
        
        fecha_inicio = datetime.now()
        url = SEARCH_URLS.get(city, SEARCH_URLS['custom'])
        
        # Obtener listado
        print(f"\n📄 Obteniendo listado...")
        html = self.obtener_pagina(url)
        
        if not html:
            print("❌ No se pudo obtener el listado")
            return
        
        urls = self.parsear_listado(html)
        print(f"✓ {len(urls)} propiedades encontradas")
        
        if limit:
            urls = urls[:limit]
        
        # Procesar cada propiedad
        print(f"\n🔍 Procesando {len(urls)} propiedades...")
        guardadas = 0
        
        for i, prop_url in enumerate(urls, 1):
            print(f"\n  [{i}/{len(urls)}] {prop_url.split('/')[-1]}")
            
            html = self.obtener_pagina(prop_url)
            if not html:
                continue
            
            datos = self.parsear_propiedad(html, prop_url)
            
            # Mostrar resumen
            print(f"    📍 {datos['colonia'] or 'N/A'}")
            print(f"    🏠 {datos['titulo'][:50] if datos['titulo'] else 'N/A'}")
            if datos['precio']:
                print(f"    💰 ${datos['precio']:,.0f}")
            print(f"    📐 {datos['construccion_m2'] or '?'} m² | 🛏 {datos['recamaras'] or '?'} rec | 🚿 {datos['banos'] or '?'} baños")
            
            if self.guardar_propiedad(datos):
                guardadas += 1
            
            time.sleep(1)
        
        # Resumen
        fecha_fin = datetime.now()
        print("\n" + "=" * 70)
        print("📊 RESUMEN")
        print("=" * 70)
        print(f"⏱ Duración: {fecha_fin - fecha_inicio}")
        print(f"🔍 Propiedades: {len(urls)}")
        print(f"✅ Guardadas: {guardadas}")
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
                property_id as 'ID',
                titulo as 'Título',
                colonia as 'Colonia',
                ciudad as 'Ciudad',
                estado as 'Estado',
                precio as 'Precio',
                terreno_m2 as 'm² Terreno',
                construccion_m2 as 'm² Construcción',
                frente_m as 'Frente (m)',
                recamaras as 'Recámaras',
                banos as 'Baños',
                medios_banos as '½ Baños',
                plantas as 'Plantas',
                ano_construccion as 'Año',
                url as 'URL'
            FROM propiedades
            ORDER BY precio ASC
        ''', conn)
        conn.close()
        
        if df.empty:
            print("⚠ No hay datos para exportar")
            return
        
        df['Precio'] = df['Precio'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else '')
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Propiedades', index=False)
            
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
    
    def mostrar_estadisticas(self):
        """Muestra estadísticas."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM propiedades')
        total = cursor.fetchone()[0]
        
        if total == 0:
            print("⚠ No hay propiedades")
            conn.close()
            return
        
        cursor.execute('SELECT AVG(precio), MIN(precio), MAX(precio) FROM propiedades WHERE precio IS NOT NULL')
        stats = cursor.fetchone()
        
        print("\n" + "=" * 70)
        print("📈 ESTADÍSTICAS")
        print("=" * 70)
        print(f"🏠 Total: {total} propiedades")
        print(f"\n💰 Precios:")
        print(f"   Promedio: ${stats[0]:,.0f}" if stats[0] else "   N/A")
        print(f"   Mínimo: ${stats[1]:,.0f}" if stats[1] else "   N/A")
        print(f"   Máximo: ${stats[2]:,.0f}" if stats[2] else "   N/A")
        print("=" * 70)
        
        conn.close()
    
    def mostrar_tabla(self, limit=20):
        """Muestra tabla de propiedades."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT colonia, precio, construccion_m2, recamaras, banos, property_id
            FROM propiedades ORDER BY precio LIMIT ?
        ''', (limit,))
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            print("⚠ No hay propiedades")
            return
        
        print("\n" + "=" * 90)
        print("🏠 PROPIEDADES - REALTY WORLD")
        print("=" * 90)
        print(f"{'Colonia':<30} {'Precio':<15} {'m²':<8} {'Rec':<4} {'Baños':<6} {'ID':<15}")
        print("-" * 90)
        
        for row in rows:
            colonia = (row[0] or 'N/A')[:28]
            precio = f"${row[1]:,.0f}" if row[1] else 'N/A'
            m2 = f"{row[2]:.0f}" if row[2] else '-'
            rec = row[3] if row[3] else '-'
            banos = row[4] if row[4] else '-'
            pid = row[5] or 'N/A'
            print(f"{colonia:<30} {precio:<15} {m2:<8} {rec:<4} {banos:<6} {pid:<15}")
        
        print("=" * 90)


def main():
    parser = argparse.ArgumentParser(description='Realty World Scraper')
    parser.add_argument('--city', choices=list(SEARCH_URLS.keys()), default='custom')
    parser.add_argument('--limit', type=int, help='Limitar número de propiedades')
    parser.add_argument('--export', action='store_true')
    parser.add_argument('--stats', action='store_true')
    parser.add_argument('--table', action='store_true')
    
    args = parser.parse_args()
    
    scraper = RealtyWorldScraper()
    
    if args.stats:
        scraper.mostrar_estadisticas()
    elif args.table:
        scraper.mostrar_tabla()
    elif args.export:
        scraper.exportar_excel()
    else:
        scraper.scrape(city=args.city, limit=args.limit)
        scraper.exportar_excel()
        scraper.mostrar_estadisticas()


if __name__ == '__main__':
    main()
