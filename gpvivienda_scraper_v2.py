#!/usr/bin/env python3
"""
GP Vivienda Scraper v2 - Casas en Venta Nuevo León
Versión mejorada con Playwright para manejar JavaScript dinámico.

Instalación:
    pip install playwright pandas openpyxl
    playwright install chromium

Uso:
    python gpvivienda_scraper_v2.py           # Ejecutar scraping completo
    python gpvivienda_scraper_v2.py --update  # Actualizar solo propiedades nuevas
    python gpvivienda_scraper_v2.py --export  # Exportar a Excel
    python gpvivienda_scraper_v2.py --stats   # Mostrar estadísticas
"""

import sqlite3
import json
import time
import re
import argparse
from datetime import datetime
from urllib.parse import urljoin, urlparse
from pathlib import Path

# Configuración
BASE_URL = "https://gpvivienda.com"
NUVO_LEON_URL = "https://gpvivienda.com/casas-venta-nuevo-leon/"
DB_PATH = "gpvivienda_nuevoleon.db"
EXCEL_PATH = "gpvivienda_nuevoleon.xlsx"

# URLs conocidas de propiedades en Nuevo León (extraídas del análisis)
PROPIEDADES_CONOCIDAS = [
    # Cadereyta
    "https://gpvivienda.com/casas-venta-cadereyta-santa-anita-residencial-lisboa/",
    "https://gpvivienda.com/casas-venta-cadereyta-santa-anita-residencial-alcala/",
    
    # El Carmen
    "https://gpvivienda.com/casas-venta-el-carmen-portal-buenavista-lisboa/",
    "https://gpvivienda.com/casas-venta-el-carmen-portal-buenavista-alcala/",
    "https://gpvivienda.com/portal-buenavista-modelo-marsella-8/",
    "https://gpvivienda.com/casas-venta-el-carmen-portal-buenavista-coruna/",
    
    # García
    "https://gpvivienda.com/casas-venta-garcia-vistabella-residencial-marsella/",
    "https://gpvivienda.com/vistabella-residencial-modelo-coruna-8/",
    
    # Juárez
    "https://gpvivienda.com/casas-venta-juarez-alba-residencial/",
    "https://gpvivienda.com/morava-residencial-modelo-marsella-viii/",
    "https://gpvivienda.com/alba-residencial-modelo-castilla-3-7/",
    "https://gpvivienda.com/casas-venta-juarez-san-patricio-residencial-castilla-iii-7/",
]


class GPViviendaScraper:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.propiedades = []
        self.init_database()
    
    def init_database(self):
        """Inicializa la base de datos SQLite con la estructura necesaria."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS propiedades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                titulo TEXT,
                modelo TEXT,
                fraccionamiento TEXT,
                ciudad TEXT,
                estado TEXT DEFAULT 'Nuevo León',
                precio INTEGER,
                precio_texto TEXT,
                recamaras INTEGER,
                banos TEXT,
                m2_construidos INTEGER,
                m2_terreno INTEGER,
                imagen_url TEXT,
                descripcion TEXT,
                amenidades TEXT,
                plano_url TEXT,
                es_promocion BOOLEAN DEFAULT 0,
                es_preventa BOOLEAN DEFAULT 0,
                fecha_scraping TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha_inicio TIMESTAMP,
                fecha_fin TIMESTAMP,
                propiedades_encontradas INTEGER,
                propiedades_nuevas INTEGER,
                propiedades_actualizadas INTEGER,
                errores INTEGER DEFAULT 0
            )
        ''')
        
        conn.commit()
        conn.close()
        print(f"✓ Base de datos inicializada: {self.db_path}")
    
    def extraer_precio(self, texto):
        """Extrae el precio numérico de un texto."""
        if not texto:
            return None
        numeros = re.findall(r'[\d,]+', texto.replace('$', ''))
        if numeros:
            try:
                return int(numeros[0].replace(',', ''))
            except ValueError:
                return None
        return None
    
    def extraer_numero(self, texto):
        """Extrae el primer número encontrado en un texto."""
        if not texto:
            return None
        numeros = re.findall(r'\d+', str(texto))
        if numeros:
            return int(numeros[0])
        return None
    
    async def scrape_con_playwright(self, solo_nuevas=False):
        """Ejecuta el scraping usando Playwright."""
        from playwright.async_api import async_playwright
        
        print("=" * 60)
        print("🏠 GP Vivienda Scraper v2 - Nuevo León")
        print("=" * 60)
        
        fecha_inicio = datetime.now()
        propiedades_nuevas = 0
        propiedades_actualizadas = 0
        errores = 0
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = await context.new_page()
            
            # 1. Obtener listado principal
            print(f"\n📄 Obteniendo listado de propiedades...")
            try:
                await page.goto(NUVO_LEON_URL, wait_until='networkidle', timeout=60000)
                await page.wait_for_timeout(3000)  # Esperar carga JS
                
                # Extraer todas las URLs de propiedades
                urls = await page.eval_on_selector_all('a[href*="casas-venta-"], a[href*="modelo-"], a[href*="residencial-"]', 
                    'elements => elements.map(e => e.href).filter(href => href.includes("gpvivienda.com"))')
                
                # Filtrar URLs únicas y válidas
                urls_unicas = []
                for url in urls:
                    if url not in urls_unicas and not url.endswith('/casas-venta-nuevo-leon/'):
                        urls_unicas.append(url)
                
                # Combinar con URLs conocidas
                todas_urls = list(set(urls_unicas + PROPIEDADES_CONOCIDAS))
                
            except Exception as e:
                print(f"  ⚠ Error obteniendo listado: {e}")
                print(f"  ℹ Usando URLs conocidas como respaldo")
                todas_urls = PROPIEDADES_CONOCIDAS
            
            print(f"✓ Se encontraron {len(todas_urls)} propiedades")
            
            # 2. Procesar cada propiedad
            print(f"\n🔍 Procesando propiedades individuales...")
            for i, url in enumerate(todas_urls, 1):
                print(f"\n  [{i}/{len(todas_urls)}] {url}")
                
                # Si solo queremos nuevas y ya existe, saltar
                if solo_nuevas and self.propiedad_existe(url):
                    print(f"    ⏭ Ya existe en la base de datos")
                    continue
                
                try:
                    await page.goto(url, wait_until='networkidle', timeout=60000)
                    await page.wait_for_timeout(2000)
                    
                    # Extraer datos con JavaScript
                    datos = await page.evaluate('''() => {
                        const data = {
                            url: window.location.href,
                            titulo: document.querySelector('h1')?.textContent?.trim() || '',
                            modelo: '',
                            fraccionamiento: '',
                            ciudad: '',
                            precio_texto: '',
                            precio: null,
                            recamaras: null,
                            banos: '',
                            m2_construidos: null,
                            m2_terreno: null,
                            imagen_url: '',
                            descripcion: '',
                            amenidades: '',
                            plano_url: '',
                            es_promocion: false,
                            es_preventa: false
                        };
                        
                        // Buscar modelo
                        const modeloMatch = document.body.textContent.match(/Modelo\s+([^\n]+)/i);
                        if (modeloMatch) data.modelo = modeloMatch[1].trim();
                        
                        // Buscar precio
                        const precioElem = document.querySelector('p:contains("$")') || 
                                          Array.from(document.querySelectorAll('p')).find(p => p.textContent.includes('$'));
                        if (precioElem) {
                            data.precio_texto = precioElem.textContent.trim();
                            const precioMatch = precioElem.textContent.match(/\$([\d,]+)/);
                            if (precioMatch) data.precio = parseInt(precioMatch[1].replace(/,/g, ''));
                        }
                        
                        // Buscar características en listas
                        const listItems = document.querySelectorAll('li');
                        listItems.forEach(li => {
                            const text = li.textContent;
                            if (text.includes('Recámara') || (/^\d+$/.test(text.trim()) && parseInt(text) < 10)) {
                                const num = text.match(/\d+/);
                                if (num && !data.recamaras) data.recamaras = parseInt(num[0]);
                            }
                            if (text.includes('Baño') || text.includes('½')) {
                                if (!data.banos) data.banos = text.trim();
                            }
                            if (text.includes('m²')) {
                                const num = text.match(/(\d+)\s*m²/);
                                if (num) {
                                    if (text.toLowerCase().includes('terreno')) {
                                        data.m2_terreno = parseInt(num[1]);
                                    } else if (text.toLowerCase().includes('constr')) {
                                        data.m2_construidos = parseInt(num[1]);
                                    } else if (!data.m2_construidos) {
                                        data.m2_construidos = parseInt(num[1]);
                                    }
                                }
                            }
                        });
                        
                        // Buscar en divs también
                        const divs = document.querySelectorAll('div');
                        divs.forEach(div => {
                            const text = div.textContent;
                            if (text.includes('m² Constr')) {
                                const match = text.match(/(\d+)\s*m²\s*Constr/i);
                                if (match) data.m2_construidos = parseInt(match[1]);
                            }
                            if (text.includes('m² Terreno')) {
                                const match = text.match(/(\d+)\s*m²\s*Terreno/i);
                                if (match) data.m2_terreno = parseInt(match[1]);
                            }
                        });
                        
                        // Imagen principal
                        const img = document.querySelector('img[src*=".jpg"], img[src*=".jpeg"], img[src*=".png"], img[src*=".webp"]');
                        if (img) data.imagen_url = img.src;
                        
                        // Descripción - buscar párrafos largos
                        const paragraphs = document.querySelectorAll('p');
                        paragraphs.forEach(p => {
                            const text = p.textContent.trim();
                            if (text.length > 100 && !text.includes('$')) {
                                data.descripcion = text;
                            }
                        });
                        
                        // Promoción y preventa
                        const bodyText = document.body.textContent.toLowerCase();
                        data.es_promocion = bodyText.includes('promoción') || bodyText.includes('promocion');
                        data.es_preventa = bodyText.includes('preventa');
                        
                        return data;
                    }''')
                    
                    # Extraer ciudad del breadcrumb
                    html_content = await page.content()
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html_content, 'lxml')
                    
                    # Buscar ciudad en breadcrumb
                    breadcrumbs = soup.find_all('a', href=re.compile(r'/casas-venta-'))
                    for bc in breadcrumbs:
                        texto = bc.get_text(strip=True)
                        if texto and 'Casas en venta' in texto:
                            datos['ciudad'] = texto.replace('Casas en venta ', '').strip()
                            break
                    
                    # Buscar fraccionamiento
                    frac_link = soup.find('a', href=re.compile(r'residencial|fraccionamiento', re.I))
                    if frac_link:
                        datos['fraccionamiento'] = frac_link.get_text(strip=True)
                    
                    # Re-procesar precio si no se encontró
                    if not datos['precio']:
                        precio_elem = soup.find('p', text=re.compile(r'\$[\d,]+'))
                        if precio_elem:
                            datos['precio_texto'] = precio_elem.get_text(strip=True)
                            datos['precio'] = self.extraer_precio(datos['precio_texto'])
                    
                    # Mostrar resumen
                    print(f"    📍 {datos.get('ciudad', 'N/A')} - {datos.get('fraccionamiento', 'N/A')}")
                    print(f"    🏠 {datos.get('modelo') or 'Modelo no especificado'}")
                    if datos.get('precio'):
                        print(f"    💰 ${datos['precio']:,}")
                    else:
                        print(f"    💰 Precio no disponible")
                    print(f"    🛏 {datos.get('recamaras')} rec | 🚿 {datos.get('banos')} baños | 📐 {datos.get('m2_construidos')} m²")
                    
                    # Guardar en base de datos
                    if self.guardar_propiedad(datos):
                        if self.propiedad_existe(url):
                            propiedades_actualizadas += 1
                        else:
                            propiedades_nuevas += 1
                    
                    self.propiedades.append(datos)
                    
                except Exception as e:
                    print(f"    ⚠ Error: {e}")
                    errores += 1
            
            await browser.close()
        
        # 3. Registrar log
        fecha_fin = datetime.now()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO scraping_log 
            (fecha_inicio, fecha_fin, propiedades_encontradas, propiedades_nuevas, propiedades_actualizadas, errores)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (fecha_inicio, fecha_fin, len(todas_urls), propiedades_nuevas, propiedades_actualizadas, errores))
        conn.commit()
        conn.close()
        
        # 4. Resumen
        print("\n" + "=" * 60)
        print("📊 RESUMEN DEL SCRAPING")
        print("=" * 60)
        print(f"⏱ Duración: {fecha_fin - fecha_inicio}")
        print(f"🔍 Propiedades encontradas: {len(todas_urls)}")
        print(f"✨ Propiedades nuevas: {propiedades_nuevas}")
        print(f"🔄 Propiedades actualizadas: {propiedades_actualizadas}")
        print(f"⚠ Errores: {errores}")
        print(f"💾 Base de datos: {self.db_path}")
        print("=" * 60)
    
    def guardar_propiedad(self, datos):
        """Guarda o actualiza una propiedad en la base de datos."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO propiedades 
                (url, titulo, modelo, fraccionamiento, ciudad, estado, precio, precio_texto,
                 recamaras, banos, m2_construidos, m2_terreno, imagen_url, descripcion,
                 amenidades, plano_url, es_promocion, es_preventa, fecha_scraping)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    titulo = excluded.titulo,
                    modelo = excluded.modelo,
                    fraccionamiento = excluded.fraccionamiento,
                    ciudad = excluded.ciudad,
                    precio = excluded.precio,
                    precio_texto = excluded.precio_texto,
                    recamaras = excluded.recamaras,
                    banos = excluded.banos,
                    m2_construidos = excluded.m2_construidos,
                    m2_terreno = excluded.m2_terreno,
                    imagen_url = excluded.imagen_url,
                    descripcion = excluded.descripcion,
                    amenidades = excluded.amenidades,
                    plano_url = excluded.plano_url,
                    es_promocion = excluded.es_promocion,
                    es_preventa = excluded.es_preventa,
                    fecha_actualizacion = CURRENT_TIMESTAMP
            ''', (
                datos.get('url'), datos.get('titulo'), datos.get('modelo'), 
                datos.get('fraccionamiento'), datos.get('ciudad'), 'Nuevo León',
                datos.get('precio'), datos.get('precio_texto'),
                datos.get('recamaras'), datos.get('banos'), 
                datos.get('m2_construidos'), datos.get('m2_terreno'),
                datos.get('imagen_url'), datos.get('descripcion'), 
                datos.get('amenidades'), datos.get('plano_url'),
                datos.get('es_promocion', False), datos.get('es_preventa', False),
                datetime.now()
            ))
            
            conn.commit()
            return True
            
        except sqlite3.Error as e:
            print(f"  ⚠ Error de base de datos: {e}")
            return False
        finally:
            conn.close()
    
    def propiedad_existe(self, url):
        """Verifica si una propiedad ya existe en la base de datos."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM propiedades WHERE url = ?', (url,))
        existe = cursor.fetchone() is not None
        conn.close()
        return existe
    
    def exportar_excel(self, output_path=EXCEL_PATH):
        """Exporta los datos a Excel."""
        try:
            import pandas as pd
        except ImportError:
            print("⚠ pandas no está instalado. Instálalo con: pip install pandas openpyxl")
            return
        
        conn = sqlite3.connect(self.db_path)
        
        # Leer datos
        df = pd.read_sql_query('''
            SELECT 
                titulo as 'Título',
                modelo as 'Modelo',
                fraccionamiento as 'Fraccionamiento',
                ciudad as 'Ciudad',
                precio as 'Precio',
                recamaras as 'Recámaras',
                banos as 'Baños',
                m2_construidos as 'm² Construidos',
                m2_terreno as 'm² Terreno',
                es_promocion as 'Promoción',
                es_preventa as 'Preventa',
                url as 'URL',
                imagen_url as 'URL Imagen',
                descripcion as 'Descripción',
                fecha_scraping as 'Fecha Scraping'
            FROM propiedades
            ORDER BY precio ASC
        ''', conn)
        
        conn.close()
        
        if df.empty:
            print("⚠ No hay datos para exportar")
            return
        
        # Formatear columnas
        df['Precio'] = df['Precio'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else '')
        df['Promoción'] = df['Promoción'].apply(lambda x: 'Sí' if x else 'No')
        df['Preventa'] = df['Preventa'].apply(lambda x: 'Sí' if x else 'No')
        
        # Guardar Excel
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Propiedades', index=False)
            
            # Ajustar anchos de columna
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
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"✓ Excel exportado: {output_path}")
        print(f"  Total de propiedades: {len(df)}")
    
    def obtener_estadisticas(self):
        """Muestra estadísticas de la base de datos."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total de propiedades
        cursor.execute('SELECT COUNT(*) FROM propiedades')
        total = cursor.fetchone()[0]
        
        if total == 0:
            print("⚠ No hay propiedades en la base de datos")
            conn.close()
            return
        
        # Por ciudad
        cursor.execute('''
            SELECT ciudad, COUNT(*) as count, AVG(precio) as avg_price 
            FROM propiedades 
            WHERE precio IS NOT NULL AND ciudad IS NOT NULL
            GROUP BY ciudad 
            ORDER BY count DESC
        ''')
        por_ciudad = cursor.fetchall()
        
        # Rango de precios
        cursor.execute('''
            SELECT MIN(precio), MAX(precio), AVG(precio) 
            FROM propiedades 
            WHERE precio IS NOT NULL
        ''')
        precios = cursor.fetchone()
        
        # Promociones y preventas
        cursor.execute('SELECT COUNT(*) FROM propiedades WHERE es_promocion = 1')
        promociones = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM propiedades WHERE es_preventa = 1')
        preventas = cursor.fetchone()[0]
        
        # Historial de scraping
        cursor.execute('''
            SELECT fecha_inicio, propiedades_encontradas, propiedades_nuevas 
            FROM scraping_log 
            ORDER BY fecha_inicio DESC 
            LIMIT 5
        ''')
        historial = cursor.fetchall()
        
        conn.close()
        
        print("\n" + "=" * 60)
        print("📈 ESTADÍSTICAS DE LA BASE DE DATOS")
        print("=" * 60)
        print(f"🏠 Total de propiedades: {total}")
        
        if por_ciudad:
            print(f"\n📍 Por ciudad:")
            for ciudad, count, avg in por_ciudad:
                avg_str = f"${avg:,.0f}" if avg else "N/A"
                print(f"   • {ciudad}: {count} propiedades (promedio: {avg_str})")
        
        if precios and precios[0]:
            print(f"\n💰 Rango de precios:")
            print(f"   • Mínimo: ${precios[0]:,.0f}")
            print(f"   • Máximo: ${precios[1]:,.0f}")
            print(f"   • Promedio: ${precios[2]:,.0f}")
        
        print(f"\n🏷 Promociones: {promociones} | 🆕 Preventas: {preventas}")
        
        if historial:
            print(f"\n📜 Últimos scraping:")
            for fecha, encontradas, nuevas in historial:
                print(f"   • {fecha}: {encontradas} encontradas, {nuevas} nuevas")
        
        print("=" * 60)


async def main():
    parser = argparse.ArgumentParser(description='GP Vivienda Scraper v2 - Nuevo León')
    parser.add_argument('--update', action='store_true', help='Solo actualizar propiedades nuevas')
    parser.add_argument('--export', action='store_true', help='Exportar a Excel')
    parser.add_argument('--stats', action='store_true', help='Mostrar estadísticas')
    
    args = parser.parse_args()
    
    scraper = GPViviendaScraper()
    
    if args.stats:
        scraper.obtener_estadisticas()
    elif args.export:
        scraper.exportar_excel()
    else:
        await scraper.scrape_con_playwright(solo_nuevas=args.update)
        scraper.exportar_excel()
        scraper.obtener_estadisticas()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
