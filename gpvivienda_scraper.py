#!/usr/bin/env python3
"""
GP Vivienda Scraper - Casas en Venta Nuevo León
Script para extraer propiedades de GP Vivienda y guardar en base de datos local SQLite.

INSTALACIÓN:
    pip install requests beautifulsoup4 pandas openpyxl lxml

USO:
    python gpvivienda_scraper.py           # Ejecutar scraping completo
    python gpvivienda_scraper.py --update  # Actualizar solo propiedades nuevas
    python gpvivienda_scraper.py --export  # Exportar a Excel
    python gpvivienda_scraper.py --stats   # Mostrar estadísticas

AUTOR: Generado para uso personal con permiso del dueño del sitio
FECHA: 2026-02-16
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re
import argparse
from datetime import datetime
from urllib.parse import urljoin
from pathlib import Path

# Configuración
BASE_URL = "https://gpvivienda.com"
NUVO_LEON_URL = "https://gpvivienda.com/casas-venta-nuevo-leon/"
DB_PATH = "gpvivienda_nuevoleon.db"
EXCEL_PATH = "gpvivienda_nuevoleon.xlsx"

# Headers para simular navegador
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8',
}


class GPViviendaScraper:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.propiedades = []
        self.init_database()
    
    def init_database(self):
        """Inicializa la base de datos SQLite."""
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
        print(f"✓ Base de datos lista: {self.db_path}")
    
    def obtener_pagina(self, url, retries=3, delay=2):
        """Obtiene el contenido HTML de una URL."""
        for intento in range(retries):
            try:
                time.sleep(delay)
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.text
            except Exception as e:
                print(f"  ⚠ Error (intento {intento + 1}/{retries}): {e}")
                if intento < retries - 1:
                    time.sleep(delay * 2)
        return None
    
    def extraer_precio(self, texto):
        """Extrae el precio numérico de un texto."""
        if not texto:
            return None
        numeros = re.findall(r'[\d,]+', texto.replace('$', ''))
        if numeros:
            try:
                return int(numeros[0].replace(',', ''))
            except:
                return None
        return None
    
    def parsear_listado(self, html):
        """Extrae las URLs de propiedades del listado."""
        soup = BeautifulSoup(html, 'lxml')
        urls = []
        
        # Buscar enlaces de propiedades
        for link in soup.find_all('a', href=True):
            href = link['href']
            if any(x in href for x in ['casas-venta-', 'modelo-', 'residencial-', 'portal-', 'vistabella']):
                full_url = urljoin(BASE_URL, href)
                if full_url not in urls and not full_url.endswith('/casas-venta-nuevo-leon/'):
                    urls.append(full_url)
        
        return urls
    
    def parsear_propiedad(self, html, url):
        """Extrae los datos de una propiedad."""
        soup = BeautifulSoup(html, 'lxml')
        
        datos = {
            'url': url,
            'titulo': '',
            'modelo': '',
            'fraccionamiento': '',
            'ciudad': '',
            'precio': None,
            'precio_texto': '',
            'recamaras': None,
            'banos': '',
            'm2_construidos': None,
            'm2_terreno': None,
            'imagen_url': '',
            'descripcion': '',
            'amenidades': '',
            'plano_url': '',
            'es_promocion': False,
            'es_preventa': False
        }
        
        try:
            # Título
            h1 = soup.find('h1')
            if h1:
                datos['titulo'] = h1.get_text(strip=True)
            
            # Modelo
            modelo_match = re.search(r'Modelo\s+([^\n]+)', datos['titulo'])
            if modelo_match:
                datos['modelo'] = modelo_match.group(1).strip()
            
            # Precio
            precio_elem = soup.find('p', text=re.compile(r'\$[\d,]+'))
            if precio_elem:
                datos['precio_texto'] = precio_elem.get_text(strip=True)
                datos['precio'] = self.extraer_precio(datos['precio_texto'])
            
            # Características
            for li in soup.find_all('li'):
                text = li.get_text(strip=True)
                
                if 'Recámara' in text or (text.isdigit() and int(text) < 10):
                    nums = re.findall(r'\d+', text)
                    if nums and not datos['recamaras']:
                        datos['recamaras'] = int(nums[0])
                
                if 'Baño' in text or '½' in text:
                    if not datos['banos']:
                        datos['banos'] = text
                
                if 'm² Constr' in text:
                    nums = re.findall(r'\d+', text)
                    if nums:
                        datos['m2_construidos'] = int(nums[0])
                
                if 'm² Terreno' in text:
                    nums = re.findall(r'\d+', text)
                    if nums:
                        datos['m2_terreno'] = int(nums[0])
            
            # Ciudad
            for bc in soup.find_all('a', href=re.compile(r'/casas-venta-')):
                txt = bc.get_text(strip=True)
                if 'Casas en venta' in txt:
                    datos['ciudad'] = txt.replace('Casas en venta ', '').strip()
                    break
            
            # Fraccionamiento
            frac = soup.find('a', href=re.compile(r'residencial|fraccionamiento', re.I))
            if frac:
                datos['fraccionamiento'] = frac.get_text(strip=True)
            
            # Imagen
            img = soup.find('img', src=re.compile(r'\.(jpg|jpeg|png|webp)', re.I))
            if img:
                datos['imagen_url'] = urljoin(BASE_URL, img['src'])
            
            # Descripción
            for p in soup.find_all('p'):
                txt = p.get_text(strip=True)
                if len(txt) > 100 and '$' not in txt:
                    datos['descripcion'] = txt
                    break
            
            # Promoción/Preventa
            body_text = soup.get_text().lower()
            datos['es_promocion'] = 'promoción' in body_text or 'promocion' in body_text
            datos['es_preventa'] = 'preventa' in body_text
            
        except Exception as e:
            print(f"  ⚠ Error parseando: {e}")
        
        return datos
    
    def guardar_propiedad(self, datos):
        """Guarda una propiedad en la base de datos."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO propiedades 
                (url, titulo, modelo, fraccionamiento, ciudad, precio, precio_texto,
                 recamaras, banos, m2_construidos, m2_terreno, imagen_url, descripcion,
                 es_promocion, es_preventa)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    titulo=excluded.titulo, modelo=excluded.modelo, fraccionamiento=excluded.fraccionamiento,
                    ciudad=excluded.ciudad, precio=excluded.precio, precio_texto=excluded.precio_texto,
                    recamaras=excluded.recamaras, banos=excluded.banos, m2_construidos=excluded.m2_construidos,
                    m2_terreno=excluded.m2_terreno, imagen_url=excluded.imagen_url, descripcion=excluded.descripcion,
                    es_promocion=excluded.es_promocion, es_preventa=excluded.es_preventa,
                    fecha_actualizacion=CURRENT_TIMESTAMP
            ''', (
                datos['url'], datos['titulo'], datos['modelo'], datos['fraccionamiento'],
                datos['ciudad'], datos['precio'], datos['precio_texto'],
                datos['recamaras'], datos['banos'], datos['m2_construidos'], datos['m2_terreno'],
                datos['imagen_url'], datos['descripcion'],
                datos['es_promocion'], datos['es_preventa']
            ))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"  ⚠ Error BD: {e}")
            return False
        finally:
            conn.close()
    
    def propiedad_existe(self, url):
        """Verifica si una propiedad ya existe."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM propiedades WHERE url = ?', (url,))
        existe = cursor.fetchone() is not None
        conn.close()
        return existe
    
    def scrape(self, solo_nuevas=False):
        """Ejecuta el scraping completo."""
        print("=" * 70)
        print("🏠 GP Vivienda Scraper - Nuevo León")
        print("=" * 70)
        
        fecha_inicio = datetime.now()
        propiedades_nuevas = 0
        propiedades_actualizadas = 0
        errores = 0
        
        # Obtener listado
        print(f"\n📄 Obteniendo listado de propiedades...")
        html = self.obtener_pagina(NUVO_LEON_URL)
        
        if not html:
            print("❌ No se pudo obtener el listado")
            return
        
        urls = self.parsear_listado(html)
        print(f"✓ {len(urls)} propiedades encontradas")
        
        # Procesar cada propiedad
        print(f"\n🔍 Procesando propiedades...")
        for i, url in enumerate(urls, 1):
            print(f"\n  [{i}/{len(urls)}] {url.split('/')[-2][:50]}")
            
            if solo_nuevas and self.propiedad_existe(url):
                print(f"    ⏭ Ya existe")
                continue
            
            html = self.obtener_pagina(url)
            if not html:
                errores += 1
                continue
            
            datos = self.parsear_propiedad(html, url)
            
            # Mostrar resumen
            print(f"    📍 {datos['ciudad'] or 'N/A'} - {datos['fraccionamiento'] or 'N/A'}")
            print(f"    🏠 {datos['modelo'] or 'N/A'}")
            if datos['precio']:
                print(f"    💰 ${datos['precio']:,}")
            print(f"    🛏 {datos['recamaras'] or '?'} rec | 🚿 {datos['banos'] or '?'} baños")
            
            if self.guardar_propiedad(datos):
                propiedades_nuevas += 1
            
            self.propiedades.append(datos)
        
        # Registrar log
        fecha_fin = datetime.now()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO scraping_log 
            (fecha_inicio, fecha_fin, propiedades_encontradas, propiedades_nuevas, propiedades_actualizadas, errores)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (fecha_inicio, fecha_fin, len(urls), propiedades_nuevas, propiedades_actualizadas, errores))
        conn.commit()
        conn.close()
        
        # Resumen
        print("\n" + "=" * 70)
        print("📊 RESUMEN")
        print("=" * 70)
        print(f"⏱ Duración: {fecha_fin - fecha_inicio}")
        print(f"🔍 Propiedades: {len(urls)}")
        print(f"✨ Nuevas: {propiedades_nuevas}")
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
                ciudad as 'Ciudad',
                fraccionamiento as 'Fraccionamiento',
                modelo as 'Modelo',
                precio as 'Precio',
                recamaras as 'Recámaras',
                banos as 'Baños',
                m2_construidos as 'm² Construidos',
                m2_terreno as 'm² Terreno',
                CASE WHEN es_promocion THEN 'Sí' ELSE 'No' END as 'Promoción',
                CASE WHEN es_preventa THEN 'Sí' ELSE 'No' END as 'Preventa',
                url as 'URL'
            FROM propiedades
            ORDER BY precio ASC
        ''', conn)
        conn.close()
        
        if df.empty:
            print("⚠ No hay datos para exportar")
            return
        
        df['Precio'] = df['Precio'].apply(lambda x: f"${x:,}" if pd.notna(x) else '')
        
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
        """Muestra estadísticas de la base de datos."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM propiedades')
        total = cursor.fetchone()[0]
        
        if total == 0:
            print("⚠ No hay propiedades en la base de datos")
            conn.close()
            return
        
        cursor.execute('SELECT AVG(precio), MIN(precio), MAX(precio) FROM propiedades WHERE precio IS NOT NULL')
        avg, min_p, max_p = cursor.fetchone()
        
        cursor.execute('SELECT ciudad, COUNT(*), AVG(precio) FROM propiedades GROUP BY ciudad')
        por_ciudad = cursor.fetchall()
        
        print("\n" + "=" * 70)
        print("📈 ESTADÍSTICAS")
        print("=" * 70)
        print(f"🏠 Total: {total} propiedades")
        print(f"\n💰 Precios:")
        print(f"   Promedio: ${avg:,.0f}")
        print(f"   Mínimo: ${min_p:,.0f}")
        print(f"   Máximo: ${max_p:,.0f}")
        print(f"\n📍 Por ciudad:")
        for ciudad, count, precio in por_ciudad:
            print(f"   • {ciudad}: {count} propiedades (${precio:,.0f} prom)")
        print("=" * 70)
        
        conn.close()
    
    def mostrar_tabla(self):
        """Muestra todas las propiedades en formato tabla."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT ciudad, fraccionamiento, modelo, precio, recamaras, banos, es_promocion 
            FROM propiedades 
            ORDER BY precio
        ''')
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            print("⚠ No hay propiedades")
            return
        
        print("\n" + "=" * 100)
        print("🏠 PROPIEDADES EN NUEVO LEÓN - GP VIVIENDA")
        print("=" * 100)
        print(f"{'Ciudad':<12} {'Fraccionamiento':<28} {'Modelo':<20} {'Precio':<14} {'Rec':<4} {'Baños':<6} {'Promo':<5}")
        print("-" * 100)
        
        for row in rows:
            ciudad = row[0] or 'N/A'
            frac = row[1] or 'N/A'
            modelo = row[2] or 'N/A'
            precio = f"${row[3]:,}" if row[3] else 'N/A'
            rec = row[4] if row[4] else '-'
            banos = row[5] if row[5] else '-'
            promo = 'Sí' if row[6] else '-'
            print(f"{ciudad:<12} {frac:<28} {modelo:<20} {precio:<14} {rec:<4} {banos:<6} {promo:<5}")
        
        print("=" * 100)


def main():
    parser = argparse.ArgumentParser(description='GP Vivienda Scraper - Nuevo León')
    parser.add_argument('--update', action='store_true', help='Solo actualizar propiedades nuevas')
    parser.add_argument('--export', action='store_true', help='Exportar a Excel')
    parser.add_argument('--stats', action='store_true', help='Mostrar estadísticas')
    parser.add_argument('--table', action='store_true', help='Mostrar tabla de propiedades')
    
    args = parser.parse_args()
    
    scraper = GPViviendaScraper()
    
    if args.stats:
        scraper.mostrar_estadisticas()
    elif args.table:
        scraper.mostrar_tabla()
    elif args.export:
        scraper.exportar_excel()
    else:
        scraper.scrape(solo_nuevas=args.update)
        scraper.exportar_excel()
        scraper.mostrar_estadisticas()


if __name__ == '__main__':
    main()
