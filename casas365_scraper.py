#!/usr/bin/env python3
"""
Casas 365 Scraper - Casas en Venta
Script para extraer propiedades de casas365.mx con soporte para MySQL (Laragon)

INSTALACIÓN:
    pip install requests beautifulsoup4 pymysql pandas openpyxl lxml

CONFIGURACIÓN MYSQL (Laragon):
    - Host: localhost
    - Puerto: 3306
    - Usuario: root (o el que tengas configurado)
    - Contraseña: (vacía por defecto en Laragon)
    - Base de datos: casas365 (se crea automáticamente)

USO:
    python casas365_scraper.py                    # Scrapear todas las propiedades
    python casas365_scraper.py --limit 10         # Limitar a 10 propiedades
    python casas365_scraper.py --export           # Exportar a Excel
    python casas365_scraper.py --stats            # Ver estadísticas
    python casas365_scraper.py --table            # Ver tabla de propiedades

AUTOR: Generado para uso personal
FECHA: 2026-02-16
"""

import requests
from bs4 import BeautifulSoup
import re
import argparse
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
from pathlib import Path

# Configuración del sitio
BASE_URL = "https://casas365.mx"
SEARCH_URL = "https://casas365.mx/busqueda-avanzada/?filter_search_type%5B%5D=casa&filter_search_action%5B%5D=casas-en-venta&advanced_city=&submit=Buscar&elementor_form_id=18642"

# Headers para simular navegador
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8',
}

# Configuración MySQL (Laragon)
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '',  # Cambiar si tienes contraseña en Laragon
    'database': 'casas365',
    'charset': 'utf8mb4'
}


class Casas365Scraper:
    def __init__(self, mysql_config=MYSQL_CONFIG):
        self.mysql_config = mysql_config
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.db_connection = None
        self.db_cursor = None
        self.connect_mysql()
        self.init_database()
    
    def connect_mysql(self):
        """Conecta a la base de datos MySQL."""
        try:
            import pymysql
            # Primero conectamos sin base de datos para crearla si no existe
            temp_config = self.mysql_config.copy()
            temp_config.pop('database', None)
            
            conn = pymysql.connect(**temp_config)
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.mysql_config['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            conn.commit()
            cursor.close()
            conn.close()
            
            # Ahora conectamos a la base de datos
            self.db_connection = pymysql.connect(**self.mysql_config)
            self.db_cursor = self.db_connection.cursor()
            print(f"✓ Conectado a MySQL - Base de datos: {self.mysql_config['database']}")
            
        except ImportError:
            print("❌ Error: pymysql no está instalado. Ejecuta: pip install pymysql")
            raise
        except Exception as e:
            print(f"❌ Error conectando a MySQL: {e}")
            print(f"   Verifica que Laragon/MySQL esté corriendo en {self.mysql_config['host']}:{self.mysql_config['port']}")
            raise
    
    def init_database(self):
        """Inicializa la tabla de propiedades en MySQL."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS propiedades (
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        self.db_cursor.execute(create_table_sql)
        
        # Tabla de log de scraping
        create_log_sql = """
        CREATE TABLE IF NOT EXISTS scraping_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            fecha_inicio TIMESTAMP NULL,
            fecha_fin TIMESTAMP NULL,
            propiedades_encontradas INT DEFAULT 0,
            propiedades_nuevas INT DEFAULT 0,
            propiedades_actualizadas INT DEFAULT 0,
            errores INT DEFAULT 0,
            notas TEXT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        
        self.db_cursor.execute(create_log_sql)
        self.db_connection.commit()
        print("✓ Tablas creadas/verificadas")
    
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
                time.sleep(1)
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.text
            except Exception as e:
                print(f"  ⚠ Error (intento {i+1}/{retries}): {e}")
                time.sleep(2)
        return None
    
    def parsear_listado(self, html):
        """Extrae URLs de propiedades del listado."""
        soup = BeautifulSoup(html, 'lxml')
        urls = []
        
        for link in soup.find_all('a', href=re.compile(r'/propiedades/[^/]+/$')):
            href = link.get('href', '')
            if href and 'propiedades' in href:
                full_url = urljoin(BASE_URL, href)
                if full_url not in urls:
                    urls.append(full_url)
        
        return urls
    
    def parsear_propiedad(self, html, url):
        """Extrae datos de una propiedad."""
        soup = BeautifulSoup(html, 'lxml')
        
        datos = {
            'url': url,
            'titulo': '',
            'tipo': '',
            'accion': '',
            'estado': '',
            'precio': None,
            'moneda': 'MXN',
            'calle': '',
            'colonia': '',
            'ciudad': '',
            'estado_geo': '',
            'pais': 'México',
            'recamaras': None,
            'banos': None,
            'habitaciones': None,
            'terreno_m2': None,
            'construccion_m2': None,
            'plantas': None,
            'estacionamientos': None,
            'clase_energetica': '',
            'descripcion': '',
            'imagenes': '',
            'latitud': None,
            'longitud': None,
            'agente_nombre': '',
            'agente_telefono': '',
            'agente_whatsapp': '',
            'agente_email': '',
            'fecha_publicacion': None
        }
        
        try:
            # Título
            h1 = soup.find('h1')
            if h1:
                datos['titulo'] = h1.get_text(strip=True)
            
            # Tipo, Acción, Estado (de las etiquetas)
            for tag in soup.find_all('a', href=re.compile(r'/listados/|/tipos/|/estado/')):
                href = tag.get('href', '')
                text = tag.get_text(strip=True)
                if '/listados/' in href:
                    datos['tipo'] = text
                elif '/tipos/' in href:
                    datos['accion'] = text
                elif '/estado/' in href:
                    if not datos['estado']:
                        datos['estado'] = text
            
            # Precio
            precio_elem = soup.find('div', class_=re.compile(r'price|precio', re.I))
            if precio_elem:
                precio_text = precio_elem.get_text(strip=True)
                datos['precio'] = self.extraer_numero(precio_text)
                if 'USD' in precio_text or 'usd' in precio_text.lower():
                    datos['moneda'] = 'USD'
            
            # Ubicación
            calle_elem = soup.find('div', class_=re.compile(r'address|direccion', re.I))
            if calle_elem:
                datos['calle'] = calle_elem.get_text(strip=True)
            
            # Ciudad y Colonia del breadcrumb
            for link in soup.find_all('a', href=re.compile(r'/ciudad/|/zona/')):
                href = link.get('href', '')
                text = link.get_text(strip=True)
                if '/ciudad/' in href:
                    datos['ciudad'] = text
                elif '/zona/' in href:
                    datos['colonia'] = text
            
            # Estado geográfico (Nuevo León)
            for link in soup.find_all('a', href=re.compile(r'/estado/')):
                text = link.get_text(strip=True)
                if text and len(text) > 3:
                    datos['estado_geo'] = text
                    break
            
            # Características del resumen
            page_text = soup.get_text()
            
            # Recámaras
            match = re.search(r'(\d+)\s*Rec[áa]maras?', page_text, re.I)
            if match:
                datos['recamaras'] = int(match.group(1))
            
            # Baños (puede ser 3.5, 2.5, etc.)
            match = re.search(r'(\d+(?:\.\d+)?)\s*Baños?', page_text, re.I)
            if match:
                datos['banos'] = float(match.group(1))
            
            # Habitaciones
            match = re.search(r'(\d+)\s*Habitaciones?', page_text, re.I)
            if match:
                datos['habitaciones'] = int(match.group(1))
            
            # Metraje
            for elem in soup.find_all(text=re.compile(r'(\d+(?:\.\d+)?)\s*m\s*²?')):
                match = re.search(r'(\d+(?:\.\d+)?)\s*m\s*²?', elem)
                if match:
                    val = float(match.group(1))
                    # El primero suele ser construcción, el segundo terreno
                    if datos['construccion_m2'] is None:
                        datos['construccion_m2'] = val
                    elif datos['terreno_m2'] is None and val != datos['construccion_m2']:
                        datos['terreno_m2'] = val
            
            # Buscar en descripción
            desc_elem = soup.find('div', class_=re.compile(r'description|descripcion', re.I))
            if desc_elem:
                datos['descripcion'] = desc_elem.get_text(strip=True)[:2000]
                
                # Extraer plantas de la descripción
                match = re.search(r'(\d+|TRES|DOS|UNA)\s*PLANTAS?', datos['descripcion'], re.I)
                if match:
                    plantas_text = match.group(1).upper()
                    plantas_map = {'UNA': 1, 'DOS': 2, 'TRES': 3, 'CUATRO': 4, 'CINCO': 5}
                    if plantas_text in plantas_map:
                        datos['plantas'] = plantas_map[plantas_text]
                    else:
                        datos['plantas'] = int(plantas_text)
                
                # Estacionamientos
                match = re.search(r'(\d+)\s*(?:auto|carro|estacionamiento|cochera)', datos['descripcion'], re.I)
                if match:
                    datos['estacionamientos'] = int(match.group(1))
            
            # Clase energética
            clase_elem = soup.find(text=re.compile(r'Clase energética', re.I))
            if clase_elem:
                match = re.search(r'Clase\s*energética\s*[:\-]?\s*([A-G])', page_text, re.I)
                if match:
                    datos['clase_energetica'] = match.group(1).upper()
            
            # Coordenadas del mapa
            map_link = soup.find('a', href=re.compile(r'google\.com/maps'))
            if map_link:
                href = map_link.get('href', '')
                match = re.search(r'll=(-?\d+\.\d+),(-?\d+\.\d+)', href)
                if match:
                    datos['latitud'] = float(match.group(1))
                    datos['longitud'] = float(match.group(2))
            
            # Imágenes
            imagenes = []
            for img in soup.find_all('img', src=re.compile(r'wp-content/uploads')):
                src = img.get('src', '')
                if src and '120x120' not in src:  # Evitar thumbnails
                    imagenes.append(src)
            datos['imagenes'] = ', '.join(imagenes[:10])
            
            # Agente/Contacto
            for elem in soup.find_all(text=re.compile(r'\+52\s*\d+')):
                telefono = re.search(r'\+52\s*\d[\d\s\-]+', elem)
                if telefono:
                    datos['agente_telefono'] = telefono.group(0).replace(' ', '').replace('-', '')
                    break
            
            # WhatsApp
            wa_link = soup.find('a', href=re.compile(r'wa\.me'))
            if wa_link:
                match = re.search(r'wa\.me/(\d+)', wa_link.get('href', ''))
                if match:
                    datos['agente_whatsapp'] = '+' + match.group(1)
            
            # Email
            email_elem = soup.find('a', href=re.compile(r'mailto:'))
            if email_elem:
                datos['agente_email'] = email_elem.get('href', '').replace('mailto:', '')
            
            # Nombre del agente
            agente_elem = soup.find(text=re.compile(r'CASAS 365', re.I))
            if agente_elem:
                datos['agente_nombre'] = 'CASAS 365'
            
        except Exception as e:
            print(f"  ⚠ Error parseando: {e}")
        
        return datos
    
    def guardar_propiedad(self, datos):
        """Guarda una propiedad en MySQL."""
        insert_sql = """
        INSERT INTO propiedades 
        (url, titulo, tipo, accion, estado, precio, moneda, calle, colonia, ciudad, 
         estado_geo, pais, recamaras, banos, habitaciones, terreno_m2, construccion_m2,
         plantas, estacionamientos, clase_energetica, descripcion, imagenes, latitud, 
         longitud, agente_nombre, agente_telefono, agente_whatsapp, agente_email, fecha_publicacion)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        titulo = VALUES(titulo), tipo = VALUES(tipo), accion = VALUES(accion), 
        estado = VALUES(estado), precio = VALUES(precio), moneda = VALUES(moneda),
        calle = VALUES(calle), colonia = VALUES(colonia), ciudad = VALUES(ciudad),
        estado_geo = VALUES(estado_geo), recamaras = VALUES(recamaras), 
        banos = VALUES(banos), habitaciones = VALUES(habitaciones),
        terreno_m2 = VALUES(terreno_m2), construccion_m2 = VALUES(construccion_m2),
        plantas = VALUES(plantas), estacionamientos = VALUES(estacionamientos),
        clase_energetica = VALUES(clase_energetica), descripcion = VALUES(descripcion),
        imagenes = VALUES(imagenes), latitud = VALUES(latitud), longitud = VALUES(longitud),
        agente_nombre = VALUES(agente_nombre), agente_telefono = VALUES(agente_telefono),
        agente_whatsapp = VALUES(agente_whatsapp), agente_email = VALUES(agente_email),
        fecha_publicacion = VALUES(fecha_publicacion),
        fecha_actualizacion = CURRENT_TIMESTAMP
        """
        
        try:
            values = (
                datos['url'], datos['titulo'], datos['tipo'], datos['accion'], datos['estado'],
                datos['precio'], datos['moneda'], datos['calle'], datos['colonia'], datos['ciudad'],
                datos['estado_geo'], datos['pais'], datos['recamaras'], datos['banos'],
                datos['habitaciones'], datos['terreno_m2'], datos['construccion_m2'],
                datos['plantas'], datos['estacionamientos'], datos['clase_energetica'],
                datos['descripcion'], datos['imagenes'], datos['latitud'], datos['longitud'],
                datos['agente_nombre'], datos['agente_telefono'], datos['agente_whatsapp'],
                datos['agente_email'], datos['fecha_publicacion']
            )
            
            self.db_cursor.execute(insert_sql, values)
            self.db_connection.commit()
            return True
            
        except Exception as e:
            print(f"  ⚠ Error guardando en MySQL: {e}")
            return False
    
    def scrape(self, limit=None):
        """Ejecuta el scraping."""
        print("=" * 70)
        print("🏠 Casas 365 Scraper - MySQL Edition")
        print("=" * 70)
        
        fecha_inicio = datetime.now()
        
        # Obtener listado
        print(f"\n📄 Obteniendo listado de propiedades...")
        html = self.obtener_pagina(SEARCH_URL)
        
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
        errores = 0
        
        for i, prop_url in enumerate(urls, 1):
            print(f"\n  [{i}/{len(urls)}] {prop_url.split('/')[-2]}")
            
            html = self.obtener_pagina(prop_url)
            if not html:
                errores += 1
                continue
            
            datos = self.parsear_propiedad(html, prop_url)
            
            # Mostrar resumen
            print(f"    📍 {datos['colonia'] or 'N/A'}, {datos['ciudad'] or 'N/A'}")
            print(f"    🏠 {datos['titulo'][:50] if datos['titulo'] else 'N/A'}")
            if datos['precio']:
                print(f"    💰 ${datos['precio']:,.0f} {datos['moneda']}")
            print(f"    📐 {datos['construccion_m2'] or '?'} m² constr | 🛏 {datos['recamaras'] or '?'} rec | 🚿 {datos['banos'] or '?'} baños")
            
            if self.guardar_propiedad(datos):
                guardadas += 1
            else:
                errores += 1
        
        # Registrar log
        fecha_fin = datetime.now()
        log_sql = """
        INSERT INTO scraping_log (fecha_inicio, fecha_fin, propiedades_encontradas, propiedades_nuevas, errores)
        VALUES (%s, %s, %s, %s, %s)
        """
        self.db_cursor.execute(log_sql, (fecha_inicio, fecha_fin, len(urls), guardadas, errores))
        self.db_connection.commit()
        
        # Resumen
        print("\n" + "=" * 70)
        print("📊 RESUMEN")
        print("=" * 70)
        print(f"⏱ Duración: {fecha_fin - fecha_inicio}")
        print(f"🔍 Propiedades encontradas: {len(urls)}")
        print(f"✅ Guardadas en MySQL: {guardadas}")
        print(f"⚠ Errores: {errores}")
        print(f"💾 Base de datos: {self.mysql_config['database']}")
        print("=" * 70)
    
    def exportar_excel(self, output_path='casas365_propiedades.xlsx'):
        """Exporta los datos a Excel."""
        try:
            import pandas as pd
        except ImportError:
            print("⚠ Instala pandas: pip install pandas openpyxl")
            return
        
        query = """
        SELECT 
            titulo as 'Título',
            colonia as 'Colonia',
            ciudad as 'Ciudad',
            estado_geo as 'Estado',
            precio as 'Precio',
            moneda as 'Moneda',
            terreno_m2 as 'm² Terreno',
            construccion_m2 as 'm² Construcción',
            recamaras as 'Recámaras',
            banos as 'Baños',
            plantas as 'Plantas',
            estacionamientos as 'Estacionamientos',
            clase_energetica as 'Clase Energética',
            agente_telefono as 'Teléfono',
            url as 'URL'
        FROM propiedades
        ORDER BY precio ASC
        """
        
        df = pd.read_sql(query, self.db_connection)
        
        if df.empty:
            print("⚠ No hay datos para exportar")
            return
        
        # Formatear precio
        df['Precio'] = df.apply(lambda x: f"${x['Precio']:,.0f} {x['Moneda']}" if pd.notna(x['Precio']) else '', axis=1)
        df.drop('Moneda', axis=1, inplace=True)
        
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
        self.db_cursor.execute('SELECT COUNT(*) FROM propiedades')
        total = self.db_cursor.fetchone()[0]
        
        if total == 0:
            print("⚠ No hay propiedades en la base de datos")
            return
        
        self.db_cursor.execute('''
            SELECT AVG(precio), MIN(precio), MAX(precio),
                   AVG(construccion_m2), AVG(terreno_m2)
            FROM propiedades 
            WHERE precio IS NOT NULL
        ''')
        stats = self.db_cursor.fetchone()
        
        self.db_cursor.execute('''
            SELECT ciudad, COUNT(*), AVG(precio) 
            FROM propiedades 
            GROUP BY ciudad 
            ORDER BY COUNT(*) DESC
        ''')
        por_ciudad = self.db_cursor.fetchall()
        
        print("\n" + "=" * 70)
        print("📈 ESTADÍSTICAS DE LA BASE DE DATOS")
        print("=" * 70)
        print(f"🏠 Total de propiedades: {total}")
        print(f"\n💰 Precios:")
        print(f"   Promedio: ${stats[0]:,.0f}" if stats[0] else "   N/A")
        print(f"   Mínimo: ${stats[1]:,.0f}" if stats[1] else "   N/A")
        print(f"   Máximo: ${stats[2]:,.0f}" if stats[2] else "   N/A")
        print(f"\n📐 Metraje promedio:")
        print(f"   Construcción: {stats[3]:.1f} m²" if stats[3] else "   N/A")
        print(f"   Terreno: {stats[4]:.1f} m²" if stats[4] else "   N/A")
        print(f"\n📍 Por ciudad:")
        for ciudad, count, precio in por_ciudad:
            precio_str = f"${precio:,.0f}" if precio else "N/A"
            print(f"   • {ciudad or 'N/A'}: {count} propiedades ({precio_str} prom)")
        print("=" * 70)
    
    def mostrar_tabla(self, limit=20):
        """Muestra propiedades en formato tabla."""
        self.db_cursor.execute('''
            SELECT colonia, ciudad, precio, construccion_m2, recamaras, banos, titulo
            FROM propiedades 
            ORDER BY precio 
            LIMIT %s
        ''', (limit,))
        rows = self.db_cursor.fetchall()
        
        if not rows:
            print("⚠ No hay propiedades")
            return
        
        print("\n" + "=" * 110)
        print("🏠 PROPIEDADES - CASAS 365")
        print("=" * 110)
        print(f"{'Colonia':<25} {'Ciudad':<15} {'Precio':<14} {'m²':<8} {'Rec':<4} {'Baños':<6} {'Título':<30}")
        print("-" * 110)
        
        for row in rows:
            colonia = (row[0] or 'N/A')[:23]
            ciudad = (row[1] or 'N/A')[:13]
            precio = f"${row[2]:,.0f}" if row[2] else 'N/A'
            m2 = f"{row[3]:.0f}" if row[3] else '-'
            rec = row[4] if row[4] else '-'
            banos = row[5] if row[5] else '-'
            titulo = (row[6] or 'N/A')[:28]
            print(f"{colonia:<25} {ciudad:<15} {precio:<14} {m2:<8} {rec:<4} {banos:<6} {titulo:<30}")
        
        print("=" * 110)
    
    def close(self):
        """Cierra la conexión a MySQL."""
        if self.db_cursor:
            self.db_cursor.close()
        if self.db_connection:
            self.db_connection.close()
            print("\n✓ Conexión a MySQL cerrada")


def main():
    parser = argparse.ArgumentParser(description='Casas 365 Scraper - MySQL')
    parser.add_argument('--limit', type=int, help='Limitar número de propiedades')
    parser.add_argument('--export', action='store_true', help='Exportar a Excel')
    parser.add_argument('--stats', action='store_true', help='Mostrar estadísticas')
    parser.add_argument('--table', action='store_true', help='Mostrar tabla')
    parser.add_argument('--host', default='localhost', help='Host MySQL (default: localhost)')
    parser.add_argument('--user', default='root', help='Usuario MySQL (default: root)')
    parser.add_argument('--password', default='', help='Contraseña MySQL (default: vacía)')
    parser.add_argument('--database', default='casas365', help='Base de datos (default: casas365)')
    
    args = parser.parse_args()
    
    # Actualizar configuración MySQL
    config = MYSQL_CONFIG.copy()
    config['host'] = args.host
    config['user'] = args.user
    config['password'] = args.password
    config['database'] = args.database
    
    scraper = None
    try:
        scraper = Casas365Scraper(mysql_config=config)
        
        if args.stats:
            scraper.mostrar_estadisticas()
        elif args.table:
            scraper.mostrar_tabla()
        elif args.export:
            scraper.exportar_excel()
        else:
            scraper.scrape(limit=args.limit)
            scraper.exportar_excel()
            scraper.mostrar_estadisticas()
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        if scraper:
            scraper.close()


if __name__ == '__main__':
    main()
