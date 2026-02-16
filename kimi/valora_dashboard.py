#!/usr/bin/env python3
"""
ValoraNL Dashboard - Monitor web ligero
Ejecuta: python valora_dashboard.py
Accede: http://localhost:5000

Muestra:
- Estadísticas en tiempo real
- Histórico de ejecuciones
- Comparativas por fuente
- Alertas de salud del sistema
"""

import os
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from functools import lru_cache
from typing import Dict, List, Any

try:
    from flask import Flask, render_template_string, jsonify
except ImportError:
    print("Flask no instalado. Instala con: pip install flask")
    raise

try:
    import pymysql
except ImportError:
    print("pymysql no instalado. Instala con: pip install pymysql")
    raise

app = Flask(__name__)

# Configuración
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': 'valoranl',
    'charset': 'utf8mb4'
}

# ============================================================================
# TEMPLATES HTML (inline para no depender de archivos externos)
# ============================================================================

DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ValoraNL - Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: #333;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }
        header {
            text-align: center;
            color: white;
            padding: 40px 0;
        }
        header h1 { font-size: 2.5em; margin-bottom: 10px; }
        header p { opacity: 0.9; font-size: 1.1em; }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-top: 30px;
        }
        .card {
            background: white;
            border-radius: 16px;
            padding: 24px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
            transition: transform 0.2s;
        }
        .card:hover { transform: translateY(-5px); }
        .card-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 16px;
        }
        .card-title {
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: #666;
        }
        .card-value {
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
        }
        .card-subtitle {
            font-size: 0.9em;
            color: #888;
            margin-top: 8px;
        }
        .status-badge {
            display: inline-block;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.75em;
            font-weight: bold;
            text-transform: uppercase;
        }
        .status-active { background: #d4edda; color: #155724; }
        .status-inactive { background: #f8d7da; color: #721c24; }
        .status-warning { background: #fff3cd; color: #856404; }
        .table-container {
            overflow-x: auto;
            margin-top: 20px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9em;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }
        th {
            background: #f8f9fa;
            font-weight: 600;
            color: #666;
        }
        tr:hover { background: #f8f9fa; }
        .progress-bar {
            height: 8px;
            background: #e9ecef;
            border-radius: 4px;
            overflow: hidden;
            margin-top: 8px;
        }
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #667eea, #764ba2);
            transition: width 0.3s;
        }
        .source-card {
            display: flex;
            align-items: center;
            gap: 16px;
            padding: 16px;
            background: #f8f9fa;
            border-radius: 12px;
            margin-bottom: 12px;
        }
        .source-icon {
            width: 48px;
            height: 48px;
            background: linear-gradient(135deg, #667eea, #764ba2);
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-size: 1.5em;
            font-weight: bold;
        }
        .source-info { flex: 1; }
        .source-name { font-weight: 600; margin-bottom: 4px; }
        .source-count { font-size: 0.9em; color: #666; }
        .refresh-btn {
            position: fixed;
            bottom: 30px;
            right: 30px;
            background: white;
            border: none;
            padding: 16px 24px;
            border-radius: 50px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.2);
            cursor: pointer;
            font-weight: bold;
            color: #667eea;
            transition: all 0.3s;
        }
        .refresh-btn:hover {
            transform: scale(1.05);
            box-shadow: 0 6px 30px rgba(0,0,0,0.3);
        }
        .alert {
            padding: 16px;
            border-radius: 12px;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 12px;
        }
        .alert-error { background: #f8d7da; color: #721c24; }
        .alert-warning { background: #fff3cd; color: #856404; }
        .alert-success { background: #d4edda; color: #155724; }
        .health-indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
        }
        .health-good { background: #28a745; }
        .health-warning { background: #ffc107; }
        .health-bad { background: #dc3545; }
        @media (max-width: 768px) {
            .grid { grid-template-columns: 1fr; }
            header h1 { font-size: 1.8em; }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🏠 ValoraNL Dashboard</h1>
            <p>Sistema de Unificación de Datos Inmobiliarios</p>
            <div style="margin-top: 20px;">
                <span class="health-indicator health-{{ health_status }}"></span>
                <span style="color: white;">Estado: {{ health_text }}</span>
            </div>
        </header>

        {% if alerts %}
            {% for alert in alerts %}
            <div class="alert alert-{{ alert.type }}">
                <strong>{{ alert.icon }}</strong> {{ alert.message }}
            </div>
            {% endfor %}
        {% endif %}

        <div class="grid">
            <!-- KPIs Principales -->
            <div class="card">
                <div class="card-header">
                    <span class="card-title">Total Listings</span>
                    <span class="status-badge status-active">Activo</span>
                </div>
                <div class="card-value">{{ stats.total_listings | default(0) | number_format }}</div>
                <div class="card-subtitle">Listings únicos en base de datos</div>
            </div>

            <div class="card">
                <div class="card-header">
                    <span class="card-title">Fuentes Activas</span>
                    <span class="status-badge status-active">{{ stats.active_sources | default(0) }}</span>
                </div>
                <div class="card-value">{{ stats.total_sources | default(0) }}</div>
                <div class="card-subtitle">Fuentes de datos configuradas</div>
            </div>

            <div class="card">
                <div class="card-header">
                    <span class="card-title">Precio Promedio</span>
                    <span class="status-badge status-warning">MXN</span>
                </div>
                <div class="card-value">${{ stats.avg_price | default(0) | number_format }}</div>
                <div class="card-subtitle">Promedio de listings activos</div>
            </div>

            <div class="card">
                <div class="card-header">
                    <span class="card-title">Última Actualización</span>
                </div>
                <div style="font-size: 1.5em; font-weight: bold; color: #667eea;">
                    {{ stats.last_update | default('Nunca') }}
                </div>
                <div class="card-subtitle">
                    {% if stats.last_update_minutes is not none %}
                        Hace {{ stats.last_update_minutes }} minutos
                    {% else %}
                        No hay datos de ejecución
                    {% endif %}
                </div>
            </div>
        </div>

        <div class="grid" style="margin-top: 30px;">
            <!-- Distribución por Fuente -->
            <div class="card" style="grid-column: span 2;">
                <div class="card-header">
                    <span class="card-title">Distribución por Fuente</span>
                </div>
                {% for source in source_stats %}
                <div class="source-card">
                    <div class="source-icon">{{ source.code[:2] | upper }}</div>
                    <div class="source-info">
                        <div class="source-name">{{ source.name }}</div>
                        <div class="source-count">{{ source.count | number_format }} listings</div>
                    </div>
                    <div style="text-align: right;">
                        <div style="font-size: 1.5em; font-weight: bold; color: #667eea;">
                            {{ source.percentage }}%
                        </div>
                        <div class="progress-bar" style="width: 100px;">
                            <div class="progress-fill" style="width: {{ source.percentage }}%;"></div>
                        </div>
                    </div>
                </div>
                {% endfor %}
            </div>

            <!-- Distribución Geográfica -->
            <div class="card">
                <div class="card-header">
                    <span class="card-title">Por Municipio</span>
                </div>
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Municipio</th>
                                <th>Cantidad</th>
                                <th>Precio Prom.</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for mun in municipality_stats[:5] %}
                            <tr>
                                <td>{{ mun.name }}</td>
                                <td>{{ mun.count }}</td>
                                <td>${{ mun.avg_price | number_format }}</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- Histórico de Ejecuciones -->
        <div class="card" style="margin-top: 30px;">
            <div class="card-header">
                <span class="card-title">Histórico de Ejecuciones</span>
            </div>
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>Fecha</th>
                            <th>Estado</th>
                            <th>Fuentes</th>
                            <th>Listings</th>
                            <th>Nuevos</th>
                            <th>Actualizados</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for exec in execution_history[:10] %}
                        <tr>
                            <td>{{ exec.started_at }}</td>
                            <td>
                                <span class="status-badge status-{{ 'active' if exec.status == 'success' else 'inactive' if exec.status == 'failed' else 'warning' }}">
                                    {{ exec.status }}
                                </span>
                            </td>
                            <td>{{ exec.sources_processed }}</td>
                            <td>{{ exec.total_listings }}</td>
                            <td>{{ exec.new_listings }}</td>
                            <td>{{ exec.updated_listings }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <button class="refresh-btn" onclick="location.reload()">
        🔄 Actualizar
    </button>

    <script>
        // Auto-refresh cada 60 segundos
        setTimeout(() => location.reload(), 60000);
    </script>
</body>
</html>
"""

# ============================================================================
# RUTAS DE LA APLICACIÓN
# ============================================================================

def get_mysql_connection():
    """Obtiene conexión a MySQL"""
    return pymysql.connect(**MYSQL_CONFIG)

@app.template_filter('number_format')
def number_format(value):
    """Formatea números con separadores de miles"""
    if value is None:
        return "0"
    try:
        return "{:,}".format(int(float(value))).replace(",", ".")
    except:
        return str(value)

@app.route('/')
def dashboard():
    """Página principal del dashboard"""

    # Obtener estadísticas
    stats = get_stats()
    source_stats = get_source_stats()
    municipality_stats = get_municipality_stats()
    execution_history = get_execution_history()

    # Determinar estado de salud
    health_status, health_text = calculate_health(stats, execution_history)

    # Generar alertas
    alerts = generate_alerts(stats, execution_history)

    return render_template_string(
        DASHBOARD_TEMPLATE,
        stats=stats,
        source_stats=source_stats,
        municipality_stats=municipality_stats,
        execution_history=execution_history,
        health_status=health_status,
        health_text=health_text,
        alerts=alerts
    )

@app.route('/api/stats')
def api_stats():
    """API endpoint para estadísticas JSON"""
    return jsonify({
        'stats': get_stats(),
        'sources': get_source_stats(),
        'municipalities': get_municipality_stats(),
        'executions': get_execution_history()
    })

@app.route('/api/health')
def api_health():
    """Health check endpoint"""
    try:
        conn = get_mysql_connection()
        conn.close()
        return jsonify({'status': 'healthy', 'database': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

# ============================================================================
# FUNCIONES DE DATOS
# ============================================================================

def get_stats() -> Dict:
    """Obtiene estadísticas generales"""
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # Total listings
        cursor.execute("SELECT COUNT(*) as total FROM listings")
        total = cursor.fetchone()['total']

        # Listings activos
        cursor.execute("SELECT COUNT(*) as active FROM listings WHERE status = 'active'")
        active = cursor.fetchone()['active']

        # Precio promedio
        cursor.execute("""
            SELECT AVG(price_amount) as avg_price 
            FROM listings 
            WHERE status = 'active' AND price_amount IS NOT NULL AND price_amount > 0
        """)
        avg_price = cursor.fetchone()['avg_price'] or 0

        # Fuentes
        cursor.execute("SELECT COUNT(*) as sources FROM sources WHERE is_active = 1")
        sources = cursor.fetchone()['sources']

        # Última actualización
        cursor.execute("SELECT MAX(seen_last_at) as last_update FROM listings")
        last_update = cursor.fetchone()['last_update']

        # Calcular minutos desde última actualización
        last_update_minutes = None
        if last_update:
            if isinstance(last_update, str):
                last_update = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
            delta = datetime.now() - last_update.replace(tzinfo=None)
            last_update_minutes = int(delta.total_seconds() / 60)

        conn.close()

        return {
            'total_listings': total,
            'active_listings': active,
            'avg_price': int(avg_price),
            'total_sources': sources,
            'active_sources': sources,
            'last_update': last_update.strftime('%Y-%m-%d %H:%M') if hasattr(last_update, 'strftime') else str(last_update)[:16] if last_update else None,
            'last_update_minutes': last_update_minutes
        }
    except Exception as e:
        print(f"Error getting stats: {e}")
        return {}

def get_source_stats() -> List[Dict]:
    """Obtiene estadísticas por fuente"""
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        cursor.execute("""
            SELECT s.source_code, s.source_name, COUNT(l.id) as count
            FROM sources s
            LEFT JOIN listings l ON s.id = l.source_id
            GROUP BY s.id
            ORDER BY count DESC
        """)

        results = cursor.fetchall()
        conn.close()

        # Calcular porcentajes
        total = sum(r['count'] for r in results) or 1

        return [
            {
                'code': r['source_code'],
                'name': r['source_name'],
                'count': r['count'],
                'percentage': round(r['count'] / total * 100, 1)
            }
            for r in results
        ]
    except Exception as e:
        print(f"Error getting source stats: {e}")
        return []

def get_municipality_stats() -> List[Dict]:
    """Obtiene estadísticas por municipio"""
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        cursor.execute("""
            SELECT municipality, COUNT(*) as count, AVG(price_amount) as avg_price
            FROM listings
            WHERE status = 'active' AND municipality IS NOT NULL AND price_amount > 0
            GROUP BY municipality
            ORDER BY count DESC
            LIMIT 10
        """)

        results = cursor.fetchall()
        conn.close()

        return [
            {
                'name': r['municipality'],
                'count': r['count'],
                'avg_price': int(r['avg_price'] or 0)
            }
            for r in results
        ]
    except Exception as e:
        print(f"Error getting municipality stats: {e}")
        return []

def get_execution_history() -> List[Dict]:
    """Obtiene histórico de ejecuciones"""
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        cursor.execute("""
            SELECT execution_id, started_at, completed_at, status,
                   sources_processed, total_listings, new_listings, updated_listings
            FROM execution_log
            ORDER BY started_at DESC
            LIMIT 20
        """)

        results = cursor.fetchall()
        conn.close()

        formatted = []
        for r in results:
            started = r['started_at']
            if started and hasattr(started, 'strftime'):
                started_str = started.strftime('%Y-%m-%d %H:%M')
            else:
                started_str = str(started)[:16] if started else 'N/A'

            formatted.append({
                'execution_id': r['execution_id'],
                'started_at': started_str,
                'status': r['status'],
                'sources_processed': r['sources_processed'] or 0,
                'total_listings': r['total_listings'] or 0,
                'new_listings': r['new_listings'] or 0,
                'updated_listings': r['updated_listings'] or 0
            })

        return formatted
    except Exception as e:
        print(f"Error getting execution history: {e}")
        return []

def calculate_health(stats: Dict, executions: List[Dict]) -> tuple:
    """Calcula estado de salud del sistema"""
    if not executions:
        return 'warning', 'Sin datos de ejecución'

    last_exec = executions[0]

    if last_exec['status'] == 'failed':
        return 'bad', 'Última ejecución fallida'

    if stats.get('last_update_minutes') is not None:
        if stats['last_update_minutes'] > 1440:  # 24 horas
            return 'warning', 'Datos desactualizados (>24h)'

    return 'good', 'Sistema operativo'

def generate_alerts(stats: Dict, executions: List[Dict]) -> List[Dict]:
    """Genera alertas basadas en el estado"""
    alerts = []

    if not executions:
        alerts.append({
            'type': 'warning',
            'icon': '⚠️',
            'message': 'No se ha ejecutado el sistema de unificación todavía.'
        })
        return alerts

    last_exec = executions[0]

    if last_exec['status'] == 'failed':
        alerts.append({
            'type': 'error',
            'icon': '❌',
            'message': f'La última ejecución ({last_exec["started_at"]}) falló. Revisa los logs.'
        })

    if stats.get('last_update_minutes', 0) > 180:  # 3 horas
        alerts.append({
            'type': 'warning',
            'icon': '⏰',
            'message': f'Los datos tienen {stats["last_update_minutes"]} minutos de antigüedad.'
        })

    if stats.get('total_listings', 0) == 0:
        alerts.append({
            'type': 'error',
            'icon': '📭',
            'message': 'No hay listings en la base de datos. Verifica las fuentes de datos.'
        })

    return alerts

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 70)
    print("ValoraNL Dashboard")
    print("=" * 70)
    print(f"Accede en tu navegador: http://localhost:5000")
    print("Presiona Ctrl+C para detener")
    print("=" * 70)

    # Verificar conexión a MySQL
    try:
        conn = get_mysql_connection()
        conn.close()
        print("✓ Conectado a MySQL")
    except Exception as e:
        print(f"✗ Error conectando a MySQL: {e}")
        print("Verifica que MySQL esté corriendo y las credenciales sean correctas")
        return 1

    app.run(host='0.0.0.0', port=5000, debug=False)
    return 0

if __name__ == '__main__':
    exit(main())