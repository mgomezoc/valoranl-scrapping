#!/usr/bin/env python3
"""
ValoraNL - Script de Instalación y Setup
Ejecuta: python setup_valora.py

Verifica dependencias, crea estructura inicial y configura el sistema.
"""

import subprocess
import sys
from pathlib import Path
from typing import List, Tuple


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    END = "\033[0m"


def print_header(text: str) -> None:
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 70}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text.center(70)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 70}{Colors.END}\n")


def print_success(text: str) -> None:
    print(f"{Colors.GREEN}✓{Colors.END} {text}")


def print_error(text: str) -> None:
    print(f"{Colors.RED}✗{Colors.END} {text}")


def print_warning(text: str) -> None:
    print(f"{Colors.YELLOW}⚠{Colors.END} {text}")


def check_python_version() -> bool:
    """Verifica versión de Python."""
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        print_success(f"Python {version.major}.{version.minor}.{version.micro} (OK)")
        return True

    print_error(f"Python {version.major}.{version.minor} (Requiere 3.8+)")
    return False


def check_module(module: str, import_name: str | None = None) -> bool:
    """Verifica si un módulo está instalado."""
    try:
        __import__(import_name or module)
        print_success(f"{module} instalado")
        return True
    except ImportError:
        print_error(f"{module} no instalado")
        return False


def install_module(module: str) -> bool:
    """Instala un módulo vía pip."""
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", module],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print_success(f"{module} instalado correctamente")
        return True
    except Exception as e:
        print_error(f"Error instalando {module}: {e}")
        return False


def check_mysql_connection() -> Tuple[bool, str]:
    """Verifica conexión a MySQL."""
    try:
        import pymysql

        conn = pymysql.connect(
            host="localhost",
            port=3306,
            user="root",
            password="",
            charset="utf8mb4",
        )
        conn.close()
        return True, "Conexión exitosa"
    except ImportError:
        return False, "pymysql no instalado"
    except Exception as e:
        return False, str(e)


def check_source_databases() -> List[Tuple[str, bool, str]]:
    """Verifica bases de datos de origen."""
    sources: List[Tuple[str, bool, str]] = []

    # Casas365 (MySQL)
    try:
        import pymysql

        conn = pymysql.connect(
            host="localhost",
            port=3306,
            user="root",
            password="",
            database="casas365",
        )
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES LIKE 'propiedades'")
        exists = cursor.fetchone() is not None
        conn.close()
        sources.append(("Casas365 (MySQL)", exists, "casas365.propiedades" if exists else "Tabla no encontrada"))
    except Exception as e:
        sources.append(("Casas365 (MySQL)", False, str(e)))

    # Realty World (SQLite)
    rw_db = Path("realtyworld_propiedades.db")
    if rw_db.exists():
        sources.append(("Realty World (SQLite)", True, str(rw_db)))
    else:
        sources.append(("Realty World (SQLite)", False, "Archivo no encontrado"))

    # GP Vivienda (SQLite)
    gp_db = Path("gpvivienda_nuevoleon.db")
    if gp_db.exists():
        sources.append(("GP Vivienda (SQLite)", True, str(gp_db)))
    else:
        sources.append(("GP Vivienda (SQLite)", False, "Archivo no encontrado"))

    return sources


def create_env_file() -> None:
    """Crea archivo .env si no existe."""
    if Path(".env").exists():
        print_warning("Archivo .env ya existe, no se sobrescribe")
        return

    env_content = """# ValoraNL Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=
MYSQL_DATABASE=valoranl

SCHEDULER_INTERVAL=3600
STALE_DAYS=30
MAX_RETRIES=3
LOG_LEVEL=INFO
"""

    with open(".env", "w", encoding="utf-8") as f:
        f.write(env_content)
    print_success("Archivo .env creado")


def test_valora_autonomous() -> bool:
    """Ejecuta prueba rápida del sistema."""
    print("\nEjecutando prueba de valora_autonomous.py...")
    try:
        result = subprocess.run(
            [sys.executable, "valora_autonomous.py", "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            print_success("valora_autonomous.py funciona correctamente")
            return True

        print_error("Error en valora_autonomous.py")
        return False
    except Exception as e:
        print_error(f"No se pudo ejecutar prueba: {e}")
        return False


def main() -> int:
    print_header("ValoraNL - Instalación y Setup")

    # 1. Verificar Python
    print(f"{Colors.BOLD}1. Verificando Python...{Colors.END}")
    if not check_python_version():
        print_error("Python 3.8+ es requerido. Abortando.")
        return 1

    # 2. Verificar dependencias
    print(f"\n{Colors.BOLD}2. Verificando dependencias...{Colors.END}")
    required = [
        ("pymysql", "pymysql"),
        ("flask", "flask"),
    ]

    missing: List[str] = []
    for module, import_name in required:
        if not check_module(module, import_name):
            missing.append(module)

    if missing:
        print(f"\n{Colors.BOLD}Instalando dependencias faltantes...{Colors.END}")
        for module in missing:
            if not install_module(module):
                print_error(f"No se pudo instalar {module}")
                return 1

    # 3. Verificar MySQL
    print(f"\n{Colors.BOLD}3. Verificando MySQL...{Colors.END}")
    mysql_ok, mysql_msg = check_mysql_connection()
    if mysql_ok:
        print_success(f"MySQL disponible: {mysql_msg}")
    else:
        print_error(f"MySQL: {mysql_msg}")
        print_warning("Asegúrate de que MySQL esté corriendo antes de continuar")

    # 4. Verificar fuentes de datos
    print(f"\n{Colors.BOLD}4. Verificando fuentes de datos...{Colors.END}")
    sources = check_source_databases()
    available = 0
    for name, exists, detail in sources:
        if exists:
            print_success(f"{name}: {detail}")
            available += 1
        else:
            print_error(f"{name}: {detail}")

    if available == 0:
        print_error("\nNo se encontraron fuentes de datos disponibles!")
        print("Asegúrate de haber ejecutado los scrapers al menos una vez:")
        print("  python casas365_scraper.py")
        print("  python realtyworld_scraper.py")
        print("  python gpvivienda_scraper.py")
    else:
        print(f"\n{available} fuente(s) disponible(s) de {len(sources)}")

    # 5. Crear configuración
    print(f"\n{Colors.BOLD}5. Configuración...{Colors.END}")
    create_env_file()

    # 6. Verificar scripts principales
    print(f"\n{Colors.BOLD}6. Verificando scripts principales...{Colors.END}")
    scripts = ["valora_autonomous.py", "valora_scheduler.py", "valora_dashboard.py"]
    for script in scripts:
        if Path(script).exists():
            print_success(f"{script} encontrado")
        else:
            print_error(f"{script} no encontrado")

    # 7. Prueba rápida
    print(f"\n{Colors.BOLD}7. Prueba de sistema...{Colors.END}")
    test_valora_autonomous()

    # Resumen final
    print_header("Resumen de Instalación")
    if not missing:
        print(f"Dependencias: {Colors.GREEN}OK{Colors.END}")
    else:
        print(f"Dependencias: {Colors.YELLOW}Instaladas{Colors.END}")

    if mysql_ok:
        print(f"MySQL: {Colors.GREEN}OK{Colors.END}")
    else:
        print(f"MySQL: {Colors.RED}Revisar{Colors.END}")

    if available > 0:
        print(f"Fuentes: {Colors.GREEN}{available}/{len(sources)}{Colors.END}")
    else:
        print(f"Fuentes: {Colors.RED}0/{len(sources)}{Colors.END}")

    print(f"\n{Colors.BOLD}Próximos pasos:{Colors.END}")
    print("1. Edita .env con tus credenciales de MySQL")
    print("2. Ejecuta una prueba: python valora_autonomous.py")
    print("3. Inicia el scheduler: python valora_scheduler.py --daemon")
    print("4. Abre el dashboard: python valora_dashboard.py")

    print(f"\n{Colors.GREEN}Setup completado!{Colors.END}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
