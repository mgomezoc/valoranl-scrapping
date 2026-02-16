"""Utilidades ligeras para cargar variables desde archivo .env sin dependencias externas."""

from __future__ import annotations

import os
from pathlib import Path


def load_env_file(env_path: Path | None = None, *, override: bool = True) -> Path:
    """Carga variables de entorno desde un archivo .env.

    - Ignora líneas vacías o comentarios (# ...).
    - Soporta formato KEY=VALUE.
    - Si `override=True`, el valor en .env reemplaza variables ya existentes.
    """
    if env_path is None:
        env_path = Path(__file__).resolve().parent / ".env"

    if not env_path.exists():
        return env_path

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if value and ((value[0] == value[-1]) and value[0] in {'"', "'"}):
            value = value[1:-1]

        if key and (override or key not in os.environ):
            os.environ[key] = value

    return env_path


def env_int(name: str, default: int) -> int:
    """Obtiene entero desde entorno con fallback seguro."""
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default
