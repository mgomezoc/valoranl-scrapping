#!/usr/bin/env python3
"""
ValoraNL Scheduler - Ejecución automática programada
Ejecuta el sistema periódicamente sin intervención humana.

Uso:
    python valora_scheduler.py --daemon        # Ejecutar como daemon
    python valora_scheduler.py --once          # Ejecutar una vez
    python valora_scheduler.py --interval 3600  # Cada hora (segundos)
"""

import os
import sys
import time
import json
import logging
import subprocess
import signal
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    handlers=[
        logging.FileHandler('valora_scheduler.log'),
        logging.StreamHandler()
    ]
)
LOGGER = logging.getLogger("valora_scheduler")

PID_FILE = 'valora_scheduler.pid'
STATE_FILE = 'valora_scheduler_state.json'

@dataclass
class SchedulerState:
    last_run: Optional[str] = None
    last_success: Optional[str] = None
    last_error: Optional[str] = None
    run_count: int = 0
    success_count: int = 0
    fail_count: int = 0
    total_listings_processed: int = 0

    def save(self):
        with open(STATE_FILE, 'w') as f:
            json.dump(asdict(self), f, indent=2)

    @classmethod
    def load(cls) -> 'SchedulerState':
        try:
            with open(STATE_FILE, 'r') as f:
                return cls(**json.load(f))
        except FileNotFoundError:
            return cls()


class ValoraDaemon:
    def __init__(self, interval_seconds: int = 3600):
        self.interval = interval_seconds
        self.running = False
        self.state = SchedulerState.load()

    def start(self):
        """Inicia el daemon"""
        self._check_pid()
        self._write_pid()

        LOGGER.info(f"Daemon iniciado. Intervalo: {self.interval}s ({self.interval/3600:.1f}h)")
        self.running = True

        # Configurar handlers de señales
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        try:
            while self.running:
                self._run_cycle()

                # Esperar hasta el próximo ciclo
                LOGGER.info(f"Esperando {self.interval}s hasta próxima ejecución...")

                # Esperar en intervalos pequeños para permitir interrupción rápida
                waited = 0
                while self.running and waited < self.interval:
                    time.sleep(5)
                    waited += 5

        finally:
            self._cleanup()

    def _run_cycle(self):
        """Ejecuta un ciclo de scraping"""
        self.state.last_run = datetime.now().isoformat()
        self.state.run_count += 1

        LOGGER.info("=" * 70)
        LOGGER.info(f"INICIANDO CICLO #{self.state.run_count}")
        LOGGER.info("=" * 70)

        try:
            # Ejecutar valora_autonomous.py
            result = subprocess.run(
                [sys.executable, 'valora_autonomous.py'],
                capture_output=True,
                text=True,
                timeout=3600  # Máximo 1 hora por ejecución
            )

            # Loggear salida
            if result.stdout:
                for line in result.stdout.split('\n'):
                    if line.strip():
                        LOGGER.info(f"[VALORA] {line}")

            if result.stderr:
                for line in result.stderr.split('\n'):
                    if line.strip():
                        LOGGER.warning(f"[VALORA-ERR] {line}")

            if result.returncode == 0:
                LOGGER.info("Ciclo completado exitosamente")
                self.state.last_success = datetime.now().isoformat()
                self.state.success_count += 1
            else:
                error_msg = f"Proceso retornó código {result.returncode}"
                LOGGER.error(error_msg)
                self.state.last_error = error_msg
                self.state.fail_count += 1

        except subprocess.TimeoutExpired:
            error_msg = "Timeout: ejecución excedió 1 hora"
            LOGGER.error(error_msg)
            self.state.last_error = error_msg
            self.state.fail_count += 1
        except Exception as e:
            error_msg = f"Error ejecutando ciclo: {e}"
            LOGGER.error(error_msg)
            self.state.last_error = error_msg
            self.state.fail_count += 1

        self.state.save()
        self._print_status()

    def _print_status(self):
        """Imprime estado actual"""
        print("\n" + "=" * 70)
        print("ESTADO DEL SCHEDULER")
        print("=" * 70)
        print(f"Ejecuciones totales: {self.state.run_count}")
        print(f"Exitosas: {self.state.success_count} ({self.state.success_count/max(1,self.state.run_count)*100:.1f}%)")
        print(f"Fallidas: {self.state.fail_count}")
        print(f"Última ejecución: {self.state.last_run or 'N/A'}")
        print(f"Último éxito: {self.state.last_success or 'N/A'}")
        if self.state.last_error:
            print(f"Último error: {self.state.last_error}")
        print("=" * 70)

    def _signal_handler(self, signum, frame):
        """Maneja señales de terminación"""
        LOGGER.info(f"Señal {signum} recibida, deteniendo daemon...")
        self.running = False

    def _check_pid(self):
        """Verifica si ya hay un daemon corriendo"""
        if Path(PID_FILE).exists():
            with open(PID_FILE, 'r') as f:
                old_pid = f.read().strip()
            LOGGER.error(f"Daemon ya corriendo (PID: {old_pid})")
            sys.exit(1)

    def _write_pid(self):
        """Escribe archivo PID"""
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))

    def _cleanup(self):
        """Limpieza al salir"""
        LOGGER.info("Deteniendo daemon...")
        Path(PID_FILE).unlink(missing_ok=True)
        self.state.save()


def run_once():
    """Ejecuta una sola vez"""
    LOGGER.info("Ejecutando una vez...")
    result = subprocess.run([sys.executable, 'valora_autonomous.py'])
    return result.returncode


def stop_daemon():
    """Detiene el daemon si está corriendo"""
    if not Path(PID_FILE).exists():
        print("No hay daemon corriendo")
        return

    with open(PID_FILE, 'r') as f:
        pid = int(f.read().strip())

    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Señal de terminación enviada al daemon (PID: {pid})")

        # Esperar a que termine
        for _ in range(30):
            if not Path(PID_FILE).exists():
                print("Daemon detenido exitosamente")
                return
            time.sleep(1)

        print("Forzando terminación...")
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        print("Proceso no encontrado, limpiando...")
        Path(PID_FILE).unlink(missing_ok=True)
    except Exception as e:
        print(f"Error deteniendo daemon: {e}")


def show_status():
    """Muestra estado del scheduler"""
    state = SchedulerState.load()

    print("\n" + "=" * 70)
    print("ESTADO DEL SCHEDULER")
    print("=" * 70)

    if Path(PID_FILE).exists():
        with open(PID_FILE, 'r') as f:
            pid = f.read().strip()
        print(f"Estado: 🟢 CORRIENDO (PID: {pid})")
    else:
        print("Estado: 🔴 DETENIDO")

    print(f"\nEstadísticas:")
    print(f"  Total ejecuciones: {state.run_count}")
    print(f"  Exitosas: {state.success_count}")
    print(f"  Fallidas: {state.fail_count}")

    if state.last_run:
        last_run = datetime.fromisoformat(state.last_run)
        print(f"\nÚltima ejecución: {last_run.strftime('%Y-%m-%d %H:%M:%S')}")

    if state.last_success:
        last_ok = datetime.fromisoformat(state.last_success)
        print(f"Último éxito: {last_ok.strftime('%Y-%m-%d %H:%M:%S')}")

    if state.last_error:
        print(f"Último error: {state.last_error}")

    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description='ValoraNL Scheduler')
    parser.add_argument('--daemon', action='store_true', help='Ejecutar como daemon')
    parser.add_argument('--once', action='store_true', help='Ejecutar una vez')
    parser.add_argument('--stop', action='store_true', help='Detener daemon')
    parser.add_argument('--status', action='store_true', help='Mostrar estado')
    parser.add_argument('--interval', type=int, default=3600, 
                       help='Intervalo en segundos (default: 3600 = 1 hora)')

    args = parser.parse_args()

    if args.stop:
        stop_daemon()
    elif args.status:
        show_status()
    elif args.daemon:
        daemon = ValoraDaemon(interval_seconds=args.interval)
        daemon.start()
    elif args.once:
        sys.exit(run_once())
    else:
        # Por defecto: ejecutar una vez
        sys.exit(run_once())


if __name__ == '__main__':
    main()