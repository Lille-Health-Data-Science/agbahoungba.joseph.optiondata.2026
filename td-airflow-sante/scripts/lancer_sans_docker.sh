#!/usr/bin/env bash
# Plan B : lance Airflow (interface, scheduler, etc.) dans un seul processus.
# Interface : http://localhost:8080 (pas de mot de passe). Arrêt : Ctrl+C.
set -euo pipefail
cd "$(dirname "$0")/.."
if .venv/bin/python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/api/v2/monitor/health', timeout=2)" 2>/dev/null; then
  echo "Airflow tourne déjà : http://localhost:8080"; exit 0
fi
if pgrep -f "airflow standalone" >/dev/null; then
  echo "Airflow est en train de démarrer dans un autre terminal : attendez 1 à 2 minutes."
  echo "Pour repartir de zéro : pkill -f airflow, puis relancez ce script."
  exit 0
fi
export PATH="$PWD/.venv/bin:$PATH"        # standalone relance la commande airflow
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
exec airflow standalone
