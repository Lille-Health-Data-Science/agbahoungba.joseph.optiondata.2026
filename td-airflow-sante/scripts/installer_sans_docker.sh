#!/usr/bin/env bash
# Plan B : installe Airflow 3.3.2 sans Docker (macOS, Linux, ou Windows dans WSL2).
# Usage, depuis le dossier td-airflow-sante :  bash scripts/installer_sans_docker.sh
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PYTHON:-python3}"
VER="$("$PY" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
case "$VER" in
  3.10|3.11|3.12|3.13|3.14) ;;
  *) echo "Python $VER n'est pas pris en charge par Airflow 3.3.2 (il faut 3.10 à 3.14)."; exit 1 ;;
esac
echo "Python $VER : création de l'environnement .venv"
"$PY" -m venv .venv
.venv/bin/python -m pip install --quiet --upgrade pip
echo "Installation d'Airflow 3.3.2 et des bibliothèques des TP (quelques minutes)..."
.venv/bin/python -m pip install --quiet "apache-airflow==3.3.2" -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-${VER}.txt"
.venv/bin/airflow version
echo "Installation terminée. Lancer Airflow avec : bash scripts/lancer_sans_docker.sh"
