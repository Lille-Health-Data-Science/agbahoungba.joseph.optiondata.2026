"""Vérifie l'installation : bibliothèques, dossier donnees/, accès au serveur FHIR.

Dans l'interface : activer le DAG, cliquer sur « Trigger », attendre trois carrés verts dans Grid.
"""
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task

DONNEES = Path(__file__).resolve().parents[1] / "donnees"


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["installation"])
def verification_installation():

    @task
    def bibliotheques() -> dict:
        import airflow
        import duckdb
        import pandas
        import pandera

        versions = {"airflow": airflow.__version__, "pandas": pandas.__version__,
                    "pandera": pandera.__version__, "duckdb": duckdb.__version__}
        print("Bibliothèques :", versions)
        return versions

    @task
    def dossier_donnees(versions: dict) -> str:
        DONNEES.mkdir(exist_ok=True)
        fichier = DONNEES / "verification.txt"
        fichier.write_text(f"Installation vérifiée le {datetime.now():%d/%m/%Y %H:%M} · {versions}\n")
        print("Fichier écrit :", fichier)
        return str(fichier)

    @task
    def serveur_fhir(fichier: str):
        import requests

        try:
            r = requests.get("https://hapi.fhir.org/baseR4/metadata", params={"_summary": "true"},
                             headers={"Accept": "application/fhir+json"}, timeout=20)
            r.raise_for_status()
            print("Serveur FHIR joignable, version", r.json().get("fhirVersion"))
        except requests.RequestException as err:
            # Pas bloquant : le réseau de l'université peut filtrer, le TP a un repli
            print("Serveur FHIR injoignable depuis Airflow (non bloquant) :", err)
        print("Installation OK. Fichier de contrôle :", fichier)

    serveur_fhir(dossier_donnees(bibliotheques()))


verification_installation()
