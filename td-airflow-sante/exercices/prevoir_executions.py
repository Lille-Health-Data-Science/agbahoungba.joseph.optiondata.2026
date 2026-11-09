"""Exercice 2 en pratique · Prévoir les exécutions (slide « Prévoir les exécutions »).

1. Avant de copier ce fichier dans dags/, notez combien d'exécutions Airflow va créer à l'activation,
   et la logical_date de la première.
2. Copiez-le dans dags/, activez-le, vérifiez dans Grid.
3. Passez catchup à False, renommez dag_id en prevoir_executions_v2 : que change l'activation ?
"""
from datetime import datetime

from airflow.sdk import dag, task


@dag(dag_id="prevoir_executions", schedule="0 6 * * *",
     start_date=datetime(2026, 11, 1), catchup=True, tags=["planification"])
def prevoir_executions():
    @task
    def afficher(logical_date=None, data_interval_start=None, data_interval_end=None):
        print(f"logical_date={logical_date} · intervalle {data_interval_start} → {data_interval_end}")

    afficher()


prevoir_executions()
