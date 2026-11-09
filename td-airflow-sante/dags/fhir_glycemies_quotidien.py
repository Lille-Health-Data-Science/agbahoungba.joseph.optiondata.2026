"""TP séance 2 · Cas d'usage : le flux FHIR quotidien des glycémies.

extraire (API FHIR, pagination) → aplatir (Parquet) → valider (Pandera) → charger (DuckDB, idempotent)
Le guide pas à pas est dans TD.md (partie 04).

Avant de commencer :
  - copier ce fichier dans dags/ ;
  - créer le pool api_fhir : Admin › Pools › + , nom api_fhir, 3 places.
Sans réseau : déclencher avec le paramètre source = fichier (formulaire « Trigger »).
"""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

DONNEES = Path(__file__).resolve().parents[1] / "donnees"
BASE = os.environ.get("FHIR_BASE", "https://hapi.fhir.org/baseR4")   # panne volontaire (TD.md, 4.4) : mettre une mauvaise URL
UNITES = ["g/L", "mg/dL", "mmol/L"]
EXEMPLE = DONNEES / "exemple" / "glycemies_exemple.ndjson"     # repli sans réseau (données de la séance 1)
glycemies = Asset("glycemies")


def jour(data_interval_start) -> str:
    """Le jour traité ; une exécution manuelle n'a pas d'intervalle : on prend la veille."""
    return (data_interval_start or datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def alerter(context):
    """Appelée quand une tâche échoue définitivement (après ses retries)."""
    ti = context["ti"]
    message = f"ALERTE {datetime.now():%H:%M:%S} · {ti.dag_id}.{ti.task_id} en échec : {context.get('exception')}"
    print(message)
    with open(DONNEES / "alertes.log", "a") as f:
        f.write(message + "\n")


@dag(schedule=CronTriggerTimetable("0 6 * * *", timezone="Europe/Paris", interval=timedelta(days=1)),
     start_date=datetime(2026, 11, 1), catchup=False, tags=["fhir", "tp"],
     default_args={"on_failure_callback": alerter},
     params={"source": Param("api", enum=["api", "fichier"],
                             description="api : serveur FHIR public ; fichier : données d'exemple, sans réseau")})
def fhir_glycemies_quotidien():

    # En production : retries=5, retry_delay=2 min ; raccourci ici pour voir la panne pendant le TP
    @task(pool="api_fhir", retries=3, retry_delay=timedelta(seconds=20))
    def extraire(params=None, data_interval_start=None) -> str:
        """Étape 1 : glycémies (LOINC 2345-7) modifiées depuis le début de l'intervalle, toutes pages."""
        import requests

        chemin = DONNEES / "brut" / f"glycemies_{jour(data_interval_start)}.ndjson"
        chemin.parent.mkdir(parents=True, exist_ok=True)
        if params["source"] == "fichier":
            chemin.write_text(EXEMPLE.read_text())
            print("Source fichier :", EXEMPLE)
            return str(chemin)

        url = f"{BASE}/Observation"
        requete = {"code": "http://loinc.org|2345-7", "_count": 100,
                   "_lastUpdated": f"ge{jour(data_interval_start)}"}
        ressources = []
        # À COMPLÉTER (étape 1) : boucle de pagination de la séance 1, qui remplit `ressources`
        #   - r = requests.get(url, params=requete, headers={"Accept": "application/fhir+json"}, timeout=30)
        #   - r.raise_for_status() : une erreur HTTP fait échouer la tâche, donc déclenche un retry
        #   - ajouter les ressources de bundle["entry"], suivre le lien « next », puis requete = None
        raise NotImplementedError("Étape 1 à compléter (ou déclencher avec source = fichier)")
        chemin.write_text("".join(json.dumps(o) + "\n" for o in ressources))
        print(len(ressources), "ressources écrites dans", chemin)
        return str(chemin)                            # XCom : un chemin, pas les données

    @task
    def aplatir(chemin: str) -> str:
        """Étape 2 : une ligne par Observation (id, patient, date, statut, valeur, unite), en Parquet."""
        import pandas as pd

        lignes = []
        with open(chemin) as f:
            for ligne in f:
                o = json.loads(ligne)          # une ligne de texte → un dictionnaire Python
                vq = o.get("valueQuantity", {})
                lignes.append({
                    "id": o["id"],               
                    "patient": o["subject"]["reference"],          
                    "date": o["effectiveDateTime"],               
                    "statut": o["status"],             
                    "valeur": vq.get("value"),
                    "unite": vq.get("unit"),            
                })
        df = pd.DataFrame(lignes)
        sortie = Path(chemin).with_suffix(".parquet")   # même nom, extension .parquet
        df.to_parquet(sortie, index=False)
        print(len(df), "lignes ;", "unités :", df["unite"].value_counts(dropna=False).to_dict())
        return str(sortie)

    @task
    def valider(chemin: str) -> str:
        """Étape 3 : schéma Pandera ; que faire des lignes invalides ?"""
        import pandas as pd
        import pandera.pandas as pa

        # À COMPLÉTER (étape 3) : schéma avec unite dans UNITES, valeur > 0, patient non nul ;
        # écrire les lignes valides en Parquet et renvoyer le chemin
        df = pd.read_parquet(chemin)

        schema = pa.DataFrameSchema({
            "patient": pa.Column(str, nullable=False),
            "valeur": pa.Column(float, pa.Check.gt(0), nullable=False),     # À COMPLÉTER : valeur > 0
            "unite": pa.Column(str, pa.Check.isin(UNITES), nullable=False),        # À COMPLÉTER : unité dans UNITES
        })

        try:
            valides = schema.validate(df, lazy=True)   # tout est bon : aucune ligne rejetée
            rejetees = df.iloc[0:0]
        except pa.errors.SchemaErrors as err:
            index_fautifs = err.failure_cases["index"].dropna().unique()
            rejetees = df.loc[index_fautifs]
            valides = df.drop(index=index_fautifs)

        print(len(valides), "lignes valides,", len(rejetees), "rejetées")
        sortie = Path(chemin).with_name(Path(chemin).stem + "_valide.parquet")
        valides.to_parquet(sortie, index=False)
        return str(sortie)


    @task(outlets=[glycemies])
    def charger(chemin: str, data_interval_start=None):
        """Étape 4 : DELETE + INSERT du jour dans donnees/entrepot.duckdb, donc rejouable sans doublon."""
        import duckdb

        # À COMPLÉTER (étape 4) : table glycemies (valeur convertie en mg/dL comme en séance 1),
        # suppression des lignes du jour puis insertion depuis read_parquet(chemin)
        j = jour(data_interval_start)          # le jour traité, ex. "2026-11-08"
        with duckdb.connect(str(DONNEES / "entrepot.duckdb")) as con:
            # 1. créer la table si elle n'existe pas encore (première exécution)
            con.execute("""
                create table if not exists glycemies (
                    jour varchar, id varchar, patient varchar, date timestamptz, statut varchar,
                    valeur_source double, unite_source varchar, valeur_mg_dl double)""")

            # 2. idempotence : effacer les lignes de ce jour…
            con.execute("delete from glycemies where jour = ?", [j])

            # 3. …puis insérer celles du fichier validé, avec conversion en mg/dL
            con.execute("""
                insert into glycemies
                select ?, id, patient, cast(date as timestamptz), statut, valeur, unite,
                       round(case unite when 'mg/dL'  then valeur
                                        when 'g/L'    then valeur * 100
                                        when 'mmol/L' then valeur * 18.016 end, 1)
                from read_parquet(?)""", [j, chemin])

            n = con.execute("select count(*) from glycemies where jour = ?", [j]).fetchall()[0][0]
            total = con.execute("select count(*) from glycemies").fetchall()[0][0]
        print(n, "glycémies chargées pour le", j, "· total dans l'entrepôt :", total)


    charger(valider(aplatir(extraire())))


fhir_glycemies_quotidien()
