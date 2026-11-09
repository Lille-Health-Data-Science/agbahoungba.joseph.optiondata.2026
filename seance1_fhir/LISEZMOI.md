# Séance 1 · FHIR — kit de travaux pratiques

M2 Data Science en santé · Université de Lille · samedi 17 octobre 2026
Antoine Teston · antoine.teston@univ-lille.fr

## Contenu

| Fichier | Rôle |
|---|---|
| `seance1_etudiant.ipynb` | Notebook à compléter : exercices des parties 01 et 02, TP 1 (API FHIR), TP 2 (export FHIR → OMOP) |
| `donnees/export_fhir/Patient.ndjson` | 150 patients fictifs |
| `donnees/export_fhir/Observation.ndjson` | 984 observations : glycémies (mg/dL, g/L, mmol/L), HbA1c, mesures annulées ou erronées |
| `donnees/export_fhir/Condition.ndjson` | 55 diagnostics de diabète de type 2 |
| `requirements.txt` | Paquets Python et versions minimales |
| `donnees/concepts.csv` | Identifiants OMOP utiles au TP 2 (extrait pédagogique, à vérifier dans Athena) |

Toutes les données sont synthétiques : aucune ne concerne une personne réelle.

## Installation (avant la séance)

```bash
cd seance1_fhir
pip install -r requirements.txt
jupyter notebook seance1_etudiant.ipynb
```

Python 3.10 ou plus. La première cellule vérifie l'environnement (versions, dossier `donnees/`). Le notebook doit être ouvert depuis ce dossier, pour que le chemin `donnees/` soit trouvé.

## Rendre le projet « application diabète » (pour le 7 novembre)

La remise se fait par `git push` sur votre dépôt GitHub :

```bash
cd seance1_fhir
git init
git add .
git commit -m "Projet diabète"
git branch -M main
git remote add origin https://github.com/<votre-compte>/<votre-depot>.git
git push -u origin main
```

Le fichier `.gitignore` fourni évite de pousser les fichiers produits (`sortie/`) et les fichiers techniques.
Ajoutez à la racine votre notebook ou script, la courbe, et une demi-page de réponses (`REPONSES.md`).

## Réseau

- Exercice A2 et TP 1 : serveur public `https://hapi.fhir.org/baseR4` (accès Internet nécessaire).
  En cas de panne, serveur local : `docker run -p 8080:8080 hapiproject/hapi:latest`, puis
  `BASE = "http://localhost:8080/fhir"`.
- Exercices A1, A3 et TP 2 : fonctionnent hors ligne.

