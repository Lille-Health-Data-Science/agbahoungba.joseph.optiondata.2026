# TD Airflow · déroulé de la séance (samedi 7 novembre)

**Objectif de fin de séance** : votre DAG `fhir_glycemies_quotidien` a tourné en entier, quatre tâches vertes,
et a rempli l'entrepôt `donnees/entrepot.duckdb`.

| Heure | Partie | Ce que vous faites | Vous avez fini quand… |
|---|---|---|---|
| 8 h 00 | Introduction · Installer | Lancer l'installation pendant l'introduction | `verification_installation` est vert |
| ≈ 8 h 45 | 01 · Premier DAG | Exercice 1 | le journal de `charger` affiche 300 lignes |
| ≈ 9 h 50 | 02 · Planifier | `prevoir_executions` (facultatif) | vous avez prédit le nombre d'exécutions |
| 10 h 10 | 03 · Fiabiliser | Cours, pas d'exercice : retries, callbacks, idempotence, Pandera | — |
| ≈ 11 h 30 | 04 · Cas d'usage | Le flux FHIR quotidien (TP, 20 min) | quatre tâches vertes, aucun doublon |

Les numéros de partie sont ceux des slides.

---

## Introduction · Installer Airflow (8 h 00)

Suivez le [README](README.md). Avec 30 personnes sur le même Wi-Fi, choisissez la voie la plus légère :

| Votre situation | Voie | À télécharger |
|---|---|---|
| Docker Desktop déjà installé | A · Docker | environ 1 Go |
| macOS ou Linux, sans Docker | B · Sans Docker | environ 150 Mo |
| Windows sans Docker | C · Codespaces | rien (tout se passe chez GitHub) |

L'installation tourne seule pendant l'introduction (10 à 20 minutes). **C'est prêt quand** le DAG
`verification_installation`, déclenché depuis l'interface, a trois tâches vertes dans la vue Grid.

---

## Partie 01 · Lancer son premier DAG (≈ 8 h 45)

1. Copier `exercices/mon_premier_dag.py` dans `dags/`. Le DAG apparaît dans la liste au bout de 30 secondes environ.
2. L'activer (interrupteur), puis **Trigger**. Deux exécutions apparaissent : la vôtre et celle planifiée du jour.
3. Ouvrir la tâche `charger`, onglet **Logs** : « Chargement de 300 lignes ».
4. Ajouter une tâche `controler` entre `transformer` et `charger`, qui lève `ValueError` si n < 100.
   Faire passer `nb_patients` à 40 : que voit-on dans Grid ?

---

## Partie 02 · Prévoir les exécutions (facultatif, ≈ 9 h 50)

1. Lire `exercices/prevoir_executions.py` (cron `0 6 * * *`, début le 1er novembre, `catchup=True`).
   **Avant** de le copier : combien d'exécutions seront créées à l'activation ? Quelle logical_date pour la première ?
2. Le copier dans `dags/`, l'activer, compter les exécutions dans Grid. Le journal de `afficher` donne la logical_date
   et l'intervalle de chaque exécution.
3. Passer `catchup=False` et renommer le `dag_id` en `prevoir_executions_v2` : que crée l'activation ?

---

## Partie 03 · Fiabiliser (cours, 10 h 10 – 11 h 00)

Pas d'exercice dans cette partie : les notions vues en cours sont celles que le TP de la partie 04 met en œuvre.

| Notion du cours | Où vous la retrouvez dans le TP |
|---|---|
| Retries | `extraire` : 3 tentatives à 20 secondes d'écart (4.4, la panne) |
| Callbacks | `on_failure_callback` écrit l'alerte dans `donnees/alertes.log` |
| Idempotence | `charger` : DELETE des lignes du jour, puis INSERT (étape 4) |
| Validation des données | `valider` : schéma Pandera (étape 3) |

Pendant la pause de 11 h, rouvrez votre code FHIR de la séance 1 : sa boucle de pagination sert à l'étape 1.

---

## Partie 04 · Cas d'usage : le flux FHIR quotidien (≈ 11 h 30)

Le DAG enchaîne `extraire` (API FHIR) → `aplatir` (Parquet) → `valider` (Pandera) → `charger` (DuckDB).
Chaque tâche à compléter échoue avec un message « Étape n à compléter » : on avance une tâche à la fois.

### 4.1 Préparer (3 min)
1. Créer le pool : **Admin › Pools › +**, nom `api_fhir`, 3 places. Sans lui, `extraire` attend indéfiniment.
2. Copier `exercices/fhir_glycemies_quotidien.py` dans `dags/`, puis activer le DAG.
   L'activation crée une exécution planifiée qui échoue sur `extraire` : c'est normal, l'étape 1 n'est pas encore écrite.

### 4.2 Premier lancement, sans réseau (2 min)
**Trigger**, choisir `source` = **fichier** dans le formulaire, valider.
`extraire` passe au vert (il copie les données d'exemple de la séance 1), `aplatir` passe au rouge :
« Étape 2 à compléter ». Une ligne apparaît aussi dans `donnees/alertes.log` : c'est l'alerte de la tâche en échec.

### 4.3 Compléter, une tâche à la fois (15 min)
Après chaque étape : enregistrer le fichier, attendre 30 secondes, cliquer sur la tâche rouge puis **Clear** :
elle repart avec votre nouveau code.

| Étape | Tâche | À écrire | Vert quand le journal affiche… |
|---|---|---|---|
| 2 | `aplatir` | une ligne par Observation (id, patient, date, statut, valeur, unite), en Parquet | le nombre de lignes et les unités |
| 3 | `valider` | schéma Pandera : unité dans g/L, mg/dL, mmol/L ; valeur > 0 ; patient non nul | lignes valides et rejetées |
| 4 | `charger` | DELETE des lignes du jour, puis INSERT depuis `read_parquet`, valeur convertie en mg/dL | glycémies chargées et total |
| 1 | `extraire` | la pagination de la séance 1, pour la source **api** | le nombre de ressources lues |

Avec la source fichier, on attend 684 ressources, 20 lignes rejetées (sans valeur ni unité) et 664 glycémies chargées.

### 4.4 Vérifier
- **Quatre tâches vertes** dans Grid.
- **Aucun doublon** : relancer avec la même source ; le total affiché par `charger` ne change pas.
- **Panne** : remplacer `BASE` par une adresse fausse (par exemple `https://hapi.fhir.org/baseR5x`), Trigger avec
  `source` = api. `extraire` passe en `up_for_retry` (orange) trois fois, à 20 secondes d'écart, puis en échec ;
  l'alerte s'ajoute à `donnees/alertes.log`.

### En retard ?
Lancez au moins le DAG avec `source` = **fichier** : les étapes qui manquent s'affichent en rouge avec leur numéro.
À 11 h 50, l'enseignant projette et commente le corrigé ; il vous le transmet ensuite.
