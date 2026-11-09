# TD Airflow · M2 Data Science en santé

Séance 2 de l'option Data (Université de Lille) : **samedi 7 novembre 2026, 8 h – 12 h**.
Ce dossier installe **Airflow 3.3.2** et son interface sur votre ordinateur, et contient les fichiers de la séance.
Le déroulé pas à pas du TD est dans **[TD.md](TD.md)**.

> L'installation se fait **en début de séance**, pendant l'introduction (10 à 20 minutes).
> Avec 30 personnes sur le même Wi-Fi, choisissez la voie la plus légère pour votre situation :
>
> | Votre situation | Voie | À télécharger |
> |---|---|---|
> | Docker Desktop déjà installé | A · Docker | environ 1 Go |
> | macOS ou Linux, sans Docker | B · Sans Docker | environ 150 Mo |
> | Windows sans Docker | C · Codespaces | rien (tout se passe chez GitHub) |

## Ce qu'il faut

- 8 Go de mémoire vive (4 Go libres pour Airflow) et 5 Go d'espace disque.
- Ce dossier :
  ```bash
  git clone https://github.com/Lille-Health-Data-Science/td-airflow-sante.git
  cd td-airflow-sante
  ```

Si une voie bloque plus de 10 minutes, passez à la C : elle ne dépend pas de votre ordinateur.

## A · Avec Docker (Windows, macOS, Linux)

1. Installer **Docker Desktop** : <https://www.docker.com/products/docker-desktop/>
   - Windows : accepter l'activation de **WSL 2** proposée par l'installeur, puis redémarrer.
   - Ouvrir Docker Desktop, *Settings › Resources* : au moins **4 Go** de mémoire.
   - Linux : Docker Engine et le plugin compose suffisent ; lancer ensuite, dans ce dossier :
     `sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env`
2. Dans un terminal (PowerShell sous Windows), dans le dossier `td-airflow-sante` :
   ```bash
   docker compose up airflow-init    # première fois seulement : construit l'image et prépare la base (5 à 10 min)
   docker compose up -d              # démarre Airflow en arrière-plan
   ```
   La première commande doit se terminer par `airflow-init-1 exited with code 0`.
3. Ouvrir **<http://localhost:8080>** (1 à 2 minutes après le démarrage) ; identifiant **airflow**, mot de passe **airflow**.

Arrêter : `docker compose stop` · Relancer : `docker compose up -d` · Tout supprimer : `docker compose down --volumes --rmi local`

## B · Sans Docker (macOS, Linux, ou Windows avec WSL 2)

Il faut Python 3.10 à 3.14. Airflow ne fonctionne pas directement sous Windows : il faut WSL 2
(PowerShell en administrateur : `wsl --install`, redémarrer, puis ouvrir « Ubuntu » et y installer
`sudo apt install python3-venv git`).

```bash
bash scripts/installer_sans_docker.sh   # une fois (3 à 5 min)
bash scripts/lancer_sans_docker.sh      # à chaque fois ; laisser ce terminal ouvert
```

Ouvrir **<http://localhost:8080>** : pas de mot de passe. Arrêter : `Ctrl+C` dans le terminal.

## C · Dans le navigateur, avec GitHub Codespaces

Sur la page GitHub du dépôt : *Code › Codespaces › Create codespace on main*. L'installation se lance seule
(5 min). **Si l'éditeur demande « Faites-vous confiance aux auteurs des fichiers de ce dossier ? », cliquez sur
« Approuver le dossier et continuer »** : en mode restreint, ni le terminal ni Airflow ne démarrent.
Airflow démarre ensuite dans un terminal du Codespace : laissez-le ouvert. S'il n'a pas démarré, ouvrez un
terminal (*Terminal › New Terminal*) et lancez `bash scripts/lancer_sans_docker.sh`. L'interface s'ouvre dans
un onglet ; sinon : onglet *Ports*, ligne 8080, icône globe. Pas de mot de passe.
Les fichiers se modifient directement dans l'éditeur du Codespace. Un compte GitHub gratuit suffit (heures incluses).

## Vérifier que tout marche

1. Dans l'interface, la liste des DAG contient **verification_installation**.
2. Activer le DAG (interrupteur), puis **Trigger**.
3. Dans la vue **Grid**, les trois tâches passent au vert en moins d'une minute, et le fichier
   `donnees/verification.txt` apparaît dans votre dossier.

La dernière tâche teste aussi l'accès au serveur FHIR de la séance 1 ; si le réseau le bloque, elle reste verte
et l'écrit dans son journal.

Ensuite, suivez **[TD.md](TD.md)**.

## En cas de problème

| Message | Solution |
|---|---|
| `port is already allocated` ou `address already in use` (8080) | Un autre Airflow tourne déjà : `docker compose down`, ou fermer l'autre terminal ; sinon redémarrer l'ordinateur. |
| `Not enough memory available for Docker` | Docker Desktop › *Settings › Resources* : 4 Go ou plus. |
| `Permission denied` sur `logs/` (Linux) | Mettre votre identifiant dans `.env` (commande `sed` de l'étape A.1), puis `docker compose up airflow-init`. |
| Le DAG n'apparaît pas | Attendre 30 secondes ; une erreur d'import s'affiche en haut de la liste des DAG. |
| Plan B : erreur au clic sur **Trigger** | La base SQLite était occupée : réessayer quelques secondes plus tard. |
| Codespaces : l'interface ne s'ouvre pas | Approuver le dossier (bannière « mode Restreint » en haut), puis `bash scripts/lancer_sans_docker.sh` dans un terminal ; adresse : onglet *Ports*, ligne 8080 |
| Autre chose | Apporter une capture d'écran samedi, ou écrire à l'enseignant avant. |

## Contenu du dossier

| Élément | Rôle |
|---|---|
| `docker-compose.yaml` | Fichier officiel d'Airflow 3.3.2, 4 modifications repérées « M2 DSS » |
| `Dockerfile`, `requirements.txt` | Image Airflow + pandas, Pandera, DuckDB pour les TP |
| `dags/` | Les DAG lus par Airflow ; au départ, seulement `00_verification.py` |
| `TD.md` | Déroulé de la séance, avec ce qu'on doit voir à chaque étape |
| `exercices/` | Fichiers de la séance (exercice 1, planification, cas d'usage FHIR), à copier dans `dags/` au fil du TD |
| `donnees/` | Fichiers produits par les DAG, partagés avec Airflow ; `donnees/exemple/` : glycémies de la séance 1, pour travailler sans réseau |
| `scripts/` | Installation sans Docker (plan B) |
| `.devcontainer/` | Configuration GitHub Codespaces (plan C) |
