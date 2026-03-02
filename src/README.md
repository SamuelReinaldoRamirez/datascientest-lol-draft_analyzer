# src/

Code source principal du projet.

## ML/

Pipeline Machine Learning.

| Module | Description |
|---|---|
| `preprocessing.py` | Feature engineering complet (draft, winrates, matchups, synergies, summoner stats, timeline) |
| `training.py` | Entraînement des modèles (XGBoost, LightGBM, hyperparamètres) |
| `inference.py` | Inférence : chargement modèle et prédiction sur nouvelles données |

## collect_data/

Modules de collecte de données.

| Module | Description |
|---|---|
| `riot_api.py` | Client Riot Games API (matchs, timelines, summoners) |
| `database.py` | Gestion SQLite (insertion, export, requêtes) |
| `champion_data.py` | Métadonnées champions (noms, rôles, IDs) via CommunityDragon |
| `collect_data_safe.py` | Collecte sécurisée avec rate limiting et retry |
| `dpmlol_scraper.py` | Scraping dpm.lol (winrates, matchups, synergies) |
| `opgg_scraper.py` | Scraping OP.GG |
| `config.py` | Configuration (clés API, chemins, constantes) |

## old/

Scripts legacy (conservés pour référence).

| Module | Description |
|---|---|
| `extract_detailed_match_data.py` | Ancien extracteur de données de match |
| `migrate_to_sqlite.py` | Script de migration vers SQLite |
