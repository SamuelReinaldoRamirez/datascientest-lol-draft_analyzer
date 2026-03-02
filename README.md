# LoL Draft Analyzer

Projet de Machine Learning pour prédire l'issue des matchs League of Legends basé sur la composition d'équipe (draft).

## Structure du projet

```
├── data/
│   ├── raw/                 # Données brutes (match_details_extended.txt)
│   ├── processed/           # Données préparées (Parquet)
│   └── lol_matches.db       # Base de données SQLite
├── src/
│   ├── riot_api.py          # Interface API Riot Games
│   ├── database.py          # Interface SQLite
│   ├── collect_data_safe.py # Collecte de données
│   ├── migrate_to_sqlite.py # Migration vers SQLite
│   ├── prepare_data.py      # Préparation des données ML
│   ├── draft_predictor.py   # Entraînement du modèle
│   └── predict_draft.py     # Prédictions
├── models/                  # Modèles entraînés
├── docs/                    # Documentation
└── requirements.txt
```

## Installation

```bash
pip install -r requirements.txt
```

## Utilisation

### 1. Migrer les données existantes (si applicable)

```bash
python src/migrate_to_sqlite.py
```

### 2. Collecter des données

```bash
# Mode continu (recommandé pour collecter beaucoup de données)
python src/collect_data_safe.py --continuous --players 50 --matches 20

# Mode simple (un seul batch)
python src/collect_data_safe.py --players 50 --matches 20
```

### 3. Préparer les données pour le ML

```bash
python src/prepare_data.py
```

Cela crée les fichiers Parquet dans `data/processed/` avec :
- One-Hot encoding des champions
- Suppression des colonnes ID
- Split train/val/test (70/15/15)

### 4. Entraîner le modèle

```bash
python src/draft_predictor.py
```

### 5. Faire des prédictions

```bash
python src/predict_draft.py
```

## Configuration

Placez votre clé API Riot Games dans `riot.txt` à la racine du projet.

## Données

- **Objectif** : 5 000+ matchs pour un modèle fiable
- **Format** : Matchs Ranked Solo/Duo (queueId 420)
- **Stockage** : SQLite pour la robustesse

## Modèles

Le script teste automatiquement :
- Random Forest
- Gradient Boosting
- Logistic Regression

Et sélectionne le meilleur basé sur la validation.
