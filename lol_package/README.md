# LoL Draft Predictor - Streamlit App

## Installation

### 1. Créer un environnement virtuel (recommandé)
```bash
python -m venv venv
source venv/bin/activate  # Mac/Linux
# ou: venv\Scripts\activate  # Windows
```

### 2. Installer les dépendances
```bash
pip install -r streamlit_app/requirements.txt
```

### 3. Ajouter la base de données
Placer le fichier `lol_matches.db` dans le dossier `data/` :
```
lol_package/
├── data/
│   └── lol_matches.db    <-- ICI
├── models/
├── streamlit_app/
└── ...
```

### 4. Lancer l'application
```bash
streamlit run streamlit_app/app.py
```

L'app sera accessible sur http://localhost:8501

## Structure
```
lol_package/
├── streamlit_app/       # Application Streamlit
│   ├── app.py           # Point d'entrée
│   ├── config.py        # Configuration (couleurs, chemins)
│   ├── pages/           # Pages de l'app
│   ├── components/      # Composants réutilisables
│   └── utils/           # Utilitaires (data loader, model loader)
├── models/              # Modèles ML entraînés
│   └── draft_predictor_model.pkl
├── data/                # Base de données (à ajouter)
│   └── lol_matches.db
└── .streamlit/          # Config Streamlit (thème)
```

## Notes
- La base de données `lol_matches.db` contient ~280k matchs
- Le modèle atteint ~72-78% de précision avec les données early game
