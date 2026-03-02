"""
Page 3 - Traitement des données + sélecteur de vecteur d'entrée
"""
import streamlit as st
import sys
from pathlib import Path

STREAMLIT_APP_DIR = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(STREAMLIT_APP_DIR))

from config import COLORS, html, VECTOR_TYPES, MODEL_BENCHMARKS

st.set_page_config(page_title="Traitement - LoL Draft Predictor", page_icon="⚙️", layout="wide")

st.title("⚙️ Traitement des Données")

# ============================
# Section 1 - Pipeline
# ============================
st.header("1. Pipeline de données")

st.markdown("""
Le pipeline transforme les données brutes de la base SQLite en vecteurs de features pour les modèles ML.
""")

st.code("""
┌──────────────┐     ┌───────────────────┐     ┌──────────────┐     ┌──────────────┐
│  Base SQLite  │ ──► │ Feature Engineering│ ──► │   Vecteur    │ ──► │  Modèle ML   │
│  (280k matchs)│     │                   │     │  d'entrée    │     │  (XGBoost/   │
│              │     │ • Draft features   │     │  (139-162    │     │   LightGBM)  │
│  7 tables    │     │ • Summoner stats   │     │   features)  │     │              │
│              │     │ • Synergies/Counter│     │              │     │  Prédiction  │
│              │     │ • Timeline gold    │     │              │     │  Win/Loss    │
└──────────────┘     └───────────────────┘     └──────────────┘     └──────────────┘
""", language=None)

st.markdown("""
**Étapes clés :**
1. **Export** des données brutes depuis SQLite (matchs, joueurs, timelines)
2. **Feature engineering** : création de 139 à 162 features selon le vecteur choisi
3. **Standardisation** (`StandardScaler`) avant entraînement
4. **Split temporel** : train (80%) / test (20%) basé sur la date de collecte
""")

st.markdown("---")

# ============================
# Section 2 - Feature Engineering
# ============================
st.header("2. Feature Engineering")

col1, col2 = st.columns(2)

with col1:
    st.subheader("🏆 Features Draft (base)")
    st.markdown("""
    - **Champion IDs** : 10 champions (5 par équipe × 5 rôles)
    - **Ban IDs** : 10 bans (5 par équipe)
    - **Summoner spells** : 10 paires de sorts d'invocateur
    """)

    st.subheader("👤 Features Invocateurs")
    st.markdown("""
    - **Role winrate** : % victoires du joueur sur son rôle
    - **Role specialization** : % de parties jouées sur ce rôle
    - **Mastery points** : maîtrise du champion
    - **Champion recent WR** : winrate récent sur ce champion
    - **Streak** : série de victoires/défaites en cours
    - **KDA, Vision** : stats moyennes par rôle
    """)

with col2:
    st.subheader("🤝 Features Synergies / Counters")
    st.markdown("""
    - **Synergy score** : score de synergie intra-équipe
    - **Counter score** : score de counter-pick inter-équipes
    - **Draft advantage** : avantage global du draft
    - Calculé sur 280k matchs (min 30 parties ensemble)
    """)

    st.subheader("⏱️ Features Timeline (optionnelles)")
    st.markdown("""
    - **Gold par rôle** à @5, @10, @15 ou @20 min
    - **Gold diff total** à chaque timestamp
    - **CS par rôle** à @10 min (uniquement pour le vecteur @10)
    - Nécessite des données timeline (collectées séparément)
    """)

st.markdown("---")

st.subheader("Agrégations par équipe")
st.markdown("""
Les features individuelles des joueurs sont agrégées au niveau équipe :
- `role_winrate_diff` = moyenne WR rôles équipe bleue − moyenne WR rôles équipe rouge
- `mastery_diff` = total mastery bleue − total mastery rouge
- `streak_momentum_diff` = momentum des séries bleue − rouge
- `role_specialization_diff` = spécialisation moyenne bleue − rouge

Ces **diffs** sont les features les plus prédictives du modèle draft-only.
""")

st.markdown("---")

# ============================
# Section 3 - Sélecteur de vecteur
# ============================
st.header("3. Sélection du vecteur d'entrée")

st.markdown("""
Choisissez le vecteur d'entrée qui détermine quelles données sont utilisées pour la prédiction.
Ce choix impacte les pages **Modèles** et **Résultats**.
""")

# Initialize session state
if "vector_type" not in st.session_state:
    st.session_state.vector_type = "draft"

# Radio selector
vector_options = list(VECTOR_TYPES.keys())
vector_labels = [
    f"{VECTOR_TYPES[k]['name']} ({VECTOR_TYPES[k]['nb_features']} features) – {MODEL_BENCHMARKS[k]['accuracy']*100:.1f}% accuracy"
    for k in vector_options
]

selected_idx = vector_options.index(st.session_state.vector_type)

selected_label = st.radio(
    "Vecteur d'entrée",
    vector_labels,
    index=selected_idx,
    help="Le vecteur détermine les features utilisées par le modèle.",
)

# Update session state
selected_key = vector_options[vector_labels.index(selected_label)]
st.session_state.vector_type = selected_key

# Display info about selected vector
vt = VECTOR_TYPES[selected_key]
bm = MODEL_BENCHMARKS[selected_key]

st.markdown("---")

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Vecteur sélectionné", vt["name"])
with col2:
    st.metric("Nombre de features", vt["nb_features"])
with col3:
    st.metric("Accuracy test", f"{bm['accuracy']*100:.1f}%")

st.info(f"**Description** : {vt['description']}")
st.markdown(f"**Algorithme** : {bm['model_type']}")

# Show extra features for non-draft vectors
if vt["extra_features"]:
    with st.expander(f"Features additionnelles par rapport au draft ({len(vt['extra_features'])} features)"):
        cols = st.columns(3)
        for i, feat in enumerate(vt["extra_features"]):
            with cols[i % 3]:
                st.code(feat, language=None)

st.markdown("---")

# Summary comparison table
st.subheader("Comparaison des vecteurs")

comparison_data = []
for key in vector_options:
    v = VECTOR_TYPES[key]
    b = MODEL_BENCHMARKS[key]
    comparison_data.append({
        "Vecteur": v["name"],
        "Features": v["nb_features"],
        "Algorithme": b["model_type"],
        "Accuracy": f"{b['accuracy']*100:.1f}%",
        "Description": v["description"],
    })

import pandas as pd
st.dataframe(pd.DataFrame(comparison_data), use_container_width=True, hide_index=True)

st.success(f"✅ Vecteur **{vt['name']}** sélectionné. Rendez-vous sur les pages **Modèles** et **Résultats** pour voir les détails.")
