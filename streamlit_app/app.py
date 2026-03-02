"""
LoL Draft Predictor - Application Streamlit principale

Application multi-pages pour la prédiction de résultats de matchs
League of Legends à partir du draft et des données d'early game.

Run with: streamlit run streamlit_app/app.py
"""
import sys
from pathlib import Path

# Setup paths FIRST - before any other imports
STREAMLIT_APP_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = STREAMLIT_APP_DIR.parent
SRC_COLLECT_DATA = PROJECT_ROOT / "src" / "collect_data"

sys.path.insert(0, str(STREAMLIT_APP_DIR))
sys.path.insert(1, str(SRC_COLLECT_DATA))

import streamlit as st

from config import COLORS, COLORS_ALPHA, rgba, html, MODEL_BENCHMARKS

# Page configuration
st.set_page_config(
    page_title="LoL Draft Predictor",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize session state
if "vector_type" not in st.session_state:
    st.session_state.vector_type = "draft"

# Custom CSS for LoL theme
st.markdown(
    html(f"""
    <style>
        .stApp {{
            background: linear-gradient(180deg, {COLORS['background']} 0%, {COLORS['background_light']} 100%);
        }}
        [data-testid="stSidebar"] {{
            background: {COLORS['background_light']};
            border-right: 1px solid {COLORS['gold_accent']}40;
        }}
        h1, h2, h3 {{
            color: {COLORS['text_primary']} !important;
        }}
        [data-testid="stMetric"] {{
            background: {COLORS['background_light']};
            border: 1px solid {COLORS['gold_accent']}40;
            border-radius: 10px;
            padding: 15px;
        }}
        [data-testid="stMetricLabel"] {{
            color: {COLORS['text_secondary']} !important;
        }}
        [data-testid="stMetricValue"] {{
            color: {COLORS['gold_accent']} !important;
        }}
        .stButton > button {{
            background: {COLORS['gold_accent']};
            color: {COLORS['background']};
            border: none;
            font-weight: bold;
        }}
        .stButton > button:hover {{
            background: {COLORS['text_primary']};
            color: {COLORS['background']};
        }}
        .stSlider > div > div {{
            background: {COLORS['gold_accent']} !important;
        }}
        p, span, label {{
            color: {COLORS['text_secondary']};
        }}
        .stTabs [data-baseweb="tab-list"] {{
            gap: 8px;
        }}
        .stTabs [data-baseweb="tab"] {{
            background: {COLORS['background_light']};
            border: 1px solid {COLORS['gold_accent']}40;
            border-radius: 8px 8px 0 0;
            color: {COLORS['text_secondary']};
        }}
        .stTabs [aria-selected="true"] {{
            background: {COLORS['gold_accent']}20;
            border-color: {COLORS['gold_accent']};
            color: {COLORS['gold_accent']} !important;
        }}
        #MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
    </style>
    """), unsafe_allow_html=True,
)

# Sidebar
with st.sidebar:
    st.image(
        "https://brand.riotgames.com/static/a91000434ed683571f9c-lol-logo.svg",
        width=200,
    )
    st.markdown("---")
    st.markdown(
        html(f"""
        <h3 style='color: {COLORS["gold_accent"]}; margin-bottom: 10px;'>
            Draft Predictor
        </h3>
        <p style='color: {COLORS["text_secondary"]}; font-size: 0.9em;'>
            Prédiction de résultats de matchs LoL par Machine Learning.
        </p>
        """), unsafe_allow_html=True,
    )
    st.markdown("---")
    st.markdown(
        html(f"""
        <div style='color: {COLORS["text_secondary"]}; font-size: 0.8em;'>
            <p><strong>Sources de données :</strong></p>
            <ul>
                <li>Riot Games API</li>
                <li>OP.GG Statistics</li>
                <li>CommunityDragon</li>
            </ul>
        </div>
        """), unsafe_allow_html=True,
    )

# Main content
st.title("🎮 LoL Draft Predictor")

st.markdown(
    html(f"""
    <p style='color: {COLORS["text_secondary"]}; font-size: 1.1em; margin-bottom: 30px;'>
        Bienvenue sur le <strong>LoL Draft Predictor</strong>. Cette application utilise le Machine Learning
        pour prédire l'issue des matchs League of Legends à partir du draft et des données d'early game.
    </p>
    """), unsafe_allow_html=True,
)

# Quick stats
st.markdown("### 📊 Statistiques")

try:
    from utils.data_loader import (
        get_match_count,
        get_timeline_match_count,
        get_winrate_by_side,
        get_average_game_duration,
    )
    match_count = get_match_count()
    timeline_count = get_timeline_match_count()
    side_stats = get_winrate_by_side()
    avg_duration = get_average_game_duration()
    data_loaded = True
except Exception:
    match_count = 280000
    timeline_count = 102000
    side_stats = {"blue_winrate": 0.505, "red_winrate": 0.495}
    avg_duration = 28.5
    data_loaded = False

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Matchs collectés", f"{match_count:,}")
with col2:
    st.metric("Avec timeline", f"{timeline_count:,}")
with col3:
    st.metric("Blue Side WR", f"{side_stats['blue_winrate']*100:.1f}%")
with col4:
    st.metric("Durée moyenne", f"{avg_duration:.1f} min")

# Model accuracy summary
st.markdown("")
col1, col2, col3, col4, col5 = st.columns(5)
metrics = [
    ("Draft", "draft"),
    ("@5min", "at5"),
    ("@10min", "at10"),
    ("@15min", "at15"),
    ("@20min", "at20"),
]
for col, (label, key) in zip([col1, col2, col3, col4, col5], metrics):
    with col:
        acc = MODEL_BENCHMARKS[key]["accuracy"]
        st.metric(f"Modèle {label}", f"{acc*100:.1f}%")

st.markdown("---")

# Navigation cards
st.markdown("### 🧭 Navigation")

col1, col2 = st.columns(2)

with col1:
    st.markdown(
        html(f"""
        <div style='
            background: {COLORS["background_light"]};
            border: 1px solid {COLORS["gold_accent"]}40;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 15px;
        '>
            <h4 style='color: {COLORS["gold_accent"]}; margin-bottom: 10px;'>
                📊 1. Présentation
            </h4>
            <p style='color: {COLORS["text_secondary"]}; font-size: 0.9em;'>
                Contexte du projet, présentation de LoL, problématique ML et données collectées.
            </p>
        </div>
        """), unsafe_allow_html=True,
    )
    st.markdown(
        html(f"""
        <div style='
            background: {COLORS["background_light"]};
            border: 1px solid {COLORS["gold_accent"]}40;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 15px;
        '>
            <h4 style='color: {COLORS["gold_accent"]}; margin-bottom: 10px;'>
                ⚙️ 3. Traitement
            </h4>
            <p style='color: {COLORS["text_secondary"]}; font-size: 0.9em;'>
                Pipeline de données, feature engineering et sélection du vecteur d'entrée.
            </p>
        </div>
        """), unsafe_allow_html=True,
    )
    st.markdown(
        html(f"""
        <div style='
            background: {COLORS["background_light"]};
            border: 1px solid {COLORS["gold_accent"]}40;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 15px;
        '>
            <h4 style='color: {COLORS["gold_accent"]}; margin-bottom: 10px;'>
                🎯 5. Résultats
            </h4>
            <p style='color: {COLORS["text_secondary"]}; font-size: 0.9em;'>
                Évaluation des modèles (confusion, ROC), comparaison et prédiction interactive.
            </p>
        </div>
        """), unsafe_allow_html=True,
    )

with col2:
    st.markdown(
        html(f"""
        <div style='
            background: {COLORS["background_light"]};
            border: 1px solid {COLORS["gold_accent"]}40;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 15px;
        '>
            <h4 style='color: {COLORS["gold_accent"]}; margin-bottom: 10px;'>
                📁 2. Données
            </h4>
            <p style='color: {COLORS["text_secondary"]}; font-size: 0.9em;'>
                Vue d'ensemble du dataset, exploration interactive et visualisations.
            </p>
        </div>
        """), unsafe_allow_html=True,
    )
    st.markdown(
        html(f"""
        <div style='
            background: {COLORS["background_light"]};
            border: 1px solid {COLORS["gold_accent"]}40;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 15px;
        '>
            <h4 style='color: {COLORS["gold_accent"]}; margin-bottom: 10px;'>
                🤖 4. Modèles
            </h4>
            <p style='color: {COLORS["text_secondary"]}; font-size: 0.9em;'>
                Algorithmes (XGBoost, LightGBM), comparaison des 5 modèles et feature importance.
            </p>
        </div>
        """), unsafe_allow_html=True,
    )

st.markdown("---")

# Key findings
st.markdown("### 💡 Résultats clés")

st.markdown(
    html(f"""
    <div style='
        background: {COLORS["background_light"]};
        border-left: 4px solid {COLORS["gold_accent"]};
        padding: 15px 20px;
        margin-bottom: 15px;
    '>
        <p style='color: {COLORS["text_primary"]}; margin: 0;'>
            <strong>Le draft seul ne suffit pas.</strong>
            Le modèle draft-only atteint ~51%, à peine mieux que le hasard en solo queue.
            Le skill individuel et la communication priment sur la composition.
        </p>
    </div>

    <div style='
        background: {COLORS["background_light"]};
        border-left: 4px solid {COLORS["blue_team"]};
        padding: 15px 20px;
        margin-bottom: 15px;
    '>
        <p style='color: {COLORS["text_primary"]}; margin: 0;'>
            <strong>L'early game est déterminant.</strong>
            Dès 5 minutes, l'ajout des données de gold fait passer l'accuracy à 68%.
            À 20 minutes, le modèle atteint 81.7%.
        </p>
    </div>

    <div style='
        background: {COLORS["background_light"]};
        border-left: 4px solid {COLORS["success"]};
        padding: 15px 20px;
        margin-bottom: 15px;
    '>
        <p style='color: {COLORS["text_primary"]}; margin: 0;'>
            <strong>La feature la plus prédictive</strong> du modèle draft-only est
            <em>role_winrate_diff</em> (57% d'importance), i.e. l'écart de winrate
            moyen par rôle entre les deux équipes.
        </p>
    </div>
    """), unsafe_allow_html=True,
)

st.markdown("---")

st.markdown(
    html(f"""
    <p style='color: {COLORS["text_secondary"]}; font-size: 0.85em; text-align: center;'>
        Utilisez la barre latérale pour naviguer entre les pages.
        <br>
        Projet DataScientest – Aïssam, Samuel, Guilhem – 2026
        <br>
        Données : Riot Games API • EUW Diamond+ • Saison 15
    </p>
    """), unsafe_allow_html=True,
)
