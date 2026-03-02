"""
Page 4 - Présentation des modèles
"""
import streamlit as st
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

STREAMLIT_APP_DIR = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(STREAMLIT_APP_DIR))
sys.path.insert(1, str(STREAMLIT_APP_DIR.parent / "src" / "collect_data"))

from config import COLORS, html, VECTOR_TYPES, MODEL_BENCHMARKS

st.set_page_config(page_title="Modèles - LoL Draft Predictor", page_icon="🤖", layout="wide")

st.title("🤖 Modèles de Prédiction")

# ============================
# Sidebar selector (for independent access)
# ============================
if "vector_type" not in st.session_state:
    st.session_state.vector_type = "draft"

with st.sidebar:
    st.markdown("### Vecteur d'entrée")
    vector_options = list(VECTOR_TYPES.keys())
    sidebar_labels = [VECTOR_TYPES[k]["name"] for k in vector_options]
    current_idx = vector_options.index(st.session_state.vector_type)
    sidebar_selection = st.selectbox(
        "Sélection rapide",
        sidebar_labels,
        index=current_idx,
        key="sidebar_vector_modeles",
    )
    st.session_state.vector_type = vector_options[sidebar_labels.index(sidebar_selection)]

vector_type = st.session_state.vector_type
vt = VECTOR_TYPES[vector_type]
bm = MODEL_BENCHMARKS[vector_type]

# ============================
# Section 1 - Algorithmes
# ============================
st.header("1. Algorithmes utilisés")

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("XGBoost")
    st.markdown("""
    **eXtreme Gradient Boosting**

    - Boosting d'arbres de décision
    - Régularisation L1/L2 intégrée
    - Gestion native des valeurs manquantes
    - Très performant sur données tabulaires
    - Utilisé pour : Draft, @10, @15, @20 min
    """)

with col2:
    st.subheader("LightGBM")
    st.markdown("""
    **Light Gradient Boosting Machine**

    - Variante optimisée du gradient boosting
    - Croissance par feuille (leaf-wise)
    - Plus rapide que XGBoost sur grands datasets
    - Moins gourmand en mémoire
    - Utilisé pour : @5 min
    """)

with col3:
    st.subheader("Approche générale")
    st.markdown("""
    **Pipeline commun**

    1. StandardScaler sur les features
    2. Entraînement sur 80% des données
    3. Validation sur 20% (split temporel)
    4. Métriques : Accuracy, AUC-ROC
    5. Sauvegarde au format pickle (.pkl)
    """)

st.markdown("---")

# ============================
# Section 2 - Modèle sélectionné
# ============================
st.header(f"2. Modèle sélectionné : {vt['name']}")

from utils.model_loader import load_production_model

model_data = load_production_model(vector_type)

if model_data is not None:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Algorithme", model_data.get("model_name", "N/A"))
    with col2:
        st.metric("Accuracy test", f"{model_data.get('test_accuracy', 0)*100:.1f}%")
    with col3:
        st.metric("Accuracy validation", f"{model_data.get('val_accuracy', 0)*100:.1f}%")
    with col4:
        st.metric("Nb features", len(model_data.get("features", [])))

    # Overfitting check
    val_acc = model_data.get("val_accuracy", 0)
    test_acc = model_data.get("test_accuracy", 0)
    if val_acc > 0 and test_acc > 0:
        diff = (val_acc - test_acc) * 100
        if diff > 5:
            st.warning(f"⚠️ Écart validation/test de {diff:.1f}% — possible overfitting")
        else:
            st.success(f"✅ Écart validation/test de {diff:.1f}% — bonne généralisation")
else:
    st.error("Impossible de charger le modèle sélectionné.")

st.markdown("---")

# ============================
# Section 3 - Comparaison des 5 modèles
# ============================
st.header("3. Comparaison des 5 modèles")

bench_df = pd.DataFrame([
    {
        "Vecteur": MODEL_BENCHMARKS[k]["name"],
        "Accuracy": MODEL_BENCHMARKS[k]["accuracy"],
        "Algorithme": MODEL_BENCHMARKS[k]["model_type"],
    }
    for k in vector_options
])

fig = go.Figure()

colors = [COLORS["gold_accent"] if k == vector_type else COLORS["text_secondary"] for k in vector_options]

fig.add_trace(go.Bar(
    x=bench_df["Vecteur"],
    y=bench_df["Accuracy"] * 100,
    marker_color=colors,
    text=[f"{a*100:.1f}%" for a in bench_df["Accuracy"]],
    textposition="auto",
    hovertemplate="<b>%{x}</b><br>Accuracy: %{y:.1f}%<extra></extra>",
))

fig.update_layout(
    title="Accuracy des modèles sur le jeu de test",
    yaxis_title="Accuracy (%)",
    yaxis_range=[40, 90],
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    showlegend=False,
    height=400,
)

# Add 50% baseline
fig.add_hline(y=50, line_dash="dash", line_color=COLORS["red_team"],
              annotation_text="Aléatoire (50%)", annotation_position="bottom right")

st.plotly_chart(fig, use_container_width=True)

st.markdown("""
**Observations :**
- Le modèle **draft-only** atteint ~51%, à peine mieux que le hasard → le draft seul ne suffit pas en solo queue
- L'ajout de données **@5min** fait bondir l'accuracy à **68%** → le gold early est très prédictif
- La progression continue jusqu'à **81.7%** à **@20min**
- Le gain marginal diminue au fil du temps (@5→@10 = +5.6%, @15→@20 = +2.7%)
""")

st.markdown("---")

# ============================
# Section 4 - Feature importance
# ============================
st.header("4. Feature Importance")

if model_data is not None:
    from utils.model_loader import get_tree_feature_importance

    model = model_data.get("model")
    features = model_data.get("features", [])

    fi_df = get_tree_feature_importance(model, features)

    if not fi_df.empty:
        n_features = st.slider("Nombre de features à afficher", 5, min(50, len(fi_df)), 20)
        top_fi = fi_df.head(n_features)

        fig = px.bar(
            top_fi,
            x="importance",
            y="feature",
            orientation="h",
            title=f"Top {n_features} Feature Importance – {vt['name']}",
            color="importance",
            color_continuous_scale="YlOrRd",
        )
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(autorange="reversed"),
            height=max(400, n_features * 25),
            showlegend=False,
        )
        fig.update_coloraxes(showscale=False)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Tableau complet"):
            display_df = fi_df.copy()
            display_df["importance"] = display_df["importance"].round(6)
            display_df["% total"] = (display_df["importance"] / display_df["importance"].sum() * 100).round(2)
            st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.warning("Impossible d'extraire le feature importance pour ce modèle.")
else:
    st.warning("Modèle non chargé – impossible d'afficher le feature importance.")
