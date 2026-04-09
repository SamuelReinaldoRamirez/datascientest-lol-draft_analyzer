import streamlit as st
import pandas as pd

# --------------------------------------------------
# Configuration de la page
# --------------------------------------------------
st.set_page_config(
    page_title="LoL Draft Analyzer",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --------------------------------------------------
# Sidebar
# --------------------------------------------------
st.sidebar.title("⚙️ Paramètres")

show_raw_data = st.sidebar.checkbox("Afficher les données brutes", value=False)

# --------------------------------------------------
# Titre principal
# --------------------------------------------------
st.title("🎮 LoL Draft Analyzer")
st.markdown("Application Streamlit de base")

# --------------------------------------------------
# Chargement des données (exemple)
# --------------------------------------------------
@st.cache_data
def load_data():
    # Exemple bidon pour l’instant
    data = {
        "champion": ["Ahri", "Garen", "Lux"],
        "winrate": [52.3, 49.8, 51.1],
        "games": [1200, 980, 1430],
    }
    return pd.DataFrame(data)

df = load_data()

# --------------------------------------------------
# Contenu principal
# --------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("📊 Aperçu")
    st.metric("Nombre de champions", df.shape[0])

with col2:
    st.subheader("🏆 Meilleur winrate")
    best = df.sort_values("winrate", ascending=False).iloc[0]
    st.write(f"**{best['champion']}** — {best['winrate']}%")

# --------------------------------------------------
# Tableau
# --------------------------------------------------
st.subheader("📋 Données")

if show_raw_data:
    st.dataframe(df, use_container_width=True)

# --------------------------------------------------
# Sélection interactive
# --------------------------------------------------
st.subheader("🔎 Analyse par champion")

champion = st.selectbox("Choisis un champion", df["champion"])

champ_df = df[df["champion"] == champion]

st.write(champ_df)

# --------------------------------------------------
# Footer
# --------------------------------------------------
st.markdown("---")
st.caption("Draft Analyzer • Streamlit")
