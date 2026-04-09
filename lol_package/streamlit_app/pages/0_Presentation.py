"""
Presentation Page - Project introduction
Based on colleague's work
"""
import streamlit as st
import sys
from pathlib import Path

# Setup paths
STREAMLIT_APP_DIR = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(STREAMLIT_APP_DIR))

from config import COLORS, html

st.set_page_config(
    page_title="Présentation - LoL Draft Predictor",
    page_icon="🎮",
    layout="wide",
)

st.title("🎮 League of Legends – Draft Predictor")

st.markdown("""
## Prédiction de victoire basée sur le Draft

**Projet DataScientest – Machine Learning**
""")

st.markdown("---")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    **Auteurs**
    Aïssam, Samuel, Guilhem

    **Date**
    Janvier 2026
    """)

with col2:
    st.markdown("""
    **Formation**
    Data Scientist – Promotion Juin MLE25
    """)

st.markdown("---")

st.header("1. Introduction & Contexte")

# ======================
# 1.1 Présentation du jeu
# ======================
st.subheader("1.1 League of Legends – Présentation du jeu")

st.markdown("""
**League of Legends** (LoL) est un jeu vidéo de type **MOBA** (Multiplayer Online Battle Arena) développé par **Riot Games**.
C'est l'un des esports les plus populaires au monde :
""")

st.markdown("""
- 📊 **180+ millions** de joueurs actifs mensuels
- 🏆 **Championnats du monde** regardés par **100M+** de spectateurs
- 💰 **Prize pools** dépassant les **2M$** par tournoi majeur
""")

st.markdown("### Principe du jeu")

st.markdown("""
- **2 équipes de 5 joueurs** s'affrontent sur une carte asymétrique
- Chaque joueur contrôle **1 champion** parmi **160+ personnages uniques**
- **Objectif** : Détruire le **Nexus adverse**
- **Durée moyenne** : **25–35 minutes**
""")

# ======================
# Rôles
# ======================
st.markdown("### Les 5 rôles")

roles_df = {
    "Rôle": ["Top", "Jungle", "Mid", "ADC", "Support"],
    "Lane": ["Top Lane", "Jungle", "Mid Lane", "Bot Lane", "Bot Lane"],
    "Description": [
        "Tank ou Bruiser – Frontline",
        "Ganker – Contrôle des objectifs",
        "Mage ou Assassin – Burst damage",
        "Marksman – Dégâts soutenus",
        "Utilitaire – Protection de l'ADC"
    ]
}

st.table(roles_df)

# Carte
st.image(
    "https://static.wikia.nocookie.net/leagueoflegends/images/7/76/Summoner%27s_Rift_Update_map.png/revision/latest?cb=20200120211206",
    caption="Carte officielle – Summoner's Rift"
)

# ======================
# 1.2 Draft
# ======================
st.subheader("1.2 La phase de Draft")

st.markdown("""
Avant chaque partie, les équipes passent par une **phase de draft cruciale**.
""")

st.markdown("### Étapes du Draft")

st.markdown("""
1. **Bans** : Chaque équipe bannit **5 champions**
2. **Picks** : Sélection alternée des **10 champions**
3. **Durée** : ~**5 minutes**
""")

st.markdown("### Importance stratégique")

st.info("""
Le draft représente **40–60% de l'issue du match** selon les joueurs professionnels.
""")

st.markdown("""
- ✅ **Synergies** : Combinaisons puissantes (ex : *Yasuo + Malphite*)
- ✅ **Counter-picks** : Champions qui dominent d'autres (ex : *Fiora vs Tanks*)
- ✅ **Composition d'équipe** : Damage, tankiness, CC, utility
- ✅ **Matchups de lane**
- ✅ **Meta du patch**
""")

# ======================
# 1.3 Problématique ML
# ======================
st.subheader("1.3 Problématique Machine Learning")

st.markdown("### Question de recherche")

st.success("""
**Peut-on prédire l'issue d'un match League of Legends uniquement à partir du draft ?**
""")

st.markdown("### Objectifs")

st.markdown("""
1. 🎯 **Prédire la victoire** (classification binaire)
2. 📊 **Identifier les facteurs clés**
3. 🔍 **Analyser les synergies de champions**
4. ⚖️ **Détecter les counter-picks**
""")

st.markdown("### Défis")

st.warning("""
- ⚠️ **160+ champions** → espace combinatoire énorme
- ⚠️ **Meta évolutive** (patch toutes les 2 semaines)
- ⚠️ **Facteur humain non observé**
- ⚠️ **Données déséquilibrées**
""")

st.markdown("### Approche")

st.code("""
Data Collection → Feature Engineering → ML Model → Predictions
    ↓                    ↓                  ↓            ↓
API Riot           Playstyle +       Gradient      Win/Loss
OP.GG Scraping    Synergies +       Boosting      Probability
CommunityDragon   Matchups          XGBoost
""")

st.markdown("### Données")

# Load real stats if available
try:
    from utils.data_loader import get_match_count
    match_count = get_match_count()
    match_str = f"{match_count:,}"
except:
    match_str = "280,000+"

st.markdown(f"""
- 📦 **{match_str} matchs** high-elo
- 🌍 **Région** : EUW (Europe West)
- 🏅 **Elo** : Diamond+ → Challenger
- 📅 **Saison 15 (2025)**
""")
