"""
Data Exploration Page - Raw data analysis
Based on colleague's work
"""
import streamlit as st
import pandas as pd
import numpy as np
import io
import sys
from pathlib import Path

# Setup paths
STREAMLIT_APP_DIR = Path(__file__).parent.parent.resolve()
PROJECT_ROOT = STREAMLIT_APP_DIR.parent
sys.path.insert(0, str(STREAMLIT_APP_DIR))

from config import COLORS, html, DATABASE_PATH

st.set_page_config(
    page_title="Exploration des données - LoL Draft Predictor",
    page_icon="🔍",
    layout="wide",
)

st.title("🔍 Exploration du jeu de données")

# Load data
@st.cache_data(ttl=3600)
def load_raw_data():
    """Load raw match data from database."""
    import sqlite3

    conn = sqlite3.connect(str(DATABASE_PATH))

    # Colonnes à garder
    query = """
        SELECT
            m.region,
            m.source_elo,
            m.game_duration,
            m.game_version,
            m.team_100_win,
            -- Team 100 champions
            p100_top.champion_name as team_100_top_champion_name,
            p100_jg.champion_name as team_100_jungle_champion_name,
            p100_mid.champion_name as team_100_mid_champion_name,
            p100_adc.champion_name as team_100_adc_champion_name,
            p100_sup.champion_name as team_100_support_champion_name,
            -- Team 200 champions
            p200_top.champion_name as team_200_top_champion_name,
            p200_jg.champion_name as team_200_jungle_champion_name,
            p200_mid.champion_name as team_200_mid_champion_name,
            p200_adc.champion_name as team_200_adc_champion_name,
            p200_sup.champion_name as team_200_support_champion_name,
            -- Bans
            t100.ban_1_champion_id as team_100_ban_1,
            t100.ban_2_champion_id as team_100_ban_2,
            t100.ban_3_champion_id as team_100_ban_3,
            t100.ban_4_champion_id as team_100_ban_4,
            t100.ban_5_champion_id as team_100_ban_5,
            t200.ban_1_champion_id as team_200_ban_1,
            t200.ban_2_champion_id as team_200_ban_2,
            t200.ban_3_champion_id as team_200_ban_3,
            t200.ban_4_champion_id as team_200_ban_4,
            t200.ban_5_champion_id as team_200_ban_5,
            -- First tower
            t100.first_tower as team_100_first_tower,
            t100.tower_kills as team_100_tower_kills,
            t200.first_tower as team_200_first_tower,
            t200.tower_kills as team_200_tower_kills
        FROM matches m
        LEFT JOIN team_stats t100 ON m.match_id = t100.match_id AND t100.team_id = 100
        LEFT JOIN team_stats t200 ON m.match_id = t200.match_id AND t200.team_id = 200
        LEFT JOIN player_stats p100_top ON m.match_id = p100_top.match_id AND p100_top.team_id = 100 AND p100_top.position = 'top'
        LEFT JOIN player_stats p100_jg ON m.match_id = p100_jg.match_id AND p100_jg.team_id = 100 AND p100_jg.position = 'jungle'
        LEFT JOIN player_stats p100_mid ON m.match_id = p100_mid.match_id AND p100_mid.team_id = 100 AND p100_mid.position = 'mid'
        LEFT JOIN player_stats p100_adc ON m.match_id = p100_adc.match_id AND p100_adc.team_id = 100 AND p100_adc.position = 'adc'
        LEFT JOIN player_stats p100_sup ON m.match_id = p100_sup.match_id AND p100_sup.team_id = 100 AND p100_sup.position = 'support'
        LEFT JOIN player_stats p200_top ON m.match_id = p200_top.match_id AND p200_top.team_id = 200 AND p200_top.position = 'top'
        LEFT JOIN player_stats p200_jg ON m.match_id = p200_jg.match_id AND p200_jg.team_id = 200 AND p200_jg.position = 'jungle'
        LEFT JOIN player_stats p200_mid ON m.match_id = p200_mid.match_id AND p200_mid.team_id = 200 AND p200_mid.position = 'mid'
        LEFT JOIN player_stats p200_adc ON m.match_id = p200_adc.match_id AND p200_adc.team_id = 200 AND p200_adc.position = 'adc'
        LEFT JOIN player_stats p200_sup ON m.match_id = p200_sup.match_id AND p200_sup.team_id = 200 AND p200_sup.position = 'support'
        LIMIT 50000
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # Convert game_duration to minutes
    df['game_duration'] = df['game_duration'] / 60

    return df

# Load data
try:
    df = load_raw_data()
    data_loaded = True
except Exception as e:
    st.error(f"Erreur lors du chargement des données: {e}")
    data_loaded = False
    df = pd.DataFrame()

if data_loaded and not df.empty:
    st.header("Informations générales")

    st.dataframe(df.head(10))
    st.write(f"**Shape:** {df.shape[0]:,} lignes × {df.shape[1]} colonnes")

    # DataFrame info
    buffer = io.StringIO()
    df.info(buf=buffer, max_cols=None)
    info_str = buffer.getvalue()

    with st.expander("📋 Informations sur le DataFrame"):
        st.text(info_str)

    # Describe
    st.subheader("📊 Statistiques descriptives")
    st.dataframe(df.describe())

    # Doublons et NA
    col1, col2 = st.columns(2)

    with col1:
        nb_doublons = df.duplicated().sum()
        st.metric(label="Nombre de doublons", value=nb_doublons)

    with col2:
        nb_na = df.isna().any(axis=1).sum()
        st.metric(label="Lignes avec valeurs manquantes", value=f"{nb_na:,}")

    # Types de variables
    st.subheader("📈 Types de variables")

    quantitatives = df.select_dtypes(include="number").columns
    qualitatives = df.select_dtypes(exclude="number").columns

    col1, col2 = st.columns(2)

    col1.metric("Variables quantitatives", len(quantitatives))
    col2.metric("Variables qualitatives", len(qualitatives))

    with st.expander("Détail des variables"):
        st.write("### Quantitatives")
        st.write(list(quantitatives))

        st.write("### Qualitatives")
        st.write(list(qualitatives))

    # Missing values detail
    st.subheader("🔍 Valeurs manquantes par colonne")

    missing = df.isnull().sum()
    missing_pct = (missing / len(df) * 100).round(2)
    missing_df = pd.DataFrame({
        'Colonne': missing.index,
        'Manquants': missing.values,
        'Pourcentage (%)': missing_pct.values
    })
    missing_df = missing_df[missing_df['Manquants'] > 0].sort_values('Manquants', ascending=False)

    if len(missing_df) > 0:
        st.dataframe(missing_df)
    else:
        st.success("✅ Aucune valeur manquante!")

else:
    st.warning("Aucune donnée disponible. Veuillez d'abord collecter des données.")
