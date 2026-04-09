"""
Data Visualization Page - Charts and analysis
Based on colleague's work
"""
import streamlit as st
import pandas as pd
import numpy as np
import math
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns

# Setup paths
STREAMLIT_APP_DIR = Path(__file__).parent.parent.resolve()
PROJECT_ROOT = STREAMLIT_APP_DIR.parent
sys.path.insert(0, str(STREAMLIT_APP_DIR))

from config import COLORS, html, DATABASE_PATH

st.set_page_config(
    page_title="Data Visualization - LoL Draft Predictor",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Data Visualization")

# Set matplotlib style for dark theme
plt.style.use('dark_background')
plt.rcParams['figure.facecolor'] = COLORS['background']
plt.rcParams['axes.facecolor'] = COLORS['background_light']
plt.rcParams['text.color'] = COLORS['text_primary']
plt.rcParams['axes.labelcolor'] = COLORS['text_secondary']
plt.rcParams['xtick.color'] = COLORS['text_secondary']
plt.rcParams['ytick.color'] = COLORS['text_secondary']


@st.cache_data(ttl=3600)
def load_visualization_data():
    """Load match data from database for visualization."""
    import sqlite3

    conn = sqlite3.connect(str(DATABASE_PATH))

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
        LIMIT 100000
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # Convert game_duration to minutes
    df['game_duration'] = df['game_duration'] / 60

    # Convert boolean columns to object for proper display
    cols_to_object = ["team_100_win", "team_100_first_tower", "team_200_first_tower"]
    for col in cols_to_object:
        if col in df.columns:
            df[col] = df[col].astype("object")

    return df


# Load data
try:
    df = load_visualization_data()
    data_loaded = True
except Exception as e:
    st.error(f"Erreur lors du chargement des données: {e}")
    data_loaded = False
    df = pd.DataFrame()

if data_loaded and not df.empty:

    # General info
    st.header("Informations générales")
    st.dataframe(df.head(10))
    st.write(f"**Shape:** {df.shape[0]:,} lignes x {df.shape[1]} colonnes")

    # Variable types
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

    st.markdown("---")

    # ======================
    # UNIVARIATE ANALYSIS
    # ======================
    st.header("Analyse univariée")

    st.subheader("Répartition des variables qualitatives")

    # Variables qualitatives for pie charts
    cat_vars = ["team_100_win", "team_100_first_tower", "team_200_first_tower"]
    available_cat_vars = [c for c in cat_vars if c in df.columns]

    if available_cat_vars:
        n = len(available_cat_vars)
        cols = min(3, n)
        rows = (n + cols - 1) // cols

        fig, axes = plt.subplots(rows, cols, figsize=(cols * 6, rows * 5))
        if n == 1:
            axes = [axes]
        else:
            axes = axes.flatten()

        for i, col in enumerate(available_cat_vars):
            counts = df[col].value_counts()

            # Custom colors
            colors = [COLORS['blue_team'], COLORS['red_team']]

            axes[i].pie(
                counts,
                labels=counts.index,
                autopct=lambda p: f'{p:.1f}%',
                startangle=90,
                textprops={'fontsize': 14, 'color': COLORS['text_primary']},
                colors=colors[:len(counts)]
            )
            axes[i].set_title(col, fontsize=14, color=COLORS['gold_accent'])

        # Hide remaining axes
        for j in range(i + 1, len(axes)):
            axes[j].set_visible(False)

        plt.tight_layout()
        st.pyplot(fig)
        plt.close()

    st.subheader("Top 10 des champions les plus joués par rôle")

    # Champion columns
    champion_cols = [col for col in df.columns if "champion_name" in col]

    if champion_cols:
        cols_per_row = 2
        n_rows = math.ceil(len(champion_cols) / cols_per_row)
        fig, axes = plt.subplots(n_rows, cols_per_row, figsize=(6 * cols_per_row, 4 * n_rows))
        axes = axes.flatten()

        for i, col in enumerate(champion_cols):
            top10 = df[col].value_counts().head(10)

            tmp = pd.DataFrame({
                "label": top10.index.astype(str),
                "count": top10.values
            })
            tmp["label"] = pd.Categorical(tmp["label"], categories=tmp["label"], ordered=True)

            sns.barplot(
                data=tmp,
                x="count",
                y="label",
                hue="label",
                palette="magma",
                dodge=False,
                legend=False,
                ax=axes[i]
            )

            axes[i].set_xlim(0, tmp["count"].max() * 1.15)
            # Simplify title
            title = col.replace("team_100_", "Blue ").replace("team_200_", "Red ").replace("_champion_name", "").title()
            axes[i].set_title(title, color=COLORS['gold_accent'])
            axes[i].set_xlabel("Occurrences")
            axes[i].set_ylabel("")

            # Display values
            for j, v in enumerate(tmp["count"]):
                axes[i].text(v + max(tmp["count"]) * 0.01, j, str(v), va="center", color=COLORS['text_secondary'])

        # Remove empty axes
        for j in range(len(champion_cols), len(axes)):
            fig.delaxes(axes[j])

        plt.tight_layout()
        st.pyplot(fig)
        plt.close()

    st.subheader("Répartition des variables quantitatives")

    quant_vars = df.select_dtypes(include=np.number).columns.tolist()
    if quant_vars:
        n = len(quant_vars)
        cols = 3
        rows = (n + cols - 1) // cols

        fig, axes = plt.subplots(rows, cols, figsize=(cols * 5, rows * 4))
        axes = axes.flatten()

        for i, var in enumerate(quant_vars):
            sns.histplot(df[var], kde=True, ax=axes[i], color=COLORS['gold_accent'])
            axes[i].set_title(var, color=COLORS['gold_accent'])

        # Hide empty axes
        for j in range(i + 1, len(axes)):
            axes[j].set_visible(False)

        plt.tight_layout()
        st.pyplot(fig)
        plt.close()

    st.markdown("---")

    # ======================
    # BIVARIATE ANALYSIS
    # ======================
    st.header("Analyse bivariée")

    st.subheader("First Tower vs Victory")

    target = "team_100_win"
    tower_vars = ["team_100_first_tower", "team_200_first_tower"]
    available_tower_vars = [col for col in tower_vars if col in df.columns]

    if available_tower_vars:
        n = len(available_tower_vars)
        fig, axes = plt.subplots(1, n, figsize=(8 * n, 6))
        if n == 1:
            axes = [axes]

        for i, col in enumerate(available_tower_vars):
            sns.countplot(
                data=df,
                x=col,
                hue=target,
                palette=[COLORS['blue_team'], COLORS['red_team']],
                ax=axes[i]
            )

            axes[i].set_title(f"{col} vs {target}", fontsize=12, color=COLORS['gold_accent'])
            axes[i].set_xlabel("")
            axes[i].set_ylabel("Count")

            # Add counts on bars
            for p in axes[i].patches:
                height = p.get_height()
                if height > 0:
                    axes[i].text(
                        p.get_x() + p.get_width() / 2,
                        height + max(df[col].value_counts()) * 0.01,
                        f"{int(height)}",
                        ha='center',
                        va='bottom',
                        fontsize=10,
                        color=COLORS['text_primary']
                    )

            axes[i].legend(title=target, loc='upper right')

        plt.tight_layout()
        st.pyplot(fig)
        plt.close()

    st.subheader("Win rate des champions TOP (Team 100)")

    var1 = "team_100_top_champion_name"
    if var1 in df.columns:
        # Calculate win rate per champion
        winrate_df = (
            df.groupby(var1)[target]
            .mean()
            .reset_index(name="win_rate")
        )

        # Keep top 10 most played
        top10_champs = df[var1].value_counts().nlargest(10).index
        winrate_df = winrate_df[winrate_df[var1].isin(top10_champs)]
        winrate_df = winrate_df.sort_values(by="win_rate", ascending=False)

        fig, ax = plt.subplots(figsize=(12, 6))

        sns.barplot(
            x=var1,
            y="win_rate",
            data=winrate_df,
            hue=var1,
            palette="viridis",
            dodge=False,
            legend=False,
            ax=ax
        )

        ax.set_title("Top 10 champions les plus joués (TOP) - Win rate", color=COLORS['gold_accent'])
        ax.set_ylabel("Win rate")
        ax.set_xlabel("Champion")
        ax.set_ylim(0, 1)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')

        # Display win rate on bars
        for i, row in enumerate(winrate_df.itertuples()):
            ax.text(
                i,
                row.win_rate + 0.02,
                f"{row.win_rate:.2f}",
                ha="center",
                fontsize=10,
                color=COLORS['text_primary']
            )

        plt.tight_layout()
        st.pyplot(fig)
        plt.close()

    st.markdown("---")

    # ======================
    # SYNERGIES ANALYSIS
    # ======================
    st.header("Analyse des synergies")

    st.subheader("Synergies Team 100 (Top + Role)")

    vars_to_check = [col for col in df.columns if "champion_name" in col and "100" in col]
    top_var = "team_100_top_champion_name"
    other_vars = [col for col in vars_to_check if col != top_var]

    results = {}
    for var in other_vars:
        temp = (
            df.groupby([top_var, var])[target]
            .agg(winrate="mean", count="size")
            .reset_index()
            .sort_values("count", ascending=False)
        )
        results[(top_var, var)] = temp

    for (top_v, var), table in results.items():
        role_name = var.replace("team_100_", "").replace("_champion_name", "").upper()
        with st.expander(f"SYNERGIES : TOP + {role_name}"):
            st.markdown(f"**Nombre total de combos uniques : {len(table)}**")
            st.dataframe(table.head(10))

    st.subheader("Synergies: Nuages de points")

    if other_vars:
        plots = {}
        for var in other_vars:
            temp = (
                df.groupby([top_var, var])[target]
                .agg(winrate="mean", count="size")
                .reset_index()
            )
            plots[var] = temp

        cols = 2
        rows = (len(plots) + 1) // cols
        fig, axes = plt.subplots(rows, cols, figsize=(12, 10))
        axes = axes.flatten()

        for ax, (role, data) in zip(axes, plots.items()):
            sns.scatterplot(
                data=data,
                x="count",
                y="winrate",
                color=COLORS['blue_team'],
                s=70,
                alpha=0.4,
                ax=ax
            )

            role_name = role.replace('team_100_', '').replace('_champion_name', '').upper()
            ax.set_title(f"SYNERGIES: TOP + {role_name}", color=COLORS['gold_accent'])
            ax.set_xlabel("Count")
            ax.set_ylabel("Winrate")
            ax.set_ylim(0, 1.05)
            ax.grid(alpha=0.3)

        # Hide empty cells
        for i in range(len(plots), len(axes)):
            axes[i].set_visible(False)

        plt.tight_layout()
        st.pyplot(fig)
        plt.close()

    st.markdown("---")

    # ======================
    # COUNTER ANALYSIS
    # ======================
    st.header("Analyse des contres")

    st.subheader("Contre Team 200 (Top Team 100 vs Roles Team 200)")

    vars_to_check_200 = [col for col in df.columns if "champion_name" in col and "200" in col]

    results_counter = {}
    for var in vars_to_check_200:
        temp = (
            df.groupby([top_var, var])[target]
            .agg(winrate="mean", count="size")
            .reset_index()
            .sort_values("count", ascending=False)
        )
        results_counter[(top_var, var)] = temp

    for (top_v, var), table in results_counter.items():
        role_name = var.replace("team_200_", "").replace("_champion_name", "").upper()
        with st.expander(f"CONTRE : TOP vs {role_name}"):
            st.markdown(f"**Nombre total de combos uniques : {len(table)}**")
            st.dataframe(table.head(10))

    st.subheader("Contres: Nuages de points")

    if vars_to_check_200:
        plots_counter = {}
        for var in vars_to_check_200:
            temp = (
                df.groupby([top_var, var])[target]
                .agg(winrate="mean", count="size")
                .reset_index()
            )
            plots_counter[var] = temp

        cols = 2
        rows = (len(plots_counter) + 1) // cols
        fig, axes = plt.subplots(rows, cols, figsize=(12, 10))
        axes = axes.flatten()

        for ax, (role, data) in zip(axes, plots_counter.items()):
            sns.scatterplot(
                data=data,
                x="count",
                y="winrate",
                color=COLORS['red_team'],
                s=70,
                alpha=0.4,
                ax=ax
            )

            role_name = role.replace('team_200_', '').replace('_champion_name', '').upper()
            ax.set_title(f"CONTRE: TOP vs {role_name}", color=COLORS['gold_accent'])
            ax.set_xlabel("Count")
            ax.set_ylabel("Winrate")
            ax.set_ylim(0, 1.05)
            ax.grid(alpha=0.3)

        # Hide empty cells
        for i in range(len(plots_counter), len(axes)):
            axes[i].set_visible(False)

        plt.tight_layout()
        st.pyplot(fig)
        plt.close()

    st.markdown("---")

    # ======================
    # GAME DURATION ANALYSIS
    # ======================
    st.header("Analyse de la durée des parties")

    st.subheader("Distribution des durées selon victoire Team 100")

    if "game_duration" in df.columns and target in df.columns:
        fig, ax = plt.subplots(figsize=(10, 5))

        # Convert target back to numeric for plotting
        df_plot = df.copy()
        df_plot[target] = pd.to_numeric(df_plot[target], errors='coerce')

        sns.histplot(
            data=df_plot,
            x='game_duration',
            hue=target,
            kde=True,
            palette=[COLORS['red_team'], COLORS['blue_team']],
            alpha=0.6,
            bins=30,
            ax=ax
        )

        ax.set_xlabel("Durée (minutes)")
        ax.set_ylabel("Nombre de parties")
        ax.set_title("Distribution des durées des parties selon la victoire", color=COLORS['gold_accent'])
        ax.legend(title="Team 100 Win", labels=["Défaite", "Victoire"])

        plt.tight_layout()
        st.pyplot(fig)
        plt.close()

    st.subheader("Résumé des durées par résultat")

    if "game_duration" in df.columns:
        df_summary = df.copy()
        df_summary[target] = pd.to_numeric(df_summary[target], errors='coerce')

        summary_table = df_summary.groupby(target)['game_duration'].agg(
            count='size',
            total_minutes='sum',
            avg_minutes='mean'
        ).reset_index()

        summary_table[target] = summary_table[target].map({0: 'Défaite', 1: 'Victoire'})
        summary_table['avg_minutes'] = summary_table['avg_minutes'].round(2)
        summary_table.columns = ['Résultat', 'Nombre de parties', 'Total minutes', 'Moyenne (min)']

        st.dataframe(summary_table)

    st.subheader("Répartition victoires/défaites pour parties courtes (4-23 min)")

    if "game_duration" in df.columns:
        df_filter = df.copy()
        df_filter[target] = pd.to_numeric(df_filter[target], errors='coerce')
        filter_df = df_filter[(df_filter["game_duration"] > 4) & (df_filter["game_duration"] < 23)]

        if not filter_df.empty:
            counts = filter_df[target].value_counts()
            freq = filter_df[target].value_counts(normalize=True) * 100

            table = pd.DataFrame({
                "Résultat": counts.index.map({0: "Défaite", 1: "Victoire"}),
                "Count": counts.values,
                "Fréquence (%)": freq.values.round(2)
            })

            st.dataframe(table)
        else:
            st.info("Pas de parties dans cet intervalle de durée.")

    st.markdown("---")

    # ======================
    # CORRELATION ANALYSIS
    # ======================
    st.header("Analyse des corrélations")

    st.subheader("Matrice de corrélation")

    quant_vars_df = df.select_dtypes(include=np.number)

    if len(quant_vars_df.columns) > 1:
        corr_matrix = quant_vars_df.corr()

        fig, ax = plt.subplots(figsize=(10, 8))
        sns.heatmap(
            corr_matrix,
            annot=True,
            fmt=".2f",
            cmap="coolwarm",
            center=0,
            linewidths=0.5,
            ax=ax,
            annot_kws={"color": COLORS['text_primary']}
        )
        ax.set_title("Matrice de corrélation des variables quantitatives", fontsize=14, color=COLORS['gold_accent'])
        plt.tight_layout()
        st.pyplot(fig)
        plt.close()

        st.subheader("Nuages de points des variables corrélées")

        # Extract correlated pairs
        pairs = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i + 1, len(corr_matrix.columns)):
                if abs(corr_matrix.iloc[i, j]) > 0.1:  # Only show correlations > 0.1
                    pairs.append((corr_matrix.columns[i], corr_matrix.columns[j], corr_matrix.iloc[i, j]))

        if pairs:
            corr_pairs = pd.DataFrame(pairs, columns=["var1", "var2", "correlation"])
            n = len(corr_pairs)

            cols = 3
            rows = math.ceil(n / cols)

            fig, axes = plt.subplots(rows, cols, figsize=(18, rows * 5))
            axes = axes.flatten()

            df_plot = df.copy()
            df_plot[target] = pd.to_numeric(df_plot[target], errors='coerce')

            for idx, row in corr_pairs.iterrows():
                v1, v2, c = row["var1"], row["var2"], row["correlation"]
                ax = axes[idx]

                sns.scatterplot(
                    data=df_plot,
                    x=v1,
                    y=v2,
                    hue=target,
                    palette=[COLORS['red_team'], COLORS['blue_team']],
                    alpha=0.6,
                    legend=True,
                    ax=ax
                )

                ax.legend(loc='upper left', title=target)
                ax.set_title(f"{v1} vs {v2}\ncorr = {c:.2f}", color=COLORS['gold_accent'])
                ax.set_xlabel(v1)
                ax.set_ylabel(v2)

            # Hide remaining axes
            for j in range(n, len(axes)):
                axes[j].set_visible(False)

            plt.tight_layout()
            st.pyplot(fig)
            plt.close()
        else:
            st.info("Aucune corrélation significative trouvée (> 0.1)")

    else:
        st.warning("Pas assez de variables quantitatives pour calculer les corrélations.")

else:
    st.warning("Aucune donnée disponible. Veuillez d'abord collecter des données.")
