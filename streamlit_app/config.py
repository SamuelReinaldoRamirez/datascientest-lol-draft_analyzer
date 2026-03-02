"""
Configuration for the LoL Draft Predictor Streamlit App

Contains colors, paths, and other constants.
"""
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATABASE_PATH = DATA_DIR / "lol_matches.db"
MODELS_DIR = PROJECT_ROOT / "models"

# Data Dragon CDN for champion icons
DDRAGON_VERSION = "15.2.1"
CHAMPION_ICON_URL = f"https://ddragon.leagueoflegends.com/cdn/{DDRAGON_VERSION}/img/champion/{{champion_key}}.png"

# LoL Theme Colors
COLORS = {
    "blue_team": "#3498db",
    "red_team": "#e74c3c",
    "gold_accent": "#c8aa6e",
    "background": "#0a1428",
    "background_light": "#1e2328",
    "text_primary": "#f0e6d2",
    "text_secondary": "#a09b8c",
    "success": "#27ae60",
    "warning": "#f39c12",
    "danger": "#e74c3c",
}


def rgba(hex_color: str, alpha: float = 1.0) -> str:
    """Convert hex color to rgba string for CSS."""
    hex_color = hex_color.lstrip("#")
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def html(text: str) -> str:
    """Strip leading indentation from multiline HTML strings for Streamlit."""
    import textwrap
    return textwrap.dedent(text).strip()


# Pre-computed colors with alpha for CSS (40 hex = 0.25 alpha, 60 hex = 0.38 alpha)
COLORS_ALPHA = {
    "gold_accent_25": rgba(COLORS["gold_accent"], 0.25),
    "gold_accent_40": rgba(COLORS["gold_accent"], 0.40),
    "blue_team_25": rgba(COLORS["blue_team"], 0.25),
    "blue_team_40": rgba(COLORS["blue_team"], 0.40),
    "red_team_25": rgba(COLORS["red_team"], 0.25),
    "red_team_40": rgba(COLORS["red_team"], 0.40),
    "success_40": rgba(COLORS["success"], 0.40),
    "warning_40": rgba(COLORS["warning"], 0.40),
}

# Vector types for model selection
VECTOR_TYPES = {
    "draft": {
        "name": "Draft uniquement",
        "description": "Composition des champions, statistiques des invocateurs, synergies et counters",
        "model_file": "model_draft.pkl",
        "nb_features": 139,
        "extra_features": [],
    },
    "at5": {
        "name": "Draft + @5min",
        "description": "Draft + gold par rôle à 5 minutes",
        "model_file": "model_at5.pkl",
        "nb_features": 152,
        "extra_features": [
            "gold_diff_at_5", "team_100_gold_at_5", "team_200_gold_at_5",
            "team_100_top_gold_at_5", "team_200_top_gold_at_5",
            "team_100_jungle_gold_at_5", "team_200_jungle_gold_at_5",
            "team_100_mid_gold_at_5", "team_200_mid_gold_at_5",
            "team_100_adc_gold_at_5", "team_200_adc_gold_at_5",
            "team_100_support_gold_at_5", "team_200_support_gold_at_5",
        ],
    },
    "at10": {
        "name": "Draft + @10min",
        "description": "Draft + gold et CS par rôle à 10 minutes",
        "model_file": "model_at10.pkl",
        "nb_features": 162,
        "extra_features": [
            "gold_diff_at_10", "team_100_gold_at_10", "team_200_gold_at_10",
            "team_100_top_gold_at_10", "team_200_top_gold_at_10",
            "team_100_jungle_gold_at_10", "team_200_jungle_gold_at_10",
            "team_100_mid_gold_at_10", "team_200_mid_gold_at_10",
            "team_100_adc_gold_at_10", "team_200_adc_gold_at_10",
            "team_100_support_gold_at_10", "team_200_support_gold_at_10",
            "team_100_top_cs_at_10", "team_200_top_cs_at_10",
            "team_100_jungle_cs_at_10", "team_200_jungle_cs_at_10",
            "team_100_mid_cs_at_10", "team_200_mid_cs_at_10",
            "team_100_adc_cs_at_10", "team_200_adc_cs_at_10",
            "team_100_support_cs_at_10", "team_200_support_cs_at_10",
        ],
    },
    "at15": {
        "name": "Draft + @15min",
        "description": "Draft + gold par rôle à 15 minutes",
        "model_file": "model_at15.pkl",
        "nb_features": 162,
        "extra_features": [
            "gold_diff_at_15", "team_100_gold_at_15", "team_200_gold_at_15",
            "team_100_top_gold_at_15", "team_200_top_gold_at_15",
            "team_100_jungle_gold_at_15", "team_200_jungle_gold_at_15",
            "team_100_mid_gold_at_15", "team_200_mid_gold_at_15",
            "team_100_adc_gold_at_15", "team_200_adc_gold_at_15",
            "team_100_support_gold_at_15", "team_200_support_gold_at_15",
        ],
    },
    "at20": {
        "name": "Draft + @20min",
        "description": "Draft + gold par rôle à 20 minutes",
        "model_file": "model_at20.pkl",
        "nb_features": 162,
        "extra_features": [
            "gold_diff_at_20", "team_100_gold_at_20", "team_200_gold_at_20",
            "team_100_top_gold_at_20", "team_200_top_gold_at_20",
            "team_100_jungle_gold_at_20", "team_200_jungle_gold_at_20",
            "team_100_mid_gold_at_20", "team_200_mid_gold_at_20",
            "team_100_adc_gold_at_20", "team_200_adc_gold_at_20",
            "team_100_support_gold_at_20", "team_200_support_gold_at_20",
        ],
    },
}

# Model accuracy benchmarks (real values from trained models)
MODEL_BENCHMARKS = {
    "draft": {
        "name": "Draft uniquement",
        "accuracy": 0.511,
        "model_type": "XGBoost",
        "description": "Prédiction basée uniquement sur la composition des champions",
    },
    "at5": {
        "name": "Draft + @5min",
        "accuracy": 0.681,
        "model_type": "LightGBM",
        "description": "Ajout du gold par rôle à 5 minutes",
    },
    "at10": {
        "name": "Draft + @10min",
        "accuracy": 0.737,
        "model_type": "XGBoost",
        "description": "Ajout du gold et CS par rôle à 10 minutes",
    },
    "at15": {
        "name": "Draft + @15min",
        "accuracy": 0.790,
        "model_type": "XGBoost",
        "description": "Ajout du gold par rôle à 15 minutes",
    },
    "at20": {
        "name": "Draft + @20min",
        "accuracy": 0.817,
        "model_type": "XGBoost",
        "description": "Ajout du gold par rôle à 20 minutes",
    },
}

# Feature descriptions for Feature Importance page
FEATURE_DESCRIPTIONS = {
    "gold_diff_at_10": "Total gold difference between teams at 10 minutes",
    "gold_advantage_pct_at_10": "Gold advantage as percentage at 10 minutes",
    "first_blood_team_100": "Whether Team 100 (Blue) got first blood",
    "first_dragon_team_100": "Whether Team 100 (Blue) got first dragon",
    "first_tower_team_100": "Whether Team 100 (Blue) got first tower",
    "first_herald_team_100": "Whether Team 100 (Blue) got first rift herald",
    "top_gold_diff_at_10": "Top lane gold difference at 10 minutes",
    "jungle_gold_diff_at_10": "Jungle gold difference at 10 minutes",
    "mid_gold_diff_at_10": "Mid lane gold difference at 10 minutes",
    "adc_gold_diff_at_10": "ADC gold difference at 10 minutes",
    "support_gold_diff_at_10": "Support gold difference at 10 minutes",
    "cs_diff_at_10": "Total CS difference at 10 minutes",
    "early_lead_score": "Composite score combining gold, CS, and objectives",
}
