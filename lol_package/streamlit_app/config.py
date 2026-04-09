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

# Model accuracy benchmarks
MODEL_BENCHMARKS = {
    "draft_only": {
        "name": "Draft Only",
        "accuracy": 0.52,
        "description": "Prediction based only on champion compositions",
    },
    "early_game": {
        "name": "Early Game (@10min)",
        "accuracy": 0.72,
        "description": "Prediction using gold diff and first objectives at 10 minutes",
    },
    "early_game_enhanced": {
        "name": "Early Game Enhanced",
        "accuracy": 0.78,
        "description": "With per-lane gold differences and CS stats",
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
