"""
Model loading and prediction utilities.
"""
import numpy as np
import pandas as pd
import streamlit as st
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler


@st.cache_resource
def load_early_game_model():
    """
    Load or train the early game prediction model.

    Returns:
        Tuple of (model, scaler, feature_names, accuracy)
    """
    from utils.data_loader import get_matches_with_timeline, load_timeline_data
    import sqlite3
    from config import DATABASE_PATH

    # Get matches with timeline data
    conn = sqlite3.connect(str(DATABASE_PATH))

    # Query matches with @10min timeline data
    query = """
        SELECT
            m.match_id,
            m.team_100_win,
            mt.gold_diff as gold_diff_at_10,
            mt.team_100_gold,
            mt.team_200_gold,
            t100.first_blood as first_blood,
            t100.first_tower as first_tower,
            t100.first_dragon as first_dragon,
            t100.first_rift_herald as first_herald
        FROM matches m
        INNER JOIN match_timeline mt ON m.match_id = mt.match_id AND mt.minute = 10
        LEFT JOIN team_stats t100 ON m.match_id = t100.match_id AND t100.team_id = 100
        WHERE m.game_duration >= 600
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    if len(df) < 100:
        st.warning("Not enough matches with timeline data for training.")
        return None, None, [], 0.0

    # Calculate gold percentage
    total_gold = df["team_100_gold"] + df["team_200_gold"]
    df["gold_pct_at_10"] = df["team_100_gold"] / total_gold.replace(0, 1)

    # Prepare features
    feature_names = [
        "gold_diff_at_10",
        "gold_pct_at_10",
        "first_blood",
        "first_tower",
        "first_dragon",
        "first_herald",
    ]

    X = df[feature_names].fillna(0).astype(float)
    y = df["team_100_win"].astype(int)

    # Train-test split
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train model
    model = LogisticRegression(random_state=42, max_iter=1000)
    model.fit(X_train_scaled, y_train)

    # Calculate accuracy
    accuracy = model.score(X_test_scaled, y_test)

    return model, scaler, feature_names, accuracy


def predict_win_probability(
    model,
    scaler,
    feature_names: list,
    gold_diff: float,
    gold_pct: float = None,
    first_blood: bool = False,
    first_tower: bool = False,
    first_dragon: bool = False,
    first_herald: bool = False,
) -> dict:
    """
    Predict win probability for Team 100 (Blue side).

    Args:
        model: Trained model
        scaler: Feature scaler
        feature_names: List of feature names
        gold_diff: Gold difference at 10 min (positive = blue advantage)
        gold_pct: Gold percentage for blue team (0.5 = even)
        first_blood: Whether blue team got first blood
        first_tower: Whether blue team got first tower
        first_dragon: Whether blue team got first dragon
        first_herald: Whether blue team got first herald

    Returns:
        Dict with prediction results
    """
    if model is None:
        return {
            "blue_win_prob": 0.5,
            "red_win_prob": 0.5,
            "prediction": "Unknown",
            "confidence": 0.0,
        }

    # Calculate gold_pct if not provided
    if gold_pct is None:
        # Assume average total gold at 10 min is ~40000 (20000 per team)
        avg_total = 40000
        blue_gold = (avg_total / 2) + (gold_diff / 2)
        gold_pct = blue_gold / avg_total

    # Create feature vector
    features = {
        "gold_diff_at_10": gold_diff,
        "gold_pct_at_10": gold_pct,
        "first_blood": 1.0 if first_blood else 0.0,
        "first_tower": 1.0 if first_tower else 0.0,
        "first_dragon": 1.0 if first_dragon else 0.0,
        "first_herald": 1.0 if first_herald else 0.0,
    }

    X = pd.DataFrame([features])[feature_names]
    X_scaled = scaler.transform(X)

    # Predict
    proba = model.predict_proba(X_scaled)[0]

    blue_win_prob = proba[1]
    red_win_prob = proba[0]

    return {
        "blue_win_prob": blue_win_prob,
        "red_win_prob": red_win_prob,
        "prediction": "Blue Team" if blue_win_prob > 0.5 else "Red Team",
        "confidence": max(blue_win_prob, red_win_prob),
    }


def get_feature_importance(model, feature_names: list) -> pd.DataFrame:
    """
    Get feature importance from the trained model.

    Args:
        model: Trained model
        feature_names: List of feature names

    Returns:
        DataFrame with feature importance
    """
    if model is None or not hasattr(model, "coef_"):
        return pd.DataFrame()

    # For logistic regression, use absolute coefficients as importance
    importance = np.abs(model.coef_[0])

    df = pd.DataFrame({
        "feature": feature_names,
        "importance": importance,
        "coefficient": model.coef_[0],
    }).sort_values("importance", ascending=False)

    return df
