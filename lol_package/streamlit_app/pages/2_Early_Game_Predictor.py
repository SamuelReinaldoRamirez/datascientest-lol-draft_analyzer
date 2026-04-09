"""
Early Game Predictor Page - Interactive win probability prediction
"""
import streamlit as st
import sys
from pathlib import Path

# Add parent directory to path
STREAMLIT_APP_DIR = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(STREAMLIT_APP_DIR))

from config import COLORS, COLORS_ALPHA, rgba, html
from components.prediction_gauge import render_prediction_gauge, render_simple_probability_bar

st.set_page_config(
    page_title="Early Game Predictor - LoL Draft Predictor",
    page_icon="🔮",
    layout="wide",
)

st.title("🔮 Early Game Predictor")
st.markdown(
    f"<p style='color: {COLORS['text_secondary']};'>Predict match outcome based on @10 minute game state.</p>",
    unsafe_allow_html=True,
)

# Load model
try:
    from utils.model_loader import load_early_game_model, predict_win_probability, get_feature_importance
    model, scaler, feature_names, model_accuracy = load_early_game_model()
    model_loaded = model is not None
except Exception as e:
    st.error(f"Could not load model: {e}")
    model_loaded = False
    model, scaler, feature_names, model_accuracy = None, None, [], 0.0

# Model info banner
if model_loaded:
    st.markdown(
        html(f"""
        <div style='
            background: {COLORS["background_light"]};
            border: 1px solid {COLORS_ALPHA["success_40"]};
            border-radius: 10px;
            padding: 15px 20px;
            margin-bottom: 20px;
        '>
            <span style='color: {COLORS["success"]}; font-weight: bold;'>✓ Model Loaded</span>
            <span style='color: {COLORS["text_secondary"]}; margin-left: 20px;'>
                Accuracy: <strong style='color: {COLORS["gold_accent"]}'>{model_accuracy*100:.1f}%</strong>
            </span>
            <span style='color: {COLORS["text_secondary"]}; margin-left: 20px;'>
                Features: {len(feature_names)}
            </span>
        </div>
        """), unsafe_allow_html=True,
    )
else:
    st.warning("Model not loaded. Using default predictions.")

st.markdown("---")

# Input Section
col1, col2 = st.columns([1, 1])

with col1:
    st.markdown("### 💰 Gold Difference")
    st.markdown(
        f"<p style='color: {COLORS['text_secondary']}; font-size: 0.9em;'>Positive = Blue team ahead, Negative = Red team ahead</p>",
        unsafe_allow_html=True,
    )

    gold_diff = st.slider(
        "Gold Difference @10min",
        min_value=-10000,
        max_value=10000,
        value=0,
        step=100,
        format="%+d g",
        help="Total gold difference between blue and red team at 10 minutes",
    )

    # Visual indicator for gold diff
    if gold_diff > 0:
        gold_color = COLORS["blue_team"]
        gold_text = f"Blue team is ahead by {gold_diff:,}g"
    elif gold_diff < 0:
        gold_color = COLORS["red_team"]
        gold_text = f"Red team is ahead by {abs(gold_diff):,}g"
    else:
        gold_color = COLORS["gold_accent"]
        gold_text = "Teams are even"

    st.markdown(
        f"<p style='color: {gold_color}; font-size: 1.1em; font-weight: bold;'>{gold_text}</p>",
        unsafe_allow_html=True,
    )

with col2:
    st.markdown("### 🎯 First Objectives")
    st.markdown(
        f"<p style='color: {COLORS['text_secondary']}; font-size: 0.9em;'>Check objectives secured by Blue team</p>",
        unsafe_allow_html=True,
    )

    obj_col1, obj_col2 = st.columns(2)

    with obj_col1:
        first_blood = st.checkbox("🩸 First Blood", help="Blue team got first kill")
        first_tower = st.checkbox("🏰 First Tower", help="Blue team destroyed first tower")

    with obj_col2:
        first_dragon = st.checkbox("🐉 First Dragon", help="Blue team killed first dragon")
        first_herald = st.checkbox("👁️ First Herald", help="Blue team killed first rift herald")

st.markdown("---")

# Prediction Section
st.markdown("### 📊 Win Probability")

if model_loaded:
    prediction = predict_win_probability(
        model, scaler, feature_names,
        gold_diff=gold_diff,
        first_blood=first_blood,
        first_tower=first_tower,
        first_dragon=first_dragon,
        first_herald=first_herald,
    )
else:
    # Fallback heuristic prediction
    base_prob = 0.5
    base_prob += gold_diff / 20000  # ~+0.5 at +10000g
    if first_blood:
        base_prob += 0.03
    if first_tower:
        base_prob += 0.05
    if first_dragon:
        base_prob += 0.04
    if first_herald:
        base_prob += 0.03
    base_prob = max(0.1, min(0.9, base_prob))

    prediction = {
        "blue_win_prob": base_prob,
        "red_win_prob": 1 - base_prob,
        "prediction": "Blue Team" if base_prob > 0.5 else "Red Team",
        "confidence": max(base_prob, 1 - base_prob),
    }

# Display gauge
col1, col2, col3 = st.columns([1, 2, 1])

with col2:
    render_prediction_gauge(prediction["blue_win_prob"], title="Blue Team Win Probability")

# Additional insights
st.markdown("---")
st.markdown("### 💡 Insights")

insight_cols = st.columns(3)

with insight_cols[0]:
    # Gold impact insight
    gold_impact = abs(gold_diff) / 3000
    gold_impact_pct = min(gold_impact * 10, 25)

    st.markdown(
        html(f"""
        <div style='
            background: {COLORS["background_light"]};
            border: 1px solid {COLORS_ALPHA["gold_accent_25"]};
            border-radius: 10px;
            padding: 15px;
        '>
            <h5 style='color: {COLORS["gold_accent"]}; margin-bottom: 10px;'>💰 Gold Impact</h5>
            <p style='color: {COLORS["text_secondary"]}; font-size: 0.9em;'>
                This gold lead shifts win probability by approximately
                <strong style='color: {COLORS["text_primary"]}'>{gold_impact_pct:.1f}%</strong>
            </p>
        </div>
        """), unsafe_allow_html=True,
    )

with insight_cols[1]:
    # Objectives impact
    obj_count = sum([first_blood, first_tower, first_dragon, first_herald])
    obj_impact = obj_count * 3.5

    st.markdown(
        html(f"""
        <div style='
            background: {COLORS["background_light"]};
            border: 1px solid {COLORS_ALPHA["gold_accent_25"]};
            border-radius: 10px;
            padding: 15px;
        '>
            <h5 style='color: {COLORS["gold_accent"]}; margin-bottom: 10px;'>🎯 Objectives Impact</h5>
            <p style='color: {COLORS["text_secondary"]}; font-size: 0.9em;'>
                {obj_count} objective(s) for Blue team adds approximately
                <strong style='color: {COLORS["text_primary"]}'>{obj_impact:.1f}%</strong> win probability
            </p>
        </div>
        """), unsafe_allow_html=True,
    )

with insight_cols[2]:
    # Confidence indicator
    confidence = prediction["confidence"] * 100

    if confidence > 70:
        conf_color = COLORS["success"]
        conf_text = "High Confidence"
    elif confidence > 55:
        conf_color = COLORS["gold_accent"]
        conf_text = "Medium Confidence"
    else:
        conf_color = COLORS["warning"]
        conf_text = "Low Confidence"

    st.markdown(
        html(f"""
        <div style='
            background: {COLORS["background_light"]};
            border: 1px solid {conf_color}60;
            border-radius: 10px;
            padding: 15px;
        '>
            <h5 style='color: {conf_color}; margin-bottom: 10px;'>🎯 {conf_text}</h5>
            <p style='color: {COLORS["text_secondary"]}; font-size: 0.9em;'>
                Model confidence:
                <strong style='color: {COLORS["text_primary"]}'>{confidence:.1f}%</strong>
            </p>
        </div>
        """), unsafe_allow_html=True,
    )

# Scenario comparisons
st.markdown("---")
st.markdown("### 📈 Scenario Comparison")

scenarios = [
    ("Even game", 0, False, False, False, False),
    ("+2000g for Blue", 2000, False, False, False, False),
    ("+5000g for Blue", 5000, False, False, False, False),
    ("All objectives Blue", 0, True, True, True, True),
    ("All objectives + 3000g", 3000, True, True, True, True),
    ("-3000g but all objectives", -3000, True, True, True, True),
]

scenario_cols = st.columns(len(scenarios))

for i, (name, gd, fb, ft, fd, fh) in enumerate(scenarios):
    with scenario_cols[i]:
        if model_loaded:
            pred = predict_win_probability(
                model, scaler, feature_names,
                gold_diff=gd,
                first_blood=fb,
                first_tower=ft,
                first_dragon=fd,
                first_herald=fh,
            )
            prob = pred["blue_win_prob"] * 100
        else:
            prob = 50 + gd / 200 + (fb + ft + fd + fh) * 3.5

        prob_color = COLORS["blue_team"] if prob > 50 else COLORS["red_team"]

        st.markdown(
            html(f"""
            <div style='
                background: {COLORS["background_light"]};
                border: 1px solid {COLORS_ALPHA["gold_accent_25"]};
                border-radius: 10px;
                padding: 10px;
                text-align: center;
                height: 100px;
            '>
                <p style='color: {COLORS["text_secondary"]}; font-size: 0.75em; margin-bottom: 5px;'>
                    {name}
                </p>
                <p style='color: {prob_color}; font-size: 1.5em; font-weight: bold; margin: 0;'>
                    {prob:.0f}%
                </p>
                <p style='color: {COLORS["text_secondary"]}; font-size: 0.7em; margin: 0;'>
                    Blue win
                </p>
            </div>
            """), unsafe_allow_html=True,
        )

# Feature importance
if model_loaded:
    st.markdown("---")
    st.markdown("### 🔬 Feature Importance")

    importance_df = get_feature_importance(model, feature_names)

    if not importance_df.empty:
        import plotly.graph_objects as go

        fig = go.Figure()

        fig.add_trace(go.Bar(
            y=importance_df["feature"],
            x=importance_df["importance"],
            orientation="h",
            marker_color=[COLORS["blue_team"] if c > 0 else COLORS["red_team"] for c in importance_df["coefficient"]],
            text=[f"{v:.3f}" for v in importance_df["importance"]],
            textposition="outside",
            textfont=dict(color=COLORS["text_primary"]),
        ))

        fig.update_layout(
            height=300,
            margin=dict(l=20, r=80, t=20, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor=COLORS["background_light"],
            font=dict(color=COLORS["text_primary"]),
            xaxis=dict(
                title="Importance (|coefficient|)",
                gridcolor="rgba(255,255,255,0.1)",
                tickfont=dict(color=COLORS["text_secondary"]),
            ),
            yaxis=dict(
                tickfont=dict(color=COLORS["text_secondary"]),
            ),
        )

        st.plotly_chart(fig, use_container_width=True)

        st.markdown(
            html(f"""
            <p style='color: {COLORS["text_secondary"]}; font-size: 0.85em;'>
                <strong style='color: {COLORS["blue_team"]}'>Blue bars</strong> = positive coefficient (increases Blue win probability)<br>
                <strong style='color: {COLORS["red_team"]}'>Red bars</strong> = negative coefficient (decreases Blue win probability)
            </p>
            """), unsafe_allow_html=True,
        )
