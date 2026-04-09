"""
Feature Importance Page - Analyze which features matter most
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sys
from pathlib import Path

# Add parent directory to path
STREAMLIT_APP_DIR = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(STREAMLIT_APP_DIR))

from config import COLORS, COLORS_ALPHA, rgba, html, FEATURE_DESCRIPTIONS

st.set_page_config(
    page_title="Feature Importance - LoL Draft Predictor",
    page_icon="🔬",
    layout="wide",
)

st.title("🔬 Feature Importance")
st.markdown(
    f"<p style='color: {COLORS['text_secondary']};'>Understand which early game factors most influence match outcomes.</p>",
    unsafe_allow_html=True,
)

# Load model and get feature importance
try:
    from utils.model_loader import load_early_game_model, get_feature_importance

    model, scaler, feature_names, accuracy = load_early_game_model()
    importance_df = get_feature_importance(model, feature_names)
    has_model = model is not None and not importance_df.empty
except Exception as e:
    st.error(f"Could not load model: {e}")
    has_model = False
    importance_df = pd.DataFrame()

if not has_model:
    st.warning("Model not loaded. Showing example feature importance data.")

    # Create example data
    importance_df = pd.DataFrame({
        "feature": ["gold_diff_at_10", "gold_pct_at_10", "first_blood", "first_tower", "first_dragon", "first_herald"],
        "importance": [0.45, 0.25, 0.12, 0.10, 0.05, 0.03],
        "coefficient": [0.45, 0.25, 0.12, 0.10, 0.05, 0.03],
    })

st.markdown("---")

# Feature Importance Bar Chart
st.markdown("### 📊 Feature Importance Ranking")

col1, col2 = st.columns([2, 1])

with col1:
    # Create horizontal bar chart
    fig = go.Figure()

    # Sort by importance
    importance_df_sorted = importance_df.sort_values("importance", ascending=True)

    # Color based on coefficient sign
    colors = [
        COLORS["blue_team"] if c > 0 else COLORS["red_team"]
        for c in importance_df_sorted["coefficient"]
    ]

    fig.add_trace(go.Bar(
        y=importance_df_sorted["feature"],
        x=importance_df_sorted["importance"],
        orientation="h",
        marker_color=colors,
        text=[f"{v:.3f}" for v in importance_df_sorted["importance"]],
        textposition="outside",
        textfont=dict(color=COLORS["text_primary"], size=12),
        hovertemplate="<b>%{y}</b><br>Importance: %{x:.4f}<extra></extra>",
    ))

    fig.update_layout(
        height=400,
        margin=dict(l=20, r=100, t=20, b=20),
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

with col2:
    howto_html = f"""<div style="background:{COLORS["background_light"]};border:1px solid {COLORS_ALPHA["gold_accent_25"]};border-radius:10px;padding:20px;">
<h4 style="color:{COLORS["gold_accent"]};margin-bottom:15px;">How to Read</h4>
<p style="color:{COLORS["text_secondary"]};font-size:0.9em;"><strong style="color:{COLORS["blue_team"]}">Blue bars</strong>: Higher values increase Blue team's win probability.</p>
<p style="color:{COLORS["text_secondary"]};font-size:0.9em;margin-top:10px;"><strong style="color:{COLORS["red_team"]}">Red bars</strong>: Higher values decrease Blue team's win probability (favor Red).</p>
<p style="color:{COLORS["text_secondary"]};font-size:0.9em;margin-top:10px;">Longer bars indicate features with more predictive power.</p>
</div>"""
    st.markdown(howto_html, unsafe_allow_html=True)

    if has_model:
        acc_html = f"""<div style="background:{COLORS["background_light"]};border:1px solid {COLORS_ALPHA["success_40"]};border-radius:10px;padding:15px;margin-top:15px;">
<p style="color:{COLORS["text_secondary"]};font-size:0.85em;margin:0;">Model Accuracy: <strong style="color:{COLORS["success"]}">{accuracy*100:.1f}%</strong></p>
</div>"""
        st.markdown(acc_html, unsafe_allow_html=True)

st.markdown("---")

# Feature Descriptions
st.markdown("### 📋 Feature Descriptions")

# Create description cards
num_cols = 2
features_list = list(importance_df_sorted["feature"])

for i in range(0, len(features_list), num_cols):
    cols = st.columns(num_cols)

    for j, col in enumerate(cols):
        if i + j < len(features_list):
            feature = features_list[i + j]
            importance = importance_df_sorted[importance_df_sorted["feature"] == feature]["importance"].values[0]
            coefficient = importance_df_sorted[importance_df_sorted["feature"] == feature]["coefficient"].values[0]

            description = FEATURE_DESCRIPTIONS.get(feature, "No description available.")

            coef_color = COLORS["blue_team"] if coefficient > 0 else COLORS["red_team"]
            coef_sign = "+" if coefficient > 0 else ""

            with col:
                card_html = f"""<div style="background:{COLORS["background_light"]};border:1px solid {COLORS_ALPHA["gold_accent_25"]};border-radius:10px;padding:15px;margin-bottom:10px;min-height:120px;">
<h5 style="color:{COLORS["text_primary"]};margin-bottom:5px;">{feature}</h5>
<p style="color:{coef_color};font-size:0.85em;margin-bottom:10px;">Coefficient: {coef_sign}{coefficient:.4f} | Importance: {importance:.4f}</p>
<p style="color:{COLORS["text_secondary"]};font-size:0.85em;margin:0;">{description}</p>
</div>"""
                st.markdown(card_html, unsafe_allow_html=True)

st.markdown("---")

# Coefficient Distribution
st.markdown("### 📈 Coefficient Distribution")

col1, col2 = st.columns(2)

with col1:
    # Pie chart of positive vs negative features
    positive_count = len(importance_df[importance_df["coefficient"] > 0])
    negative_count = len(importance_df[importance_df["coefficient"] <= 0])

    fig = go.Figure()

    fig.add_trace(go.Pie(
        labels=["Positive (favor Blue)", "Negative (favor Red)"],
        values=[positive_count, negative_count],
        marker_colors=[COLORS["blue_team"], COLORS["red_team"]],
        hole=0.5,
        textinfo="label+value",
        textfont=dict(color=COLORS["text_primary"]),
    ))

    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color=COLORS["text_primary"]),
        showlegend=False,
        annotations=[
            dict(
                text="Features",
                x=0.5,
                y=0.5,
                font_size=14,
                showarrow=False,
                font_color=COLORS["text_secondary"],
            )
        ],
    )

    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Summary stats
    total_importance = importance_df["importance"].sum()
    top_feature = importance_df.iloc[0]["feature"]
    top_importance = importance_df.iloc[0]["importance"]
    top_share = (top_importance / total_importance) * 100
    num_features = len(importance_df)

    summary_html = f"""<div style="background:{COLORS["background_light"]};border:1px solid {COLORS_ALPHA["gold_accent_25"]};border-radius:10px;padding:20px;">
<h4 style="color:{COLORS["gold_accent"]};margin-bottom:15px;">Summary Statistics</h4>
<p style="color:{COLORS["text_secondary"]};font-size:0.9em;"><strong style="color:{COLORS["text_primary"]}">Most Important Feature:</strong><br>{top_feature} ({top_importance:.4f})</p>
<p style="color:{COLORS["text_secondary"]};font-size:0.9em;margin-top:15px;"><strong style="color:{COLORS["text_primary"]}">Top Feature Share:</strong><br>{top_share:.1f}% of total importance</p>
<p style="color:{COLORS["text_secondary"]};font-size:0.9em;margin-top:15px;"><strong style="color:{COLORS["text_primary"]}">Total Features:</strong><br>{num_features} features in model</p>
</div>"""

    st.markdown(summary_html, unsafe_allow_html=True)

st.markdown("---")

# Key Insights
st.markdown("### 💡 Key Insights")

insights_html = f"""<div style="background:{COLORS["background_light"]};border-left:4px solid {COLORS["gold_accent"]};padding:15px 20px;margin-bottom:15px;">
<p style="color:{COLORS["text_primary"]};margin:0;"><strong>Gold difference at 10 minutes is the strongest predictor.</strong> This single feature captures the overall state of the early game.</p>
</div>
<div style="background:{COLORS["background_light"]};border-left:4px solid {COLORS["blue_team"]};padding:15px 20px;margin-bottom:15px;">
<p style="color:{COLORS["text_primary"]};margin:0;"><strong>First objectives have diminishing returns.</strong> First blood has the highest impact, while first herald has the lowest.</p>
</div>
<div style="background:{COLORS["background_light"]};border-left:4px solid {COLORS["red_team"]};padding:15px 20px;margin-bottom:15px;">
<p style="color:{COLORS["text_primary"]};margin:0;"><strong>Relative metrics (percentages) complement absolute values.</strong> Gold percentage at 10min captures proportional advantage.</p>
</div>"""

st.markdown(insights_html, unsafe_allow_html=True)

# Model explanation
st.markdown("---")
st.markdown("### 🤖 About the Model")

model_html = f"""<div style="background:{COLORS["background_light"]};border:1px solid {COLORS_ALPHA["gold_accent_25"]};border-radius:10px;padding:20px;">
<h4 style="color:{COLORS["gold_accent"]};margin-bottom:15px;">Logistic Regression</h4>
<p style="color:{COLORS["text_secondary"]};font-size:0.9em;">The early game predictor uses <strong style="color:{COLORS["text_primary"]}">Logistic Regression</strong>, a simple but interpretable model that outputs probabilities.</p>
<p style="color:{COLORS["text_secondary"]};font-size:0.9em;margin-top:10px;"><strong style="color:{COLORS["text_primary"]}">Why Logistic Regression?</strong></p>
<ul style="color:{COLORS["text_secondary"]};font-size:0.85em;">
<li>Interpretable coefficients show feature impact direction</li>
<li>Fast training and inference</li>
<li>Outputs calibrated probabilities</li>
<li>Works well with small feature sets</li>
</ul>
<p style="color:{COLORS["text_secondary"]};font-size:0.9em;margin-top:10px;">For more complex models (Random Forest, XGBoost), feature importance would be calculated using different methods (tree-based importance, SHAP values).</p>
</div>"""

st.markdown(model_html, unsafe_allow_html=True)
