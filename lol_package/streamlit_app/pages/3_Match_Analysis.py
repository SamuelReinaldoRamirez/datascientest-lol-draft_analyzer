"""
Match Analysis Page - Analyze historical matches with timeline visualization
"""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path
STREAMLIT_APP_DIR = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(STREAMLIT_APP_DIR))

from config import COLORS, COLORS_ALPHA, rgba, html, CHAMPION_ICON_URL

st.set_page_config(
    page_title="Match Analysis - LoL Draft Predictor",
    page_icon="🎯",
    layout="wide",
)

st.title("🎯 Match Analysis")
st.markdown(
    f"<p style='color: {COLORS['text_secondary']};'>Analyze historical matches with timeline visualization.</p>",
    unsafe_allow_html=True,
)


def get_champion_icon_url(champion_name: str) -> str:
    """Get Data Dragon icon URL for a champion."""
    # Handle special cases where champion name differs from key
    name_map = {
        "Wukong": "MonkeyKing",
        "Nunu & Willump": "Nunu",
        "Renata Glasc": "Renata",
        "Bel'Veth": "Belveth",
        "Kai'Sa": "Kaisa",
        "Kha'Zix": "Khazix",
        "LeBlanc": "Leblanc",
        "Rek'Sai": "RekSai",
        "Vel'Koz": "Velkoz",
        "Cho'Gath": "Chogath",
        "Kog'Maw": "KogMaw",
    }
    key = name_map.get(champion_name, champion_name.replace(" ", "").replace("'", ""))
    return CHAMPION_ICON_URL.format(champion_key=key)


# Load matches with timeline
try:
    from utils.data_loader import get_matches_with_timeline, load_timeline_data, get_match_details

    matches_df = get_matches_with_timeline(limit=500)
    has_data = len(matches_df) > 0
except Exception as e:
    st.error(f"Could not load match data: {e}")
    has_data = False
    matches_df = pd.DataFrame()

if not has_data:
    st.warning("No matches with timeline data found. Please run data collection first.")
    st.stop()

st.markdown("---")

# Match selection
st.markdown("### 🎮 Select a Match")

col1, col2 = st.columns([2, 1])

with col1:
    # Format match options
    match_options = []
    for _, row in matches_df.head(100).iterrows():
        winner = "🔵 Blue" if row["team_100_win"] else "🔴 Red"
        gold_at_10 = row.get("gold_diff_at_10", 0)
        gold_str = f"+{gold_at_10:,.0f}g" if gold_at_10 and gold_at_10 > 0 else f"{gold_at_10:,.0f}g" if gold_at_10 else "N/A"
        duration = row.get("game_duration", 0) / 60 if row.get("game_duration") else 0
        match_options.append(f"{row['match_id']} | {winner} won | @10: {gold_str} | {duration:.0f}min")

    selected_option = st.selectbox(
        "Choose a match to analyze",
        options=match_options,
        help="Select a match to view detailed analysis",
    )

    selected_match_id = selected_option.split(" | ")[0] if selected_option else None

with col2:
    # Quick filters
    st.markdown("#### Filters")
    show_blue_wins = st.checkbox("Blue wins only", value=False)
    show_red_wins = st.checkbox("Red wins only", value=False)

if selected_match_id:
    st.markdown("---")

    # Load match details
    match_details = get_match_details(selected_match_id)
    timeline_df = load_timeline_data(selected_match_id)

    if not match_details:
        st.error("Could not load match details.")
        st.stop()

    # Match Overview
    st.markdown("### 📊 Match Overview")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        winner = "Blue Team" if match_details.get("team_100_win") else "Red Team"
        winner_color = COLORS["blue_team"] if match_details.get("team_100_win") else COLORS["red_team"]
        st.markdown(
            html(f"""
            <div style='text-align: center;'>
                <p style='color: {COLORS["text_secondary"]}; margin-bottom: 5px;'>Winner</p>
                <p style='color: {winner_color}; font-size: 1.3em; font-weight: bold;'>{winner}</p>
            </div>
            """), unsafe_allow_html=True,
        )

    with col2:
        duration = match_details.get("game_duration", 0) / 60
        st.markdown(
            html(f"""
            <div style='text-align: center;'>
                <p style='color: {COLORS["text_secondary"]}; margin-bottom: 5px;'>Duration</p>
                <p style='color: {COLORS["text_primary"]}; font-size: 1.3em; font-weight: bold;'>{duration:.1f} min</p>
            </div>
            """), unsafe_allow_html=True,
        )

    with col3:
        version = match_details.get("game_version", "Unknown")[:6]
        st.markdown(
            html(f"""
            <div style='text-align: center;'>
                <p style='color: {COLORS["text_secondary"]}; margin-bottom: 5px;'>Patch</p>
                <p style='color: {COLORS["text_primary"]}; font-size: 1.3em; font-weight: bold;'>{version}</p>
            </div>
            """), unsafe_allow_html=True,
        )

    with col4:
        elo = match_details.get("source_elo", "Unknown")
        st.markdown(
            html(f"""
            <div style='text-align: center;'>
                <p style='color: {COLORS["text_secondary"]}; margin-bottom: 5px;'>Elo</p>
                <p style='color: {COLORS["gold_accent"]}; font-size: 1.3em; font-weight: bold;'>{elo}</p>
            </div>
            """), unsafe_allow_html=True,
        )

    st.markdown("---")

    # Team Compositions
    st.markdown("### 👥 Team Compositions")

    players = match_details.get("players", [])
    team_100_players = [p for p in players if p.get("team_id") == 100]
    team_200_players = [p for p in players if p.get("team_id") == 200]

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"<h4 style='color: {COLORS['blue_team']};'>🔵 Blue Team</h4>",
            unsafe_allow_html=True,
        )

        for player in sorted(team_100_players, key=lambda x: ["top", "jungle", "mid", "adc", "support"].index(x.get("position", "support")) if x.get("position") in ["top", "jungle", "mid", "adc", "support"] else 99):
            champ_name = player.get("champion_name", "Unknown")
            position = player.get("position", "?").upper()
            kills = player.get("kills", 0)
            deaths = player.get("deaths", 0)
            assists = player.get("assists", 0)
            gold = player.get("gold_earned", 0)

            st.markdown(
                html(f"""
                <div style='
                    background: {COLORS["background_light"]};
                    border: 1px solid {COLORS_ALPHA["blue_team_25"]};
                    border-radius: 8px;
                    padding: 10px;
                    margin-bottom: 8px;
                    display: flex;
                    align-items: center;
                '>
                    <img src="{get_champion_icon_url(champ_name)}" width="40" height="40"
                         style="border-radius: 5px; margin-right: 10px;"
                         onerror="this.src='https://via.placeholder.com/40'">
                    <div>
                        <p style='color: {COLORS["text_primary"]}; margin: 0; font-weight: bold;'>
                            {champ_name}
                            <span style='color: {COLORS["text_secondary"]}; font-weight: normal;'> ({position})</span>
                        </p>
                        <p style='color: {COLORS["text_secondary"]}; margin: 0; font-size: 0.85em;'>
                            {kills}/{deaths}/{assists} • {gold:,}g
                        </p>
                    </div>
                </div>
                """), unsafe_allow_html=True,
            )

    with col2:
        st.markdown(
            f"<h4 style='color: {COLORS['red_team']};'>🔴 Red Team</h4>",
            unsafe_allow_html=True,
        )

        for player in sorted(team_200_players, key=lambda x: ["top", "jungle", "mid", "adc", "support"].index(x.get("position", "support")) if x.get("position") in ["top", "jungle", "mid", "adc", "support"] else 99):
            champ_name = player.get("champion_name", "Unknown")
            position = player.get("position", "?").upper()
            kills = player.get("kills", 0)
            deaths = player.get("deaths", 0)
            assists = player.get("assists", 0)
            gold = player.get("gold_earned", 0)

            st.markdown(
                html(f"""
                <div style='
                    background: {COLORS["background_light"]};
                    border: 1px solid {COLORS_ALPHA["red_team_25"]};
                    border-radius: 8px;
                    padding: 10px;
                    margin-bottom: 8px;
                    display: flex;
                    align-items: center;
                '>
                    <img src="{get_champion_icon_url(champ_name)}" width="40" height="40"
                         style="border-radius: 5px; margin-right: 10px;"
                         onerror="this.src='https://via.placeholder.com/40'">
                    <div>
                        <p style='color: {COLORS["text_primary"]}; margin: 0; font-weight: bold;'>
                            {champ_name}
                            <span style='color: {COLORS["text_secondary"]}; font-weight: normal;'> ({position})</span>
                        </p>
                        <p style='color: {COLORS["text_secondary"]}; margin: 0; font-size: 0.85em;'>
                            {kills}/{deaths}/{assists} • {gold:,}g
                        </p>
                    </div>
                </div>
                """), unsafe_allow_html=True,
            )

    st.markdown("---")

    # Timeline Chart
    st.markdown("### 📈 Gold Timeline")

    if not timeline_df.empty:
        from components.gold_chart import render_gold_timeline_chart, render_team_gold_comparison

        tab1, tab2 = st.tabs(["Gold Difference", "Team Gold Comparison"])

        with tab1:
            render_gold_timeline_chart(timeline_df)

        with tab2:
            render_team_gold_comparison(timeline_df)

        # Key moments
        st.markdown("#### 🎯 Key Moments")

        if len(timeline_df) >= 10:
            gold_10 = timeline_df[timeline_df["minute"] == 10]
            if not gold_10.empty:
                g10 = gold_10.iloc[0]["gold_diff"]
                color = COLORS["blue_team"] if g10 >= 0 else COLORS["red_team"]
                team = "Blue" if g10 >= 0 else "Red"
                st.markdown(
                    f"- **@10 min**: {team} team ahead by <span style='color: {color}'>{abs(g10):,.0f}g</span>",
                    unsafe_allow_html=True,
                )

        if len(timeline_df) >= 15:
            gold_15 = timeline_df[timeline_df["minute"] == 15]
            if not gold_15.empty:
                g15 = gold_15.iloc[0]["gold_diff"]
                color = COLORS["blue_team"] if g15 >= 0 else COLORS["red_team"]
                team = "Blue" if g15 >= 0 else "Red"
                st.markdown(
                    f"- **@15 min**: {team} team ahead by <span style='color: {color}'>{abs(g15):,.0f}g</span>",
                    unsafe_allow_html=True,
                )

        max_diff = timeline_df["gold_diff"].max()
        min_diff = timeline_df["gold_diff"].min()
        max_row = timeline_df[timeline_df["gold_diff"] == max_diff].iloc[0]
        min_row = timeline_df[timeline_df["gold_diff"] == min_diff].iloc[0]

        st.markdown(
            f"- **Biggest Blue lead**: <span style='color: {COLORS['blue_team']}'>{max_diff:,.0f}g</span> at minute {max_row['minute']}",
            unsafe_allow_html=True,
        )
        st.markdown(
            f"- **Biggest Red lead**: <span style='color: {COLORS['red_team']}'>{abs(min_diff):,.0f}g</span> at minute {min_row['minute']}",
            unsafe_allow_html=True,
        )

    else:
        st.info("No timeline data available for this match.")

    st.markdown("---")

    # Prediction vs Reality
    st.markdown("### 🔮 Prediction vs Reality")

    # Get @10min prediction
    if not timeline_df.empty and len(timeline_df) >= 10:
        gold_10_row = timeline_df[timeline_df["minute"] == 10]
        if not gold_10_row.empty:
            gold_diff_10 = gold_10_row.iloc[0]["gold_diff"]

            # Get first objectives
            team_stats = match_details.get("team_stats", {})
            t100_stats = team_stats.get(100, {})

            first_blood = t100_stats.get("first_blood", False)
            first_tower = t100_stats.get("first_tower", False)
            first_dragon = t100_stats.get("first_dragon", False)
            first_herald = t100_stats.get("first_rift_herald", False)

            try:
                from utils.model_loader import load_early_game_model, predict_win_probability
                model, scaler, feature_names, _ = load_early_game_model()

                if model:
                    pred = predict_win_probability(
                        model, scaler, feature_names,
                        gold_diff=gold_diff_10,
                        first_blood=first_blood,
                        first_tower=first_tower,
                        first_dragon=first_dragon,
                        first_herald=first_herald,
                    )
                    predicted_winner = "Blue" if pred["blue_win_prob"] > 0.5 else "Red"
                    actual_winner = "Blue" if match_details.get("team_100_win") else "Red"
                    correct = predicted_winner == actual_winner

                    col1, col2, col3 = st.columns(3)

                    with col1:
                        pred_color = COLORS["blue_team"] if predicted_winner == "Blue" else COLORS["red_team"]
                        st.markdown(
                            html(f"""
                            <div style='
                                background: {COLORS["background_light"]};
                                border: 1px solid {pred_color}60;
                                border-radius: 10px;
                                padding: 20px;
                                text-align: center;
                            '>
                                <p style='color: {COLORS["text_secondary"]}; margin-bottom: 5px;'>@10min Prediction</p>
                                <p style='color: {pred_color}; font-size: 1.5em; font-weight: bold;'>
                                    {predicted_winner} Team
                                </p>
                                <p style='color: {COLORS["text_secondary"]}; font-size: 0.9em;'>
                                    {pred['blue_win_prob']*100:.1f}% Blue / {pred['red_win_prob']*100:.1f}% Red
                                </p>
                            </div>
                            """), unsafe_allow_html=True,
                        )

                    with col2:
                        actual_color = COLORS["blue_team"] if actual_winner == "Blue" else COLORS["red_team"]
                        st.markdown(
                            html(f"""
                            <div style='
                                background: {COLORS["background_light"]};
                                border: 1px solid {actual_color}60;
                                border-radius: 10px;
                                padding: 20px;
                                text-align: center;
                            '>
                                <p style='color: {COLORS["text_secondary"]}; margin-bottom: 5px;'>Actual Winner</p>
                                <p style='color: {actual_color}; font-size: 1.5em; font-weight: bold;'>
                                    {actual_winner} Team
                                </p>
                            </div>
                            """), unsafe_allow_html=True,
                        )

                    with col3:
                        result_color = COLORS["success"] if correct else COLORS["danger"]
                        result_text = "✓ Correct" if correct else "✗ Incorrect"
                        st.markdown(
                            html(f"""
                            <div style='
                                background: {COLORS["background_light"]};
                                border: 1px solid {result_color}60;
                                border-radius: 10px;
                                padding: 20px;
                                text-align: center;
                            '>
                                <p style='color: {COLORS["text_secondary"]}; margin-bottom: 5px;'>Prediction Result</p>
                                <p style='color: {result_color}; font-size: 1.5em; font-weight: bold;'>
                                    {result_text}
                                </p>
                            </div>
                            """), unsafe_allow_html=True,
                        )
            except Exception as e:
                st.info("Could not generate prediction for this match.")
    else:
        st.info("Not enough timeline data to make @10min prediction.")
