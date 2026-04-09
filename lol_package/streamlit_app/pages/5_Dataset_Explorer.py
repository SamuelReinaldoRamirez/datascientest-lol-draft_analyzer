"""
Dataset Explorer Page - Browse and filter the match database
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

# Add parent directory to path
STREAMLIT_APP_DIR = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(STREAMLIT_APP_DIR))

from config import COLORS, COLORS_ALPHA, rgba, html

st.set_page_config(
    page_title="Dataset Explorer - LoL Draft Predictor",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Dataset Explorer")
st.markdown(
    f"<p style='color: {COLORS['text_secondary']};'>Explore the match database with filters and statistics.</p>",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=3600)
def load_explorer_data(limit: int = 10000) -> pd.DataFrame:
    """Load match data for exploration."""
    import sqlite3
    from config import DATABASE_PATH

    conn = sqlite3.connect(str(DATABASE_PATH))

    query = f"""
        SELECT
            m.match_id,
            m.game_duration,
            m.game_version,
            m.team_100_win,
            m.source_elo,
            m.collected_at,
            m.queue_id,
            t100.first_blood as team_100_first_blood,
            t100.first_dragon as team_100_first_dragon,
            t100.first_tower as team_100_first_tower,
            t100.dragon_kills as team_100_dragons,
            t100.baron_kills as team_100_barons,
            t200.dragon_kills as team_200_dragons,
            t200.baron_kills as team_200_barons
        FROM matches m
        LEFT JOIN team_stats t100 ON m.match_id = t100.match_id AND t100.team_id = 100
        LEFT JOIN team_stats t200 ON m.match_id = t200.match_id AND t200.team_id = 200
        ORDER BY m.collected_at DESC
        LIMIT {limit}
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # Process data
    if not df.empty:
        df["game_duration_min"] = df["game_duration"] / 60
        df["winner"] = df["team_100_win"].apply(lambda x: "Blue" if x else "Red")
        df["patch"] = df["game_version"].apply(lambda x: ".".join(x.split(".")[:2]) if pd.notna(x) else "Unknown")
        df["collected_date"] = pd.to_datetime(df["collected_at"]).dt.date

    return df


# Load data
try:
    df = load_explorer_data()
    has_data = len(df) > 0
except Exception as e:
    st.error(f"Could not load data: {e}")
    has_data = False
    df = pd.DataFrame()

if not has_data:
    st.warning("No data available. Please run data collection first.")
    st.stop()

st.markdown("---")

# Filters
st.markdown("### 🔍 Filters")

col1, col2, col3, col4 = st.columns(4)

with col1:
    # Elo filter
    elo_options = ["All"] + sorted(df["source_elo"].dropna().unique().tolist())
    selected_elo = st.selectbox("Elo", elo_options, index=0)

with col2:
    # Winner filter
    winner_options = ["All", "Blue", "Red"]
    selected_winner = st.selectbox("Winner", winner_options, index=0)

with col3:
    # Patch filter
    patch_options = ["All"] + sorted(df["patch"].dropna().unique().tolist(), reverse=True)
    selected_patch = st.selectbox("Patch", patch_options, index=0)

with col4:
    # Duration filter
    duration_range = st.slider(
        "Game Duration (min)",
        min_value=10,
        max_value=60,
        value=(15, 45),
        step=5,
    )

# Apply filters
filtered_df = df.copy()

if selected_elo != "All":
    filtered_df = filtered_df[filtered_df["source_elo"] == selected_elo]

if selected_winner != "All":
    filtered_df = filtered_df[filtered_df["winner"] == selected_winner]

if selected_patch != "All":
    filtered_df = filtered_df[filtered_df["patch"] == selected_patch]

filtered_df = filtered_df[
    (filtered_df["game_duration_min"] >= duration_range[0]) &
    (filtered_df["game_duration_min"] <= duration_range[1])
]

# Show filter results
st.markdown(
    f"<p style='color: {COLORS['text_secondary']};'>Showing <strong style='color: {COLORS['gold_accent']}'>{len(filtered_df):,}</strong> matches (filtered from {len(df):,})</p>",
    unsafe_allow_html=True,
)

st.markdown("---")

# Statistics Section
st.markdown("### 📈 Statistics")

stat_cols = st.columns(5)

with stat_cols[0]:
    total_matches = len(filtered_df)
    st.metric("Total Matches", f"{total_matches:,}")

with stat_cols[1]:
    blue_wr = filtered_df["team_100_win"].mean() * 100 if len(filtered_df) > 0 else 50
    st.metric("Blue Winrate", f"{blue_wr:.1f}%")

with stat_cols[2]:
    avg_duration = filtered_df["game_duration_min"].mean() if len(filtered_df) > 0 else 0
    st.metric("Avg Duration", f"{avg_duration:.1f} min")

with stat_cols[3]:
    fb_wr = filtered_df[filtered_df["team_100_first_blood"] == True]["team_100_win"].mean() * 100 if len(filtered_df[filtered_df["team_100_first_blood"] == True]) > 0 else 50
    st.metric("First Blood → Win", f"{fb_wr:.1f}%")

with stat_cols[4]:
    fd_wr = filtered_df[filtered_df["team_100_first_dragon"] == True]["team_100_win"].mean() * 100 if len(filtered_df[filtered_df["team_100_first_dragon"] == True]) > 0 else 50
    st.metric("First Dragon → Win", f"{fd_wr:.1f}%")

st.markdown("---")

# Visualizations
st.markdown("### 📊 Visualizations")

tab1, tab2, tab3 = st.tabs(["Win Rate by Side", "Game Duration", "Matches Over Time"])

with tab1:
    col1, col2 = st.columns([1, 1])

    with col1:
        # Win rate pie chart
        blue_wins = filtered_df["team_100_win"].sum()
        red_wins = len(filtered_df) - blue_wins

        fig = go.Figure()

        fig.add_trace(go.Pie(
            labels=["Blue Wins", "Red Wins"],
            values=[blue_wins, red_wins],
            marker_colors=[COLORS["blue_team"], COLORS["red_team"]],
            hole=0.5,
            textinfo="label+percent",
            textfont=dict(color=COLORS["text_primary"]),
        ))

        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color=COLORS["text_primary"]),
            showlegend=False,
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Win rate by elo
        if len(filtered_df["source_elo"].dropna().unique()) > 1:
            elo_wr = filtered_df.groupby("source_elo")["team_100_win"].mean().reset_index()
            elo_wr.columns = ["Elo", "Blue Winrate"]
            elo_wr["Blue Winrate"] = elo_wr["Blue Winrate"] * 100

            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=elo_wr["Elo"],
                y=elo_wr["Blue Winrate"],
                marker_color=COLORS["blue_team"],
                text=[f"{v:.1f}%" for v in elo_wr["Blue Winrate"]],
                textposition="outside",
                textfont=dict(color=COLORS["text_primary"]),
            ))

            fig.add_hline(y=50, line_dash="dash", line_color=COLORS["gold_accent"])

            fig.update_layout(
                title="Blue Win Rate by Elo",
                height=350,
                margin=dict(l=20, r=20, t=50, b=20),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor=COLORS["background_light"],
                font=dict(color=COLORS["text_primary"]),
                xaxis=dict(
                    tickfont=dict(color=COLORS["text_secondary"]),
                ),
                yaxis=dict(
                    title="Win Rate (%)",
                    range=[40, 60],
                    gridcolor="rgba(255,255,255,0.1)",
                    tickfont=dict(color=COLORS["text_secondary"]),
                ),
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Select 'All' elo to see win rate comparison.")

with tab2:
    col1, col2 = st.columns(2)

    with col1:
        # Duration distribution
        fig = go.Figure()

        fig.add_trace(go.Histogram(
            x=filtered_df["game_duration_min"],
            nbinsx=30,
            marker_color=COLORS["gold_accent"],
            opacity=0.8,
        ))

        fig.update_layout(
            title="Game Duration Distribution",
            height=350,
            margin=dict(l=20, r=20, t=50, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor=COLORS["background_light"],
            font=dict(color=COLORS["text_primary"]),
            xaxis=dict(
                title="Duration (minutes)",
                gridcolor="rgba(255,255,255,0.1)",
                tickfont=dict(color=COLORS["text_secondary"]),
            ),
            yaxis=dict(
                title="Number of Matches",
                gridcolor="rgba(255,255,255,0.1)",
                tickfont=dict(color=COLORS["text_secondary"]),
            ),
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Duration by winner
        fig = go.Figure()

        blue_durations = filtered_df[filtered_df["team_100_win"] == True]["game_duration_min"]
        red_durations = filtered_df[filtered_df["team_100_win"] == False]["game_duration_min"]

        fig.add_trace(go.Box(
            y=blue_durations,
            name="Blue Wins",
            marker_color=COLORS["blue_team"],
        ))

        fig.add_trace(go.Box(
            y=red_durations,
            name="Red Wins",
            marker_color=COLORS["red_team"],
        ))

        fig.update_layout(
            title="Game Duration by Winner",
            height=350,
            margin=dict(l=20, r=20, t=50, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor=COLORS["background_light"],
            font=dict(color=COLORS["text_primary"]),
            yaxis=dict(
                title="Duration (minutes)",
                gridcolor="rgba(255,255,255,0.1)",
                tickfont=dict(color=COLORS["text_secondary"]),
            ),
            showlegend=False,
        )

        st.plotly_chart(fig, use_container_width=True)

with tab3:
    # Matches over time
    if "collected_date" in filtered_df.columns:
        daily_counts = filtered_df.groupby("collected_date").size().reset_index()
        daily_counts.columns = ["Date", "Matches"]

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=daily_counts["Date"],
            y=daily_counts["Matches"],
            mode="lines+markers",
            marker=dict(color=COLORS["gold_accent"], size=6),
            line=dict(color=COLORS["gold_accent"], width=2),
            fill="tozeroy",
            fillcolor=rgba(COLORS['gold_accent'], 0.19),
        ))

        fig.update_layout(
            title="Matches Collected Over Time",
            height=350,
            margin=dict(l=20, r=20, t=50, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor=COLORS["background_light"],
            font=dict(color=COLORS["text_primary"]),
            xaxis=dict(
                title="Date",
                gridcolor="rgba(255,255,255,0.1)",
                tickfont=dict(color=COLORS["text_secondary"]),
            ),
            yaxis=dict(
                title="Number of Matches",
                gridcolor="rgba(255,255,255,0.1)",
                tickfont=dict(color=COLORS["text_secondary"]),
            ),
        )

        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# Data Table
st.markdown("### 📋 Match Data")

# Pagination
page_size = 25
total_pages = max(1, len(filtered_df) // page_size + (1 if len(filtered_df) % page_size else 0))

col1, col2, col3 = st.columns([1, 3, 1])
with col2:
    page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)

start_idx = (page - 1) * page_size
end_idx = start_idx + page_size

# Display columns
display_cols = [
    "match_id",
    "winner",
    "game_duration_min",
    "patch",
    "source_elo",
    "team_100_first_blood",
    "team_100_first_dragon",
    "team_100_dragons",
    "team_200_dragons",
]

display_df = filtered_df[display_cols].iloc[start_idx:end_idx].copy()
display_df.columns = [
    "Match ID",
    "Winner",
    "Duration (min)",
    "Patch",
    "Elo",
    "Blue FB",
    "Blue FD",
    "Blue Dragons",
    "Red Dragons",
]

# Format columns
display_df["Duration (min)"] = display_df["Duration (min)"].round(1)
display_df["Blue FB"] = display_df["Blue FB"].apply(lambda x: "✓" if x else "✗")
display_df["Blue FD"] = display_df["Blue FD"].apply(lambda x: "✓" if x else "✗")

# Apply styling
def color_winner(val):
    if val == "Blue":
        return f"color: {COLORS['blue_team']}"
    else:
        return f"color: {COLORS['red_team']}"

styled_df = display_df.style.map(color_winner, subset=["Winner"])

st.dataframe(
    styled_df,
    use_container_width=True,
    height=400,
)

st.markdown(
    f"<p style='color: {COLORS['text_secondary']}; font-size: 0.85em; text-align: center;'>Showing rows {start_idx + 1} - {min(end_idx, len(filtered_df))} of {len(filtered_df)}</p>",
    unsafe_allow_html=True,
)

# Download button
st.markdown("---")

col1, col2, col3 = st.columns([1, 1, 1])

with col2:
    csv = filtered_df.to_csv(index=False)
    st.download_button(
        label="📥 Download Filtered Data (CSV)",
        data=csv,
        file_name=f"lol_matches_filtered_{len(filtered_df)}.csv",
        mime="text/csv",
    )
