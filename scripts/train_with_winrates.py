#!/usr/bin/env python3
"""
Entraînement des modèles avec données de winrates externes (dpm.lol).

Intègre:
- Winrates simples par champion/role (data/winrates/les winrates simples/)
- Matchups par lane (data/winrates/les winrates matchups/)
- Synergies/Counters calculés depuis nos matchs
- Timeline data pour les modèles in-game

Modèles entraînés:
- Draft-only: composition + winrates externes + matchups + synergies
- @5min:  Draft + gold/level/cs par position @5min
- @10min: Draft + gold/level/cs par position @10min + CS@10
- @15min: Draft + gold/level/cs par position @15min
- @20min: Draft + gold/level/cs par position @20min

Usage:
    python scripts/train_with_winrates.py
    python scripts/train_with_winrates.py --no-synergy-recompute
"""

import sys
import os
import argparse
import pandas as pd
import numpy as np
import pickle
import glob
import time
import warnings
from itertools import combinations
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from sklearn.metrics import classification_report, accuracy_score

warnings.filterwarnings('ignore')

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ============================================================================
# CHAMPION NAME NORMALIZATION
# ============================================================================

# Mapping: winrate CSV name -> DB/Riot API name
CHAMPION_NAME_TO_DB = {
    "Aurelion Sol": "AurelionSol",
    "Bel'Veth": "Belveth",
    "Cho'Gath": "Chogath",
    "Dr.Mundo": "DrMundo",
    "Fiddlesticks": "FiddleSticks",
    "Jarvan IV": "JarvanIV",
    "K'Sante": "KSante",
    "Kai'Sa": "Kaisa",
    "Kha'Zix": "Khazix",
    "Kog'Maw": "KogMaw",
    "Master Yi": "MasterYi",
    "Miss Fortune": "MissFortune",
    "Rek'Sai": "RekSai",
    "Tahm Kench": "TahmKench",
    "Twisted Fate": "TwistedFate",
    "Vel'Koz": "Velkoz",
    "Wukong": "MonkeyKing",
    "Xin Zhao": "XinZhao",
}

# Reverse mapping for matchup data (which already uses DB-style names like "KSante", "DrMundo")
CHAMPION_MATCHUP_TO_DB = {
    "KSante": "KSante",
    "DrMundo": "DrMundo",
    "MasterYi": "MasterYi",
    "MissFortune": "MissFortune",
    "MonkeyKing": "MonkeyKing",
    "TahmKench": "TahmKench",
    "TwistedFate": "TwistedFate",
    "XinZhao": "XinZhao",
    "FiddleSticks": "FiddleSticks",
    "JarvanIV": "JarvanIV",
    "AurelionSol": "AurelionSol",
    "Chogath": "Chogath",
    "KogMaw": "KogMaw",
    "Belveth": "Belveth",
    "RekSai": "RekSai",
    "Kaisa": "Kaisa",
    "Velkoz": "Velkoz",
    "Khazix": "Khazix",
}

# Role mapping: winrate CSV roles -> DB position names
ROLE_WR_TO_DB = {
    'top': 'top', 'jun': 'jungle', 'jungle': 'jungle',
    'mid': 'mid', 'adc': 'adc', 'sup': 'support', 'support': 'support'
}

# Tier encoding
TIER_ENCODING = {
    'S+': 6, 'S': 5, 'A': 4, 'B': 3, 'C': 2, 'D': 1,
    'S+ ': 6, 'S ': 5, 'A ': 4, 'B ': 3, 'C ': 2, 'D ': 1,
}


def normalize_champion_name(name):
    """Normalize champion name to DB format."""
    if pd.isna(name):
        return None
    name = str(name).strip()
    if name in CHAMPION_NAME_TO_DB:
        return CHAMPION_NAME_TO_DB[name]
    if name in CHAMPION_MATCHUP_TO_DB:
        return CHAMPION_MATCHUP_TO_DB[name]
    return name


def parse_winrate(wr_str):
    """Parse winrate string like '52.8%' or '52.8 %' to float 0.528."""
    if pd.isna(wr_str):
        return np.nan
    s = str(wr_str).replace('%', '').replace(' ', '').replace('+', '').strip()
    try:
        return float(s) / 100.0
    except ValueError:
        return np.nan


def parse_games(games_str):
    """Parse games string like '313 906' or '1 386 160' to int."""
    if pd.isna(games_str):
        return 0
    s = str(games_str).replace(' ', '').replace(',', '').strip()
    try:
        return int(float(s))
    except ValueError:
        return 0


# ============================================================================
# 1. LOAD EXTERNAL WINRATE DATA
# ============================================================================

def load_simple_winrates():
    """
    Load champion winrates from the refined CSV.
    Returns dict: (champion_db_name, role_db) -> {winrate, pickrate, tier, tier_num, games}
    """
    refined_path = os.path.join(
        PROJECT_ROOT, 'data', 'winrates', 'les winrates simples',
        'données raffinées', 'df_Simple_WR_FULL.csv'
    )

    if not os.path.exists(refined_path):
        print("  [WARN] Refined winrate file not found, trying individual CSVs...")
        return _load_simple_winrates_from_individual()

    df = pd.read_csv(refined_path)

    # Filter: use TOUT/TOUT (global data), latest patch
    global_data = df[(df['elo'] == 'TOUT') & (df['server'] == 'TOUT')]
    if len(global_data) == 0:
        global_data = df[df['elo'] == 'TOUT']

    # Use latest patch
    latest_patch = global_data['patch'].max()
    global_data = global_data[global_data['patch'] == latest_patch]
    print(f"  Winrates: {len(global_data)} entries, patch {latest_patch}, TOUT/TOUT")

    wr_dict = {}
    for _, row in global_data.iterrows():
        name_db = normalize_champion_name(row['name'])
        role_csv = str(row['role']).strip().lower()
        role_db = ROLE_WR_TO_DB.get(role_csv, role_csv)

        if name_db is None:
            continue

        wr = parse_winrate(row.get('winrate'))
        pr = parse_winrate(row.get('pickrate'))
        tier_str = str(row.get('tier', '')).strip()
        tier_num = TIER_ENCODING.get(tier_str, 0)
        games = parse_games(row.get('games'))

        wr_dict[(name_db, role_db)] = {
            'winrate': wr if not np.isnan(wr) else 0.5,
            'pickrate': pr if not np.isnan(pr) else 0.0,
            'tier': tier_str,
            'tier_num': tier_num,
            'games': games,
        }

    print(f"  Loaded {len(wr_dict)} champion-role winrate entries")
    return wr_dict


def _load_simple_winrates_from_individual():
    """Fallback: load from individual CSV files."""
    simples_dir = os.path.join(PROJECT_ROOT, 'data', 'winrates', 'les winrates simples')
    # Prefer TOUT_Emerald+ or TOUT_Diamond+ with latest patch
    candidates = glob.glob(os.path.join(simples_dir, 'TOUT_Emerald+_*.csv'))
    if not candidates:
        candidates = glob.glob(os.path.join(simples_dir, 'TOUT_Diamond+_*.csv'))
    if not candidates:
        candidates = glob.glob(os.path.join(simples_dir, 'TOUT_*.csv'))

    if not candidates:
        print("  [WARN] No winrate CSVs found!")
        return {}

    # Sort by patch (extract from filename)
    candidates.sort(key=lambda f: f.split('_')[-1].replace('.csv', ''), reverse=True)
    chosen = candidates[0]
    print(f"  Loading from: {os.path.basename(chosen)}")

    df = pd.read_csv(chosen)
    wr_dict = {}
    name_col = 'champion' if 'champion' in df.columns else 'name'

    for _, row in df.iterrows():
        name_db = normalize_champion_name(row[name_col])
        role_csv = str(row['role']).strip().lower()
        role_db = ROLE_WR_TO_DB.get(role_csv, role_csv)

        if name_db is None:
            continue

        wr = parse_winrate(row.get('winrate'))
        pr = parse_winrate(row.get('pickrate'))
        tier_str = str(row.get('tier', '')).strip()
        tier_num = TIER_ENCODING.get(tier_str, 0)
        games = parse_games(row.get('games'))

        wr_dict[(name_db, role_db)] = {
            'winrate': wr if not np.isnan(wr) else 0.5,
            'pickrate': pr if not np.isnan(pr) else 0.0,
            'tier': tier_str,
            'tier_num': tier_num,
            'games': games,
        }

    print(f"  Loaded {len(wr_dict)} champion-role winrate entries")
    return wr_dict


# ============================================================================
# 2. LOAD MATCHUP DATA
# ============================================================================

def load_matchup_data():
    """
    Load matchup data from CSVs.
    Returns dict: (championA_db, championB_db, role) -> winrate_of_A_vs_B
    """
    matchup_dir = os.path.join(PROJECT_ROOT, 'data', 'winrates', 'les winrates matchups')
    matchup_dict = {}

    # Source 1: Top-level 16.3 matchup files (top lane only)
    top_files = glob.glob(os.path.join(matchup_dir, '*_top_matchup_TOUT_TOUT_16.3.csv'))
    if top_files:
        _load_matchup_files(top_files, 'top', matchup_dict)

    # Source 2: TOUTTOUT15.24 subdirectories (all lanes)
    lane_dir = os.path.join(matchup_dir, 'TOUTTOUT15.24', 'matchups')
    if os.path.exists(lane_dir):
        for lane_folder in os.listdir(lane_dir):
            lane_path = os.path.join(lane_dir, lane_folder)
            if not os.path.isdir(lane_path):
                continue
            # Extract lane from folder name (e.g., "top matchup" -> "top")
            lane = lane_folder.split()[0].lower()
            role_db = ROLE_WR_TO_DB.get(lane, lane)
            files = glob.glob(os.path.join(lane_path, '*.csv'))
            if files:
                _load_matchup_files(files, role_db, matchup_dict)

    print(f"  Loaded {len(matchup_dict)} matchup entries across all lanes")
    return matchup_dict


def _load_matchup_files(files, role_db, matchup_dict):
    """Parse matchup CSV files and populate matchup_dict."""
    for filepath in files:
        try:
            df = pd.read_csv(filepath)
        except Exception:
            continue

        for _, row in df.iterrows():
            champ_name = normalize_champion_name(row.get('champion'))
            if champ_name is None:
                continue

            # Find matchup columns
            for i in range(1, 60):
                name_col = f'matchup_{role_db.replace("jungle","jun").replace("support","sup")}_{i}_name'
                # Try different column naming patterns
                found_name_col = None
                found_wr_col = None
                for prefix in [f'matchup_top_{i}', f'matchup_jun_{i}', f'matchup_mid_{i}',
                               f'matchup_adc_{i}', f'matchup_sup_{i}']:
                    nc = f'{prefix}_name'
                    wc = f'{prefix}_winrate'
                    if nc in df.columns:
                        found_name_col = nc
                        found_wr_col = wc
                        break

                if found_name_col is None or found_name_col not in row.index:
                    break

                opp_name = normalize_champion_name(row.get(found_name_col))
                opp_wr = parse_winrate(row.get(found_wr_col))

                if opp_name is None or np.isnan(opp_wr):
                    continue

                matchup_dict[(champ_name, opp_name, role_db)] = opp_wr


# ============================================================================
# 3. LOAD SYNERGY DATA
# ============================================================================

def load_external_synergies():
    """
    Load synergy data from CSVs.
    Returns dict: (champA_db, champB_db, role) -> synergy_winrate
    """
    synergy_dir = os.path.join(
        PROJECT_ROOT, 'data', 'winrates', 'les winrates matchups',
        'TOUTTOUT15.24', 'synergies'
    )
    synergy_dict = {}

    if not os.path.exists(synergy_dir):
        print("  [WARN] No synergy directory found")
        return synergy_dict

    for lane_folder in os.listdir(synergy_dir):
        lane_path = os.path.join(synergy_dir, lane_folder)
        if not os.path.isdir(lane_path):
            continue

        lane = lane_folder.split()[0].lower()
        role_db = ROLE_WR_TO_DB.get(lane, lane)
        files = glob.glob(os.path.join(lane_path, '*.csv'))

        for filepath in files:
            try:
                df = pd.read_csv(filepath)
            except Exception:
                continue

            for _, row in df.iterrows():
                champ_name = normalize_champion_name(row.get('champion'))
                if champ_name is None:
                    continue

                for i in range(1, 65):
                    found_name_col = None
                    found_wr_col = None
                    for prefix_lane in ['top', 'jun', 'mid', 'adc', 'sup']:
                        nc = f'synergy_{prefix_lane}_{i}_name'
                        wc = f'synergy_{prefix_lane}_{i}_winrate'
                        if nc in df.columns:
                            found_name_col = nc
                            found_wr_col = wc
                            break

                    if found_name_col is None:
                        break

                    partner_name = normalize_champion_name(row.get(found_name_col))
                    partner_wr = parse_winrate(row.get(found_wr_col))

                    if partner_name is None or np.isnan(partner_wr):
                        continue

                    key = tuple(sorted([champ_name, partner_name]) + [role_db])
                    synergy_dict[key] = partner_wr

    print(f"  Loaded {len(synergy_dict)} external synergy entries")
    return synergy_dict


# ============================================================================
# 4. ADD EXTERNAL FEATURES TO DATAFRAME
# ============================================================================

def add_external_winrate_features(df, wr_dict):
    """
    Add external winrate features for each champion in each position.
    Features added per team/position:
      - ext_wr_{team}_{pos}: champion winrate from external data
      - ext_tier_{team}_{pos}: tier encoding
      - ext_pickrate_{team}_{pos}: pickrate
    Plus aggregated:
      - ext_avg_wr_{team}: average team winrate
      - ext_avg_tier_{team}: average team tier
      - ext_wr_diff: team_100 avg WR - team_200 avg WR
      - ext_tier_diff: team_100 avg tier - team_200 avg tier
    """
    positions = ['top', 'jungle', 'mid', 'adc', 'support']
    n = len(df)

    features = {}
    for team in [100, 200]:
        wr_list = []
        tier_list = []
        for pos in positions:
            col_name = f'team_{team}_{pos}_champion_name'
            wr_col = f'ext_wr_{team}_{pos}'
            tier_col = f'ext_tier_{team}_{pos}'
            pr_col = f'ext_pickrate_{team}_{pos}'

            wr_vals = np.full(n, 0.5)
            tier_vals = np.zeros(n)
            pr_vals = np.zeros(n)

            if col_name in df.columns:
                for i, name in enumerate(df[col_name].values):
                    if pd.notna(name):
                        key = (str(name).strip(), pos)
                        data = wr_dict.get(key)
                        if data:
                            wr_vals[i] = data['winrate']
                            tier_vals[i] = data['tier_num']
                            pr_vals[i] = data['pickrate']

            features[wr_col] = wr_vals
            features[tier_col] = tier_vals
            features[pr_col] = pr_vals
            wr_list.append(wr_vals)
            tier_list.append(tier_vals)

        # Team averages
        features[f'ext_avg_wr_{team}'] = np.mean(wr_list, axis=0)
        features[f'ext_avg_tier_{team}'] = np.mean(tier_list, axis=0)

    # Diffs
    features['ext_wr_diff'] = features['ext_avg_wr_100'] - features['ext_avg_wr_200']
    features['ext_tier_diff'] = features['ext_avg_tier_100'] - features['ext_avg_tier_200']

    df = df.copy()
    for name, vals in features.items():
        df[name] = vals

    return df


def add_matchup_features(df, matchup_dict):
    """
    Add lane matchup advantage features.
    For each position: how well does team_100's champion do vs team_200's champion in that lane?
    Features:
      - matchup_wr_{pos}: team_100 champion's winrate vs team_200 champion
      - matchup_advantage_{pos}: matchup_wr - 0.5 (positive = team_100 advantage)
      - avg_matchup_advantage: average across all positions
    """
    positions = ['top', 'jungle', 'mid', 'adc', 'support']
    n = len(df)
    features = {}

    advantages = []
    for pos in positions:
        col_100 = f'team_100_{pos}_champion_name'
        col_200 = f'team_200_{pos}_champion_name'
        wr_col = f'matchup_wr_{pos}'
        adv_col = f'matchup_advantage_{pos}'

        wr_vals = np.full(n, 0.5)

        if col_100 in df.columns and col_200 in df.columns:
            for i in range(n):
                c100 = df[col_100].iloc[i]
                c200 = df[col_200].iloc[i]
                if pd.notna(c100) and pd.notna(c200):
                    c100_str = str(c100).strip()
                    c200_str = str(c200).strip()
                    # Try exact position match
                    wr = matchup_dict.get((c100_str, c200_str, pos))
                    if wr is not None:
                        wr_vals[i] = wr
                    else:
                        # Try reverse: if A vs B = x, then B vs A = 1-x
                        wr_rev = matchup_dict.get((c200_str, c100_str, pos))
                        if wr_rev is not None:
                            wr_vals[i] = 1.0 - wr_rev

        features[wr_col] = wr_vals
        features[adv_col] = wr_vals - 0.5
        advantages.append(wr_vals - 0.5)

    features['avg_matchup_advantage'] = np.mean(advantages, axis=0)
    features['max_matchup_advantage'] = np.max(advantages, axis=0)
    features['min_matchup_advantage'] = np.min(advantages, axis=0)
    features['matchup_advantage_std'] = np.std(advantages, axis=0)

    df = df.copy()
    for name, vals in features.items():
        df[name] = vals

    return df


def add_external_synergy_features(df, ext_synergy_dict):
    """
    Add synergy features from external data.
    For each team: average synergy winrate of all champion pairs.
    """
    positions = ['top', 'jungle', 'mid', 'adc', 'support']
    n = len(df)

    features = {
        'ext_synergy_100': np.full(n, 0.5),
        'ext_synergy_200': np.full(n, 0.5),
    }

    if not ext_synergy_dict:
        df = df.copy()
        for name, vals in features.items():
            df[name] = vals
        df['ext_synergy_diff'] = 0.0
        return df

    for team in [100, 200]:
        key = f'ext_synergy_{team}'
        for i in range(n):
            champs = []
            for pos in positions:
                col = f'team_{team}_{pos}_champion_name'
                if col in df.columns:
                    val = df[col].iloc[i]
                    if pd.notna(val):
                        champs.append((str(val).strip(), pos))

            if len(champs) < 2:
                continue

            syn_scores = []
            for (c1, r1), (c2, r2) in combinations(champs, 2):
                # Try with champion's own role
                for role in [r1, r2]:
                    k = tuple(sorted([c1, c2]) + [role])
                    wr = ext_synergy_dict.get(k)
                    if wr is not None:
                        syn_scores.append(wr)
                        break

            if syn_scores:
                features[key][i] = np.mean(syn_scores)

    df = df.copy()
    for name, vals in features.items():
        df[name] = vals
    df['ext_synergy_diff'] = df['ext_synergy_100'] - df['ext_synergy_200']

    return df


# ============================================================================
# 5. COMPUTED SYNERGIES/COUNTERS FROM OUR DATA
# ============================================================================

def load_or_compute_synergies(train_df, force_recompute=False):
    """Load or compute champion synergy/counter data from our match data."""
    syn_path = os.path.join(PROJECT_ROOT, 'data', 'features', 'champion_synergy_counter.pkl')

    if os.path.exists(syn_path) and not force_recompute:
        print("  Chargement synergies/counters existantes...")
        with open(syn_path, 'rb') as f:
            return pickle.load(f)

    print("  Calcul des synergies/counters depuis nos matchs...")
    champ_cols_100 = [f'team_100_{pos}_champion_id' for pos in ['top', 'jungle', 'mid', 'adc', 'support']]
    champ_cols_200 = [f'team_200_{pos}_champion_id' for pos in ['top', 'jungle', 'mid', 'adc', 'support']]

    from collections import defaultdict
    synergy_wins = defaultdict(int)
    synergy_games = defaultdict(int)
    counter_wins = defaultdict(int)
    counter_games = defaultdict(int)

    for idx, row in train_df.iterrows():
        team_100_win = row['team_100_win']
        champs_100 = [int(row[c]) for c in champ_cols_100 if pd.notna(row[c])]
        champs_200 = [int(row[c]) for c in champ_cols_200 if pd.notna(row[c])]

        for team_champs, is_winner in [(champs_100, team_100_win == 1), (champs_200, team_100_win == 0)]:
            for c1, c2 in combinations(team_champs, 2):
                pair = tuple(sorted([c1, c2]))
                synergy_games[pair] += 1
                if is_winner:
                    synergy_wins[pair] += 1

        for c100 in champs_100:
            for c200 in champs_200:
                pair = tuple(sorted([c100, c200]))
                counter_games[pair] += 1
                if team_100_win == 1:
                    counter_wins[(c100, c200)] += 1
                else:
                    counter_wins[(c200, c100)] += 1

    synergy_wr = {pair: wins / synergy_games[pair]
                  for pair, wins in synergy_wins.items() if synergy_games[pair] >= 30}
    counter_wr = {pair: wins / counter_games[tuple(sorted(pair))]
                  for pair, wins in counter_wins.items()
                  if counter_games[tuple(sorted(pair))] >= 30}

    result = {
        'synergy_winrate': dict(synergy_wr),
        'counter_winrate': dict(counter_wr),
        'synergy_games': dict(synergy_games),
        'counter_games': dict(counter_games)
    }

    os.makedirs(os.path.dirname(syn_path), exist_ok=True)
    with open(syn_path, 'wb') as f:
        pickle.dump(result, f)

    print(f"    {len(synergy_wr):,} synergies, {len(counter_wr):,} counters")
    return result


def add_synergy_counter_features(df, synergy_wr, counter_wr):
    """Add synergy/counter features from our computed data."""
    champ_cols = {
        100: [f'team_100_{pos}_champion_id' for pos in ['top', 'jungle', 'mid', 'adc', 'support']],
        200: [f'team_200_{pos}_champion_id' for pos in ['top', 'jungle', 'mid', 'adc', 'support']]
    }

    n = len(df)
    features = {
        'team_100_synergy_score': np.full(n, 0.5),
        'team_200_synergy_score': np.full(n, 0.5),
        'team_100_counter_score': np.full(n, 0.5),
        'team_200_counter_score': np.full(n, 0.5),
    }

    for i, (idx, row) in enumerate(df.iterrows()):
        champs_100 = [int(row[c]) for c in champ_cols[100] if pd.notna(row[c])]
        champs_200 = [int(row[c]) for c in champ_cols[200] if pd.notna(row[c])]

        syn_100 = [synergy_wr.get(tuple(sorted([c1, c2])), 0.5) for c1, c2 in combinations(champs_100, 2)]
        syn_200 = [synergy_wr.get(tuple(sorted([c1, c2])), 0.5) for c1, c2 in combinations(champs_200, 2)]
        if syn_100: features['team_100_synergy_score'][i] = np.mean(syn_100)
        if syn_200: features['team_200_synergy_score'][i] = np.mean(syn_200)

        cnt_100 = [counter_wr.get((c100, c200), 0.5) for c100 in champs_100 for c200 in champs_200]
        cnt_200 = [counter_wr.get((c200, c100), 0.5) for c200 in champs_200 for c100 in champs_100]
        if cnt_100: features['team_100_counter_score'][i] = np.mean(cnt_100)
        if cnt_200: features['team_200_counter_score'][i] = np.mean(cnt_200)

        if i % 50000 == 0 and i > 0:
            print(f"    {i:,}...")

    df = df.copy()
    for name, values in features.items():
        df[name] = values
    df['synergy_diff'] = df['team_100_synergy_score'] - df['team_200_synergy_score']
    df['counter_diff'] = df['team_100_counter_score'] - df['team_200_counter_score']
    df['draft_advantage'] = df['synergy_diff'] + df['counter_diff']
    return df


# ============================================================================
# 6. FEATURE SELECTION
# ============================================================================

def get_draft_features(df):
    """Get all draft-related features (no in-game data)."""
    features = []

    # Champion IDs, Bans, Summoner spells
    for col in df.columns:
        if ('champion_id' in col.lower() or 'ban_' in col.lower() or
            ('summoner_' in col.lower() and '_id' in col.lower() and 'spell' not in col.lower())):
            features.append(col)

    # Summoner stats historiques
    summoner_patterns = ['_role_pct', '_role_winrate', '_mastery_points', '_streak_type',
                        '_streak_length', '_role_kda', '_role_vision', '_champ_recent_wr',
                        '_avg_role_', '_min_role_', '_total_mastery', 'role_specialization_diff',
                        'role_winrate_diff', 'streak_momentum_diff', 'mastery_diff']
    for col in df.columns:
        if any(p in col.lower() for p in summoner_patterns) and col not in features:
            features.append(col)

    # Synergy/Counter (computed)
    for col in ['team_100_synergy_score', 'team_200_synergy_score',
                'team_100_counter_score', 'team_200_counter_score',
                'synergy_diff', 'counter_diff', 'draft_advantage']:
        if col in df.columns:
            features.append(col)

    # EXTERNAL winrate features
    ext_patterns = ['ext_wr_', 'ext_tier_', 'ext_pickrate_', 'ext_avg_wr_', 'ext_avg_tier_',
                    'ext_wr_diff', 'ext_tier_diff',
                    'matchup_wr_', 'matchup_advantage_', 'avg_matchup_advantage',
                    'max_matchup_advantage', 'min_matchup_advantage', 'matchup_advantage_std',
                    'ext_synergy_', 'ext_synergy_diff']
    for col in df.columns:
        if any(col.startswith(p) or col == p for p in ext_patterns) and col not in features:
            features.append(col)

    # Filter to numeric only, exclude target and leaky columns
    numeric_cols = set(df.select_dtypes(include=[np.number]).columns)
    leaky_cols = {'team_100_win', 'game_duration', 'gold_diff_at_5', 'gold_diff_at_10',
                  'gold_diff_at_15', 'gold_diff_at_20'}
    for m in [5, 10, 15, 20]:
        leaky_cols.update([c for c in df.columns if f'_at_{m}' in c])
    # Also exclude post-game stats
    postgame_patterns = ['_kills', '_deaths', '_assists', '_gold', '_damage', '_cs',
                         '_vision', '_kda', '_cc_score', '_penta', '_quadra', '_double',
                         '_triple', '_champ_level', '_first_blood', '_first_tower',
                         '_inhibitor', '_gold_per_minute', '_damage_per_minute',
                         '_kill_participation', '_killing_spree', '_largest', '_neutral_cs',
                         '_physical_damage', '_magic_damage', '_true_damage', '_damage_taken',
                         '_total_damage', '_tower_kills', '_dragon_kills', '_baron_kills',
                         '_rift_herald', '_turret_plates']
    for col in list(features):
        if col in leaky_cols:
            features.remove(col)
            continue
        # Check postgame patterns only for per-player stats
        for team in [100, 200]:
            for pos in ['top', 'jungle', 'mid', 'adc', 'support']:
                prefix = f'team_{team}_{pos}_'
                if col.startswith(prefix):
                    suffix = col[len(prefix):]
                    if any(suffix.startswith(p.lstrip('_')) or suffix == p.lstrip('_')
                           for p in postgame_patterns):
                        if col in features:
                            features.remove(col)

    features = [f for f in features if f in numeric_cols and f != 'team_100_win']
    return list(set(features))


def get_timeline_features(df, minute):
    """Get timeline features for a specific minute."""
    features = []

    # Gold features at this minute
    gold_cols = [c for c in df.columns if f'gold_at_{minute}' in c.lower()]
    features.extend(gold_cols)

    # Gold diff
    gold_diff_col = f'gold_diff_at_{minute}'
    if gold_diff_col in df.columns:
        features.append(gold_diff_col)

    # Level features
    level_cols = [c for c in df.columns if f'level_at_{minute}' in c.lower()]
    features.extend(level_cols)

    # XP features
    xp_cols = [c for c in df.columns if f'xp_at_{minute}' in c.lower()]
    features.extend(xp_cols)

    # CS features at this minute
    cs_cols = [c for c in df.columns if f'cs_at_{minute}' in c.lower()]
    features.extend(cs_cols)

    # CS@10 from player_stats (always available at minute >= 10)
    if minute >= 10:
        cs10_cols = [c for c in df.columns if 'cs_at_10' in c.lower() or 'lane_minions_10min' in c.lower()]
        features.extend(cs10_cols)

    return list(set(features))


# ============================================================================
# 7. MODEL TRAINING
# ============================================================================

def train_model(X_train, X_test, y_train, y_test, model_name):
    """Train XGBoost and LightGBM, return best model."""
    X_tr, X_val, y_tr, y_val = train_test_split(X_train, y_train, test_size=0.15, random_state=42)
    X_tr = X_tr.fillna(0)
    X_val = X_val.fillna(0)
    X_test_clean = X_test.fillna(0)

    scaler = StandardScaler()
    X_tr_scaled = scaler.fit_transform(X_tr)
    X_val_scaled = scaler.transform(X_val)
    X_test_scaled = scaler.transform(X_test_clean)

    models = {
        'XGBoost': XGBClassifier(
            n_estimators=500, max_depth=6, learning_rate=0.05,
            random_state=42, n_jobs=-1, verbosity=0,
            subsample=0.8, colsample_bytree=0.8,
            min_child_weight=5, reg_alpha=0.1, reg_lambda=1.0,
            eval_metric='logloss',
        ),
        'LightGBM': LGBMClassifier(
            n_estimators=500, max_depth=6, learning_rate=0.05,
            random_state=42, n_jobs=-1, verbose=-1,
            subsample=0.8, colsample_bytree=0.8,
            min_child_samples=20, reg_alpha=0.1, reg_lambda=1.0,
        ),
    }

    best = None
    best_val = 0

    for name, model in models.items():
        start = time.time()
        if name == 'XGBoost':
            model.fit(X_tr_scaled, y_tr, eval_set=[(X_val_scaled, y_val)],
                      verbose=False)
        else:
            model.fit(X_tr_scaled, y_tr, eval_set=[(X_val_scaled, y_val)])

        val_acc = accuracy_score(y_val, model.predict(X_val_scaled))
        test_acc = accuracy_score(y_test, model.predict(X_test_scaled))
        elapsed = time.time() - start
        print(f"    {name:15} Val: {val_acc:.4f}  Test: {test_acc:.4f}  ({elapsed:.1f}s)")

        if val_acc > best_val:
            best_val = val_acc
            best = {
                'model': model,
                'scaler': scaler,
                'model_name': name,
                'val_accuracy': val_acc,
                'test_accuracy': test_acc,
                'feature_columns': list(X_train.columns),
                'metadata': {
                    'model_type': name,
                    'accuracy': test_acc,
                    'val_accuracy': val_acc,
                    'n_features': len(X_train.columns),
                    'n_train_samples': len(X_train),
                    'n_test_samples': len(X_test),
                    'trained_at': pd.Timestamp.now().isoformat(),
                }
            }

    return best


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Train models with external winrate data')
    parser.add_argument('--no-synergy-recompute', action='store_true',
                       help='Use existing synergy data without recomputing')
    args = parser.parse_args()

    print("=" * 70)
    print("ENTRAINEMENT AVEC WINRATES EXTERNES")
    print("Draft + @5min + @10min + @15min + @20min")
    print("=" * 70)

    total_start = time.time()

    # ======================================================================
    # STEP 1: Load base data
    # ======================================================================
    print("\n[1/7] Chargement des donnees de base...")
    train_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'train_with_summoner_stats.parquet')
    test_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'test_with_summoner_stats.parquet')
    timeline_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'matches_with_multi_timeline.parquet')

    if not os.path.exists(train_path):
        print(f"  ERREUR: {train_path} n'existe pas!")
        print("  Lancez d'abord le preprocessing des summoner stats.")
        return

    train_df = pd.read_parquet(train_path)
    test_df = pd.read_parquet(test_path)
    print(f"  Train: {len(train_df):,} matchs, Test: {len(test_df):,} matchs")

    # Merge timeline data
    if os.path.exists(timeline_path):
        print("  Chargement timelines multi-minutes...")
        timeline_df = pd.read_parquet(timeline_path)
        timeline_cols = [c for c in timeline_df.columns
                        if any(f'_at_{m}' in c for m in [5, 10, 15, 20]) and c not in train_df.columns]
        if timeline_cols and 'match_id' in train_df.columns and 'match_id' in timeline_df.columns:
            timeline_subset = timeline_df[['match_id'] + timeline_cols].drop_duplicates(subset='match_id')
            train_df = train_df.merge(timeline_subset, on='match_id', how='left')
            test_df = test_df.merge(timeline_subset, on='match_id', how='left')
            print(f"  Merged {len(timeline_cols)} colonnes timeline")
    else:
        print("  [WARN] Pas de fichier timeline multi-minutes")

    # ======================================================================
    # STEP 2: Load external winrate data
    # ======================================================================
    print("\n[2/7] Chargement des winrates externes...")
    wr_dict = load_simple_winrates()

    print("\n[3/7] Chargement des matchups externes...")
    matchup_dict = load_matchup_data()

    print("\n[4/7] Chargement des synergies externes...")
    ext_synergy_dict = load_external_synergies()

    # ======================================================================
    # STEP 3: Add external features
    # ======================================================================
    print("\n[5/7] Ajout des features externes...")

    print("  Winrates par champion/role...")
    train_df = add_external_winrate_features(train_df, wr_dict)
    test_df = add_external_winrate_features(test_df, wr_dict)

    print("  Matchups par lane...")
    train_df = add_matchup_features(train_df, matchup_dict)
    test_df = add_matchup_features(test_df, matchup_dict)

    print("  Synergies externes...")
    train_df = add_external_synergy_features(train_df, ext_synergy_dict)
    test_df = add_external_synergy_features(test_df, ext_synergy_dict)

    # Computed synergies from our data
    print("  Synergies/counters depuis nos matchs...")
    champ_data = load_or_compute_synergies(train_df, force_recompute=not args.no_synergy_recompute)
    synergy_wr = champ_data['synergy_winrate']
    counter_wr = champ_data['counter_winrate']

    print("  Ajout features synergies/counters (train)...")
    train_df = add_synergy_counter_features(train_df, synergy_wr, counter_wr)
    print("  Ajout features synergies/counters (test)...")
    test_df = add_synergy_counter_features(test_df, synergy_wr, counter_wr)

    # Count new features
    ext_cols = [c for c in train_df.columns if c.startswith('ext_') or c.startswith('matchup_')]
    print(f"\n  Total features externes ajoutees: {len(ext_cols)}")
    for c in sorted(ext_cols):
        non_default = (train_df[c] != 0.5).sum() if 'wr' in c or 'synergy' in c else (train_df[c] != 0).sum()
        print(f"    {c:40} non-default: {non_default:,}/{len(train_df):,}")

    # ======================================================================
    # STEP 4: Define features per timestamp
    # ======================================================================
    print("\n[6/7] Definition des features par timestamp...")

    target = 'team_100_win'
    draft_features = get_draft_features(train_df)
    print(f"  Draft-only: {len(draft_features)} features")

    timestamps = {
        'draft': {'features': draft_features, 'filter_col': None},
    }

    for minute in [5, 10, 15, 20]:
        timeline_feats = get_timeline_features(train_df, minute)
        if timeline_feats:
            all_feats = list(set(draft_features + timeline_feats))
            gold_diff_col = f'gold_diff_at_{minute}'
            if gold_diff_col in train_df.columns:
                n_train = train_df[gold_diff_col].notna().sum()
                n_test = test_df[gold_diff_col].notna().sum() if gold_diff_col in test_df.columns else 0
                print(f"  @{minute}min: {len(all_feats)} features ({n_train:,} train, {n_test:,} test avec timeline)")
                timestamps[f'at{minute}'] = {
                    'features': all_feats,
                    'filter_col': gold_diff_col,
                    'minute': minute,
                }
            else:
                print(f"  @{minute}min: colonnes gold non disponibles")
        else:
            print(f"  @{minute}min: pas de features timeline")

    # ======================================================================
    # STEP 5: Train models
    # ======================================================================
    print("\n[7/7] Entrainement des modeles...")
    results = {}

    for name, config in timestamps.items():
        print(f"\n{'='*60}")
        print(f"MODELE: {name.upper()}")
        print(f"{'='*60}")

        features = config['features']
        filter_col = config['filter_col']

        # Filter to matches with data
        if filter_col:
            train_subset = train_df[train_df[filter_col].notna()].copy()
            if filter_col in test_df.columns:
                test_subset = test_df[test_df[filter_col].notna()].copy()
            else:
                test_subset = pd.DataFrame()
        else:
            train_subset = train_df.copy()
            test_subset = test_df.copy()

        # If test set is empty, split from train
        if len(test_subset) == 0 or (filter_col and filter_col in test_subset.columns
                                      and test_subset[filter_col].notna().sum() == 0):
            print(f"  Test set vide pour {name}, split depuis train")
            train_subset, test_subset = train_test_split(train_subset, test_size=0.2, random_state=42)

        # Only numeric features that exist
        available_features = [f for f in features
                             if f in train_subset.columns
                             and f in train_subset.select_dtypes(include=[np.number]).columns]

        print(f"  Features: {len(available_features)}, Train: {len(train_subset):,}, Test: {len(test_subset):,}")

        X_train = train_subset[available_features]
        X_test = test_subset[available_features]
        y_train = train_subset[target]
        y_test = test_subset[target]

        result = train_model(X_train, X_test, y_train, y_test, name)
        result['synergy_data'] = {'synergy_wr': synergy_wr, 'counter_wr': counter_wr}
        result['external_data'] = {
            'wr_dict': wr_dict,
            'matchup_dict': matchup_dict if name == 'draft' else {},  # Save matchup only for draft
        }
        result['metadata']['minute'] = config.get('minute', 0)
        results[name] = result

        print(f"  Best: {result['model_name']} (Test: {result['test_accuracy']:.4f})")

        # Feature importance
        if hasattr(result['model'], 'feature_importances_'):
            imp = pd.DataFrame({
                'feature': available_features,
                'importance': result['model'].feature_importances_
            }).sort_values('importance', ascending=False)
            print(f"\n  Top 15 features:")
            for _, row in imp.head(15).iterrows():
                print(f"    {row['feature']:45} {row['importance']:.4f}")

    # ======================================================================
    # SAVE
    # ======================================================================
    print(f"\n{'='*60}")
    print("SAUVEGARDE")
    print(f"{'='*60}")

    models_dir = os.path.join(PROJECT_ROOT, 'models')
    os.makedirs(models_dir, exist_ok=True)

    for name, result in results.items():
        path = os.path.join(models_dir, f'model_{name}.pkl')
        with open(path, 'wb') as f:
            pickle.dump(result, f)
        size_mb = os.path.getsize(path) / (1024 * 1024)
        print(f"  {path} ({size_mb:.1f} MB)")

    # ======================================================================
    # SUMMARY
    # ======================================================================
    total_time = time.time() - total_start

    print(f"\n{'='*70}")
    print("RESUME FINAL")
    print(f"{'='*70}")
    print(f"\n{'Modele':<15} {'Algo':<12} {'Features':<10} {'Val':<10} {'Test':<10} {'Train N':<12}")
    print("-" * 70)
    for name, result in results.items():
        print(f"{name:<15} {result['model_name']:<12} {len(result['feature_columns']):<10} "
              f"{result['val_accuracy']:.4f}     {result['test_accuracy']:.4f}     "
              f"{result['metadata']['n_train_samples']:>10,}")
    print("-" * 70)
    print(f"\nTemps total: {total_time:.0f}s ({total_time/60:.1f} min)")

    # Compare with previous models
    print(f"\n{'='*70}")
    print("COMPARAISON AVEC MODELES PRECEDENTS")
    print(f"{'='*70}")
    prev_models = {
        'draft': 0.511,
        'at5': 0.681,
        'at10': 0.737,
        'at15': 0.790,
        'at20': 0.817,
    }
    for name, result in results.items():
        prev = prev_models.get(name, None)
        if prev:
            diff = result['test_accuracy'] - prev
            arrow = "+" if diff > 0 else ""
            print(f"  {name:<15} Ancien: {prev:.3f}  Nouveau: {result['test_accuracy']:.4f}  ({arrow}{diff:.4f})")
        else:
            print(f"  {name:<15} Nouveau: {result['test_accuracy']:.4f}")

    print(f"\n{'='*70}")
    print("DONE!")
    print(f"{'='*70}")


if __name__ == '__main__':
    main()
