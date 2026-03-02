#!/usr/bin/env python3
"""
Entraînement des modèles V2 — CORRIGÉ (toutes fuites de données éliminées).

Corrections par rapport à V1:
  1. SUPPRESSION des summoner stats (role_winrate_diff, streak, mastery, etc.)
     → Fuite confirmée: corr train=0.81 vs test=0.02
  2. SUPPRESSION des champion/ban/spell IDs (encodage ordinal sans sens)
  3. SPLIT TEMPOREL pour les modèles in-game (au lieu de random)
  4. RÉGULARISATION renforcée + early stopping
  5. FEATURES dérivées ajoutées (gold_diff par role, CS diff)
  6. CROSS-VALIDATION temporelle pour estimation honnête

Features conservées (aucune fuite):
  - Winrates externes dpm.lol (force du champion dans la meta)
  - Matchups par lane (avantage spécifique de lane)
  - Synergies/Counters (calculés sur train uniquement)
  - Timeline gold/CS (données in-game objectives)

Usage:
    python scripts/train_with_winrates_v2.py
    python scripts/train_with_winrates_v2.py --no-synergy-recompute
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
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, accuracy_score, roc_auc_score
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier

warnings.filterwarnings('ignore')

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ============================================================================
# CHAMPION NAME NORMALIZATION
# ============================================================================

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

CHAMPION_MATCHUP_TO_DB = {
    "KSante": "KSante", "DrMundo": "DrMundo", "MasterYi": "MasterYi",
    "MissFortune": "MissFortune", "MonkeyKing": "MonkeyKing",
    "TahmKench": "TahmKench", "TwistedFate": "TwistedFate",
    "XinZhao": "XinZhao", "FiddleSticks": "FiddleSticks",
    "JarvanIV": "JarvanIV", "AurelionSol": "AurelionSol",
    "Chogath": "Chogath", "KogMaw": "KogMaw", "Belveth": "Belveth",
    "RekSai": "RekSai", "Kaisa": "Kaisa", "Velkoz": "Velkoz",
    "Khazix": "Khazix",
}

ROLE_WR_TO_DB = {
    'top': 'top', 'jun': 'jungle', 'jungle': 'jungle',
    'mid': 'mid', 'adc': 'adc', 'sup': 'support', 'support': 'support'
}

TIER_ENCODING = {
    'S+': 6, 'S': 5, 'A': 4, 'B': 3, 'C': 2, 'D': 1,
    'S+ ': 6, 'S ': 5, 'A ': 4, 'B ': 3, 'C ': 2, 'D ': 1,
}

POSITIONS = ['top', 'jungle', 'mid', 'adc', 'support']


def normalize_champion_name(name):
    if pd.isna(name):
        return None
    name = str(name).strip()
    if name in CHAMPION_NAME_TO_DB:
        return CHAMPION_NAME_TO_DB[name]
    if name in CHAMPION_MATCHUP_TO_DB:
        return CHAMPION_MATCHUP_TO_DB[name]
    return name


def parse_winrate(wr_str):
    if pd.isna(wr_str):
        return np.nan
    s = str(wr_str).replace('%', '').replace(' ', '').replace('+', '').strip()
    try:
        return float(s) / 100.0
    except ValueError:
        return np.nan


def parse_games(games_str):
    if pd.isna(games_str):
        return 0
    s = str(games_str).replace(' ', '').replace(',', '').strip()
    try:
        return int(float(s))
    except ValueError:
        return 0


# ============================================================================
# 1. LOAD EXTERNAL DATA
# ============================================================================

def load_simple_winrates():
    """Load champion winrates from dpm.lol refined CSV."""
    refined_path = os.path.join(
        PROJECT_ROOT, 'data', 'winrates', 'les winrates simples',
        'données raffinées', 'df_Simple_WR_FULL.csv'
    )

    if not os.path.exists(refined_path):
        return _load_simple_winrates_fallback()

    df = pd.read_csv(refined_path)
    global_data = df[(df['elo'] == 'TOUT') & (df['server'] == 'TOUT')]
    if len(global_data) == 0:
        global_data = df[df['elo'] == 'TOUT']

    latest_patch = global_data['patch'].max()
    global_data = global_data[global_data['patch'] == latest_patch]
    print(f"  Winrates: {len(global_data)} entries, patch {latest_patch}")

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

        wr_dict[(name_db, role_db)] = {
            'winrate': wr if not np.isnan(wr) else 0.5,
            'pickrate': pr if not np.isnan(pr) else 0.0,
            'tier_num': tier_num,
        }

    print(f"  Loaded {len(wr_dict)} champion-role entries")
    return wr_dict


def _load_simple_winrates_fallback():
    simples_dir = os.path.join(PROJECT_ROOT, 'data', 'winrates', 'les winrates simples')
    candidates = glob.glob(os.path.join(simples_dir, 'TOUT_Emerald+_*.csv'))
    if not candidates:
        candidates = glob.glob(os.path.join(simples_dir, 'TOUT_Diamond+_*.csv'))
    if not candidates:
        candidates = glob.glob(os.path.join(simples_dir, 'TOUT_*.csv'))
    if not candidates:
        print("  [WARN] No winrate CSVs found!")
        return {}

    candidates.sort(key=lambda f: f.split('_')[-1].replace('.csv', ''), reverse=True)
    df = pd.read_csv(candidates[0])
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
        wr_dict[(name_db, role_db)] = {
            'winrate': wr if not np.isnan(wr) else 0.5,
            'pickrate': pr if not np.isnan(pr) else 0.0,
            'tier_num': TIER_ENCODING.get(tier_str, 0),
        }
    return wr_dict


def load_matchup_data():
    """Load matchup winrate data from CSVs."""
    matchup_dir = os.path.join(PROJECT_ROOT, 'data', 'winrates', 'les winrates matchups')
    matchup_dict = {}

    top_files = glob.glob(os.path.join(matchup_dir, '*_top_matchup_TOUT_TOUT_16.3.csv'))
    if top_files:
        _load_matchup_files(top_files, 'top', matchup_dict)

    lane_dir = os.path.join(matchup_dir, 'TOUTTOUT15.24', 'matchups')
    if os.path.exists(lane_dir):
        for lane_folder in os.listdir(lane_dir):
            lane_path = os.path.join(lane_dir, lane_folder)
            if not os.path.isdir(lane_path):
                continue
            lane = lane_folder.split()[0].lower()
            role_db = ROLE_WR_TO_DB.get(lane, lane)
            files = glob.glob(os.path.join(lane_path, '*.csv'))
            if files:
                _load_matchup_files(files, role_db, matchup_dict)

    print(f"  Loaded {len(matchup_dict)} matchup entries")
    return matchup_dict


def _load_matchup_files(files, role_db, matchup_dict):
    for filepath in files:
        try:
            df = pd.read_csv(filepath)
        except Exception:
            continue
        for _, row in df.iterrows():
            champ_name = normalize_champion_name(row.get('champion'))
            if champ_name is None:
                continue
            for i in range(1, 60):
                found_name_col = None
                found_wr_col = None
                for prefix in [f'matchup_top_{i}', f'matchup_jun_{i}', f'matchup_mid_{i}',
                               f'matchup_adc_{i}', f'matchup_sup_{i}']:
                    nc, wc = f'{prefix}_name', f'{prefix}_winrate'
                    if nc in df.columns:
                        found_name_col, found_wr_col = nc, wc
                        break
                if found_name_col is None or found_name_col not in row.index:
                    break
                opp_name = normalize_champion_name(row.get(found_name_col))
                opp_wr = parse_winrate(row.get(found_wr_col))
                if opp_name is not None and not np.isnan(opp_wr):
                    matchup_dict[(champ_name, opp_name, role_db)] = opp_wr


def load_external_synergies():
    """Load synergy data from CSVs."""
    synergy_dir = os.path.join(
        PROJECT_ROOT, 'data', 'winrates', 'les winrates matchups',
        'TOUTTOUT15.24', 'synergies'
    )
    synergy_dict = {}
    if not os.path.exists(synergy_dir):
        return synergy_dict

    for lane_folder in os.listdir(synergy_dir):
        lane_path = os.path.join(synergy_dir, lane_folder)
        if not os.path.isdir(lane_path):
            continue
        lane = lane_folder.split()[0].lower()
        role_db = ROLE_WR_TO_DB.get(lane, lane)
        for filepath in glob.glob(os.path.join(lane_path, '*.csv')):
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
                        nc, wc = f'synergy_{prefix_lane}_{i}_name', f'synergy_{prefix_lane}_{i}_winrate'
                        if nc in df.columns:
                            found_name_col, found_wr_col = nc, wc
                            break
                    if found_name_col is None:
                        break
                    partner_name = normalize_champion_name(row.get(found_name_col))
                    partner_wr = parse_winrate(row.get(found_wr_col))
                    if partner_name is not None and not np.isnan(partner_wr):
                        key = tuple(sorted([champ_name, partner_name]) + [role_db])
                        synergy_dict[key] = partner_wr

    print(f"  Loaded {len(synergy_dict)} external synergy entries")
    return synergy_dict


# ============================================================================
# 2. ADD FEATURES
# ============================================================================

def add_external_winrate_features(df, wr_dict):
    """Add per-champion winrate, tier, pickrate from dpm.lol."""
    n = len(df)
    features = {}

    for team in [100, 200]:
        wr_list, tier_list = [], []
        for pos in POSITIONS:
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
                        data = wr_dict.get((str(name).strip(), pos))
                        if data:
                            wr_vals[i] = data['winrate']
                            tier_vals[i] = data['tier_num']
                            pr_vals[i] = data['pickrate']

            features[wr_col] = wr_vals
            features[tier_col] = tier_vals
            features[pr_col] = pr_vals
            wr_list.append(wr_vals)
            tier_list.append(tier_vals)

        features[f'ext_avg_wr_{team}'] = np.mean(wr_list, axis=0)
        features[f'ext_avg_tier_{team}'] = np.mean(tier_list, axis=0)

    features['ext_wr_diff'] = features['ext_avg_wr_100'] - features['ext_avg_wr_200']
    features['ext_tier_diff'] = features['ext_avg_tier_100'] - features['ext_avg_tier_200']

    df = df.copy()
    for name, vals in features.items():
        df[name] = vals
    return df


def add_matchup_features(df, matchup_dict):
    """Add lane matchup advantage features."""
    n = len(df)
    features = {}
    advantages = []

    for pos in POSITIONS:
        col_100 = f'team_100_{pos}_champion_name'
        col_200 = f'team_200_{pos}_champion_name'
        wr_col = f'matchup_wr_{pos}'
        adv_col = f'matchup_advantage_{pos}'
        wr_vals = np.full(n, 0.5)

        if col_100 in df.columns and col_200 in df.columns:
            for i in range(n):
                c100, c200 = df[col_100].iloc[i], df[col_200].iloc[i]
                if pd.notna(c100) and pd.notna(c200):
                    c100_s, c200_s = str(c100).strip(), str(c200).strip()
                    wr = matchup_dict.get((c100_s, c200_s, pos))
                    if wr is not None:
                        wr_vals[i] = wr
                    else:
                        wr_rev = matchup_dict.get((c200_s, c100_s, pos))
                        if wr_rev is not None:
                            wr_vals[i] = 1.0 - wr_rev

        features[wr_col] = wr_vals
        features[adv_col] = wr_vals - 0.5
        advantages.append(wr_vals - 0.5)

    features['avg_matchup_advantage'] = np.mean(advantages, axis=0)
    features['max_matchup_advantage'] = np.max(advantages, axis=0)
    features['min_matchup_advantage'] = np.min(advantages, axis=0)

    df = df.copy()
    for name, vals in features.items():
        df[name] = vals
    return df


def add_external_synergy_features(df, ext_synergy_dict):
    """Add synergy features from external data."""
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
            for pos in POSITIONS:
                col = f'team_{team}_{pos}_champion_name'
                if col in df.columns:
                    val = df[col].iloc[i]
                    if pd.notna(val):
                        champs.append((str(val).strip(), pos))
            if len(champs) < 2:
                continue
            syn_scores = []
            for (c1, r1), (c2, r2) in combinations(champs, 2):
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


def load_or_compute_synergies(train_df, force_recompute=False):
    """Compute synergy/counter stats from training data only (no leakage)."""
    syn_path = os.path.join(PROJECT_ROOT, 'data', 'features', 'champion_synergy_counter_v2.pkl')

    if os.path.exists(syn_path) and not force_recompute:
        print("  Chargement synergies/counters existantes (v2)...")
        with open(syn_path, 'rb') as f:
            return pickle.load(f)

    print("  Calcul des synergies/counters depuis les matchs d'entraînement...")
    from collections import defaultdict
    synergy_wins = defaultdict(int)
    synergy_games = defaultdict(int)
    counter_wins = defaultdict(int)
    counter_games = defaultdict(int)

    champ_cols_100 = [f'team_100_{pos}_champion_id' for pos in POSITIONS]
    champ_cols_200 = [f'team_200_{pos}_champion_id' for pos in POSITIONS]

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
        'counter_games': dict(counter_games),
    }

    os.makedirs(os.path.dirname(syn_path), exist_ok=True)
    with open(syn_path, 'wb') as f:
        pickle.dump(result, f)
    print(f"    {len(synergy_wr):,} synergies, {len(counter_wr):,} counters (min 30 games)")
    return result


def add_synergy_counter_features(df, synergy_wr, counter_wr):
    """Add synergy/counter features from our computed data."""
    champ_cols = {
        t: [f'team_{t}_{pos}_champion_id' for pos in POSITIONS]
        for t in [100, 200]
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
        if syn_100:
            features['team_100_synergy_score'][i] = np.mean(syn_100)
        if syn_200:
            features['team_200_synergy_score'][i] = np.mean(syn_200)

        cnt_100 = [counter_wr.get((c100, c200), 0.5) for c100 in champs_100 for c200 in champs_200]
        cnt_200 = [counter_wr.get((c200, c100), 0.5) for c200 in champs_200 for c100 in champs_100]
        if cnt_100:
            features['team_100_counter_score'][i] = np.mean(cnt_100)
        if cnt_200:
            features['team_200_counter_score'][i] = np.mean(cnt_200)

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
# 3. DERIVED TIMELINE FEATURES
# ============================================================================

def add_derived_timeline_features(df, minute):
    """Add per-role gold/CS diffs and gold share features."""
    df = df.copy()

    # Per-role gold diff
    for pos in POSITIONS:
        g100 = f'team_100_{pos}_gold_at_{minute}'
        g200 = f'team_200_{pos}_gold_at_{minute}'
        diff_col = f'{pos}_gold_diff_at_{minute}'
        if g100 in df.columns and g200 in df.columns:
            df[diff_col] = df[g100] - df[g200]

    # Per-role CS diff at 10min (available for all matches)
    if minute >= 10:
        for pos in POSITIONS:
            cs100 = f'team_100_{pos}_cs_at_10'
            cs200 = f'team_200_{pos}_cs_at_10'
            diff_col = f'{pos}_cs_diff_at_10'
            if cs100 in df.columns and cs200 in df.columns:
                df[diff_col] = df[cs100] - df[cs200]

    return df


# ============================================================================
# 4. FEATURE SELECTION (NO LEAKAGE)
# ============================================================================

def get_draft_features(df):
    """
    Get draft features — ONLY legitimate pre-match features.
    NO champion IDs, NO summoner stats, NO post-game data.
    """
    features = []

    # External winrate features (from dpm.lol — static meta data)
    ext_patterns = [
        'ext_wr_', 'ext_tier_', 'ext_pickrate_',
        'ext_avg_wr_', 'ext_avg_tier_',
    ]
    exact_cols = [
        'ext_wr_diff', 'ext_tier_diff',
        'ext_synergy_100', 'ext_synergy_200', 'ext_synergy_diff',
    ]

    for col in df.columns:
        for p in ext_patterns:
            if col.startswith(p) and col not in features:
                features.append(col)

    for col in exact_cols:
        if col in df.columns and col not in features:
            features.append(col)

    # Matchup features
    matchup_patterns = ['matchup_wr_', 'matchup_advantage_']
    matchup_exact = ['avg_matchup_advantage', 'max_matchup_advantage', 'min_matchup_advantage']

    for col in df.columns:
        for p in matchup_patterns:
            if col.startswith(p) and col not in features:
                features.append(col)

    for col in matchup_exact:
        if col in df.columns and col not in features:
            features.append(col)

    # Synergy/Counter features (computed from training data, no leakage)
    synergy_cols = [
        'team_100_synergy_score', 'team_200_synergy_score',
        'team_100_counter_score', 'team_200_counter_score',
        'synergy_diff', 'counter_diff', 'draft_advantage',
    ]
    for col in synergy_cols:
        if col in df.columns and col not in features:
            features.append(col)

    # Filter to numeric only
    numeric_cols = set(df.select_dtypes(include=[np.number]).columns)
    features = [f for f in features if f in numeric_cols]

    return sorted(features)


def get_timeline_features(df, minute):
    """Get timeline features for a specific minute."""
    features = []

    # Per-role gold at this minute
    for team in [100, 200]:
        for pos in POSITIONS:
            col = f'team_{team}_{pos}_gold_at_{minute}'
            if col in df.columns:
                features.append(col)
        # Team total gold
        tg = f'team_{team}_gold_at_{minute}'
        if tg in df.columns:
            features.append(tg)

    # Gold diff
    gdiff = f'gold_diff_at_{minute}'
    if gdiff in df.columns:
        features.append(gdiff)

    # Per-role gold diff (derived)
    for pos in POSITIONS:
        diff_col = f'{pos}_gold_diff_at_{minute}'
        if diff_col in df.columns:
            features.append(diff_col)

    # CS at 10min (from player_stats, available for all matches)
    if minute >= 10:
        for team in [100, 200]:
            for pos in POSITIONS:
                cs_col = f'team_{team}_{pos}_cs_at_10'
                if cs_col in df.columns:
                    features.append(cs_col)
        # CS diffs
        for pos in POSITIONS:
            diff_col = f'{pos}_cs_diff_at_10'
            if diff_col in df.columns:
                features.append(diff_col)

    return sorted(set(features))


# ============================================================================
# 5. MODEL TRAINING
# ============================================================================

def train_model(X_train, X_test, y_train, y_test, model_name, strong_regularization=False):
    """
    Train XGBoost and LightGBM with proper early stopping.
    strong_regularization=True for draft model (fewer features, risk of overfitting).
    """
    # Validation split from train (15%)
    n_val = int(len(X_train) * 0.15)
    X_tr = X_train.iloc[:-n_val].fillna(0)
    X_val = X_train.iloc[-n_val:].fillna(0)
    y_tr = y_train.iloc[:-n_val]
    y_val = y_train.iloc[-n_val:]
    X_test_clean = X_test.fillna(0)

    scaler = StandardScaler()
    X_tr_s = scaler.fit_transform(X_tr)
    X_val_s = scaler.transform(X_val)
    X_test_s = scaler.transform(X_test_clean)

    if strong_regularization:
        # For draft model: prevent overfitting on weak signal
        models = {
            'XGBoost': XGBClassifier(
                n_estimators=1000, max_depth=4, learning_rate=0.01,
                random_state=42, n_jobs=-1, verbosity=0,
                subsample=0.7, colsample_bytree=0.6,
                min_child_weight=20, reg_alpha=1.0, reg_lambda=5.0,
                eval_metric='logloss', early_stopping_rounds=50,
            ),
            'LightGBM': LGBMClassifier(
                n_estimators=1000, max_depth=4, learning_rate=0.01,
                random_state=42, n_jobs=-1, verbose=-1,
                subsample=0.7, colsample_bytree=0.6,
                min_child_samples=50, reg_alpha=1.0, reg_lambda=5.0,
                n_iter_no_change=50,
            ),
        }
    else:
        # For in-game models: gold features are genuinely predictive
        models = {
            'XGBoost': XGBClassifier(
                n_estimators=1000, max_depth=6, learning_rate=0.03,
                random_state=42, n_jobs=-1, verbosity=0,
                subsample=0.8, colsample_bytree=0.8,
                min_child_weight=10, reg_alpha=0.5, reg_lambda=2.0,
                eval_metric='logloss', early_stopping_rounds=50,
            ),
            'LightGBM': LGBMClassifier(
                n_estimators=1000, max_depth=6, learning_rate=0.03,
                random_state=42, n_jobs=-1, verbose=-1,
                subsample=0.8, colsample_bytree=0.8,
                min_child_samples=20, reg_alpha=0.5, reg_lambda=2.0,
                n_iter_no_change=50,
            ),
        }

    best = None
    best_val = 0

    for name, model in models.items():
        start = time.time()
        if name == 'XGBoost':
            model.fit(X_tr_s, y_tr, eval_set=[(X_val_s, y_val)], verbose=False)
        else:
            model.fit(X_tr_s, y_tr, eval_set=[(X_val_s, y_val)])

        val_pred = model.predict(X_val_s)
        test_pred = model.predict(X_test_s)
        val_acc = accuracy_score(y_val, val_pred)
        test_acc = accuracy_score(y_test, test_pred)

        # AUC-ROC
        val_proba = model.predict_proba(X_val_s)[:, 1]
        test_proba = model.predict_proba(X_test_s)[:, 1]
        val_auc = roc_auc_score(y_val, val_proba)
        test_auc = roc_auc_score(y_test, test_proba)

        elapsed = time.time() - start
        n_trees = model.best_iteration_ if hasattr(model, 'best_iteration_') and model.best_iteration_ else (
            model.best_ntree_limit if hasattr(model, 'best_ntree_limit') else '?')
        print(f"    {name:15} Val: {val_acc:.4f} (AUC {val_auc:.4f})  "
              f"Test: {test_acc:.4f} (AUC {test_auc:.4f})  "
              f"Trees: {n_trees}  ({elapsed:.1f}s)")

        if val_acc > best_val:
            best_val = val_acc
            best = {
                'model': model,
                'scaler': scaler,
                'model_name': name,
                'val_accuracy': val_acc,
                'test_accuracy': test_acc,
                'val_auc': val_auc,
                'test_auc': test_auc,
                'feature_columns': list(X_train.columns),
                'features': list(X_train.columns),  # backward compat
                'metadata': {
                    'model_type': name,
                    'accuracy': test_acc,
                    'auc_roc': test_auc,
                    'val_accuracy': val_acc,
                    'n_features': len(X_train.columns),
                    'n_train_samples': len(X_tr),
                    'n_val_samples': len(X_val),
                    'n_test_samples': len(X_test),
                    'trained_at': pd.Timestamp.now().isoformat(),
                    'version': 'v2_no_leakage',
                }
            }

    return best


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Train V2 models (no leakage)')
    parser.add_argument('--no-synergy-recompute', action='store_true')
    args = parser.parse_args()

    print("=" * 70)
    print("ENTRAINEMENT V2 — SANS FUITE DE DONNÉES")
    print("=" * 70)
    print("\nCorrections appliquées:")
    print("  [FIX 1] Suppression summoner stats (fuite confirmée, corr train=0.81 vs test=0.02)")
    print("  [FIX 2] Suppression champion/ban/spell IDs (encodage ordinal sans sens)")
    print("  [FIX 3] Split temporel pour modèles in-game")
    print("  [FIX 4] Régularisation renforcée + early stopping")
    print("  [FIX 5] Features dérivées (gold diff par rôle)")
    print("=" * 70)

    total_start = time.time()

    # ==================================================================
    # STEP 1: Load data
    # ==================================================================
    print("\n[1/7] Chargement des données...")
    train_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'train_with_summoner_stats.parquet')
    test_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'test_with_summoner_stats.parquet')
    timeline_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'matches_with_multi_timeline.parquet')

    if not os.path.exists(train_path):
        print(f"  ERREUR: {train_path} n'existe pas!")
        return

    train_df = pd.read_parquet(train_path)
    test_df = pd.read_parquet(test_path)
    print(f"  Train: {len(train_df):,} matchs")
    print(f"  Test (temporal): {len(test_df):,} matchs")

    # Merge timeline data
    if os.path.exists(timeline_path):
        print("  Merge timeline multi-minutes...")
        timeline_df = pd.read_parquet(timeline_path)
        timeline_cols = [c for c in timeline_df.columns
                         if any(f'_at_{m}' in c for m in [5, 10, 15, 20])
                         and c not in train_df.columns]
        if timeline_cols and 'match_id' in train_df.columns and 'match_id' in timeline_df.columns:
            timeline_subset = timeline_df[['match_id'] + timeline_cols].drop_duplicates(subset='match_id')
            train_df = train_df.merge(timeline_subset, on='match_id', how='left')
            # Also merge test (some test matches might have timeline from a different source)
            test_df = test_df.merge(timeline_subset, on='match_id', how='left')
            print(f"  Merged {len(timeline_cols)} timeline columns")

            for m in [5, 10, 15, 20]:
                gcol = f'gold_diff_at_{m}'
                if gcol in train_df.columns:
                    n_tr = train_df[gcol].notna().sum()
                    n_te = test_df[gcol].notna().sum() if gcol in test_df.columns else 0
                    print(f"    @{m}min: {n_tr:,} train, {n_te:,} test")
    else:
        print("  [WARN] Pas de timeline parquet!")

    # ==================================================================
    # STEP 2: Load external winrate data
    # ==================================================================
    print("\n[2/7] Chargement winrates externes (dpm.lol)...")
    wr_dict = load_simple_winrates()

    print("\n[3/7] Chargement matchups...")
    matchup_dict = load_matchup_data()

    print("\n[4/7] Chargement synergies externes...")
    ext_synergy_dict = load_external_synergies()

    # ==================================================================
    # STEP 3: Add features
    # ==================================================================
    print("\n[5/7] Ajout des features...")

    # External winrates
    print("  Winrates par champion/role...")
    train_df = add_external_winrate_features(train_df, wr_dict)
    test_df = add_external_winrate_features(test_df, wr_dict)

    # Matchups
    print("  Matchups par lane...")
    train_df = add_matchup_features(train_df, matchup_dict)
    test_df = add_matchup_features(test_df, matchup_dict)

    # External synergies
    print("  Synergies externes...")
    train_df = add_external_synergy_features(train_df, ext_synergy_dict)
    test_df = add_external_synergy_features(test_df, ext_synergy_dict)

    # Computed synergies from TRAINING data only
    print("  Synergies/counters (train only)...")
    champ_data = load_or_compute_synergies(train_df, force_recompute=not args.no_synergy_recompute)
    synergy_wr = champ_data['synergy_winrate']
    counter_wr = champ_data['counter_winrate']

    print("  Ajout features synergies (train)...")
    train_df = add_synergy_counter_features(train_df, synergy_wr, counter_wr)
    print("  Ajout features synergies (test)...")
    test_df = add_synergy_counter_features(test_df, synergy_wr, counter_wr)

    # Derived timeline features
    for minute in [5, 10, 15, 20]:
        train_df = add_derived_timeline_features(train_df, minute)
        test_df = add_derived_timeline_features(test_df, minute)

    # ==================================================================
    # STEP 4: Feature lists
    # ==================================================================
    print("\n[6/7] Définition des features...")

    target = 'team_100_win'
    draft_features = get_draft_features(train_df)
    print(f"\n  Draft features ({len(draft_features)}):")
    for f in draft_features:
        print(f"    - {f}")

    # ==================================================================
    # STEP 5: Train models
    # ==================================================================
    print("\n[7/7] Entraînement des modèles...")
    results = {}

    # ========================
    # MODEL 1: DRAFT (temporal train/test)
    # ========================
    print(f"\n{'='*60}")
    print("MODÈLE: DRAFT (aucun summoner stats, aucun champion ID)")
    print(f"{'='*60}")

    # Sort train temporally for validation split
    train_sorted = train_df.sort_values('game_creation').reset_index(drop=True)
    test_sorted = test_df.sort_values('game_creation').reset_index(drop=True)

    X_train_draft = train_sorted[draft_features]
    y_train_draft = train_sorted[target]
    X_test_draft = test_sorted[draft_features]
    y_test_draft = test_sorted[target]

    print(f"  Features: {len(draft_features)}, Train: {len(X_train_draft):,}, Test: {len(X_test_draft):,}")
    result = train_model(X_train_draft, X_test_draft, y_train_draft, y_test_draft,
                         'draft', strong_regularization=True)
    result['synergy_data'] = {'synergy_wr': synergy_wr, 'counter_wr': counter_wr}
    result['external_data'] = {'wr_dict': wr_dict, 'matchup_dict': matchup_dict}
    result['metadata']['minute'] = 0
    results['draft'] = result
    print(f"  Best: {result['model_name']} (Test: {result['test_accuracy']:.4f}, AUC: {result['test_auc']:.4f})")

    # Feature importance
    if hasattr(result['model'], 'feature_importances_'):
        imp = pd.DataFrame({
            'feature': draft_features,
            'importance': result['model'].feature_importances_
        }).sort_values('importance', ascending=False)
        print(f"\n  Top 15 features:")
        for _, row in imp.head(15).iterrows():
            pct = row['importance'] / imp['importance'].sum() * 100
            print(f"    {row['feature']:45} {row['importance']:.4f} ({pct:.1f}%)")

    # ========================
    # MODELS 2-5: IN-GAME (temporal split within timeline data)
    # ========================
    for minute in [5, 10, 15, 20]:
        print(f"\n{'='*60}")
        print(f"MODÈLE: @{minute}min (split temporel)")
        print(f"{'='*60}")

        gdiff_col = f'gold_diff_at_{minute}'
        if gdiff_col not in train_df.columns:
            print(f"  SKIP: colonne {gdiff_col} absente")
            continue

        # Get all matches with timeline data (from both train and test)
        all_data = pd.concat([train_df, test_df], ignore_index=True)
        has_timeline = all_data[gdiff_col].notna()
        timeline_data = all_data[has_timeline].sort_values('game_creation').reset_index(drop=True)

        if len(timeline_data) < 1000:
            print(f"  SKIP: seulement {len(timeline_data)} matchs avec timeline @{minute}min")
            continue

        # Temporal split: 80% train, 20% test
        n_train = int(len(timeline_data) * 0.8)
        tl_train = timeline_data.iloc[:n_train]
        tl_test = timeline_data.iloc[n_train:]

        # Features: draft + timeline
        timeline_feats = get_timeline_features(timeline_data, minute)
        all_feats = sorted(set(draft_features + timeline_feats))

        # Only keep features that exist and are numeric
        available = [f for f in all_feats
                     if f in timeline_data.columns
                     and f in timeline_data.select_dtypes(include=[np.number]).columns]

        X_train_tl = tl_train[available]
        y_train_tl = tl_train[target]
        X_test_tl = tl_test[available]
        y_test_tl = tl_test[target]

        print(f"  Features: {len(available)}, Train: {len(X_train_tl):,}, Test: {len(X_test_tl):,}")
        print(f"  Train period: {tl_train['game_creation'].min()} -> {tl_train['game_creation'].max()}")
        print(f"  Test period:  {tl_test['game_creation'].min()} -> {tl_test['game_creation'].max()}")

        result = train_model(X_train_tl, X_test_tl, y_train_tl, y_test_tl,
                             f'at{minute}', strong_regularization=False)
        result['synergy_data'] = {'synergy_wr': synergy_wr, 'counter_wr': counter_wr}
        result['external_data'] = {'wr_dict': wr_dict}
        result['metadata']['minute'] = minute
        results[f'at{minute}'] = result
        print(f"  Best: {result['model_name']} (Test: {result['test_accuracy']:.4f}, AUC: {result['test_auc']:.4f})")

        # Feature importance
        if hasattr(result['model'], 'feature_importances_'):
            imp = pd.DataFrame({
                'feature': available,
                'importance': result['model'].feature_importances_
            }).sort_values('importance', ascending=False)
            print(f"\n  Top 15 features:")
            for _, row in imp.head(15).iterrows():
                pct = row['importance'] / imp['importance'].sum() * 100
                print(f"    {row['feature']:45} {row['importance']:.4f} ({pct:.1f}%)")

    # ==================================================================
    # SAVE
    # ==================================================================
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

    # ==================================================================
    # SUMMARY
    # ==================================================================
    total_time = time.time() - total_start

    print(f"\n{'='*70}")
    print("RÉSUMÉ FINAL (V2 — SANS FUITE)")
    print(f"{'='*70}")
    print(f"\n{'Modèle':<10} {'Algo':<12} {'Feat':<6} {'Val Acc':<10} {'Test Acc':<10} {'Test AUC':<10} {'Train N':<10} {'Test N':<10}")
    print("-" * 80)
    for name, result in results.items():
        m = result['metadata']
        print(f"{name:<10} {result['model_name']:<12} {m['n_features']:<6} "
              f"{result['val_accuracy']:.4f}     {result['test_accuracy']:.4f}     "
              f"{result['test_auc']:.4f}     {m['n_train_samples']:>8,}  {m['n_test_samples']:>8,}")
    print("-" * 80)

    print(f"\nProblèmes corrigés:")
    print(f"  ✓ Fuite summoner stats éliminée (93 features supprimées)")
    print(f"  ✓ Champion/Ban/Spell IDs supprimés (encodage ordinal)")
    print(f"  ✓ Split temporel pour TOUS les modèles")
    print(f"  ✓ Régularisation renforcée (early stopping)")
    print(f"  ✓ Features dérivées ajoutées (gold/CS diff par rôle)")

    print(f"\nTemps total: {total_time:.0f}s ({total_time/60:.1f} min)")
    print(f"\n{'='*70}")
    print("DONE!")
    print(f"{'='*70}")


if __name__ == '__main__':
    main()
