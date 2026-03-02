#!/usr/bin/env python3
"""
Entraînement des modèles multi-timestamps.
Entraîne 5 modèles: Draft-only, @5min, @10min, @15min, @20min

Chaque modèle utilise les features disponibles à ce moment:
- Draft-only: Champion IDs, Bans, Summoner Spells, Summoner Stats, Synergies/Counters
- @5min:  Draft + gold par position @5min
- @10min: Draft + gold par position @10min + CS@10
- @15min: Draft + gold par position @15min
- @20min: Draft + gold par position @20min

Usage:
    python scripts/train_multi_timestamp.py
    python scripts/train_multi_timestamp.py --skip-preprocessing  # Si données déjà prêtes
"""

import sys
import os
import argparse
import pandas as pd
import numpy as np
import pickle
import time
import warnings
from itertools import combinations
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from sklearn.metrics import classification_report, accuracy_score

warnings.filterwarnings('ignore')

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src', 'collect_data'))


def load_or_compute_synergies(train_df, force_recompute=False):
    """Load or compute champion synergy/counter data."""
    syn_path = os.path.join(PROJECT_ROOT, 'data', 'features', 'champion_synergy_counter.pkl')

    if os.path.exists(syn_path) and not force_recompute:
        print("  Chargement synergies/counters existantes...")
        with open(syn_path, 'rb') as f:
            return pickle.load(f)

    print("  Calcul des synergies/counters...")
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

        # Synergies
        for team_champs, is_winner in [(champs_100, team_100_win == 1), (champs_200, team_100_win == 0)]:
            for c1, c2 in combinations(team_champs, 2):
                pair = tuple(sorted([c1, c2]))
                synergy_games[pair] += 1
                if is_winner:
                    synergy_wins[pair] += 1

        # Counters
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
    """Add synergy/counter features to DataFrame."""
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


def get_draft_features(df):
    """Get draft-only feature names."""
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

    # Synergy/Counter
    for col in ['team_100_synergy_score', 'team_200_synergy_score',
                'team_100_counter_score', 'team_200_counter_score',
                'synergy_diff', 'counter_diff', 'draft_advantage']:
        if col in df.columns:
            features.append(col)

    # Filter to numeric only, exclude target
    numeric_cols = set(df.select_dtypes(include=[np.number]).columns)
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

    # CS features (only available at 10)
    if minute >= 10:
        cs_cols = [c for c in df.columns if 'cs_at_10' in c.lower()]
        features.extend(cs_cols)

    return list(set(features))


def train_model(X_train, X_test, y_train, y_test, model_name):
    """Train and evaluate XGBoost and LightGBM."""
    X_tr, X_val, y_tr, y_val = train_test_split(X_train, y_train, test_size=0.15, random_state=42)
    X_tr, X_val, X_test_clean = X_tr.fillna(0), X_val.fillna(0), X_test.fillna(0)

    scaler = StandardScaler()

    models = {
        'XGBoost': XGBClassifier(n_estimators=300, max_depth=6, learning_rate=0.1,
                                  random_state=42, n_jobs=-1, verbosity=0,
                                  subsample=0.8, colsample_bytree=0.8),
        'LightGBM': LGBMClassifier(n_estimators=300, max_depth=6, learning_rate=0.1,
                                    random_state=42, n_jobs=-1, verbose=-1,
                                    subsample=0.8, colsample_bytree=0.8),
    }

    best = None
    best_val = 0

    for name, model in models.items():
        start = time.time()
        model.fit(X_tr, y_tr)
        val = model.score(X_val, y_val)
        test = model.score(X_test_clean, y_test)
        elapsed = time.time() - start
        print(f"    {name:15} Val: {val:.4f}  Test: {test:.4f}  ({elapsed:.1f}s)")
        if val > best_val:
            best_val = val
            best = {
                'model': model, 'scaler': scaler, 'model_name': name,
                'val_accuracy': val, 'test_accuracy': test,
                'features': list(X_train.columns)
            }

    return best


def main():
    parser = argparse.ArgumentParser(description='Train multi-timestamp models')
    parser.add_argument('--skip-preprocessing', action='store_true',
                       help='Skip preprocessing if data files already exist')
    args = parser.parse_args()

    print("=" * 70)
    print("ENTRAÎNEMENT MULTI-TIMESTAMPS")
    print("Draft-only, @5min, @10min, @15min, @20min")
    print("=" * 70)

    # 1. Load data
    print("\n[1/5] Chargement des données...")
    train_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'train_with_summoner_stats.parquet')
    test_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'test_with_summoner_stats.parquet')
    timeline_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'matches_with_multi_timeline.parquet')

    if not os.path.exists(train_path):
        print(f"  ERREUR: {train_path} n'existe pas!")
        print("  Lancez d'abord le preprocessing des summoner stats.")
        return

    train_df = pd.read_parquet(train_path)
    test_df = pd.read_parquet(test_path)
    print(f"  Train: {len(train_df):,}, Test: {len(test_df):,}")

    # Load timeline data and merge
    if os.path.exists(timeline_path):
        print("  Chargement timelines multi-minutes...")
        timeline_df = pd.read_parquet(timeline_path)

        # Merge timeline columns into train/test
        timeline_cols = [c for c in timeline_df.columns
                        if any(f'_at_{m}' in c for m in [5, 15, 20]) and c not in train_df.columns]

        if timeline_cols:
            # We need match_id to merge
            if 'match_id' in train_df.columns and 'match_id' in timeline_df.columns:
                timeline_subset = timeline_df[['match_id'] + timeline_cols].drop_duplicates(subset='match_id')
                train_df = train_df.merge(timeline_subset, on='match_id', how='left')
                test_df = test_df.merge(timeline_subset, on='match_id', how='left')
                print(f"  Merged {len(timeline_cols)} colonnes timeline")
            else:
                print("  ⚠️ Pas de match_id pour merge, timeline @5/@15/@20 non disponibles")

    # 2. Compute synergies/counters
    print("\n[2/5] Synergies et counters...")
    champ_data = load_or_compute_synergies(train_df)
    synergy_wr = champ_data['synergy_winrate']
    counter_wr = champ_data['counter_winrate']

    print("  Ajout features synergies/counters (train)...")
    train_df = add_synergy_counter_features(train_df, synergy_wr, counter_wr)
    print("  Ajout features synergies/counters (test)...")
    test_df = add_synergy_counter_features(test_df, synergy_wr, counter_wr)

    # 3. Define feature sets for each timestamp
    print("\n[3/5] Définition des features par timestamp...")

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
            # Check if we have data for this timestamp
            gold_diff_col = f'gold_diff_at_{minute}'
            if gold_diff_col in train_df.columns:
                n_train = train_df[gold_diff_col].notna().sum()
                n_test = test_df[gold_diff_col].notna().sum() if gold_diff_col in test_df.columns else 0
                print(f"  @{minute}min: {len(all_feats)} features ({n_train:,} train, {n_test:,} test)")
                timestamps[f'at{minute}'] = {
                    'features': all_feats,
                    'filter_col': gold_diff_col
                }
            else:
                print(f"  @{minute}min: colonnes gold non disponibles")
        else:
            print(f"  @{minute}min: pas de features timeline")

    # 4. Train models
    print("\n[4/5] Entraînement des modèles...")
    results = {}

    for name, config in timestamps.items():
        print(f"\n{'='*50}")
        print(f"MODÈLE: {name.upper()}")
        print(f"{'='*50}")

        features = config['features']
        filter_col = config['filter_col']

        # Filter to matches with data for this timestamp
        if filter_col:
            train_subset = train_df[train_df[filter_col].notna()]
            test_subset = test_df[test_df[filter_col].notna()] if filter_col in test_df.columns else test_df
        else:
            train_subset = train_df
            test_subset = test_df

        # If test set is empty, use train split for testing
        if len(test_subset) == 0 or (filter_col and filter_col in test_subset.columns and test_subset[filter_col].notna().sum() == 0):
            print(f"  ⚠️ Test set vide pour {name}, split depuis train")
            train_subset, test_subset = train_test_split(train_subset, test_size=0.2, random_state=42)

        # Only use features that exist in data
        features = [f for f in features if f in train_subset.columns and f in train_subset.select_dtypes(include=[np.number]).columns]

        print(f"  Features: {len(features)}, Train: {len(train_subset):,}, Test: {len(test_subset):,}")

        X_train = train_subset[features]
        X_test = test_subset[features]
        y_train = train_subset[target]
        y_test = test_subset[target]

        result = train_model(X_train, X_test, y_train, y_test, name)
        result['synergy_data'] = {'synergy_wr': synergy_wr, 'counter_wr': counter_wr}
        results[name] = result

        print(f"  ✓ Best: {result['model_name']} (Test: {result['test_accuracy']:.4f})")

    # 5. Save and summarize
    print("\n[5/5] Sauvegarde...")
    models_dir = os.path.join(PROJECT_ROOT, 'models')
    os.makedirs(models_dir, exist_ok=True)

    for name, result in results.items():
        path = os.path.join(models_dir, f'model_{name}.pkl')
        with open(path, 'wb') as f:
            pickle.dump(result, f)
        print(f"  {path}")

    # Summary
    print("\n" + "=" * 70)
    print("RÉSUMÉ FINAL")
    print("=" * 70)
    print(f"\n{'Modèle':<15} {'Algo':<12} {'Features':<10} {'Val':<10} {'Test':<10}")
    print("-" * 60)
    for name, result in results.items():
        print(f"{name:<15} {result['model_name']:<12} {len(result['features']):<10} "
              f"{result['val_accuracy']:.4f}     {result['test_accuracy']:.4f}")
    print("-" * 60)

    # Feature importance for best model
    best_name = max(results, key=lambda k: results[k]['test_accuracy'])
    best_result = results[best_name]
    print(f"\nMeilleur modèle: {best_name} ({best_result['test_accuracy']:.4f})")

    if hasattr(best_result['model'], 'feature_importances_'):
        imp = pd.DataFrame({
            'feature': best_result['features'],
            'importance': best_result['model'].feature_importances_
        }).sort_values('importance', ascending=False)
        print(f"\nTop 10 features ({best_name}):")
        for _, row in imp.head(10).iterrows():
            print(f"  {row['feature']:40} {row['importance']:.4f}")

    print("\n" + "=" * 70)


if __name__ == '__main__':
    main()
