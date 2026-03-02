#!/usr/bin/env python3
"""
Attend la fin du backfill timelines puis:
1. Re-exporte les données avec @5/@10/@15/@20
2. Recalcule les summoner stats
3. Ré-entraîne tous les modèles

Usage:
    python scripts/wait_and_retrain.py --pid 25791
"""

import sys
import os
import time
import argparse
import subprocess
import psutil
import pandas as pd
import numpy as np
import pickle
import warnings
from itertools import combinations
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier

warnings.filterwarnings('ignore')

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src', 'collect_data'))


def wait_for_pid(pid, check_interval=60):
    """Wait for a process to finish, printing status periodically."""
    print(f"\n⏳ Attente fin du processus PID {pid}...")
    print(f"   (vérification toutes les {check_interval}s)")

    while True:
        try:
            proc = psutil.Process(pid)
            if proc.status() == psutil.STATUS_ZOMBIE:
                break
        except psutil.NoSuchProcess:
            break

        # Show backfill progress
        log_path = os.path.join(PROJECT_ROOT, 'data', 'backfill_timelines.log')
        if os.path.exists(log_path):
            try:
                with open(log_path, 'r') as f:
                    lines = f.readlines()
                    for line in reversed(lines):
                        if 'Progress:' in line or 'complete' in line.lower():
                            print(f"   [{time.strftime('%H:%M:%S')}] {line.strip().split(' - ')[-1]}")
                            break
            except:
                pass

        time.sleep(check_interval)

    print(f"✅ Processus {pid} terminé!")


def step1_reexport():
    """Re-export data with all timeline timestamps."""
    print("\n" + "=" * 70)
    print("ÉTAPE 1: RE-EXPORT AVEC TIMELINES @5/@10/@15/@20")
    print("=" * 70)

    from database import MatchDatabase
    db = MatchDatabase(os.path.join(PROJECT_ROOT, 'data', 'lol_matches.db'))

    # Check timeline coverage
    import sqlite3
    conn = sqlite3.connect(os.path.join(PROJECT_ROOT, 'data', 'lol_matches.db'))
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(DISTINCT match_id) FROM match_timeline")
    timeline_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM matches")
    total_count = cursor.fetchone()[0]
    print(f"\n  Matchs avec timeline: {timeline_count:,} / {total_count:,} ({timeline_count/total_count*100:.1f}%)")

    for minute in [5, 10, 15, 20]:
        cursor.execute("SELECT COUNT(*) FROM match_timeline WHERE minute = ?", (minute,))
        count = cursor.fetchone()[0]
        print(f"  @{minute}min: {count:,}")
    conn.close()

    # Full export with all data
    start = time.time()
    print("\n  Export complet...")
    df_full = db.export_match_data()
    print(f"  Export principal: {len(df_full):,} matchs, {len(df_full.columns)} colonnes ({time.time()-start:.1f}s)")

    # Export timelines
    start = time.time()
    df_timeline = db.export_with_timeline_at_minutes([5, 10, 15, 20])
    print(f"  Export timelines: {len(df_timeline):,} matchs, {len(df_timeline.columns)} colonnes ({time.time()-start:.1f}s)")

    # Merge timeline columns into full data
    timeline_only_cols = [c for c in df_timeline.columns if c not in df_full.columns]
    if 'match_id' in df_full.columns and 'match_id' in df_timeline.columns:
        df_merged = df_full.merge(
            df_timeline[['match_id'] + timeline_only_cols],
            on='match_id', how='left'
        )
    else:
        df_merged = df_full

    print(f"  Merged: {len(df_merged):,} matchs, {len(df_merged.columns)} colonnes")

    # Save
    out_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'full_data_with_timelines.parquet')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df_merged.to_parquet(out_path)
    print(f"  Sauvegardé: {out_path}")

    return df_merged


def step2_summoner_stats(df):
    """Compute summoner stats with temporal split."""
    print("\n" + "=" * 70)
    print("ÉTAPE 2: CALCUL DES SUMMONER STATS")
    print("=" * 70)

    from database import MatchDatabase
    db = MatchDatabase(os.path.join(PROJECT_ROOT, 'data', 'lol_matches.db'))

    # Temporal split (80/20)
    df = df.sort_values('game_creation')
    split_idx = int(len(df) * 0.8)
    train_df = df.iloc[:split_idx].copy()
    test_df = df.iloc[split_idx:].copy()
    print(f"\n  Train: {len(train_df):,}, Test: {len(test_df):,}")

    cutoff_timestamp = train_df['game_creation'].max()
    print(f"  Cutoff temporel: {cutoff_timestamp}")

    # Get unique players
    puuid_cols = [c for c in train_df.columns if 'puuid' in c.lower()]
    all_puuids = set()
    for col in puuid_cols:
        all_puuids.update(train_df[col].dropna().unique())
        all_puuids.update(test_df[col].dropna().unique())
    all_puuids = [p for p in all_puuids if isinstance(p, str) and len(p) > 10]
    print(f"  {len(all_puuids):,} joueurs uniques")

    # Compute stats in chunks
    print(f"  Calcul des stats (chunked)...")
    start = time.time()
    CHUNK_SIZE = 500
    all_stats = {}
    for i in range(0, len(all_puuids), CHUNK_SIZE):
        chunk = all_puuids[i:i + CHUNK_SIZE]
        stats = db.get_all_summoner_stats_batch(chunk, before_timestamp=int(cutoff_timestamp), last_n_games=20)
        all_stats.update(stats)
        if (i // CHUNK_SIZE) % 50 == 0:
            print(f"    {i:,}/{len(all_puuids):,}...")
    print(f"  Stats calculées pour {len(all_stats):,} joueurs en {time.time()-start:.1f}s")

    # Apply summoner stats to train and test
    positions = ['top', 'jungle', 'mid', 'adc', 'support']

    def apply_summoner_stats(df_chunk, stats_dict):
        """Apply summoner stats features to a DataFrame."""
        for team in ['100', '200']:
            for pos in positions:
                puuid_col = f'team_{team}_{pos}_puuid'
                champ_col = f'team_{team}_{pos}_champion_id'

                if puuid_col not in df_chunk.columns:
                    continue

                role_pct = []
                role_wr = []
                mastery_pts = []
                streak_type = []
                streak_len = []
                role_kda_vals = []
                role_vision_vals = []
                champ_wr = []

                for idx, row in df_chunk.iterrows():
                    puuid = row.get(puuid_col)
                    s = stats_dict.get(puuid, {}) if pd.notna(puuid) else {}

                    # Role stats
                    role_dist = s.get('role_distribution', {})
                    role_winrates = s.get('role_winrates', {})
                    role_pct.append(role_dist.get(pos, 0))
                    role_wr.append(role_winrates.get(pos, 0.5))

                    # Mastery
                    mastery_pts.append(s.get('mastery_points', 0))

                    # Streaks
                    streaks = s.get('streaks', {})
                    st = 1 if streaks.get('current_streak_type') == 'win' else -1
                    streak_type.append(st)
                    streak_len.append(streaks.get('current_streak_length', 0))

                    # KDA/Vision
                    role_kda = s.get('role_kda', {})
                    role_vision = s.get('role_vision_score', {})
                    role_kda_vals.append(role_kda.get(pos, 0))
                    role_vision_vals.append(role_vision.get(pos, 0))

                    # Champion recent winrate
                    champ_recent = s.get('champion_recent_winrate', {})
                    champ_id = row.get(champ_col)
                    if pd.notna(champ_id):
                        champ_wr.append(champ_recent.get(int(champ_id), {}).get('winrate', 0.5))
                    else:
                        champ_wr.append(0.5)

                df_chunk[f'team_{team}_{pos}_role_pct'] = role_pct
                df_chunk[f'team_{team}_{pos}_role_winrate'] = role_wr
                df_chunk[f'team_{team}_{pos}_mastery_points'] = mastery_pts
                df_chunk[f'team_{team}_{pos}_streak_type'] = streak_type
                df_chunk[f'team_{team}_{pos}_streak_length'] = streak_len
                df_chunk[f'team_{team}_{pos}_role_kda'] = role_kda_vals
                df_chunk[f'team_{team}_{pos}_role_vision'] = role_vision_vals
                df_chunk[f'team_{team}_{pos}_champ_recent_wr'] = champ_wr

            # Team aggregates
            role_pcts = [df_chunk[f'team_{team}_{pos}_role_pct'] for pos in positions]
            role_wrs = [df_chunk[f'team_{team}_{pos}_role_winrate'] for pos in positions]

            df_chunk[f'team_{team}_avg_role_specialization'] = np.mean(role_pcts, axis=0)
            df_chunk[f'team_{team}_min_role_specialization'] = np.min(role_pcts, axis=0)
            df_chunk[f'team_{team}_avg_role_winrate'] = np.mean(role_wrs, axis=0)
            df_chunk[f'team_{team}_total_mastery_points'] = sum(
                df_chunk[f'team_{team}_{pos}_mastery_points'] for pos in positions
            )
            df_chunk[f'team_{team}_streak_momentum'] = sum(
                df_chunk[f'team_{team}_{pos}_streak_type'] * df_chunk[f'team_{team}_{pos}_streak_length']
                for pos in positions
            )

        # Diffs
        df_chunk['role_specialization_diff'] = (
            df_chunk['team_100_avg_role_specialization'] - df_chunk['team_200_avg_role_specialization']
        )
        df_chunk['role_winrate_diff'] = (
            df_chunk['team_100_avg_role_winrate'] - df_chunk['team_200_avg_role_winrate']
        )
        df_chunk['streak_momentum_diff'] = (
            df_chunk['team_100_streak_momentum'] - df_chunk['team_200_streak_momentum']
        )
        df_chunk['mastery_diff'] = (
            df_chunk['team_100_total_mastery_points'] - df_chunk['team_200_total_mastery_points']
        )

        return df_chunk

    # Process in batches for memory efficiency
    print("\n  Application des stats (train)...")
    BATCH = 20000
    train_chunks = []
    for i in range(0, len(train_df), BATCH):
        chunk = train_df.iloc[i:i+BATCH].copy()
        chunk = apply_summoner_stats(chunk, all_stats)
        train_chunks.append(chunk)
        print(f"    Lot {i//BATCH + 1}/{(len(train_df)-1)//BATCH + 1} - {(i+BATCH)/len(train_df)*100:.0f}%")
    train_df = pd.concat(train_chunks)

    print("  Application des stats (test)...")
    test_chunks = []
    for i in range(0, len(test_df), BATCH):
        chunk = test_df.iloc[i:i+BATCH].copy()
        chunk = apply_summoner_stats(chunk, all_stats)
        test_chunks.append(chunk)
        print(f"    Lot {i//BATCH + 1}/{(len(test_df)-1)//BATCH + 1} - {(i+BATCH)/len(test_df)*100:.0f}%")
    test_df = pd.concat(test_chunks)

    # Save
    train_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'train_with_summoner_stats.parquet')
    test_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'test_with_summoner_stats.parquet')
    train_df.to_parquet(train_path)
    test_df.to_parquet(test_path)
    print(f"\n  Train: {len(train_df):,} matchs, {len(train_df.columns)} colonnes")
    print(f"  Test: {len(test_df):,} matchs, {len(test_df.columns)} colonnes")

    return train_df, test_df


def step3_train_models(train_df, test_df):
    """Train all multi-timestamp models."""
    print("\n" + "=" * 70)
    print("ÉTAPE 3: ENTRAÎNEMENT DES MODÈLES")
    print("=" * 70)

    # Load synergies
    syn_path = os.path.join(PROJECT_ROOT, 'data', 'features', 'champion_synergy_counter.pkl')
    with open(syn_path, 'rb') as f:
        champ_data = pickle.load(f)
    synergy_wr = champ_data['synergy_winrate']
    counter_wr = champ_data['counter_winrate']

    # Add synergy/counter features
    champ_cols = {
        100: [f'team_100_{pos}_champion_id' for pos in ['top', 'jungle', 'mid', 'adc', 'support']],
        200: [f'team_200_{pos}_champion_id' for pos in ['top', 'jungle', 'mid', 'adc', 'support']]
    }

    def add_syn(df):
        n = len(df)
        t100_syn, t200_syn = np.full(n, 0.5), np.full(n, 0.5)
        t100_cnt, t200_cnt = np.full(n, 0.5), np.full(n, 0.5)

        for i, (idx, row) in enumerate(df.iterrows()):
            c100 = [int(row[c]) for c in champ_cols[100] if pd.notna(row[c])]
            c200 = [int(row[c]) for c in champ_cols[200] if pd.notna(row[c])]

            s100 = [synergy_wr.get(tuple(sorted([a, b])), 0.5) for a, b in combinations(c100, 2)]
            s200 = [synergy_wr.get(tuple(sorted([a, b])), 0.5) for a, b in combinations(c200, 2)]
            if s100: t100_syn[i] = np.mean(s100)
            if s200: t200_syn[i] = np.mean(s200)

            cn100 = [counter_wr.get((a, b), 0.5) for a in c100 for b in c200]
            cn200 = [counter_wr.get((b, a), 0.5) for b in c200 for a in c100]
            if cn100: t100_cnt[i] = np.mean(cn100)
            if cn200: t200_cnt[i] = np.mean(cn200)

            if i % 50000 == 0 and i > 0:
                print(f"    {i:,}...")

        df = df.copy()
        df['team_100_synergy_score'] = t100_syn
        df['team_200_synergy_score'] = t200_syn
        df['team_100_counter_score'] = t100_cnt
        df['team_200_counter_score'] = t200_cnt
        df['synergy_diff'] = t100_syn - t200_syn
        df['counter_diff'] = t100_cnt - t200_cnt
        df['draft_advantage'] = (t100_syn - t200_syn) + (t100_cnt - t200_cnt)
        return df

    print("\n  Synergies/counters (train)...")
    train_df = add_syn(train_df)
    print("  Synergies/counters (test)...")
    test_df = add_syn(test_df)

    # Define features
    target = 'team_100_win'

    def get_draft_feats(df):
        feats = []
        for col in df.columns:
            if ('champion_id' in col.lower() or 'ban_' in col.lower() or
                ('summoner_' in col.lower() and '_id' in col.lower() and 'spell' not in col.lower())):
                feats.append(col)
        summoner_pats = ['_role_pct', '_role_winrate', '_mastery_points', '_streak_type',
                        '_streak_length', '_role_kda', '_role_vision', '_champ_recent_wr',
                        '_avg_role_', '_min_role_', '_total_mastery', 'role_specialization_diff',
                        'role_winrate_diff', 'streak_momentum_diff', 'mastery_diff']
        for col in df.columns:
            if any(p in col.lower() for p in summoner_pats) and col not in feats:
                feats.append(col)
        for col in ['team_100_synergy_score', 'team_200_synergy_score',
                    'team_100_counter_score', 'team_200_counter_score',
                    'synergy_diff', 'counter_diff', 'draft_advantage']:
            if col in df.columns:
                feats.append(col)
        numeric = set(df.select_dtypes(include=[np.number]).columns)
        return list(set(f for f in feats if f in numeric and f != target))

    draft_feats = get_draft_feats(train_df)
    print(f"\n  Draft features: {len(draft_feats)}")

    # Models config
    timestamps = {'draft': {'features': draft_feats, 'filter_col': None}}
    for minute in [5, 10, 15, 20]:
        gold_cols = [c for c in train_df.columns if f'gold_at_{minute}' in c.lower() or f'gold_diff_at_{minute}' in c.lower()]
        cs_cols = [c for c in train_df.columns if 'cs_at_10' in c.lower()] if minute >= 10 else []
        timeline_feats = list(set(gold_cols + cs_cols))

        gdiff = f'gold_diff_at_{minute}'
        if gdiff in train_df.columns and train_df[gdiff].notna().sum() > 1000:
            all_feats = list(set(draft_feats + timeline_feats))
            n_train = train_df[gdiff].notna().sum()
            n_test = test_df[gdiff].notna().sum() if gdiff in test_df.columns else 0
            print(f"  @{minute}min: {len(all_feats)} features ({n_train:,} train, {n_test:,} test)")
            timestamps[f'at{minute}'] = {'features': all_feats, 'filter_col': gdiff}

    # Train each model
    results = {}

    for name, config in timestamps.items():
        print(f"\n{'='*50}")
        print(f"  MODÈLE: {name.upper()}")
        print(f"{'='*50}")

        features = config['features']
        fc = config['filter_col']

        if fc:
            tr = train_df[train_df[fc].notna()]
            te = test_df[test_df[fc].notna()] if fc in test_df.columns else test_df
        else:
            tr, te = train_df, test_df

        if len(te) == 0 or (fc and fc in te.columns and te[fc].notna().sum() == 0):
            print(f"  ⚠️ Test set vide, split depuis train")
            tr, te = train_test_split(tr, test_size=0.2, random_state=42)

        features = [f for f in features if f in tr.select_dtypes(include=[np.number]).columns]
        print(f"  Features: {len(features)}, Train: {len(tr):,}, Test: {len(te):,}")

        X_tr, X_val, y_tr, y_val = train_test_split(tr[features], tr[target], test_size=0.15, random_state=42)
        X_tr, X_val = X_tr.fillna(0), X_val.fillna(0)
        X_te = te[features].fillna(0)
        y_te = te[target]

        best = None
        best_val = 0

        for mname, model in [
            ('XGBoost', XGBClassifier(n_estimators=300, max_depth=6, learning_rate=0.1,
                                       random_state=42, n_jobs=-1, verbosity=0,
                                       subsample=0.8, colsample_bytree=0.8)),
            ('LightGBM', LGBMClassifier(n_estimators=300, max_depth=6, learning_rate=0.1,
                                         random_state=42, n_jobs=-1, verbose=-1,
                                         subsample=0.8, colsample_bytree=0.8)),
        ]:
            start = time.time()
            model.fit(X_tr, y_tr)
            val = model.score(X_val, y_val)
            test = model.score(X_te, y_te)
            print(f"    {mname:15} Val: {val:.4f}  Test: {test:.4f}  ({time.time()-start:.1f}s)")
            if val > best_val:
                best_val = val
                best = {
                    'model': model, 'model_name': mname,
                    'val_accuracy': val, 'test_accuracy': test,
                    'features': features,
                    'synergy_data': {'synergy_wr': synergy_wr, 'counter_wr': counter_wr}
                }

        results[name] = best
        print(f"  ✓ Best: {best['model_name']} (Test: {best['test_accuracy']:.4f})")

    # Save all models
    models_dir = os.path.join(PROJECT_ROOT, 'models')
    os.makedirs(models_dir, exist_ok=True)

    for name, result in results.items():
        path = os.path.join(models_dir, f'model_{name}.pkl')
        with open(path, 'wb') as f:
            pickle.dump(result, f)

    # Summary
    print("\n" + "=" * 70)
    print("RÉSUMÉ FINAL")
    print("=" * 70)
    print(f"\n{'Modèle':<15} {'Algo':<12} {'Features':<10} {'Val':<10} {'Test':<10}")
    print("-" * 60)
    for name, r in results.items():
        print(f"{name:<15} {r['model_name']:<12} {len(r['features']):<10} "
              f"{r['val_accuracy']:.4f}     {r['test_accuracy']:.4f}")
    print("-" * 60)

    return results


def main():
    parser = argparse.ArgumentParser(description='Wait for backfill then retrain')
    parser.add_argument('--pid', type=int, required=True,
                       help='PID of the backfill process to wait for')
    parser.add_argument('--skip-wait', action='store_true',
                       help='Skip waiting (backfill already done)')
    parser.add_argument('--skip-summoner-stats', action='store_true',
                       help='Skip summoner stats recalculation (reuse existing)')
    args = parser.parse_args()

    total_start = time.time()

    print("=" * 70)
    print("PIPELINE COMPLET: ATTENTE BACKFILL → RE-EXPORT → ENTRAÎNEMENT")
    print("=" * 70)

    # Step 0: Wait for backfill
    if not args.skip_wait:
        wait_for_pid(args.pid)
    else:
        print("\n⏭️  Skip attente (--skip-wait)")

    # Step 1: Re-export
    df = step1_reexport()

    # Step 2: Summoner stats
    if not args.skip_summoner_stats:
        train_df, test_df = step2_summoner_stats(df)
    else:
        print("\n⏭️  Skip summoner stats (--skip-summoner-stats)")
        train_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'train_with_summoner_stats.parquet')
        test_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'test_with_summoner_stats.parquet')
        train_df = pd.read_parquet(train_path)
        test_df = pd.read_parquet(test_path)

        # Merge new timeline columns
        timeline_cols = [c for c in df.columns if any(f'_at_{m}' in c for m in [5, 15, 20]) and c not in train_df.columns]
        if timeline_cols and 'match_id' in train_df.columns and 'match_id' in df.columns:
            timeline_subset = df[['match_id'] + timeline_cols].drop_duplicates(subset='match_id')
            train_df = train_df.merge(timeline_subset, on='match_id', how='left')
            test_df = test_df.merge(timeline_subset, on='match_id', how='left')
            print(f"  Merged {len(timeline_cols)} nouvelles colonnes timeline")

    # Step 3: Train models
    results = step3_train_models(train_df, test_df)

    # Done
    total_elapsed = time.time() - total_start
    print(f"\n🏁 Pipeline complet en {total_elapsed/60:.1f} minutes")
    print("=" * 70)


if __name__ == '__main__':
    main()
