#!/usr/bin/env python3
"""
Compute temporal summoner stats for V3 models.

For each match, summoner stats are calculated using ONLY matches that occurred
BEFORE the current match (temporal correctness = no data leakage).

Strategy:
  - Split training data into monthly buckets by game_creation
  - For each bucket, fetch summoner stats with before_timestamp = bucket start
  - For test set, use max(train.game_creation) as cutoff
  - Save enriched parquets to data/processed/

Output:
  - data/processed/train_temporal_stats.parquet
  - data/processed/test_temporal_stats.parquet

Usage:
    python scripts/compute_temporal_summoner_stats.py
    python scripts/compute_temporal_summoner_stats.py --skip-mastery  # skip mastery lookups (faster)
"""

import sys
import os
import argparse
import time
import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src', 'collect_data'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src', 'ML'))

POSITIONS = ['top', 'jungle', 'mid', 'adc', 'support']


def get_all_puuids(df):
    """Extract all unique PUUIDs from a DataFrame."""
    puuids = set()
    for team in ['team_100', 'team_200']:
        for pos in POSITIONS:
            col = f'{team}_{pos}_puuid'
            if col in df.columns:
                puuids.update(df[col].dropna().unique())
    return list(puuids)


def apply_summoner_stats(df, all_stats, db=None, skip_mastery=False):
    """
    Apply pre-calculated summoner stats to a DataFrame.
    Reimplemented here to avoid importing the full preprocessing pipeline.

    Args:
        df: DataFrame to add features to
        all_stats: Dict mapping puuid -> stats dict
        db: MatchDatabase instance (for mastery lookups)
        skip_mastery: If True, skip mastery point lookups

    Returns:
        DataFrame with 93+ summoner features added
    """
    result_df = df.copy()
    mastery_cache = {}

    for team in ['team_100', 'team_200']:
        role_spec_cols = []
        role_wr_cols = []
        streak_cols = []
        mastery_cols = []

        for pos in POSITIONS:
            puuid_col = f'{team}_{pos}_puuid'
            champ_col = f'{team}_{pos}_champion_id'

            # Feature column names
            role_pct_col = f'{team}_{pos}_role_pct'
            role_wr_col = f'{team}_{pos}_role_winrate'
            mastery_col = f'{team}_{pos}_mastery_points'
            streak_type_col = f'{team}_{pos}_streak_type'
            streak_len_col = f'{team}_{pos}_streak_length'
            kda_col = f'{team}_{pos}_role_kda'
            vision_col = f'{team}_{pos}_role_vision'
            champ_wr_col = f'{team}_{pos}_champ_recent_wr'

            # Initialize with defaults
            result_df[role_pct_col] = 0.2
            result_df[role_wr_col] = 0.5
            result_df[mastery_col] = 0.0
            result_df[streak_type_col] = 0
            result_df[streak_len_col] = 0
            result_df[kda_col] = 2.0
            result_df[vision_col] = 20.0
            result_df[champ_wr_col] = 0.5

            if puuid_col not in result_df.columns:
                role_spec_cols.append(role_pct_col)
                role_wr_cols.append(role_wr_col)
                streak_cols.append(streak_type_col)
                mastery_cols.append(mastery_col)
                continue

            puuids = result_df[puuid_col].values
            champ_ids = result_df[champ_col].values if champ_col in result_df.columns else [None] * len(result_df)

            role_pcts = []
            role_wrs = []
            kdas = []
            visions = []
            streak_types = []
            streak_lens = []
            champ_wrs = []
            masteries = []

            for i, (puuid, champ_id) in enumerate(zip(puuids, champ_ids)):
                if pd.isna(puuid) or puuid not in all_stats:
                    role_pcts.append(0.2)
                    role_wrs.append(0.5)
                    kdas.append(2.0)
                    visions.append(20.0)
                    streak_types.append(0)
                    streak_lens.append(0)
                    champ_wrs.append(0.5)
                    masteries.append(0.0)
                    continue

                stats = all_stats[puuid]

                # Role stats
                role_stats = stats.get('role_stats', {})
                role_pcts.append(role_stats.get('role_distribution', {}).get(pos, 0.2))
                role_wrs.append(role_stats.get('role_winrates', {}).get(pos, 0.5))

                # Performance
                perf = stats.get('performance', {})
                kdas.append(perf.get('role_kda', {}).get(pos, 2.0))
                visions.append(perf.get('role_vision_score', {}).get(pos, 20.0))

                # Streaks
                streaks = stats.get('streaks', {})
                st = streaks.get('current_streak_type', 'none')
                streak_types.append(1 if st == 'win' else (-1 if st == 'loss' else 0))
                streak_lens.append(streaks.get('current_streak_length', 0))

                # Champion recent winrate
                recent_champs = stats.get('recent_champions', [])
                champ_wr = 0.5
                if champ_id and not pd.isna(champ_id):
                    for champ_stat in recent_champs:
                        if champ_stat.get('champion_id') == int(champ_id):
                            champ_wr = champ_stat.get('winrate', 0.5)
                            break
                champ_wrs.append(champ_wr)

                # Mastery points (optional, slow)
                mastery_val = 0.0
                if not skip_mastery and db is not None and champ_id and not pd.isna(champ_id):
                    mastery_key = (puuid, int(champ_id))
                    if mastery_key not in mastery_cache:
                        mastery_data = db.get_mastery_for_champion(puuid, int(champ_id))
                        mastery_cache[mastery_key] = mastery_data.get('champion_points', 0) if mastery_data else 0
                    mp = mastery_cache[mastery_key]
                    mastery_val = np.log1p(mp) if mp > 0 else 0
                masteries.append(mastery_val)

            result_df[role_pct_col] = role_pcts
            result_df[role_wr_col] = role_wrs
            result_df[kda_col] = kdas
            result_df[vision_col] = visions
            result_df[streak_type_col] = streak_types
            result_df[streak_len_col] = streak_lens
            result_df[champ_wr_col] = champ_wrs
            result_df[mastery_col] = masteries

            role_spec_cols.append(role_pct_col)
            role_wr_cols.append(role_wr_col)
            streak_cols.append(streak_type_col)
            mastery_cols.append(mastery_col)

        # Team aggregates
        spec_matrix = result_df[role_spec_cols].values
        wr_matrix = result_df[role_wr_cols].values
        mastery_matrix = result_df[mastery_cols].values

        result_df[f'{team}_avg_role_specialization'] = np.nanmean(spec_matrix, axis=1)
        result_df[f'{team}_min_role_specialization'] = np.nanmin(spec_matrix, axis=1)
        result_df[f'{team}_avg_role_winrate'] = np.nanmean(wr_matrix, axis=1)
        result_df[f'{team}_total_mastery_points'] = np.nansum(mastery_matrix, axis=1)

        # Streak momentum
        streak_momentum = 0
        for pos in POSITIONS:
            streak_type = result_df[f'{team}_{pos}_streak_type']
            streak_len = result_df[f'{team}_{pos}_streak_length']
            streak_momentum = streak_momentum + (streak_type * streak_len)
        result_df[f'{team}_streak_momentum'] = streak_momentum

    # Differentials
    result_df['role_specialization_diff'] = (
        result_df['team_100_avg_role_specialization'] - result_df['team_200_avg_role_specialization']
    )
    result_df['role_winrate_diff'] = (
        result_df['team_100_avg_role_winrate'] - result_df['team_200_avg_role_winrate']
    )
    result_df['streak_momentum_diff'] = (
        result_df['team_100_streak_momentum'] - result_df['team_200_streak_momentum']
    )
    result_df['mastery_diff'] = (
        result_df['team_100_total_mastery_points'] - result_df['team_200_total_mastery_points']
    )

    return result_df


def compute_diagnostic(df, target='team_100_win'):
    """Compute correlation of summoner features with target."""
    summoner_patterns = [
        '_role_pct', '_role_winrate', '_mastery_points', '_streak_type',
        '_streak_length', '_role_kda', '_role_vision', '_champ_recent_wr',
        '_avg_role_specialization', '_min_role_specialization', '_avg_role_winrate',
        '_total_mastery_points', '_streak_momentum',
        'role_specialization_diff', 'role_winrate_diff', 'streak_momentum_diff', 'mastery_diff',
    ]

    results = []
    for col in df.columns:
        if any(p in col for p in summoner_patterns) and col != target:
            if df[col].dtype in [np.float64, np.int64, float, int]:
                corr = df[col].corr(df[target])
                results.append({'feature': col, 'correlation': corr, 'abs_corr': abs(corr)})

    return pd.DataFrame(results).sort_values('abs_corr', ascending=False)


def main():
    parser = argparse.ArgumentParser(description='Compute temporal summoner stats')
    parser.add_argument('--skip-mastery', action='store_true',
                        help='Skip mastery point lookups (faster)')
    args = parser.parse_args()

    from database import MatchDatabase

    print("=" * 70)
    print("CALCUL DES SUMMONER STATS TEMPORELLES (V3)")
    print("=" * 70)

    total_start = time.time()

    # =============================================
    # STEP 1: Load existing parquets
    # =============================================
    print("\n[1/5] Chargement des parquets existants...")
    train_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'train_with_summoner_stats.parquet')
    test_path = os.path.join(PROJECT_ROOT, 'data', 'processed', 'test_with_summoner_stats.parquet')

    if not os.path.exists(train_path):
        print(f"  ERREUR: {train_path} n'existe pas!")
        return

    train_df = pd.read_parquet(train_path)
    test_df = pd.read_parquet(test_path)
    print(f"  Train: {len(train_df):,} matchs")
    print(f"  Test:  {len(test_df):,} matchs")

    if 'game_creation' not in train_df.columns:
        print("  ERREUR: game_creation column not found!")
        return

    # Sort by time
    train_df = train_df.sort_values('game_creation').reset_index(drop=True)
    test_df = test_df.sort_values('game_creation').reset_index(drop=True)

    # =============================================
    # STEP 2: Define monthly buckets
    # =============================================
    print("\n[2/5] Definition des buckets mensuels...")

    # Convert game_creation (ms timestamp) to datetime for bucketing
    train_df['_datetime'] = pd.to_datetime(train_df['game_creation'], unit='ms')
    train_df['_month'] = train_df['_datetime'].dt.to_period('M')

    months = sorted(train_df['_month'].unique())
    print(f"  {len(months)} buckets mensuels: {months[0]} -> {months[-1]}")

    for m in months:
        n = (train_df['_month'] == m).sum()
        print(f"    {m}: {n:,} matchs")

    # =============================================
    # STEP 3: Process each bucket
    # =============================================
    print("\n[3/5] Traitement des buckets (stats temporelles)...")

    db = MatchDatabase(os.path.join(PROJECT_ROOT, 'data', 'lol_matches.db'))
    processed_chunks = []

    for i, month in enumerate(months):
        bucket_mask = train_df['_month'] == month
        bucket_df = train_df[bucket_mask].copy()
        n_matches = len(bucket_df)

        # before_timestamp = start of this month (in ms)
        month_start = month.start_time
        before_ts = int(month_start.timestamp() * 1000)

        print(f"\n  Bucket {i+1}/{len(months)}: {month} ({n_matches:,} matchs)")
        print(f"    Stats from matches before {month_start.strftime('%Y-%m-%d')}")

        # Get unique PUUIDs in this bucket
        puuids = get_all_puuids(bucket_df)
        print(f"    {len(puuids):,} joueurs uniques")

        # Fetch stats for all players before this month
        start_t = time.time()
        all_stats = db.get_all_summoner_stats_batch(
            puuids,
            before_timestamp=before_ts,
            last_n_games=20
        )
        fetch_time = time.time() - start_t
        print(f"    Stats recuperees pour {len(all_stats):,} joueurs ({fetch_time:.1f}s)")

        # Apply stats
        start_t = time.time()
        bucket_result = apply_summoner_stats(
            bucket_df, all_stats, db=db, skip_mastery=args.skip_mastery
        )
        apply_time = time.time() - start_t
        print(f"    Features appliquees ({apply_time:.1f}s)")

        # Quick diagnostic for this bucket
        if 'team_100_win' in bucket_result.columns:
            wr_diff_corr = bucket_result['role_winrate_diff'].corr(bucket_result['team_100_win'])
            print(f"    role_winrate_diff corr with target: {wr_diff_corr:.4f}")

        processed_chunks.append(bucket_result)

    # Concatenate all chunks
    print("\n  Concatenation des buckets...")
    train_result = pd.concat(processed_chunks, ignore_index=True)

    # Drop temp columns
    train_result = train_result.drop(columns=['_datetime', '_month'], errors='ignore')

    # =============================================
    # STEP 4: Process test set
    # =============================================
    print("\n[4/5] Traitement du jeu de test...")

    max_train_ts = int(train_df['game_creation'].max())
    print(f"  Cutoff: max(train.game_creation) = {max_train_ts}")
    print(f"  = {pd.to_datetime(max_train_ts, unit='ms').strftime('%Y-%m-%d %H:%M')}")

    puuids_test = get_all_puuids(test_df)
    print(f"  {len(puuids_test):,} joueurs uniques dans le test")

    start_t = time.time()
    test_stats = db.get_all_summoner_stats_batch(
        puuids_test,
        before_timestamp=max_train_ts,
        last_n_games=20
    )
    fetch_time = time.time() - start_t
    print(f"  Stats recuperees pour {len(test_stats):,} joueurs ({fetch_time:.1f}s)")

    start_t = time.time()
    test_result = apply_summoner_stats(
        test_df, test_stats, db=db, skip_mastery=args.skip_mastery
    )
    apply_time = time.time() - start_t
    print(f"  Features appliquees ({apply_time:.1f}s)")

    # =============================================
    # STEP 5: Diagnostic and save
    # =============================================
    print("\n[5/5] Diagnostic et sauvegarde...")

    print("\n  --- Diagnostic: correlation summoner features vs target ---")
    train_diag = compute_diagnostic(train_result)
    test_diag = compute_diagnostic(test_result)

    # Merge for comparison
    diag = train_diag.merge(
        test_diag[['feature', 'correlation']],
        on='feature', suffixes=('_train', '_test'),
        how='outer'
    ).sort_values('abs_corr', ascending=False)

    print(f"\n  {'Feature':<50} {'Train corr':>12} {'Test corr':>12} {'Ratio':>8}")
    print("  " + "-" * 85)

    problem_features = []
    for _, row in diag.head(30).iterrows():
        tr = row.get('correlation_train', 0)
        te = row.get('correlation_test', 0)
        ratio = abs(tr / te) if te != 0 else float('inf')
        flag = " ***" if ratio > 10 else ""
        if ratio > 10:
            problem_features.append(row['feature'])
        print(f"  {row['feature']:<50} {tr:>12.4f} {te:>12.4f} {ratio:>8.1f}{flag}")

    if problem_features:
        print(f"\n  ATTENTION: {len(problem_features)} features avec ratio train/test > 10x:")
        for f in problem_features:
            print(f"    - {f}")
    else:
        print("\n  OK: Aucune feature avec ratio train/test > 10x")

    # Save
    out_dir = os.path.join(PROJECT_ROOT, 'data', 'processed')
    os.makedirs(out_dir, exist_ok=True)

    train_out = os.path.join(out_dir, 'train_temporal_stats.parquet')
    test_out = os.path.join(out_dir, 'test_temporal_stats.parquet')

    train_result.to_parquet(train_out, index=False)
    test_result.to_parquet(test_out, index=False)

    print(f"\n  Sauvegarde:")
    print(f"    {train_out} ({len(train_result):,} matchs, {len(train_result.columns)} colonnes)")
    print(f"    {test_out} ({len(test_result):,} matchs, {len(test_result.columns)} colonnes)")

    # List summoner feature columns
    summoner_cols = [c for c in train_result.columns
                     if any(p in c for p in ['_role_pct', '_role_winrate', '_mastery_points',
                                              '_streak_type', '_streak_length', '_role_kda',
                                              '_role_vision', '_champ_recent_wr',
                                              '_avg_role_specialization', '_min_role_specialization',
                                              '_avg_role_winrate', '_total_mastery_points',
                                              '_streak_momentum', 'role_specialization_diff',
                                              'role_winrate_diff', 'streak_momentum_diff', 'mastery_diff'])]
    print(f"\n  {len(summoner_cols)} summoner features ajoutees")

    total_time = time.time() - total_start
    print(f"\nTemps total: {total_time:.0f}s ({total_time/60:.1f} min)")
    print("\nDONE!")


if __name__ == '__main__':
    main()
