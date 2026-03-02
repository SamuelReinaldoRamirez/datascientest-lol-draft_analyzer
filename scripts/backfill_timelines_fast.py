#!/usr/bin/env python3
"""
Backfill RAPIDE des timelines.

Optimisations vs le backfill standard:
1. Utilise la DB pour les positions (pas de 2e appel API match_details)
   → 1 seul appel API par match au lieu de 2 = x2 throughput
2. Batch size plus large (4 matchs par clé au lieu de 2)
3. Skip la collecte d'events (on veut juste le gold)
4. Pre-load les positions de tous les matchs en une seule requête SQL

Usage:
    python scripts/backfill_timelines_fast.py
    python scripts/backfill_timelines_fast.py --limit 1000
"""

import sys
import os
import time
import argparse
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'src', 'collect_data'))

from riot_api import get_match_timeline, get_api_key_count, get_key_rotator
from database import MatchDatabase

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/backfill_timelines_fast.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_standard_position_map():
    """
    Standard Riot API participant ID mapping:
    - Participants 1-5 = Team 100 (Blue), 6-10 = Team 200 (Red)
    - Order: top, jungle, mid, adc, support

    This is the standard ordering in ranked games.
    Team gold totals are always correct (1-5 vs 6-10).
    """
    pos_names = ['top', 'jungle', 'mid', 'adc', 'support']
    positions = {}
    for i in range(5):
        positions[i + 1] = (100, pos_names[i])
        positions[i + 6] = (200, pos_names[i])
    return positions


def fetch_and_store_timeline_fast(match_id, participant_positions, db, rotator):
    """
    Fetch timeline using only 1 API call (no match_details needed).
    On 429, wait properly instead of failing immediately.
    """
    max_retries = 6

    for attempt in range(max_retries):
        try:
            # Get next available key - wait properly if all in cooldown
            result = rotator.get_next_available_key()
            if len(result) == 3:
                key_index, key, wait_time = result
                time.sleep(min(wait_time, 30))  # Wait up to 30s for keys to free up
                result = rotator.get_next_available_key()
                if len(result) == 3:
                    # Still all in cooldown, wait the full time
                    _, _, wait_time2 = result
                    time.sleep(wait_time2)
                    result = rotator.get_next_available_key()
                    if len(result) == 3:
                        return (match_id, False, "All keys in cooldown")

            key_index, key = result

            # Small stagger to avoid burst
            time.sleep(0.05 * (hash(match_id) % 4))

            # Single API call - timeline only
            timeline_data = get_match_timeline(match_id, use_rotation=False, api_key_index=key_index)

            if not timeline_data:
                return (match_id, False, "Empty timeline response")

            # Mark key success
            rotator.mark_key_success(key_index)

            # Process frames using pre-loaded positions
            frames = timeline_data.get("info", {}).get("frames", [])

            for frame in frames:
                minute = frame.get("timestamp", 0) // 60000
                if minute == 0:
                    continue

                participant_frames = frame.get("participantFrames", {})
                team_gold = {"team_100": 0, "team_200": 0}
                position_gold = {}

                for pid_str, pf in participant_frames.items():
                    pid = int(pid_str)
                    gold = pf.get("totalGold", 0)

                    if pid in participant_positions:
                        team_id, pos = participant_positions[pid]
                        team_key = f"team_{team_id}"
                        team_gold[team_key] = team_gold.get(team_key, 0) + gold
                        position_gold[f"{team_key}_{pos}"] = gold

                db.insert_timeline_frame(match_id, minute, team_gold, position_gold)

            return (match_id, True, None)

        except Exception as e:
            error_msg = str(e)
            if '429' in error_msg:
                retry_after = None
                if hasattr(e, 'response') and e.response:
                    retry_after = e.response.headers.get('Retry-After')
                cooldown = rotator.mark_key_rate_limited(key_index, retry_after)
                # Wait before retrying with another key
                time.sleep(min(cooldown, 10))
                continue
            elif '404' in error_msg:
                return (match_id, False, "404")
            elif '403' in error_msg:
                return (match_id, False, "403")
            else:
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return (match_id, False, error_msg)

    return (match_id, False, "Max retries")


def main():
    parser = argparse.ArgumentParser(description='Fast timeline backfill')
    parser.add_argument('--db', type=str, default='data/lol_matches.db')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--workers', type=int, default=None,
                       help='Number of parallel workers (default: num_keys * 2)')
    args = parser.parse_args()

    num_keys = get_api_key_count()
    # 6 workers with 4 keys = good balance (not too aggressive on rate limits)
    workers = args.workers or max(num_keys + 2, 6)

    print("=" * 60)
    print("BACKFILL RAPIDE DES TIMELINES")
    print("=" * 60)
    print(f"Database: {args.db}")
    print(f"API keys: {num_keys}")
    print(f"Workers: {workers}")
    print(f"Optimisation: 1 appel API par match (pas de match_details)")
    print("=" * 60)

    db = MatchDatabase(args.db)
    rotator = get_key_rotator()

    # Step 1: Standard position mapping (no DB query needed)
    standard_positions = get_standard_position_map()
    logger.info(f"Using standard participant position mapping (1-5=T100, 6-10=T200)")

    # Step 2: Get match IDs without timeline
    conn = sqlite3.connect(args.db)
    cursor = conn.cursor()
    query = '''
        SELECT DISTINCT m.match_id
        FROM matches m
        LEFT JOIN match_timeline mt ON m.match_id = mt.match_id
        WHERE mt.match_id IS NULL
    '''
    if args.limit:
        query += f' LIMIT {args.limit}'
    cursor.execute(query)
    match_ids = [row[0] for row in cursor.fetchall()]
    conn.close()

    total = len(match_ids)
    logger.info(f"Found {total:,} matches without timeline data")

    if total == 0:
        logger.info("All matches already have timeline data!")
        return

    # Step 3: Process in parallel
    success = 0
    failed = 0
    processed = 0
    start_time = time.time()

    # Process in batches with inter-batch delay to smooth rate limiting
    batch_size = workers * 2
    failed_ids = []

    for batch_start in range(0, total, batch_size):
        batch_end = min(batch_start + batch_size, total)
        batch = match_ids[batch_start:batch_end]

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for mid in batch:
                future = executor.submit(
                    fetch_and_store_timeline_fast,
                    mid, standard_positions, db, rotator
                )
                futures[future] = mid

            for future in as_completed(futures):
                try:
                    mid, is_success, error = future.result()
                    processed += 1
                    if is_success:
                        success += 1
                    else:
                        failed += 1
                        if error not in ("404", "403"):
                            failed_ids.append(mid)
                except Exception as e:
                    processed += 1
                    failed += 1

        # Small inter-batch delay to let rate limits recover
        time.sleep(0.3)

        # Log progress periodically
        elapsed = time.time() - start_time
        rate = success / elapsed if elapsed > 0 else 0
        remaining = total - processed
        eta = remaining / rate / 60 if rate > 0 else 0

        if processed % (batch_size * 10) == 0 or batch_end >= total:
            logger.info(
                f"Progress: {processed:,}/{total:,} ({success:,} success, {failed:,} failed) "
                f"- {rate:.1f} ok/sec - ETA: {eta:.0f} min"
            )

    # Retry pass for failed matches (rate limit failures)
    if failed_ids:
        logger.info(f"\n--- RETRY PASS: {len(failed_ids):,} matchs échoués ---")
        retry_success = 0
        for i in range(0, len(failed_ids), batch_size):
            batch = failed_ids[i:i + batch_size]
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(fetch_and_store_timeline_fast, mid, standard_positions, db, rotator): mid
                    for mid in batch
                }
                for future in as_completed(futures):
                    try:
                        _, is_success, _ = future.result()
                        if is_success:
                            retry_success += 1
                            success += 1
                            failed -= 1
                    except:
                        pass
            time.sleep(0.5)

        logger.info(f"  Retry: {retry_success:,}/{len(failed_ids):,} récupérés")

    # Final report
    elapsed = time.time() - start_time
    rate = success / elapsed if elapsed > 0 else 0

    logger.info("=" * 60)
    logger.info(f"BACKFILL TERMINÉ")
    logger.info(f"  Total: {processed:,} matchs")
    logger.info(f"  Succès: {success:,} ({success/max(processed,1)*100:.1f}%)")
    logger.info(f"  Échecs: {failed:,}")
    logger.info(f"  Vitesse: {rate:.1f} ok/sec")
    logger.info(f"  Durée: {elapsed/60:.1f} minutes")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
