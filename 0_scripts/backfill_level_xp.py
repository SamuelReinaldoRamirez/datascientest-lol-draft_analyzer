#!/usr/bin/env python3
"""
Backfill level/XP/CS for existing timeline rows that only have gold data.

Re-fetches timeline JSON from Riot API and UPDATEs existing match_timeline rows
with level, xp, and cs columns.

Usage:
    python scripts/backfill_level_xp.py
    python scripts/backfill_level_xp.py --limit 100
    python scripts/backfill_level_xp.py --limit 1000 --workers 8
"""

import sys
import os
import time
import argparse
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'src', 'collect_data'))

from riot_api import get_match_timeline, get_api_key_count, get_key_rotator
from database import MatchDatabase

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/backfill_level_xp.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_standard_position_map():
    """Standard Riot API participant ID mapping."""
    pos_names = ['top', 'jungle', 'mid', 'adc', 'support']
    positions = {}
    for i in range(5):
        positions[i + 1] = (100, pos_names[i])
        positions[i + 6] = (200, pos_names[i])
    return positions


def fetch_and_update_level_xp_cs(match_id, participant_positions, db, rotator):
    """
    Fetch timeline and UPDATE existing rows with level/xp/cs data.
    """
    max_retries = 6

    for attempt in range(max_retries):
        try:
            result = rotator.get_next_available_key()
            if len(result) == 3:
                key_index, key, wait_time = result
                time.sleep(min(wait_time, 15))
                result = rotator.get_next_available_key()
                if len(result) == 3:
                    # All keys in cooldown - bail out fast instead of blocking
                    return (match_id, False, "All keys in cooldown")

            key_index, key = result

            time.sleep(0.05 * (hash(match_id) % 4))

            timeline_data = get_match_timeline(match_id, use_rotation=False, api_key_index=key_index)

            if not timeline_data:
                return (match_id, False, "Empty timeline response")

            rotator.mark_key_success(key_index)

            frames = timeline_data.get("info", {}).get("frames", [])
            updates = 0

            for frame in frames:
                minute = frame.get("timestamp", 0) // 60000
                if minute == 0:
                    continue

                participant_frames = frame.get("participantFrames", {})
                position_level = {}
                position_xp = {}
                position_cs = {}

                for pid_str, pf in participant_frames.items():
                    pid = int(pid_str)
                    if pid in participant_positions:
                        team_id, pos = participant_positions[pid]
                        team_key = f"team_{team_id}"
                        position_level[f"{team_key}_{pos}"] = pf.get("level", 0)
                        position_xp[f"{team_key}_{pos}"] = pf.get("xp", 0)
                        position_cs[f"{team_key}_{pos}"] = pf.get("minionsKilled", 0) + pf.get("jungleMinionsKilled", 0)

                db.update_timeline_level_xp_cs(match_id, minute, position_level, position_xp, position_cs)
                updates += 1

            return (match_id, True, f"{updates} frames updated")

        except Exception as e:
            error_msg = str(e)
            if '429' in error_msg:
                retry_after = None
                if hasattr(e, 'response') and e.response:
                    retry_after = e.response.headers.get('Retry-After')
                cooldown = rotator.mark_key_rate_limited(key_index, retry_after)
                time.sleep(min(cooldown, 5))
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
    parser = argparse.ArgumentParser(description='Backfill level/XP/CS for existing timeline data')
    parser.add_argument('--db', type=str, default='data/lol_matches.db')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--workers', type=int, default=None,
                        help='Number of parallel workers (default: num_keys * 2)')
    args = parser.parse_args()

    # Ensure logs directory exists
    os.makedirs('logs', exist_ok=True)

    num_keys = get_api_key_count()
    workers = args.workers or max(num_keys + 2, 6)

    print("=" * 60)
    print("BACKFILL LEVEL / XP / CS")
    print("=" * 60)
    print(f"Database: {args.db}")
    print(f"API keys: {num_keys}")
    print(f"Workers: {workers}")
    print("=" * 60)

    db = MatchDatabase(args.db)
    rotator = get_key_rotator()

    standard_positions = get_standard_position_map()

    # Find matches that have gold but NOT level data
    conn = sqlite3.connect(args.db)
    cursor = conn.cursor()
    query = '''
        SELECT DISTINCT match_id
        FROM match_timeline
        WHERE team_100_top_gold IS NOT NULL
          AND team_100_top_level IS NULL
    '''
    if args.limit:
        query += f' LIMIT {args.limit}'
    cursor.execute(query)
    match_ids = [row[0] for row in cursor.fetchall()]
    conn.close()

    total = len(match_ids)
    logger.info(f"Found {total:,} matches with gold but no level/XP/CS data")

    if total == 0:
        logger.info("All timeline rows already have level/XP/CS data!")
        return

    success = 0
    failed = 0
    processed = 0
    start_time = time.time()
    batch_size = workers * 2
    failed_ids = []

    for batch_start in range(0, total, batch_size):
        batch_end = min(batch_start + batch_size, total)
        batch = match_ids[batch_start:batch_end]

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for mid in batch:
                future = executor.submit(
                    fetch_and_update_level_xp_cs,
                    mid, standard_positions, db, rotator
                )
                futures[future] = mid

            try:
                for future in as_completed(futures, timeout=120):
                    try:
                        mid, is_success, error = future.result(timeout=60)
                        processed += 1
                        if is_success:
                            success += 1
                        else:
                            failed += 1
                            if error not in ("404", "403"):
                                failed_ids.append(mid)
                    except Exception:
                        processed += 1
                        failed += 1
            except TimeoutError:
                # Some futures didn't complete in time - count them as failed and move on
                not_done = sum(1 for f in futures if not f.done())
                processed += not_done
                failed += not_done
                logger.warning(f"Batch timeout: {not_done} tasks didn't complete in 120s, skipping")

        time.sleep(0.3)

        elapsed = time.time() - start_time
        rate = success / elapsed if elapsed > 0 else 0
        remaining = total - processed
        eta = remaining / rate / 60 if rate > 0 else 0

        if processed % (batch_size * 10) == 0 or batch_end >= total:
            logger.info(
                f"Progress: {processed:,}/{total:,} ({success:,} success, {failed:,} failed) "
                f"- {rate:.1f} ok/sec - ETA: {eta:.0f} min"
            )

    # Retry pass
    if failed_ids:
        logger.info(f"\n--- RETRY PASS: {len(failed_ids):,} failed matches ---")
        retry_success = 0
        for i in range(0, len(failed_ids), batch_size):
            batch = failed_ids[i:i + batch_size]
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(fetch_and_update_level_xp_cs, mid, standard_positions, db, rotator): mid
                    for mid in batch
                }
                for future in as_completed(futures):
                    try:
                        _, is_success, _ = future.result()
                        if is_success:
                            retry_success += 1
                            success += 1
                            failed -= 1
                    except Exception:
                        pass
            time.sleep(0.5)

        logger.info(f"  Retry: {retry_success:,}/{len(failed_ids):,} recovered")

    elapsed = time.time() - start_time
    rate = success / elapsed if elapsed > 0 else 0

    logger.info("=" * 60)
    logger.info(f"BACKFILL LEVEL/XP/CS COMPLETE")
    logger.info(f"  Total: {processed:,} matches")
    logger.info(f"  Success: {success:,} ({success / max(processed, 1) * 100:.1f}%)")
    logger.info(f"  Failed: {failed:,}")
    logger.info(f"  Speed: {rate:.1f} ok/sec")
    logger.info(f"  Duration: {elapsed / 60:.1f} minutes")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
