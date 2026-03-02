"""
Collect summoner data (champion mastery + current rank) for all players in the database.

Uses all 4 API keys via SmartKeyRotator for maximum throughput.
Progress is tracked so the script can be stopped and resumed.

Usage:
    python scripts/collect_summoner_data.py
    python scripts/collect_summoner_data.py --limit 1000
    python scripts/collect_summoner_data.py --skip-mastery   # rank only
    python scripts/collect_summoner_data.py --skip-rank      # mastery only
"""

import sys
import os
import time
import argparse
import logging
import sqlite3
from datetime import datetime, timedelta

# Add src/collect_data to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'collect_data'))

from riot_api import (
    get_champion_mastery_by_puuid,
    get_summoner_by_puuid,
    get_summoner_rank,
    get_key_rotator,
    get_headers_for_key,
    get_api_key_count,
)
from database import MatchDatabase

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'lol_matches.db')
LOG_PATH = os.path.join(PROJECT_ROOT, 'data', 'collect_summoner_data.log')


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_PATH),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def get_all_puuids(db_path):
    """Get all unique PUUIDs from player_stats table."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT puuid FROM player_stats WHERE puuid IS NOT NULL')
    puuids = [row[0] for row in cursor.fetchall()]
    conn.close()
    return puuids


def get_processed_puuids(db_path, task='mastery'):
    """Get PUUIDs that already have data collected.
    task: 'mastery' or 'rank'
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if task == 'mastery':
        cursor.execute('SELECT DISTINCT puuid FROM champion_mastery')
    elif task == 'rank':
        # Summoners with current_tier set (not NULL) were already fetched
        cursor.execute('SELECT puuid FROM summoners WHERE current_tier IS NOT NULL')
    else:
        return set()

    processed = {row[0] for row in cursor.fetchall()}
    conn.close()
    return processed


class SimpleKeyRotator:
    """Simple key rotator for a subset of API keys."""

    def __init__(self, key_indices):
        from config import API_KEYS
        self.keys = {i: API_KEYS[i] for i in key_indices}
        self.indices = list(key_indices)
        self.current = 0
        self.cooldowns = {i: 0.0 for i in key_indices}
        self.request_times = {i: [] for i in key_indices}

    def get_next(self):
        """Get next available key index, waiting if needed."""
        now = time.time()

        # Try each key
        for _ in range(len(self.indices)):
            idx = self.indices[self.current]
            self.current = (self.current + 1) % len(self.indices)

            if now >= self.cooldowns[idx]:
                # Clean old request times (keep last 2 min)
                self.request_times[idx] = [t for t in self.request_times[idx] if now - t < 2]

                # Check per-key short limit (20 req/1s)
                recent_1s = sum(1 for t in self.request_times[idx] if now - t <= 1)
                if recent_1s >= 18:  # Stay under 20
                    time.sleep(0.15)

                # Check per-key long limit (100 req/2min)
                if len(self.request_times[idx]) >= 95:  # Stay under 100
                    oldest = self.request_times[idx][0]
                    wait = 120 - (now - oldest) + 0.1
                    if wait > 0:
                        time.sleep(wait)

                self.request_times[idx].append(time.time())
                return idx

        # All in cooldown, wait for shortest
        min_cooldown = min(self.cooldowns.values())
        wait = min_cooldown - now + 0.1
        if wait > 0:
            time.sleep(min(wait, 10))
        return self.get_next()

    def mark_rate_limited(self, idx, retry_after=None):
        cooldown = float(retry_after) if retry_after else 5.0
        self.cooldowns[idx] = time.time() + cooldown


# Global simple rotator (initialized in main)
_simple_rotator = None


def make_api_call(func, *args, allowed_keys=None, **kwargs):
    """Make an API call with rate limit handling and key rotation."""
    global _simple_rotator
    max_retries = 5

    for attempt in range(max_retries):
        key_index = _simple_rotator.get_next()
        kwargs['api_key_index'] = key_index

        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            status_code = getattr(getattr(e, 'response', None), 'status_code', None)

            if status_code == 429:
                retry_after = None
                if hasattr(e, 'response') and e.response is not None:
                    retry_after = e.response.headers.get('Retry-After')
                _simple_rotator.mark_rate_limited(key_index, retry_after)
                time.sleep(0.5)
            elif status_code in (400, 404):
                return None  # Player not found or invalid PUUID
            elif status_code == 403:
                _simple_rotator.mark_rate_limited(key_index, 3600)
                logging.getLogger(__name__).error(f"API key #{key_index} returned 403 - possibly expired")
                if attempt >= max_retries - 1:
                    return None
            else:
                if attempt >= max_retries - 1:
                    return None
                time.sleep(1)

    return None


def collect_mastery(db, puuids, logger):
    """Collect champion mastery data for a list of PUUIDs."""
    total = len(puuids)
    success = 0
    errors = 0
    start_time = time.time()

    logger.info(f"Collecting champion mastery for {total} summoners...")

    for i, puuid in enumerate(puuids):
        # Progress logging - every 100, or every 10 for first 100
        log_interval = 10 if i < 100 else 500
        if i > 0 and i % log_interval == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            eta_min = (total - i) / (rate * 60) if rate > 0 else 0
            logger.info(
                f"Mastery progress: {i}/{total} ({success} ok, {errors} err) "
                f"- {rate:.1f}/s - ETA: {eta_min:.0f} min"
            )

        mastery_data = make_api_call(get_champion_mastery_by_puuid, puuid)

        if mastery_data is None:
            errors += 1
            continue

        # Store each mastery entry
        for entry in mastery_data:
            db.upsert_champion_mastery(
                puuid=puuid,
                champion_id=entry.get('championId'),
                champion_level=entry.get('championLevel', 0),
                champion_points=entry.get('championPoints', 0),
                last_play_time=entry.get('lastPlayTime', 0),
                tokens_earned=entry.get('tokensEarned', 0)
            )

        success += 1

        # Small delay to be respectful of rate limits
        time.sleep(0.05)

    elapsed = time.time() - start_time
    logger.info(
        f"Mastery collection complete: {success}/{total} summoners "
        f"({errors} errors) in {elapsed/60:.1f} min"
    )
    return success, errors


def collect_rank(db, puuids, logger):
    """Collect current rank data for a list of PUUIDs."""
    total = len(puuids)
    success = 0
    errors = 0
    start_time = time.time()

    logger.info(f"Collecting rank data for {total} summoners...")

    for i, puuid in enumerate(puuids):
        if i > 0 and i % 100 == 0:
            elapsed = time.time() - start_time
            rate = success / elapsed if elapsed > 0 else 0
            eta_min = (total - i) / (rate * 60) if rate > 0 else 0
            logger.info(
                f"Rank progress: {i}/{total} ({success} ok, {errors} err) "
                f"- {rate:.1f}/s - ETA: {eta_min:.0f} min"
            )

        # Step 1: Get summoner info (need summoner_id for rank endpoint)
        summoner_data = make_api_call(get_summoner_by_puuid, puuid)
        if summoner_data is None:
            errors += 1
            continue

        summoner_id = summoner_data.get('id')
        if not summoner_id:
            errors += 1
            continue

        # Step 2: Get rank info
        rank_data = make_api_call(get_summoner_rank, summoner_id)
        if rank_data is None:
            errors += 1
            continue

        # Find solo queue entry
        solo_q = None
        for entry in rank_data:
            if entry.get('queueType') == 'RANKED_SOLO_5x5':
                solo_q = entry
                break

        if solo_q:
            tier = solo_q.get('tier')
            rank = solo_q.get('rank')
            lp = solo_q.get('leaguePoints', 0)

            # Update summoner info
            db.upsert_summoner(
                puuid=puuid,
                tier=tier,
                rank=rank,
                lp=lp
            )

            # Record elo history with current patch approximation
            current_patch = datetime.now().strftime('%y.%m')  # Rough patch
            db.record_elo_history(puuid, current_patch, tier, rank, lp)

        success += 1
        time.sleep(0.05)

    elapsed = time.time() - start_time
    logger.info(
        f"Rank collection complete: {success}/{total} summoners "
        f"({errors} errors) in {elapsed/60:.1f} min"
    )
    return success, errors


def main():
    parser = argparse.ArgumentParser(description='Collect summoner data (mastery + rank)')
    parser.add_argument('--db', type=str, default=DB_PATH, help='Database path')
    parser.add_argument('--limit', type=int, help='Limit number of summoners to process')
    parser.add_argument('--skip-mastery', action='store_true', help='Skip mastery collection')
    parser.add_argument('--skip-rank', action='store_true', help='Skip rank collection')
    parser.add_argument('--force', action='store_true', help='Re-collect even if already done')
    parser.add_argument('--api-keys', type=str, help='Comma-separated API key indices to use (e.g. "2,3")')
    args = parser.parse_args()

    # Parse allowed keys
    allowed_keys = None
    if args.api_keys:
        allowed_keys = [int(k.strip()) for k in args.api_keys.split(',')]

    logger = setup_logging()
    db = MatchDatabase(args.db)
    num_keys = get_api_key_count()

    # Initialize the simple key rotator
    global _simple_rotator
    if allowed_keys:
        _simple_rotator = SimpleKeyRotator(allowed_keys)
        logger.info(f"Using API keys: {allowed_keys}")
    else:
        _simple_rotator = SimpleKeyRotator(list(range(num_keys)))
        logger.info(f"Using all {num_keys} API keys")

    # Get all PUUIDs from matches
    all_puuids = get_all_puuids(args.db)
    logger.info(f"Found {len(all_puuids)} unique summoners in database")

    # === MASTERY COLLECTION ===
    if not args.skip_mastery:
        if args.force:
            mastery_todo = all_puuids
        else:
            done_mastery = get_processed_puuids(args.db, 'mastery')
            mastery_todo = [p for p in all_puuids if p not in done_mastery]
            logger.info(f"Mastery: {len(done_mastery)} already done, {len(mastery_todo)} remaining")

        if args.limit:
            mastery_todo = mastery_todo[:args.limit]

        if mastery_todo:
            collect_mastery(db, mastery_todo, logger)
        else:
            logger.info("All mastery data already collected!")

    # === RANK COLLECTION ===
    if not args.skip_rank:
        if args.force:
            rank_todo = all_puuids
        else:
            done_rank = get_processed_puuids(args.db, 'rank')
            rank_todo = [p for p in all_puuids if p not in done_rank]
            logger.info(f"Rank: {len(done_rank)} already done, {len(rank_todo)} remaining")

        if args.limit:
            rank_todo = rank_todo[:args.limit]

        if rank_todo:
            collect_rank(db, rank_todo, logger)
        else:
            logger.info("All rank data already collected!")

    logger.info("Done!")


if __name__ == "__main__":
    main()
