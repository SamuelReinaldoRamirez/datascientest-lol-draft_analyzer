"""
Safe Data Collection Script for League of Legends Match Data

Features:
- Advanced rate limiting with sliding window tracking
- SQLite storage for crash-resistant data persistence
- Automatic resume from last position
- Incremental saving to avoid data loss
- Parallel collection support with dedicated API keys per elo tier

Usage:
    # Single process (rotation between all keys):
    python src/collect_data_safe.py --continuous --players 50 --matches 20

    # Parallel collection (2 terminals):
    # Terminal 1 - Diamond I only:
    python src/collect_data_safe.py --continuous --elo diamond --api-key-index 0

    # Terminal 2 - Master+ only:
    python src/collect_data_safe.py --continuous --elo master --api-key-index 1
"""

import time
import json
import argparse
import os
from collections import deque
from datetime import datetime
import logging

from riot_api import (
    get_entries, get_matches_by_puuid, get_match_details, get_account_by_puuid,
    get_challenger_league, get_grandmaster_league, get_master_league,
    get_high_elo_players, get_summoner_by_summoner_id, get_api_key_count,
    get_key_rotator
)
from database import MatchDatabase


class RateLimiter:
    """
    Advanced rate limiter for Riot API with sliding window tracking
    """
    def __init__(self):
        # Personal API key limits
        self.limits = {
            'short': {'requests': 20, 'window': 1},      # 20 requests per 1 second
            'long': {'requests': 100, 'window': 120}     # 100 requests per 2 minutes
        }

        # Track requests per endpoint
        self.request_history = {
            'default': deque(),
            'match': deque(),
            'league': deque(),
            'account': deque()
        }

        # Track 429 errors for exponential backoff
        self.error_count = 0
        self.last_429_time = 0

    def _clean_old_requests(self, endpoint='default'):
        """Remove requests older than the longest window"""
        current_time = time.time()
        history = self.request_history[endpoint]

        # Remove requests older than 2 minutes
        while history and current_time - history[0] > 120:
            history.popleft()

    def can_make_request(self, endpoint='default'):
        """Check if we can make a request without hitting rate limits"""
        self._clean_old_requests(endpoint)
        current_time = time.time()
        history = self.request_history[endpoint]

        # Check short window (1 second)
        recent_1s = sum(1 for req_time in history if current_time - req_time <= 1)
        if recent_1s >= self.limits['short']['requests']:
            return False, 1.1 - (current_time - history[-self.limits['short']['requests']])

        # Check long window (2 minutes)
        if len(history) >= self.limits['long']['requests']:
            oldest_in_window = history[len(history) - self.limits['long']['requests']]
            wait_time = 120 - (current_time - oldest_in_window) + 0.1
            if wait_time > 0:
                return False, wait_time

        return True, 0

    def record_request(self, endpoint='default'):
        """Record a request timestamp"""
        self.request_history[endpoint].append(time.time())

    def handle_429_error(self, retry_after=None):
        """Handle rate limit error with exponential backoff"""
        self.error_count += 1
        self.last_429_time = time.time()

        # Use retry-after header if available, otherwise exponential backoff
        if retry_after:
            return float(retry_after)
        else:
            # Exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s, 60s max
            return min(2 ** self.error_count, 60)

    def reset_error_count(self):
        """Reset error count after successful request"""
        if time.time() - self.last_429_time > 300:  # 5 minutes
            self.error_count = 0


class DataCollector:
    """
    Main data collector class using SQLite for storage.

    This replaces the old JSON-based progress tracking with a more robust
    SQLite database that provides:
    - Atomic transactions (no partial saves)
    - Automatic deduplication
    - Crash recovery
    - Efficient querying
    - Parallel collection with dedicated API keys per elo tier
    """

    def __init__(self, db_path: str = 'data/lol_matches.db', api_key_index: int = None):
        self.rate_limiter = RateLimiter()
        self.db = MatchDatabase(db_path)
        self.api_key_index = api_key_index  # None = use rotation, 0/1/etc = use specific key

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('data_collection.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

        # Load stats from database
        self._load_stats()

    def _load_stats(self):
        """Load statistics from database"""
        self.stats = {
            'total_requests': self.db.get_stat('total_requests', 0),
            'successful_requests': self.db.get_stat('successful_requests', 0),
            'rate_limit_errors': self.db.get_stat('rate_limit_errors', 0),
            'other_errors': self.db.get_stat('other_errors', 0)
        }

    def _save_stats(self):
        """Save statistics to database"""
        for key, value in self.stats.items():
            self.db.update_stat(key, value)

    def wait_for_rate_limit(self, endpoint='default'):
        """Wait if necessary to respect rate limits"""
        can_proceed, wait_time = self.rate_limiter.can_make_request(endpoint)

        if not can_proceed:
            self.logger.info(f"Rate limit approaching, waiting {wait_time:.1f}s...")
            time.sleep(wait_time)

    def make_api_request(self, func, endpoint='default', *args, **kwargs):
        """
        Make API request with smart key rotation and failover.

        Features:
        - Automatic failover to next available key on 429
        - Per-key cooldown tracking
        - No global sleep on rate limit - just switch keys
        """
        max_retries = 5  # Increased for key rotation
        rotator = get_key_rotator()

        # If using dedicated key mode, use traditional approach
        if self.api_key_index is not None:
            kwargs['api_key_index'] = self.api_key_index
            return self._make_request_with_dedicated_key(func, endpoint, *args, **kwargs)

        # Smart rotation mode
        for attempt in range(max_retries):
            try:
                # Wait for global rate limit if necessary
                self.wait_for_rate_limit(endpoint)

                # Get next available key (skips rate-limited ones)
                result = rotator.get_next_available_key()

                if len(result) == 3:
                    # All keys in cooldown - must wait
                    key_index, key, wait_time = result
                    self.logger.warning(f"All {rotator.key_count} keys rate-limited, waiting {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    # Retry with the key that has shortest cooldown
                else:
                    key_index, key = result

                # Make request with this specific key
                kwargs['api_key_index'] = key_index
                self.rate_limiter.record_request(endpoint)
                self.stats['total_requests'] += 1

                result = func(*args, **kwargs)

                # Success! Mark key as successful
                rotator.mark_key_success(key_index)
                self.stats['successful_requests'] += 1
                self.rate_limiter.reset_error_count()

                # Small delay between requests
                time.sleep(0.05)

                return result

            except Exception as e:
                error_msg = str(e)

                if '429' in error_msg:
                    self.stats['rate_limit_errors'] += 1

                    # Extract retry-after header if available
                    retry_after = None
                    if hasattr(e, 'response') and e.response:
                        retry_after = e.response.headers.get('Retry-After')

                    # Mark THIS specific key as rate-limited
                    cooldown = rotator.mark_key_rate_limited(key_index, retry_after)
                    self.logger.warning(f"Key #{key_index} rate-limited (cooldown: {cooldown:.1f}s), trying next key...")

                    # Don't sleep - just try next available key immediately
                    continue

                elif '400' in error_msg:
                    # 400 Bad Request = Invalid PUUID, don't retry - skip immediately
                    self.stats['other_errors'] += 1
                    return None

                else:
                    self.stats['other_errors'] += 1
                    self.logger.error(f"API error (attempt {attempt + 1}/{max_retries}): {error_msg}")

                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff for non-429 errors
                    else:
                        raise

        return None

    def _make_request_with_dedicated_key(self, func, endpoint, *args, **kwargs):
        """Legacy method for dedicated key mode (parallel processes)"""
        max_retries = 3

        for attempt in range(max_retries):
            try:
                self.wait_for_rate_limit(endpoint)
                self.rate_limiter.record_request(endpoint)
                self.stats['total_requests'] += 1

                result = func(*args, **kwargs)

                self.stats['successful_requests'] += 1
                self.rate_limiter.reset_error_count()
                time.sleep(0.05)

                return result

            except Exception as e:
                error_msg = str(e)

                if '429' in error_msg:
                    self.stats['rate_limit_errors'] += 1
                    retry_after = None

                    if hasattr(e, 'response') and e.response:
                        retry_after = e.response.headers.get('Retry-After')

                    wait_time = self.rate_limiter.handle_429_error(retry_after)
                    self.logger.warning(f"Rate limited (429), waiting {wait_time}s...")
                    time.sleep(wait_time)

                elif '400' in error_msg:
                    # 400 Bad Request = Invalid PUUID, don't retry - skip immediately
                    self.stats['other_errors'] += 1
                    return None

                else:
                    self.stats['other_errors'] += 1
                    self.logger.error(f"API error (attempt {attempt + 1}/{max_retries}): {error_msg}")

                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                    else:
                        raise

        return None

    def collect_matches(self, num_players: int = 50, matches_per_player: int = 20,
                        high_elo_only: bool = False, elo_filter: str = None):
        """
        Collect matches with proper rate limiting and progress tracking.

        Args:
            num_players: Number of players to process per batch
            matches_per_player: Maximum matches to collect per player
            high_elo_only: If True, collect from Master/GM/Challenger only (legacy)
            elo_filter: 'diamond' for Diamond I only, 'master' for Master/GM/Challenger only

        Returns:
            int: Total number of matches in database
        """
        self.logger.info(f"Starting collection: {num_players} players, {matches_per_player} matches each")

        # elo_filter takes precedence over high_elo_only
        if elo_filter == 'diamond':
            self.logger.info("Mode: DIAMOND I ONLY (parallel mode)")
        elif elo_filter == 'master':
            self.logger.info("Mode: MASTER+ ONLY (Master/GM/Challenger - parallel mode)")
        elif high_elo_only:
            self.logger.info("Mode: HIGH ELO ONLY (Master/GM/Challenger)")
        else:
            self.logger.info("Mode: ALL HIGH ELO (Challenger + GM + Master + Diamond I)")

        # Load existing match IDs from database
        existing_matches = self.db.get_collected_match_ids()
        self.logger.info(f"Database contains {len(existing_matches)} matches")

        new_entries = []

        # Determine what to collect based on elo_filter
        collect_master_plus = (elo_filter == 'master') or (elo_filter is None and not high_elo_only) or high_elo_only
        collect_diamond = (elo_filter == 'diamond') or (elo_filter is None and not high_elo_only)

        # Skip diamond if elo_filter is 'master'
        if elo_filter == 'master':
            collect_diamond = False

        # Skip master+ if elo_filter is 'diamond'
        if elo_filter == 'diamond':
            collect_master_plus = False

        # Collect from Master+ leagues
        if collect_master_plus:
            self.logger.info("Fetching high elo players (Challenger/GM/Master)...")

            try:
                high_elo = self.make_api_request(get_high_elo_players, 'league')

                if high_elo:
                    # Filter out already processed players
                    for entry in high_elo:
                        # New API returns puuid directly, old entries may have summonerId
                        puuid = entry.get("puuid")
                        summoner_id = entry.get("summonerId")

                        if puuid:
                            # New format - has puuid directly
                            if not self.db.is_player_processed(puuid):
                                new_entries.append(entry)
                        elif summoner_id:
                            # Old format - needs puuid lookup
                            if not self.db.is_player_processed(f"sid_{summoner_id}"):
                                entry['_needs_puuid'] = True
                                new_entries.append(entry)

                    self.logger.info(f"Found {len(new_entries)} new high elo players")
            except Exception as e:
                self.logger.error(f"Error fetching high elo players: {e}")

        # Add Diamond I players if needed
        if collect_diamond and len(new_entries) < num_players:
            self.logger.info("Fetching Diamond I players...")
            start_page = self.db.get_stat('last_page', 1)
            current_page = start_page
            max_pages = 50

            while len(new_entries) < num_players and current_page <= max_pages:
                entries = self.make_api_request(get_entries, 'league', current_page)

                if not entries:
                    self.logger.warning(f"No more entries found at page {current_page}")
                    break

                # Filter out already processed players
                for entry in entries:
                    puuid = entry.get("puuid")
                    if puuid and not self.db.is_player_processed(puuid):
                        entry['tier'] = 'DIAMOND'
                        new_entries.append(entry)

                self.logger.info(f"Page {current_page}: {len(entries)} players, {len(new_entries)} new so far")
                self.db.update_stat('last_page', current_page)

                current_page += 1

                if len(new_entries) >= num_players:
                    break

        # Limit to requested number
        if len(new_entries) > num_players:
            new_entries = new_entries[:num_players]

        if not new_entries:
            self.logger.warning("No new players found! All players have been processed.")
            self.logger.info("Auto-resetting to re-check for new matches...")

            # Auto-reset the appropriate elo tier
            self.reset_progress(clear_players=True, elo_filter=elo_filter)

            # Re-fetch players after reset
            if collect_master_plus:
                self.logger.info("Re-fetching high elo players after reset...")
                try:
                    high_elo = self.make_api_request(get_high_elo_players, 'league')
                    if high_elo:
                        for entry in high_elo:
                            # New API returns puuid directly
                            puuid = entry.get("puuid")
                            summoner_id = entry.get("summonerId")
                            if puuid or summoner_id:
                                if summoner_id and not puuid:
                                    entry['_needs_puuid'] = True
                                new_entries.append(entry)
                        self.logger.info(f"Found {len(new_entries)} players after reset")
                except Exception as e:
                    self.logger.error(f"Error re-fetching high elo players: {e}")

            if collect_diamond and not new_entries:
                self.logger.info("Re-fetching Diamond I players after reset...")
                entries = self.make_api_request(get_entries, 'league', 1)
                if entries:
                    for entry in entries:
                        puuid = entry.get("puuid")
                        if puuid:
                            entry['tier'] = 'DIAMOND'
                            new_entries.append(entry)
                    self.logger.info(f"Found {len(new_entries)} Diamond I players after reset")

            # If still no entries after reset, return
            if not new_entries:
                self.logger.warning("Still no players found after reset!")
                return self.db.get_match_count()

        self.logger.info(f"Found {len(new_entries)} new players to process")

        # Process each NEW player
        new_matches_total = 0

        for i, entry in enumerate(new_entries):
            tier = entry.get('tier', 'DIAMOND')
            summoner_id = entry.get("summonerId")
            puuid = entry.get("puuid")

            # For Master+ players, we need to fetch PUUID from summonerId
            if entry.get('_needs_puuid') and summoner_id:
                try:
                    summoner_info = self.make_api_request(
                        get_summoner_by_summoner_id, 'account', summoner_id
                    )
                    puuid = summoner_info.get('puuid')
                except Exception as e:
                    self.logger.error(f"Failed to get PUUID for summoner {summoner_id}: {e}")
                    continue

            if not puuid:
                self.logger.warning(f"No PUUID for entry, skipping")
                continue

            # Get account info for display
            try:
                account_info = self.make_api_request(
                    get_account_by_puuid, 'account', puuid
                )
                summoner_name = f"{account_info.get('gameName', 'Unknown')}#{account_info.get('tagLine', 'EUW')}"
            except:
                summoner_name = "Unknown"

            self.logger.info(f"[{i+1}/{len(new_entries)}] [{tier}] Processing {summoner_name}")

            # Get matches for this player
            try:
                match_ids = self.make_api_request(
                    get_matches_by_puuid, 'match', puuid, count=matches_per_player
                )

                if not match_ids:
                    continue

                # Process each match
                new_matches_count = 0

                for match_id in match_ids:
                    # Skip if already in database
                    if match_id in existing_matches:
                        continue

                    try:
                        match_detail = self.make_api_request(
                            get_match_details, 'match', match_id
                        )

                        # Only save ranked solo/duo games (queueId 420)
                        if match_detail and match_detail.get("info", {}).get("queueId") == 420:
                            # Save directly to SQLite
                            if self.db.insert_match(match_detail):
                                existing_matches.add(match_id)
                                new_matches_count += 1
                                new_matches_total += 1

                    except Exception as e:
                        self.logger.error(f"Failed to get match {match_id}: {e}")

                self.logger.info(f"  + Added {new_matches_count} new matches from {summoner_name}")

                # Mark player as processed (use both puuid and summoner_id tracking)
                self.db.save_player_progress(puuid)
                if summoner_id:
                    self.db.save_player_progress(f"sid_{summoner_id}")

                # Save stats periodically
                if i % 5 == 0:
                    self._save_stats()

            except Exception as e:
                self.logger.error(f"Failed to process {summoner_name}: {e}")
                continue

        # Final save
        self._save_stats()
        self.print_stats()

        return self.db.get_match_count()

    def print_stats(self):
        """Print collection statistics"""
        db_stats = self.db.get_stats()

        self.logger.info("\n=== Collection Statistics ===")
        self.logger.info(f"Total API requests: {self.stats['total_requests']}")
        self.logger.info(f"Successful requests: {self.stats['successful_requests']}")
        self.logger.info(f"Rate limit errors: {self.stats['rate_limit_errors']}")
        self.logger.info(f"Other errors: {self.stats['other_errors']}")
        self.logger.info(f"Total unique matches: {db_stats['total_matches']}")
        self.logger.info(f"Players processed: {db_stats['processed_players']}")

    def export_to_csv(self):
        """Export database to CSV files for backward compatibility"""
        try:
            self.logger.info("Exporting matches to CSV...")

            df = self.db.export_to_dataframe()

            if df.empty:
                self.logger.warning("No matches found to export")
                return

            # Save full dataset
            df.to_csv("match_data_from_db.csv", index=False)
            self.logger.info(f"Exported {len(df)} matches to match_data_from_db.csv")

            # Save simplified draft-focused dataset
            draft_columns = [col for col in df.columns if any(x in col.lower() for x in [
                'champion_id', 'champion_name', 'ban', 'win',
                'match_id', 'game_duration', 'first_',
                'kills', 'deaths', 'assists', 'gold', 'cs', 'vision', 'kda'
            ])]

            df_draft = df[draft_columns]
            df_draft.to_csv("draft_data_from_db.csv", index=False)
            self.logger.info(f"Exported draft data to draft_data_from_db.csv")

        except Exception as e:
            self.logger.error(f"Error exporting to CSV: {e}")

    def reset_progress(self, clear_players=True, elo_filter=None):
        """Reset collection progress (keeps existing matches)

        Args:
            clear_players: If True, clear processed players
            elo_filter: If 'diamond', only reset Diamond page tracking.
                       If 'master', only reset Master+ player tracking (sid_ prefixed).
        """
        if elo_filter == 'diamond':
            # Only reset Diamond I page tracking
            self.db.update_stat('last_page', 1)
            self.db.update_stat('last_player_index', 0)
            self.logger.info("Reset Diamond I page tracking.")
        elif elo_filter == 'master':
            # Reset Master+ players - try sid_ prefix first, if none found do full clear
            if clear_players:
                cleared = self.db.clear_processed_players_by_prefix('sid_')
                if cleared == 0:
                    # No sid_ prefixed players found - this means we haven't collected
                    # Master+ yet with the new system. Do a full clear to start fresh.
                    self.logger.info("No sid_ prefixed players found. Doing full reset for Master+ collection.")
                    cleared = self.db.clear_processed_players()
                self.logger.info(f"Cleared {cleared} processed players for Master+ collection.")
        else:
            # Full reset
            self.db.update_stat('last_page', 1)
            self.db.update_stat('last_player_index', 0)
            if clear_players:
                cleared = self.db.clear_processed_players()
                self.logger.info(f"Cleared {cleared} processed players. Will re-fetch their new matches.")

        self.logger.info("Progress reset. Existing matches are preserved.")


def main():
    parser = argparse.ArgumentParser(description='Collect LoL match data with rate limiting (SQLite version)')
    parser.add_argument('--players', type=int, default=50,
                       help='Number of players to process per batch (default: 50)')
    parser.add_argument('--matches', type=int, default=20,
                       help='Matches per player (default: 20)')
    parser.add_argument('--db', type=str, default='data/lol_matches.db',
                       help='SQLite database path (default: data/lol_matches.db)')
    parser.add_argument('--reset', action='store_true',
                       help='Reset progress and start fresh (keeps existing matches)')
    parser.add_argument('--continuous', action='store_true',
                       help='Run continuously until stopped')
    parser.add_argument('--export-csv', action='store_true',
                       help='Export database to CSV files')
    parser.add_argument('--high-elo-only', action='store_true',
                       help='Collect only from Master/GM/Challenger (skip Diamond)')
    parser.add_argument('--elo', type=str, choices=['diamond', 'master'],
                       help='Elo filter for parallel collection: "diamond" for Diamond I only, "master" for Master/GM/Challenger only')
    parser.add_argument('--api-key-index', type=int,
                       help='Index of API key to use (0, 1, etc.) for parallel collection. If not specified, rotates between all keys.')

    args = parser.parse_args()

    collector = DataCollector(db_path=args.db, api_key_index=args.api_key_index)

    if args.reset:
        collector.reset_progress(elo_filter=args.elo)
        print(f"Progress reset{f' for {args.elo}' if args.elo else ''}.")

    if args.export_csv:
        collector.export_to_csv()
        return

    if args.continuous:
        num_keys = get_api_key_count()
        print("=" * 60)
        print("Running in continuous mode. Press Ctrl+C to stop.")
        print(f"Database: {args.db}")

        # Determine mode display
        if args.elo == 'diamond':
            mode_str = "DIAMOND I ONLY (parallel mode)"
        elif args.elo == 'master':
            mode_str = "MASTER+ ONLY (Master/GM/Challenger - parallel mode)"
        elif args.high_elo_only:
            mode_str = "Master/GM/Challenger ONLY"
        else:
            mode_str = "All high elo (Chall/GM/Master/Diamond I)"
        print(f"Mode: {mode_str}")

        # API Key info
        if args.api_key_index is not None:
            print(f"API Key: Using key #{args.api_key_index} (dedicated for this process)")
        else:
            print(f"API Keys: {num_keys} configured {'(🚀 TURBO MODE!)' if num_keys > 1 else '(add more keys in config.py for faster collection)'}")
            if num_keys > 1:
                print(f"  -> Rate limit capacity: {num_keys}x faster!")

        # Parallel mode instructions
        if args.elo and args.api_key_index is not None:
            print("\n📦 PARALLEL MODE ACTIVE")
            print("  Run another terminal with the other elo/key combination!")

        print("=" * 60)
        batch_number = 1

        try:
            while True:
                print(f"\n=== Starting Batch {batch_number} ===")

                # Collect data
                num_matches = collector.collect_matches(
                    args.players, args.matches,
                    high_elo_only=args.high_elo_only,
                    elo_filter=args.elo
                )

                print(f"Batch {batch_number} complete! Total matches in database: {num_matches}")

                # Display API key stats every 10 batches (if using rotation mode)
                if batch_number % 10 == 0 and args.api_key_index is None:
                    rotator = get_key_rotator()
                    stats = rotator.get_key_stats()
                    print("\n📊 API Key Statistics:")
                    for idx, s in stats.items():
                        status = f"⏸️  cooldown {s['cooldown']:.0f}s" if s['cooldown'] > 0 else "✅ available"
                        print(f"  Key #{idx}: {s['success']}/{s['total']} success, {s['errors']} errors ({status})")
                    print()

                # Wait before next batch to be respectful to API
                print("Waiting 60 seconds before next batch...")
                time.sleep(60)

                batch_number += 1

        except KeyboardInterrupt:
            print(f"\n\nStopped by user after {batch_number - 1} batches.")
            stats = collector.db.get_stats()
            print(f"Total matches in database: {stats['total_matches']}")

            # Offer to export to CSV
            print("\nTo export data for ML training:")
            print(f"  python src/collect_data_safe.py --export-csv --db {args.db}")
            print("\nTo prepare data for training:")
            print("  python src/prepare_data.py")

    else:
        # Single run mode
        num_matches = collector.collect_matches(
            args.players, args.matches,
            high_elo_only=args.high_elo_only,
            elo_filter=args.elo
        )

        if num_matches > 0:
            print(f"\nCollection complete! Total matches: {num_matches}")
            print("\nNext steps:")
            print("1. Run: python src/prepare_data.py")
            print("2. Run: python src/draft_predictor.py")


if __name__ == "__main__":
    main()
