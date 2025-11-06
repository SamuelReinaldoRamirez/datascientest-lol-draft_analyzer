import time
import json
import csv
import argparse
import os
from collections import deque
from datetime import datetime
import logging
from riot_api import get_entries, get_matches_by_puuid, get_match_details, get_account_by_puuid
from extract_detailed_match_data import read_match_details_from_txt, extract_detailed_match_data, save_detailed_dataset

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
    def __init__(self, progress_file='collection_progress.json'):
        self.rate_limiter = RateLimiter()
        self.progress_file = progress_file
        self.progress = self.load_progress()
        
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
    
    def load_progress(self):
        """Load progress from file if exists"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        
        return {
            'processed_players': [],
            'collected_matches': {},
            'last_page': 1,
            'last_player_index': 0,
            'stats': {
                'total_requests': 0,
                'successful_requests': 0,
                'rate_limit_errors': 0,
                'other_errors': 0
            }
        }
    
    def save_progress(self):
        """Save current progress to file"""
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    def wait_for_rate_limit(self, endpoint='default'):
        """Wait if necessary to respect rate limits"""
        can_proceed, wait_time = self.rate_limiter.can_make_request(endpoint)
        
        if not can_proceed:
            self.logger.info(f"Rate limit approaching, waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
    
    def make_api_request(self, func, endpoint='default', *args, **kwargs):
        """Make API request with rate limiting and error handling"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Wait for rate limit if necessary
                self.wait_for_rate_limit(endpoint)
                
                # Make request
                self.rate_limiter.record_request(endpoint)
                self.progress['stats']['total_requests'] += 1
                
                result = func(*args, **kwargs)
                
                self.progress['stats']['successful_requests'] += 1
                self.rate_limiter.reset_error_count()
                
                # Small delay between requests
                time.sleep(0.05)
                
                return result
                
            except Exception as e:
                error_msg = str(e)
                
                if '429' in error_msg:
                    self.progress['stats']['rate_limit_errors'] += 1
                    retry_after = None
                    
                    # Try to extract retry-after from error
                    if hasattr(e, 'response') and e.response:
                        retry_after = e.response.headers.get('Retry-After')
                    
                    wait_time = self.rate_limiter.handle_429_error(retry_after)
                    self.logger.warning(f"Rate limited (429), waiting {wait_time}s...")
                    time.sleep(wait_time)
                    
                else:
                    self.progress['stats']['other_errors'] += 1
                    self.logger.error(f"API error (attempt {attempt + 1}/{max_retries}): {error_msg}")
                    
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        raise
        
        return None
    
    def collect_matches(self, num_players=50, matches_per_player=20):
        """Collect matches with proper rate limiting and progress tracking"""
        self.logger.info(f"Starting collection: {num_players} players, {matches_per_player} matches each")
        
        # Load existing match data
        existing_matches = set(self.progress['collected_matches'].keys())
        match_details_list = []
        
        # Resume from last position
        start_page = self.progress['last_page']
        start_index = self.progress['last_player_index']
        
        all_entries = []
        pages_needed = (num_players // 205) + 1
        
        # Collect players
        for page in range(start_page, pages_needed + 1):
            entries = self.make_api_request(get_entries, 'league', page)
            
            if not entries:
                break
                
            all_entries.extend(entries)
            self.logger.info(f"Page {page} retrieved: {len(entries)} players")
            self.progress['last_page'] = page
            
            if len(all_entries) >= num_players:
                all_entries = all_entries[:num_players]
                break
        
        # Process each player
        for i, entry in enumerate(all_entries[start_index:], start=start_index):
            puuid = entry.get("puuid")
            
            # Skip if already processed
            if puuid in self.progress['processed_players']:
                continue
            
            # Get account info
            try:
                account_info = self.make_api_request(
                    get_account_by_puuid, 'account', puuid
                )
                summoner_name = f"{account_info.get('gameName', 'Unknown')}#{account_info.get('tagLine', 'EUW')}"
            except:
                summoner_name = "Unknown"
            
            self.logger.info(f"[{i+1}/{num_players}] Processing {summoner_name}")
            
            # Get matches
            try:
                match_ids = self.make_api_request(
                    get_matches_by_puuid, 'match', puuid, count=matches_per_player
                )
                
                if not match_ids:
                    continue
                
                # Process each match
                new_matches_count = 0
                for match_id in match_ids:
                    if match_id in existing_matches:
                        continue
                    
                    try:
                        match_detail = self.make_api_request(
                            get_match_details, 'match', match_id
                        )
                        
                        if match_detail and match_detail.get("info", {}).get("queueId") == 420:
                            self.progress['collected_matches'][match_id] = True
                            match_details_list.append(match_detail)
                            existing_matches.add(match_id)
                            new_matches_count += 1
                            
                    except Exception as e:
                        self.logger.error(f"Failed to get match {match_id}: {e}")
                
                self.logger.info(f"  ✓ Added {new_matches_count} new matches from {summoner_name}")
                
                # Mark player as processed
                self.progress['processed_players'].append(puuid)
                self.progress['last_player_index'] = i + 1
                
                # Save progress periodically
                if i % 5 == 0:
                    self.save_progress()
                    self.save_matches_incremental(match_details_list)
                    match_details_list = []  # Clear to save memory
                    
            except Exception as e:
                self.logger.error(f"Failed to process {summoner_name}: {e}")
                continue
        
        # Save final matches
        if match_details_list:
            self.save_matches_incremental(match_details_list)
        
        self.save_progress()
        self.print_stats()
        
        return len(self.progress['collected_matches'])
    
    def save_matches_incremental(self, matches):
        """Save matches incrementally to avoid memory issues"""
        if not matches:
            return
            
        output_file = "match_details_extended.txt"
        mode = 'a' if os.path.exists(output_file) else 'w'
        
        with open(output_file, mode, encoding='utf-8') as f:
            for match in matches:
                f.write(f"=== Détails du match {match['metadata']['matchId']} ===\n")
                f.write(json.dumps(match, indent=2, ensure_ascii=False))
                f.write("\n\n")
    
    def extract_to_csv(self):
        """Extract collected matches to CSV files"""
        try:
            self.logger.info("Extracting matches to CSV...")
            
            # Read matches from the extended file
            matches = read_match_details_from_txt("match_details_extended.txt")
            if not matches:
                self.logger.warning("No matches found to extract")
                return
            
            # Extract detailed data
            detailed_data = []
            for match in matches:
                detailed_data.append(extract_detailed_match_data(match))
            
            # Save full detailed dataset
            save_detailed_dataset(detailed_data, "match_data_detailed.csv")
            
            # Save simplified dataset with key columns
            draft_columns = [col for col in detailed_data[0].keys() if 
                            'championId' in col or 'championName' in col or 
                            'ban' in col or 'win' in col or 
                            'matchId' in col or 'gameDuration' in col or
                            'first_' in col or '_kills' in col or
                            'goldEarned' in col or 'totalMinionsKilled' in col or
                            'visionScore' in col or 'enemyChampionImmobilizations' in col or
                            'teamEarlySurrendered' in col or 'kda' in col]
            
            draft_data = [{k: v for k, v in match.items() if k in draft_columns} 
                         for match in detailed_data]
            save_detailed_dataset(draft_data, "draft_data_with_bans.csv")
            
            self.logger.info(f"Successfully extracted {len(detailed_data)} matches to CSV files")
            
        except Exception as e:
            self.logger.error(f"Error extracting to CSV: {e}")

    def print_stats(self):
        """Print collection statistics"""
        stats = self.progress['stats']
        self.logger.info("\n=== Collection Statistics ===")
        self.logger.info(f"Total API requests: {stats['total_requests']}")
        self.logger.info(f"Successful requests: {stats['successful_requests']}")
        self.logger.info(f"Rate limit errors: {stats['rate_limit_errors']}")
        self.logger.info(f"Other errors: {stats['other_errors']}")
        self.logger.info(f"Total unique matches: {len(self.progress['collected_matches'])}")
        self.logger.info(f"Players processed: {len(self.progress['processed_players'])}")

def main():
    parser = argparse.ArgumentParser(description='Collect LoL match data with rate limiting')
    parser.add_argument('--players', type=int, default=50, help='Number of players to process per batch')
    parser.add_argument('--matches', type=int, default=20, help='Matches per player')
    parser.add_argument('--resume', action='store_true', help='Resume from previous progress')
    parser.add_argument('--reset', action='store_true', help='Reset progress and start fresh')
    parser.add_argument('--continuous', action='store_true', help='Run continuously until stopped')
    
    args = parser.parse_args()
    
    collector = DataCollector()
    
    if args.reset and os.path.exists(collector.progress_file):
        os.remove(collector.progress_file)
        collector.progress = collector.load_progress()
        print("Progress reset.")
    
    if args.continuous:
        print("Running in continuous mode. Press Ctrl+C to stop.")
        batch_number = 1
        
        try:
            while True:
                print(f"\n=== Starting Batch {batch_number} ===")
                
                # Collect data
                num_matches = collector.collect_matches(args.players, args.matches)
                
                print(f"Batch {batch_number} complete! Collected {num_matches} total matches so far.")
                
                # Extract to CSV after each batch
                collector.extract_to_csv()
                
                # Wait before next batch to be respectful to API
                print("Waiting 60 seconds before next batch...")
                time.sleep(60)
                
                batch_number += 1
                
        except KeyboardInterrupt:
            print(f"\n\nStopped by user after {batch_number - 1} batches.")
            print(f"Total matches collected: {len(collector.progress['collected_matches'])}")
            
            # Final extraction to CSV
            collector.extract_to_csv()
            
            print("\nFinal CSV files updated:")
            print("- match_data_detailed.csv (full dataset)")
            print("- draft_data_with_bans.csv (simplified dataset)")
            print("\nReady for AI training:")
            print("python src/draft_predictor.py")
    else:
        # Single run mode
        num_matches = collector.collect_matches(args.players, args.matches)
        
        if num_matches > 0:
            print(f"\nCollection complete! Total matches: {num_matches}")
            print("\nNext steps:")
            print("1. Run: python src/extract_draft_data.py")
            print("2. Run: python src/draft_predictor.py")

if __name__ == "__main__":
    main()