import time
import json
import csv
from riot_api import get_entries, get_matches_by_puuid, get_match_details, get_account_by_puuid

def collect_match_data(num_players=50, matches_per_player=20):
    """
    Collect more match data for training
    """
    print(f"=== Collecting data from {num_players} players, {matches_per_player} matches each ===")
    
    all_entries = []
    unique_matches = set()
    match_details_list = []
    
    # Collect players from multiple pages
    pages_needed = (num_players // 205) + 1  # ~205 players per page
    
    for page in range(1, pages_needed + 1):
        entries = get_entries(page)
        if not entries:
            break
        all_entries.extend(entries)
        print(f"Page {page} retrieved: {len(entries)} players")
        
        if len(all_entries) >= num_players:
            all_entries = all_entries[:num_players]
            break
    
    print(f"\nTotal players to process: {len(all_entries)}")
    
    # Keep track of API requests for rate limiting
    request_count = 0
    request_timestamps = []
    
    # Collect matches from each player
    for i, entry in enumerate(all_entries):
        puuid = entry.get("puuid")
        
        # Get summoner name via account API
        try:
            account_info = get_account_by_puuid(puuid)
            summoner_name = f"{account_info.get('gameName', 'Unknown')}#{account_info.get('tagLine', 'EUW')}"
            time.sleep(0.5)
        except:
            summoner_name = "Unknown"
        
        print(f"\n[{i+1}/{len(all_entries)}] Processing {summoner_name}")
        
        try:
            # Rate limit check
            current_time = time.time()
            request_timestamps = [t for t in request_timestamps if current_time - t < 120]
            
            if len(request_timestamps) >= 95:
                wait_time = 120 - (current_time - request_timestamps[0]) + 1
                print(f"  ⏳ Rate limit approaching, waiting {wait_time:.0f} seconds...")
                time.sleep(wait_time)
                request_timestamps = []
            
            # Get match IDs
            match_ids = get_matches_by_puuid(puuid, count=matches_per_player)
            request_timestamps.append(time.time())
            request_count += 1
            
            for j, match_id in enumerate(match_ids):
                if match_id not in unique_matches:
                    unique_matches.add(match_id)
                    
                    try:
                        # Rate limit check again
                        current_time = time.time()
                        request_timestamps = [t for t in request_timestamps if current_time - t < 120]
                        
                        if len(request_timestamps) >= 95:
                            wait_time = 120 - (current_time - request_timestamps[0]) + 1
                            print(f"  ⏳ Rate limit approaching, waiting {wait_time:.0f} seconds...")
                            time.sleep(wait_time)
                            request_timestamps = []
                        
                        # Get match details
                        match_detail = get_match_details(match_id)
                        request_timestamps.append(time.time())
                        request_count += 1
                        
                        # Filter for ranked solo queue matches
                        if match_detail.get("info", {}).get("queueId") == 420:  # Ranked Solo/Duo
                            match_details_list.append(match_detail)
                            print(f"  ✓ Match {match_id} added (Total: {len(match_details_list)})")
                        
                        time.sleep(1.3)  # Base delay between requests
                        
                    except Exception as e:
                        if "429" in str(e):
                            print(f"  ⏳ Rate limited, waiting 30 seconds...")
                            time.sleep(30)
                        else:
                            print(f"  ✗ Error getting match {match_id}: {e}")
                            time.sleep(2)
                        
        except Exception as e:
            if "429" in str(e):
                print(f"  ⏳ Rate limited, waiting 30 seconds...")
                time.sleep(30)
            else:
                print(f"  ✗ Error getting matches for {summoner_name}: {e}")
                time.sleep(2)
    
    # Save match details to file
    output_file = "match_details_extended.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        for match in match_details_list:
            f.write(f"=== Détails du match {match['metadata']['matchId']} ===\n")
            f.write(json.dumps(match, indent=2, ensure_ascii=False))
            f.write("\n\n")
    
    print(f"\n=== Collection Complete ===")
    print(f"Total unique matches collected: {len(match_details_list)}")
    print(f"Match details saved to: {output_file}")
    
    return len(match_details_list)

def update_draft_dataset():
    """
    Update the draft dataset with new matches
    """
    from extract_draft_data import read_match_details_from_txt, create_draft_dataset, save_draft_dataset
    
    # Read new matches
    matches = read_match_details_from_txt("match_details_extended.txt")
    print(f"Read {len(matches)} matches from extended file")
    
    # Extract draft data
    draft_data = create_draft_dataset(matches)
    
    # Save to new file
    save_draft_dataset(draft_data, "draft_dataset_extended.csv")
    
    # Print statistics
    if draft_data:
        team_100_wins = sum(1 for d in draft_data if d["team_100_win"] == 1)
        print(f"Team 100 wins: {team_100_wins}/{len(draft_data)} ({team_100_wins/len(draft_data)*100:.1f}%)")

def main():
    # Collect more matches
    num_matches = collect_match_data(num_players=50, matches_per_player=20)
    
    if num_matches > 0:
        # Update draft dataset
        print("\n=== Updating Draft Dataset ===")
        update_draft_dataset()
        
        print("\nTo retrain the model with new data:")
        print("1. Update draft_predictor.py to use 'draft_dataset_extended.csv'")
        print("2. Run: python src/draft_predictor.py")

if __name__ == "__main__":
    main()