import json
import csv
import os
from collections import defaultdict

def extract_draft_from_match(match_data):
    """
    Extract draft information from a single match
    Returns: dict with team compositions and winner
    """
    info = match_data.get("info", {})
    participants = info.get("participants", [])
    
    # Separate players by team
    team_100 = []
    team_200 = []
    team_100_win = None
    
    for player in participants:
        champion_id = player.get("championId")
        champion_name = player.get("championName")
        team_id = player.get("teamId")
        win = player.get("win")
        position = player.get("teamPosition")
        
        player_info = {
            "championId": champion_id,
            "championName": champion_name,
            "position": position,
            "win": win
        }
        
        if team_id == 100:
            team_100.append(player_info)
            if team_100_win is None:
                team_100_win = win
        elif team_id == 200:
            team_200.append(player_info)
    
    return {
        "matchId": match_data.get("metadata", {}).get("matchId"),
        "gameDuration": info.get("gameDuration"),
        "team_100": team_100,
        "team_200": team_200,
        "team_100_win": team_100_win
    }

def read_match_details_from_txt(filepath):
    """
    Read match details from the txt file
    """
    matches = []
    current_match = ""
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if line.startswith("=== Détails du match"):
                if current_match:
                    try:
                        match_data = json.loads(current_match)
                        matches.append(match_data)
                    except json.JSONDecodeError:
                        print(f"Error parsing match JSON")
                current_match = ""
            elif line.strip() and not line.startswith("==="):
                current_match += line
    
    # Don't forget the last match
    if current_match:
        try:
            match_data = json.loads(current_match)
            matches.append(match_data)
        except json.JSONDecodeError:
            print(f"Error parsing last match JSON")
    
    return matches

def create_draft_dataset(matches):
    """
    Create a dataset with draft features
    """
    draft_data = []
    
    for match in matches:
        draft_info = extract_draft_from_match(match)
        
        # Skip if incomplete data
        if not draft_info["team_100"] or not draft_info["team_200"]:
            continue
        
        # Create a row with champion IDs for each position
        row = {
            "matchId": draft_info["matchId"],
            "gameDuration": draft_info["gameDuration"],
            "team_100_win": 1 if draft_info["team_100_win"] else 0
        }
        
        # Map positions
        position_map = {
            "TOP": "top",
            "JUNGLE": "jungle", 
            "MIDDLE": "mid",
            "BOTTOM": "adc",
            "UTILITY": "support"
        }
        
        # Add team 100 champions
        for player in draft_info["team_100"]:
            pos = position_map.get(player["position"], player["position"])
            row[f"team_100_{pos}_champion"] = player["championId"]
            row[f"team_100_{pos}_name"] = player["championName"]
        
        # Add team 200 champions
        for player in draft_info["team_200"]:
            pos = position_map.get(player["position"], player["position"])
            row[f"team_200_{pos}_champion"] = player["championId"]
            row[f"team_200_{pos}_name"] = player["championName"]
        
        draft_data.append(row)
    
    return draft_data

def save_draft_dataset(draft_data, output_file):
    """
    Save draft dataset to CSV
    """
    if not draft_data:
        print("No draft data to save")
        return
    
    # Get all keys for CSV headers
    all_keys = set()
    for row in draft_data:
        all_keys.update(row.keys())
    
    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=sorted(all_keys))
        writer.writeheader()
        writer.writerows(draft_data)
    
    print(f"Draft dataset saved to {output_file} with {len(draft_data)} matches")

def main():
    # Read match details
    matches = read_match_details_from_txt("match_details.txt")
    print(f"Read {len(matches)} matches from file")
    
    # Extract draft data
    draft_data = create_draft_dataset(matches)
    
    # Save to CSV
    save_draft_dataset(draft_data, "draft_dataset.csv")
    
    # Print some statistics
    if draft_data:
        team_100_wins = sum(1 for d in draft_data if d["team_100_win"] == 1)
        print(f"Team 100 wins: {team_100_wins}/{len(draft_data)} ({team_100_wins/len(draft_data)*100:.1f}%)")

if __name__ == "__main__":
    main()