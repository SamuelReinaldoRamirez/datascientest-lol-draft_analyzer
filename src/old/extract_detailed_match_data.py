import json
import csv
import pandas as pd
from collections import defaultdict

def extract_detailed_match_data(match_data):
    """
    Extract comprehensive match information including:
    - Champion picks/bans
    - Player stats (KDA, CS, damage, etc.)
    - Objectives (dragons, barons, towers)
    - Gold/XP differences
    - Item builds
    - Runes
    """
    info = match_data.get("info", {})
    metadata = match_data.get("metadata", {})
    participants = info.get("participants", [])
    teams = info.get("teams", [])
    
    # Basic match info
    match_info = {
        "matchId": metadata.get("matchId"),
        "gameCreation": info.get("gameCreation"),
        "gameDuration": info.get("gameDuration"),
        "gameVersion": info.get("gameVersion"),
        "queueId": info.get("queueId"),
        "mapId": info.get("mapId"),
        "gameMode": info.get("gameMode"),
        "gameType": info.get("gameType")
    }
    
    # Team objectives and stats
    for team in teams:
        team_id = team.get("teamId")
        prefix = f"team_{team_id}"
        
        # Objectives
        objectives = team.get("objectives", {})
        match_info[f"{prefix}_first_blood"] = objectives.get("champion", {}).get("first", False)
        match_info[f"{prefix}_first_tower"] = objectives.get("tower", {}).get("first", False)
        match_info[f"{prefix}_first_inhibitor"] = objectives.get("inhibitor", {}).get("first", False)
        match_info[f"{prefix}_first_dragon"] = objectives.get("dragon", {}).get("first", False)
        match_info[f"{prefix}_first_riftHerald"] = objectives.get("riftHerald", {}).get("first", False)
        match_info[f"{prefix}_first_baron"] = objectives.get("baron", {}).get("first", False)
        
        # Kill counts
        match_info[f"{prefix}_dragon_kills"] = objectives.get("dragon", {}).get("kills", 0)
        match_info[f"{prefix}_baron_kills"] = objectives.get("baron", {}).get("kills", 0)
        match_info[f"{prefix}_tower_kills"] = objectives.get("tower", {}).get("kills", 0)
        match_info[f"{prefix}_inhibitor_kills"] = objectives.get("inhibitor", {}).get("kills", 0)
        match_info[f"{prefix}_riftHerald_kills"] = objectives.get("riftHerald", {}).get("kills", 0)
        
        # Bans
        bans = team.get("bans", [])
        for i, ban in enumerate(bans):
            match_info[f"{prefix}_ban_{i+1}_championId"] = ban.get("championId")
            match_info[f"{prefix}_ban_{i+1}_pickTurn"] = ban.get("pickTurn")
        
        # Win/loss
        match_info[f"{prefix}_win"] = team.get("win")
        
        # Early surrender
        match_info[f"{prefix}_teamEarlySurrendered"] = team.get("teamEarlySurrendered", False)
    
    # Individual player stats
    position_map = {
        "TOP": "top",
        "JUNGLE": "jungle", 
        "MIDDLE": "mid",
        "BOTTOM": "adc",
        "UTILITY": "support"
    }
    
    for participant in participants:
        team_id = participant.get("teamId")
        position = position_map.get(participant.get("teamPosition"), participant.get("teamPosition"))
        prefix = f"team_{team_id}_{position}"
        
        # Champion info
        match_info[f"{prefix}_championId"] = participant.get("championId")
        match_info[f"{prefix}_championName"] = participant.get("championName")
        match_info[f"{prefix}_champLevel"] = participant.get("champLevel")
        
        # Summoner spells
        match_info[f"{prefix}_summoner1Id"] = participant.get("summoner1Id")
        match_info[f"{prefix}_summoner2Id"] = participant.get("summoner2Id")
        
        # KDA
        match_info[f"{prefix}_kills"] = participant.get("kills")
        match_info[f"{prefix}_deaths"] = participant.get("deaths")
        match_info[f"{prefix}_assists"] = participant.get("assists")
        
        # Damage stats
        match_info[f"{prefix}_totalDamageDealt"] = participant.get("totalDamageDealt")
        match_info[f"{prefix}_totalDamageDealtToChampions"] = participant.get("totalDamageDealtToChampions")
        match_info[f"{prefix}_totalDamageTaken"] = participant.get("totalDamageTaken")
        match_info[f"{prefix}_trueDamageDealt"] = participant.get("trueDamageDealt")
        match_info[f"{prefix}_physicalDamageDealt"] = participant.get("physicalDamageDealt")
        match_info[f"{prefix}_magicDamageDealt"] = participant.get("magicDamageDealt")
        
        # Gold and CS
        match_info[f"{prefix}_goldEarned"] = participant.get("goldEarned")
        match_info[f"{prefix}_totalMinionsKilled"] = participant.get("totalMinionsKilled")
        match_info[f"{prefix}_neutralMinionsKilled"] = participant.get("neutralMinionsKilled")
        
        # Vision
        match_info[f"{prefix}_visionScore"] = participant.get("visionScore")
        match_info[f"{prefix}_wardsPlaced"] = participant.get("wardsPlaced")
        match_info[f"{prefix}_wardsKilled"] = participant.get("wardsKilled")
        match_info[f"{prefix}_visionWardsBoughtInGame"] = participant.get("visionWardsBoughtInGame")
        
        # Crowd control
        match_info[f"{prefix}_enemyChampionImmobilizations"] = participant.get("enemyChampionImmobilizations", 0)
        
        # Items
        for i in range(7):
            match_info[f"{prefix}_item{i}"] = participant.get(f"item{i}")
        
        # Runes
        perks = participant.get("perks", {})
        styles = perks.get("styles", [])
        
        if len(styles) > 0:
            # Primary rune tree
            primary = styles[0]
            match_info[f"{prefix}_primaryStyle"] = primary.get("style")
            primary_perks = primary.get("selections", [])
            for i, perk in enumerate(primary_perks):
                match_info[f"{prefix}_primaryPerk{i}"] = perk.get("perk")
            
            # Secondary rune tree
            if len(styles) > 1:
                secondary = styles[1]
                match_info[f"{prefix}_secondaryStyle"] = secondary.get("style")
                secondary_perks = secondary.get("selections", [])
                for i, perk in enumerate(secondary_perks):
                    match_info[f"{prefix}_secondaryPerk{i}"] = perk.get("perk")
        
        # Additional stats
        match_info[f"{prefix}_firstBloodKill"] = participant.get("firstBloodKill")
        match_info[f"{prefix}_firstTowerKill"] = participant.get("firstTowerKill")
        match_info[f"{prefix}_turretKills"] = participant.get("turretKills")
        match_info[f"{prefix}_inhibitorKills"] = participant.get("inhibitorKills")
        match_info[f"{prefix}_largestKillingSpree"] = participant.get("largestKillingSpree")
        match_info[f"{prefix}_largestMultiKill"] = participant.get("largestMultiKill")
        match_info[f"{prefix}_killingSprees"] = participant.get("killingSprees")
        match_info[f"{prefix}_doubleKills"] = participant.get("doubleKills")
        match_info[f"{prefix}_tripleKills"] = participant.get("tripleKills")
        match_info[f"{prefix}_quadraKills"] = participant.get("quadraKills")
        match_info[f"{prefix}_pentaKills"] = participant.get("pentaKills")
        
        # Challenges (advanced metrics)
        challenges = participant.get("challenges", {})
        match_info[f"{prefix}_damagePerMinute"] = challenges.get("damagePerMinute")
        match_info[f"{prefix}_damageTakenOnTeamPercentage"] = challenges.get("damageTakenOnTeamPercentage")
        match_info[f"{prefix}_goldPerMinute"] = challenges.get("goldPerMinute")
        match_info[f"{prefix}_teamDamagePercentage"] = challenges.get("teamDamagePercentage")
        match_info[f"{prefix}_killParticipation"] = challenges.get("killParticipation")
        match_info[f"{prefix}_kda"] = challenges.get("kda")
        match_info[f"{prefix}_laneMinionsFirst10Minutes"] = challenges.get("laneMinionsFirst10Minutes")
        match_info[f"{prefix}_turretPlatesTaken"] = challenges.get("turretPlatesTaken")
        match_info[f"{prefix}_soloKills"] = challenges.get("soloKills")
        
    return match_info

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

def save_detailed_dataset(match_data_list, output_file):
    """
    Save detailed match data to CSV
    """
    if not match_data_list:
        print("No match data to save")
        return
    
    # Convert to dataframe for easier handling
    df = pd.DataFrame(match_data_list)
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"Detailed dataset saved to {output_file}")
    print(f"Shape: {df.shape[0]} matches x {df.shape[1]} features")
    
    # Print some statistics
    if 'team_100_win' in df.columns:
        team_100_wins = df['team_100_win'].sum()
        print(f"Team 100 wins: {team_100_wins}/{len(df)} ({team_100_wins/len(df)*100:.1f}%)")

def main():
    # Read match details from both files
    detailed_data = []
    
    # Try to read from extended file first
    try:
        matches = read_match_details_from_txt("match_details_extended.txt")
        print(f"Read {len(matches)} matches from extended file")
        
        for match in matches:
            detailed_data.append(extract_detailed_match_data(match))
    except FileNotFoundError:
        print("Extended file not found, using original file")
    
    # Also read from original file
    try:
        matches = read_match_details_from_txt("match_details.txt")
        print(f"Read {len(matches)} matches from original file")
        
        for match in matches:
            detailed_data.append(extract_detailed_match_data(match))
    except FileNotFoundError:
        print("Original file not found")
    
    # Save detailed dataset
    if detailed_data:
        save_detailed_dataset(detailed_data, "match_data_detailed.csv")
        
        # Also create a simplified version focused on draft
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
        print("\nAlso saved draft-focused data to draft_data_with_bans.csv")

if __name__ == "__main__":
    main()