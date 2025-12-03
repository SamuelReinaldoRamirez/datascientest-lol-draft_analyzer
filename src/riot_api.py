# import requests
# from config import API_KEY, REGION, QUEUE, TIER, DIVISION

# HEADERS = {"X-Riot-Token": API_KEY}

# # 1️⃣ Entrées Diamond I
# def get_entries(page=1):
#     url = f"https://{REGION}.api.riotgames.com/lol/league/v4/entries/{QUEUE}/{TIER}/{DIVISION}?page={page}"
#     r = requests.get(url, headers=HEADERS)
#     r.raise_for_status()
#     return r.json()

# # 2️⃣ Détails d'une ligue par leagueId
# def get_league(league_id):
#     url = f"https://{REGION}.api.riotgames.com/lol/league/v4/leagues/{league_id}"
#     r = requests.get(url, headers=HEADERS)
#     r.raise_for_status()
#     return r.json()

# # 3️⃣ Match IDs par PUUID (Match API v5)
# def get_matches_by_puuid(puuid, count=5):
#     url = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?count={count}"
#     r = requests.get(url, headers=HEADERS)
#     r.raise_for_status()
#     return r.json()

# # 4️⃣ Détails d’un match
# def get_match_details(match_id):
#     url = f"https://europe.api.riotgames.com/lol/match/v5/matches/{match_id}"
#     r = requests.get(url, headers=HEADERS)
#     r.raise_for_status()
#     return r.json()



import requests
from config import API_KEY, REGION, QUEUE, TIER, DIVISION

HEADERS = {"X-Riot-Token": API_KEY}

# 1️⃣ Entrées Diamond I (paginated)
def get_entries(page=1):
    url = f"https://{REGION}.api.riotgames.com/lol/league/v4/entries/{QUEUE}/{TIER}/{DIVISION}?page={page}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

# 2️⃣ Challenger League (all players in one call)
def get_challenger_league():
    """Get all Challenger players (returns full league with ~300 players)"""
    url = f"https://{REGION}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/{QUEUE}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    # Return entries with summonerId -> need to get PUUID separately
    return data.get('entries', [])

# 3️⃣ Grandmaster League (all players in one call)
def get_grandmaster_league():
    """Get all Grandmaster players (returns full league with ~700 players)"""
    url = f"https://{REGION}.api.riotgames.com/lol/league/v4/grandmasterleagues/by-queue/{QUEUE}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    return data.get('entries', [])

# 4️⃣ Master League (all players in one call)
def get_master_league():
    """Get all Master players (returns full league with ~3000+ players)"""
    url = f"https://{REGION}.api.riotgames.com/lol/league/v4/masterleagues/by-queue/{QUEUE}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    return data.get('entries', [])

# 5️⃣ Get all high elo players (Challenger + GM + Master + Diamond I)
def get_high_elo_players():
    """
    Get players from all high elo tiers.
    Returns list of entries with tier info added.
    """
    all_players = []

    # Challenger
    try:
        challenger = get_challenger_league()
        for p in challenger:
            p['tier'] = 'CHALLENGER'
        all_players.extend(challenger)
        print(f"  Found {len(challenger)} Challenger players")
    except Exception as e:
        print(f"  Error fetching Challenger: {e}")

    # Grandmaster
    try:
        grandmaster = get_grandmaster_league()
        for p in grandmaster:
            p['tier'] = 'GRANDMASTER'
        all_players.extend(grandmaster)
        print(f"  Found {len(grandmaster)} Grandmaster players")
    except Exception as e:
        print(f"  Error fetching Grandmaster: {e}")

    # Master
    try:
        master = get_master_league()
        for p in master:
            p['tier'] = 'MASTER'
        all_players.extend(master)
        print(f"  Found {len(master)} Master players")
    except Exception as e:
        print(f"  Error fetching Master: {e}")

    return all_players

# 6️⃣ Détails d'une ligue par leagueId
def get_league(league_id):
    url = f"https://{REGION}.api.riotgames.com/lol/league/v4/leagues/{league_id}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

# 3️⃣ Match IDs par PUUID (Match API v5)
def get_matches_by_puuid(puuid, count=5):
    url = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?count={count}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

# 4️⃣ Détails d’un match
def get_match_details(match_id):
    url = f"https://europe.api.riotgames.com/lol/match/v5/matches/{match_id}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()


def get_summoner_by_puuid(puuid):
    url = f"https://{REGION}.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()


def get_summoner_by_summoner_id(summoner_id):
    """Get summoner info (including PUUID) from summonerId"""
    url = f"https://{REGION}.api.riotgames.com/lol/summoner/v4/summoners/{summoner_id}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()


def get_summoner_by_name(summoner_name):
    """
    Récupère les informations d'un invocateur à partir de son Summoner Name
    """
    url = f"https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name}"
    # url = f"https://{REGION}.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()


def get_account_by_puuid(puuid):
    """
    Récupère les informations de compte Riot (Riot ID + Tag Line) à partir d'un PUUID
    via l'endpoint /riot/account/v1/accounts/by-puuid/{puuid}.
    """
    url = f"https://europe.api.riotgames.com/riot/account/v1/accounts/by-puuid/{puuid}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()


# endpoint for summoner by name:
#      /riot/account/v1/accounts/by-puuid/{puuid} 


