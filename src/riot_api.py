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

# 1️⃣ Entrées Diamond I
def get_entries(page=1):
    url = f"https://{REGION}.api.riotgames.com/lol/league/v4/entries/{QUEUE}/{TIER}/{DIVISION}?page={page}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

# 2️⃣ Détails d'une ligue par leagueId
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


def get_summoner_by_name(summoner_name):
    """
    Récupère les informations d'un invocateur à partir de son Summoner Name
    """
    url = f"https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name}"
    # url = f"https://{REGION}.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()



# endpoint for summoner by name:
#      /riot/account/v1/accounts/by-puuid/{puuid} 


