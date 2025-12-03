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
from config import API_KEY, API_KEYS, REGION, QUEUE, TIER, DIVISION

# API Key rotation for faster collection
class APIKeyRotator:
    """
    Rotates between multiple API keys to double (or more) the rate limit capacity.
    Each key has its own rate limits, so using 2 keys = 2x speed.
    """
    def __init__(self, api_keys):
        self.api_keys = [k for k in api_keys if k and not k.startswith('#')]
        self.current_index = 0
        self.key_count = len(self.api_keys)

    def get_next_key(self):
        """Get the next API key in rotation"""
        if self.key_count == 0:
            raise ValueError("No API keys configured!")
        key = self.api_keys[self.current_index]
        self.current_index = (self.current_index + 1) % self.key_count
        return key

    def get_headers(self):
        """Get headers with the next API key"""
        return {"X-Riot-Token": self.get_next_key()}

# Initialize rotator with all keys
_key_rotator = APIKeyRotator(API_KEYS)

# For backward compatibility (single key usage)
HEADERS = {"X-Riot-Token": API_KEY}

def get_rotating_headers():
    """Get headers with rotating API key for faster collection"""
    return _key_rotator.get_headers()

def get_headers_for_key(api_key_index: int = None):
    """
    Get headers for a specific API key index.
    If api_key_index is None, uses rotation.
    """
    if api_key_index is not None:
        if 0 <= api_key_index < len(API_KEYS):
            return {"X-Riot-Token": API_KEYS[api_key_index]}
        else:
            raise ValueError(f"Invalid api_key_index: {api_key_index}. Must be 0-{len(API_KEYS)-1}")
    return get_rotating_headers()

# 1️⃣ Entrées Diamond I (paginated)
def get_entries(page=1, use_rotation=True, api_key_index=None):
    url = f"https://{REGION}.api.riotgames.com/lol/league/v4/entries/{QUEUE}/{TIER}/{DIVISION}?page={page}"
    headers = get_headers_for_key(api_key_index) if api_key_index is not None else (get_rotating_headers() if use_rotation else HEADERS)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

# 2️⃣ Challenger League (all players in one call)
def get_challenger_league(use_rotation=True, api_key_index=None):
    """Get all Challenger players (returns full league with ~300 players)"""
    url = f"https://{REGION}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/{QUEUE}"
    headers = get_headers_for_key(api_key_index) if api_key_index is not None else (get_rotating_headers() if use_rotation else HEADERS)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    data = r.json()
    # Return entries with summonerId -> need to get PUUID separately
    return data.get('entries', [])

# 3️⃣ Grandmaster League (all players in one call)
def get_grandmaster_league(use_rotation=True, api_key_index=None):
    """Get all Grandmaster players (returns full league with ~700 players)"""
    url = f"https://{REGION}.api.riotgames.com/lol/league/v4/grandmasterleagues/by-queue/{QUEUE}"
    headers = get_headers_for_key(api_key_index) if api_key_index is not None else (get_rotating_headers() if use_rotation else HEADERS)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    data = r.json()
    return data.get('entries', [])

# 4️⃣ Master League (all players in one call)
def get_master_league(use_rotation=True, api_key_index=None):
    """Get all Master players (returns full league with ~3000+ players)"""
    url = f"https://{REGION}.api.riotgames.com/lol/league/v4/masterleagues/by-queue/{QUEUE}"
    headers = get_headers_for_key(api_key_index) if api_key_index is not None else (get_rotating_headers() if use_rotation else HEADERS)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    data = r.json()
    return data.get('entries', [])

# 5️⃣ Get all high elo players (Challenger + GM + Master + Diamond I)
def get_high_elo_players(api_key_index=None):
    """
    Get players from all high elo tiers.
    Returns list of entries with tier info added.

    Args:
        api_key_index: Optional index of API key to use (for parallel collection)
    """
    all_players = []

    # Challenger
    try:
        challenger = get_challenger_league(api_key_index=api_key_index)
        for p in challenger:
            p['tier'] = 'CHALLENGER'
        all_players.extend(challenger)
        print(f"  Found {len(challenger)} Challenger players")
    except Exception as e:
        print(f"  Error fetching Challenger: {e}")

    # Grandmaster
    try:
        grandmaster = get_grandmaster_league(api_key_index=api_key_index)
        for p in grandmaster:
            p['tier'] = 'GRANDMASTER'
        all_players.extend(grandmaster)
        print(f"  Found {len(grandmaster)} Grandmaster players")
    except Exception as e:
        print(f"  Error fetching Grandmaster: {e}")

    # Master
    try:
        master = get_master_league(api_key_index=api_key_index)
        for p in master:
            p['tier'] = 'MASTER'
        all_players.extend(master)
        print(f"  Found {len(master)} Master players")
    except Exception as e:
        print(f"  Error fetching Master: {e}")

    return all_players

# 6️⃣ Détails d'une ligue par leagueId
def get_league(league_id, use_rotation=True, api_key_index=None):
    url = f"https://{REGION}.api.riotgames.com/lol/league/v4/leagues/{league_id}"
    headers = get_headers_for_key(api_key_index) if api_key_index is not None else (get_rotating_headers() if use_rotation else HEADERS)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

# 7️⃣ Match IDs par PUUID (Match API v5)
def get_matches_by_puuid(puuid, count=5, use_rotation=True, api_key_index=None):
    url = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?count={count}"
    headers = get_headers_for_key(api_key_index) if api_key_index is not None else (get_rotating_headers() if use_rotation else HEADERS)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

# 8️⃣ Détails d'un match
def get_match_details(match_id, use_rotation=True, api_key_index=None):
    url = f"https://europe.api.riotgames.com/lol/match/v5/matches/{match_id}"
    headers = get_headers_for_key(api_key_index) if api_key_index is not None else (get_rotating_headers() if use_rotation else HEADERS)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


def get_summoner_by_puuid(puuid, use_rotation=True, api_key_index=None):
    url = f"https://{REGION}.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}"
    headers = get_headers_for_key(api_key_index) if api_key_index is not None else (get_rotating_headers() if use_rotation else HEADERS)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


def get_summoner_by_summoner_id(summoner_id, use_rotation=True, api_key_index=None):
    """Get summoner info (including PUUID) from summonerId"""
    url = f"https://{REGION}.api.riotgames.com/lol/summoner/v4/summoners/{summoner_id}"
    headers = get_headers_for_key(api_key_index) if api_key_index is not None else (get_rotating_headers() if use_rotation else HEADERS)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


def get_summoner_by_name(summoner_name, use_rotation=True, api_key_index=None):
    """
    Récupère les informations d'un invocateur à partir de son Summoner Name
    """
    url = f"https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name}"
    headers = get_headers_for_key(api_key_index) if api_key_index is not None else (get_rotating_headers() if use_rotation else HEADERS)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


def get_account_by_puuid(puuid, use_rotation=True, api_key_index=None):
    """
    Récupère les informations de compte Riot (Riot ID + Tag Line) à partir d'un PUUID
    via l'endpoint /riot/account/v1/accounts/by-puuid/{puuid}.
    """
    url = f"https://europe.api.riotgames.com/riot/account/v1/accounts/by-puuid/{puuid}"
    headers = get_headers_for_key(api_key_index) if api_key_index is not None else (get_rotating_headers() if use_rotation else HEADERS)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


def get_api_key_count():
    """Returns the number of API keys configured"""
    return _key_rotator.key_count


# endpoint for summoner by name:
#      /riot/account/v1/accounts/by-puuid/{puuid} 


