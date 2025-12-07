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
import time
from config import API_KEY, API_KEYS, REGION, QUEUE, TIER, DIVISION

# Smart API Key rotation with automatic failover on rate limits
class SmartKeyRotator:
    """
    Smart API key rotator with automatic failover on rate limits.

    Features:
    - Tracks state of each API key (available / in cooldown)
    - Automatically skips rate-limited keys
    - Exponential backoff per key
    - Statistics tracking per key
    """
    def __init__(self, api_keys):
        self.api_keys = [k for k in api_keys if k and not k.startswith('#')]
        self.key_count = len(self.api_keys)
        self.current_index = 0

        # Track state per key
        self.key_states = {}
        for i in range(self.key_count):
            self.key_states[i] = {
                'cooldown_until': 0,
                'last_429_time': None,
                'error_count': 0,
                'total_requests': 0,
                'successful_requests': 0
            }

    def get_next_available_key(self):
        """
        Get next key that's not in cooldown.

        Returns:
            tuple: (key_index, key) if available
                   (key_index, key, wait_time) if all keys in cooldown
        """
        current_time = time.time()

        # Try keys starting from current index
        for offset in range(self.key_count):
            idx = (self.current_index + offset) % self.key_count
            state = self.key_states[idx]

            # Skip if in cooldown
            if current_time < state['cooldown_until']:
                continue

            # Found available key
            self.current_index = (idx + 1) % self.key_count
            state['total_requests'] += 1
            return idx, self.api_keys[idx]

        # All keys in cooldown - return the one with shortest wait
        best_idx = min(self.key_states.keys(),
                      key=lambda k: self.key_states[k]['cooldown_until'])
        wait_time = self.key_states[best_idx]['cooldown_until'] - current_time

        return best_idx, self.api_keys[best_idx], wait_time

    def mark_key_rate_limited(self, key_index, retry_after=None):
        """
        Mark a specific key as rate-limited.

        Args:
            key_index: Index of the key that was rate-limited
            retry_after: Optional retry-after value from response header

        Returns:
            float: Cooldown duration in seconds
        """
        state = self.key_states[key_index]
        state['error_count'] += 1
        state['last_429_time'] = time.time()

        # Calculate cooldown
        if retry_after:
            cooldown = float(retry_after)
        else:
            # Exponential backoff: 2, 4, 8, 16, 32, max 60s
            cooldown = min(2 ** state['error_count'], 60)

        state['cooldown_until'] = time.time() + cooldown
        return cooldown

    def mark_key_success(self, key_index):
        """Mark successful request - gradually reset error count"""
        state = self.key_states[key_index]
        state['successful_requests'] += 1

        # Reset error count after 5 successful requests
        if state['successful_requests'] % 5 == 0:
            state['error_count'] = max(0, state['error_count'] - 1)

    def get_key_stats(self):
        """Get statistics for all keys"""
        return {
            i: {
                'total': s['total_requests'],
                'success': s['successful_requests'],
                'errors': s['error_count'],
                'cooldown': max(0, s['cooldown_until'] - time.time())
            }
            for i, s in self.key_states.items()
        }

    # Legacy methods for backward compatibility
    def get_next_key(self):
        """Get the next API key (legacy compatibility)"""
        result = self.get_next_available_key()
        if len(result) == 2:
            return result[1]  # Return just the key
        else:
            return result[1]  # Return key even if all in cooldown

    def get_headers(self):
        """Get headers with the next API key (legacy compatibility)"""
        return {"X-Riot-Token": self.get_next_key()}

# Initialize rotator with all keys
_key_rotator = SmartKeyRotator(API_KEYS)

# For backward compatibility (single key usage)
HEADERS = {"X-Riot-Token": API_KEY}

def get_key_rotator():
    """Get the global key rotator instance"""
    return _key_rotator

def get_api_key_count():
    """Get number of configured API keys"""
    return _key_rotator.key_count

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


