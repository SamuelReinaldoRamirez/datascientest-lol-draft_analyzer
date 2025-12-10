"""
Champion Data Module

Fetches and manages champion metadata from Riot Data Dragon API.
Provides champion classes, damage types, and base statistics for feature engineering.

Usage:
    from champion_data import ChampionData

    champion_data = ChampionData()
    champion_data.load()  # Fetches from Data Dragon or loads from cache

    # Get champion info
    champion_data.get_champion_class(86)  # Returns ['Fighter', 'Tank'] for Garen
    champion_data.get_damage_type(86)     # Returns 'physical'
"""

import json
import os
import requests
from pathlib import Path
from typing import Dict, List, Optional


# ============================================================
# Summoner Spells Mapping (ID -> Name)
# ============================================================
SUMMONER_SPELLS = {
    1: "Cleanse",
    3: "Exhaust",
    4: "Flash",
    6: "Ghost",
    7: "Heal",
    11: "Smite",
    12: "Teleport",
    13: "Clarity",
    14: "Ignite",
    21: "Barrier",
    30: "To the King!",
    31: "Poro Toss",
    32: "Mark",
    39: "Mark",
    54: "Placeholder",
    55: "Placeholder",
}


def get_summoner_spell_name(spell_id: int) -> str:
    """
    Get summoner spell name from ID.

    Args:
        spell_id: Riot summoner spell ID

    Returns:
        Spell name string (e.g., "Flash", "Ignite")
    """
    return SUMMONER_SPELLS.get(spell_id, f"Unknown_{spell_id}")


# ============================================================
# Champion CC Scores (ID -> CC capability score 0-10)
# ============================================================
# Score interpretation:
# 0-2: Minimal CC (assassins, some marksmen)
# 3-4: Light CC (fighters, some mages)
# 5-6: Moderate CC (bruisers, utility mages)
# 7-8: Good CC (supports, control mages)
# 9-10: Heavy CC (tanks, engage supports)
CHAMPION_CC_SCORES = {
    # Heavy CC tanks (9-10)
    111: 10,  # Nautilus - hook, root, slow, knockup
    89: 10,   # Leona - stun, root, slow
    12: 9,    # Alistar - knockup, stun
    201: 9,   # Braum - stun, slow, knockup
    113: 9,   # Sejuani - stun, slow, knockup
    154: 9,   # Zac - knockup, knockback
    57: 9,    # Maokai - root, knockback, slow
    516: 9,   # Ornn - knockup, slow
    78: 8,    # Poppy - stun, knockback, ground
    32: 8,    # Amumu - stun, root
    54: 8,    # Malphite - knockup, slow
    79: 8,    # Gragas - knockback, stun, slow

    # Good CC supports/mages (7-8)
    412: 9,   # Thresh - hook, knockback, slow
    497: 8,   # Rakan - charm, knockup
    350: 8,   # Yuumi - root (when detached)
    25: 7,    # Morgana - root, stun
    99: 7,    # Lux - root, slow
    117: 7,   # Lulu - polymorph, slow, knockup
    40: 7,    # Janna - knockup, slow, knockback
    267: 7,   # Nami - knockup, slow, stun
    43: 7,    # Karma - root, slow
    37: 6,    # Sona - stun, slow
    16: 6,    # Soraka - root, silence

    # Control mages (6-7)
    134: 7,   # Syndra - stun, slow
    112: 7,   # Viktor - stun, slow
    161: 7,   # Vel'Koz - knockup, slow
    101: 7,   # Xerath - stun, slow
    127: 7,   # Lissandra - root, stun, slow
    163: 7,   # Taliyah - knockback, slow
    142: 6,   # Zoe - sleep
    13: 6,    # Ryze - root
    69: 6,    # Cassiopeia - stun, slow
    3: 6,     # Galio - taunt, knockup

    # Bruisers/Fighters (4-6)
    58: 6,    # Renekton - stun
    164: 6,   # Camille - stun, slow
    122: 5,   # Darius - pull, slow
    59: 5,    # Jarvan IV - knockup, slow
    254: 5,   # Vi - knockup, stun
    5: 5,     # Xin Zhao - knockup, slow
    80: 5,    # Pantheon - stun
    2: 5,     # Olaf - slow
    420: 5,   # Illaoi - slow
    6: 5,     # Urgot - fear, slow, suppress
    83: 4,    # Yorick - slow, wall
    48: 4,    # Trundle - slow, knockback
    75: 4,    # Nasus - slow
    36: 4,    # Dr. Mundo - slow
    24: 4,    # Jax - stun
    114: 4,   # Fiora - slow
    39: 4,    # Irelia - stun, slow
    266: 4,   # Aatrox - knockup, slow

    # Light CC champions (3-4)
    86: 3,    # Garen - silence
    92: 3,    # Riven - stun, knockup (small)
    157: 3,   # Yasuo - knockup, wind wall
    777: 3,   # Yone - knockup
    98: 3,    # Shen - taunt
    8: 3,     # Vladimir - slow
    50: 3,    # Swain - root, slow
    518: 3,   # Neeko - root, stun
    245: 3,   # Ekko - stun, slow
    105: 3,   # Fizz - knockup, slow
    61: 3,    # Orianna - slow, pull
    4: 3,     # Twisted Fate - stun
    63: 3,    # Brand - stun
    115: 3,   # Ziggs - slow, knockback

    # Marksmen (2-3)
    51: 3,    # Caitlyn - trap, slow
    202: 3,   # Jhin - root, slow
    222: 2,   # Jinx - slow, root
    18: 2,    # Tristana - knockback, slow
    81: 2,    # Ezreal - slow
    110: 3,   # Varus - root, slow
    22: 3,    # Ashe - stun, slow
    96: 2,    # Kog'Maw - slow
    145: 2,   # Kai'Sa - none (plasma slow)
    498: 2,   # Xayah - root
    67: 2,    # Vayne - stun (conditional)
    29: 2,    # Twitch - slow
    15: 2,    # Sivir - none
    21: 2,    # Miss Fortune - slow
    119: 2,   # Draven - knockback, slow
    236: 2,   # Lucian - none
    895: 2,   # Nilah - none
    360: 2,   # Samira - none
    147: 2,   # Seraphine - root, slow, charm
    234: 2,   # Viego - stun

    # Assassins (1-3)
    238: 2,   # Zed - slow
    91: 2,    # Talon - slow
    121: 2,   # Kha'Zix - slow
    141: 2,   # Kayn - knockup (Rhaast)
    84: 2,   # Akali - none
    7: 1,     # LeBlanc - root, slow
    55: 2,    # Katarina - none
    28: 2,    # Evelynn - charm
    107: 2,   # Rengar - root
    35: 2,    # Shaco - fear
    56: 2,    # Nocturne - fear
    76: 2,    # Nidalee - none
    60: 3,    # Elise - stun
    131: 3,   # Diana - pull, slow
    517: 3,   # Sylas - knockup, slow
    246: 2,   # Qiyana - stun, root, slow
    523: 2,   # Aphelios - root, slow
    200: 3,   # Bel'Veth - knockup
    902: 3,   # Milio - knockback
    897: 3,   # K'Sante - knockback, stun
    901: 2,   # Smolder - slow
    950: 3,   # Naafiri - none
    233: 3,   # Briar - fear, stun
    893: 3,   # Aurora - slow, root
    910: 3,   # Hwei - various CC
    887: 3,   # Gwen - slow
    711: 3,   # Vex - fear, interrupt
    166: 3,   # Akshan - none
    526: 5,   # Rell - stun, knockup, pull
    888: 5,   # Renata Glasc - berserk, slow
    876: 6,   # Lillia - sleep, slow
    221: 4,   # Zeri - slow
    203: 4,   # Kindred - slow
    104: 3,   # Graves - slow, knockback
    64: 4,    # Lee Sin - knockback, slow
    19: 5,    # Warwick - fear, suppress
    77: 4,    # Udyr - stun
    102: 4,   # Shyvana - knockback
    421: 5,   # Rek'Sai - knockup
    427: 5,   # Ivern - root, slow, knockup
    136: 3,   # Aurelion Sol - stun, knockback
    268: 3,   # Azir - knockback
    30: 5,    # Karthus - slow
    9: 4,     # Fiddlesticks - fear, silence
    10: 4,    # Kayle - slow
    38: 3,    # Kassadin - slow, interrupt
    31: 5,    # Cho'Gath - knockup, silence, slow
    74: 3,    # Heimerdinger - stun, slow
    85: 5,    # Kennen - stun
    68: 3,    # Rumble - slow
    27: 4,    # Singed - flip, slow, root
    14: 4,    # Sion - knockup, stun, slow
    17: 3,    # Teemo - blind, slow
    45: 5,    # Veigar - stun cage
    26: 4,    # Zilean - stun, slow
    143: 6,   # Zyra - knockup, root, slow
    90: 6,    # Malzahar - suppress, silence
    72: 5,    # Skarner - suppress, slow
    106: 5,   # Volibear - stun, slow
    82: 4,    # Mordekaiser - pull
    240: 4,   # Kled - pull
    223: 6,   # Tahm Kench - devour, stun, slow
    33: 5,    # Rammus - taunt, slow
    20: 6,    # Nunu & Willump - root, slow, knockup
    62: 5,    # Wukong - knockup
    120: 5,   # Hecarim - fear, knockback
    34: 5,    # Anivia - stun, slow, wall
    432: 6,   # Bard - stun, stasis
    44: 6,    # Taric - stun
    53: 7,    # Blitzcrank - hook, knockup, silence
    235: 4,   # Senna - root, slow
    429: 4,   # Kalista - knockup
    133: 3,   # Quinn - blind, knockback
    23: 2,    # Tryndamere - slow
    11: 2,    # Master Yi - slow
}


class ChampionData:
    """
    Manages champion metadata from Riot Data Dragon.

    Features:
    - Fetches champion data from Data Dragon API
    - Caches data locally to avoid repeated API calls
    - Provides champion classes (Assassin, Fighter, Mage, etc.)
    - Determines primary damage type (physical, magic, mixed)
    """

    # Data Dragon API endpoints
    VERSIONS_URL = "https://ddragon.leagueoflegends.com/api/versions.json"
    CHAMPIONS_URL = "https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json"
    CHAMPION_DETAIL_URL = "https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion/{champion}.json"

    # Champion class categories for feature engineering
    DAMAGE_TYPE_MAP = {
        # Primarily physical damage dealers
        'physical': ['Marksman', 'Assassin'],
        # Primarily magic damage dealers
        'magic': ['Mage'],
        # Mixed or depends on build
        'mixed': ['Fighter', 'Tank', 'Support']
    }

    def __init__(self, cache_dir: str = 'data'):
        """
        Initialize ChampionData.

        Args:
            cache_dir: Directory to cache champion data
        """
        self.cache_dir = Path(cache_dir)
        self.cache_file = self.cache_dir / 'champion_metadata.json'
        self.champions: Dict[int, dict] = {}  # {champion_id: metadata}
        self.champion_name_to_id: Dict[str, int] = {}
        self.version: str = ""

    def load(self, force_refresh: bool = False) -> bool:
        """
        Load champion data from cache or fetch from Data Dragon.

        Args:
            force_refresh: If True, fetch fresh data even if cache exists

        Returns:
            bool: True if data loaded successfully
        """
        # Try to load from cache first
        if not force_refresh and self._load_from_cache():
            print(f"Loaded {len(self.champions)} champions from cache")
            return True

        # Fetch from Data Dragon
        print("Fetching champion data from Data Dragon...")
        if self._fetch_from_data_dragon():
            self._save_to_cache()
            print(f"Loaded {len(self.champions)} champions from Data Dragon")
            return True

        return False

    def _load_from_cache(self) -> bool:
        """Load champion data from local cache file."""
        if not self.cache_file.exists():
            return False

        try:
            with open(self.cache_file, 'r') as f:
                data = json.load(f)

            self.version = data.get('version', '')
            self.champions = {int(k): v for k, v in data.get('champions', {}).items()}
            self._build_name_index()
            return True

        except Exception as e:
            print(f"Error loading cache: {e}")
            return False

    def _save_to_cache(self):
        """Save champion data to local cache file."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        data = {
            'version': self.version,
            'champions': self.champions
        }

        with open(self.cache_file, 'w') as f:
            json.dump(data, f, indent=2)

    def _fetch_from_data_dragon(self) -> bool:
        """Fetch champion data from Riot Data Dragon API."""
        try:
            # Get latest version
            response = requests.get(self.VERSIONS_URL, timeout=10)
            response.raise_for_status()
            versions = response.json()
            self.version = versions[0]

            # Get champion list
            url = self.CHAMPIONS_URL.format(version=self.version)
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Process each champion
            for champ_name, champ_data in data['data'].items():
                champion_id = int(champ_data['key'])

                self.champions[champion_id] = {
                    'id': champion_id,
                    'name': champ_data['name'],
                    'key': champ_name,
                    'title': champ_data['title'],
                    'tags': champ_data['tags'],  # ['Fighter', 'Tank'], etc.
                    'stats': champ_data['stats'],
                    'info': champ_data['info'],  # attack, defense, magic, difficulty
                }

            self._build_name_index()
            return True

        except Exception as e:
            print(f"Error fetching from Data Dragon: {e}")
            return False

    def _build_name_index(self):
        """Build name to ID index for quick lookups."""
        self.champion_name_to_id = {}
        for champ_id, data in self.champions.items():
            self.champion_name_to_id[data['name'].lower()] = champ_id
            self.champion_name_to_id[data['key'].lower()] = champ_id

    def get_champion_by_id(self, champion_id: int) -> Optional[dict]:
        """Get champion data by ID."""
        return self.champions.get(champion_id)

    def get_champion_by_name(self, name: str) -> Optional[dict]:
        """Get champion data by name."""
        champion_id = self.champion_name_to_id.get(name.lower())
        if champion_id:
            return self.champions.get(champion_id)
        return None

    def get_champion_classes(self, champion_id: int) -> List[str]:
        """
        Get champion class tags.

        Args:
            champion_id: Riot champion ID

        Returns:
            List of class tags (e.g., ['Fighter', 'Tank'])
        """
        champ = self.champions.get(champion_id)
        if champ:
            return champ.get('tags', [])
        return []

    def get_damage_type(self, champion_id: int) -> str:
        """
        Determine primary damage type for a champion.

        Args:
            champion_id: Riot champion ID

        Returns:
            'physical', 'magic', or 'mixed'
        """
        champ = self.champions.get(champion_id)
        if not champ:
            return 'mixed'

        tags = champ.get('tags', [])
        info = champ.get('info', {})

        # Use info stats to determine damage type
        attack = info.get('attack', 5)
        magic = info.get('magic', 5)

        # If champion is primarily a marksman or physical assassin
        if 'Marksman' in tags:
            return 'physical'

        # If champion is primarily a mage
        if 'Mage' in tags and 'Fighter' not in tags:
            return 'magic'

        # Use attack/magic ratio
        if attack > magic + 2:
            return 'physical'
        elif magic > attack + 2:
            return 'magic'

        return 'mixed'

    def get_base_stats(self, champion_id: int) -> dict:
        """
        Get champion base statistics.

        Args:
            champion_id: Riot champion ID

        Returns:
            Dict with base stats (hp, armor, mr, etc.)
        """
        champ = self.champions.get(champion_id)
        if champ:
            return champ.get('stats', {})
        return {}

    def is_tank(self, champion_id: int) -> bool:
        """Check if champion is a tank."""
        return 'Tank' in self.get_champion_classes(champion_id)

    def is_assassin(self, champion_id: int) -> bool:
        """Check if champion is an assassin."""
        return 'Assassin' in self.get_champion_classes(champion_id)

    def is_mage(self, champion_id: int) -> bool:
        """Check if champion is a mage."""
        return 'Mage' in self.get_champion_classes(champion_id)

    def is_marksman(self, champion_id: int) -> bool:
        """Check if champion is a marksman (ADC)."""
        return 'Marksman' in self.get_champion_classes(champion_id)

    def is_support(self, champion_id: int) -> bool:
        """Check if champion is a support."""
        return 'Support' in self.get_champion_classes(champion_id)

    def is_fighter(self, champion_id: int) -> bool:
        """Check if champion is a fighter."""
        return 'Fighter' in self.get_champion_classes(champion_id)

    def get_all_champion_ids(self) -> List[int]:
        """Get list of all champion IDs."""
        return list(self.champions.keys())

    def get_champion_name(self, champion_id: int) -> str:
        """Get champion name by ID."""
        champ = self.champions.get(champion_id)
        if champ:
            return champ.get('name', f'Champion_{champion_id}')
        return f'Unknown_{champion_id}'

    def get_champion_cc_score(self, champion_id: int) -> int:
        """
        Get CC capability score for a champion (0-10 scale).

        Args:
            champion_id: Riot champion ID

        Returns:
            CC score (0=no CC, 10=heavy CC like Nautilus/Leona)
        """
        return CHAMPION_CC_SCORES.get(champion_id, 4)  # Default 4 for unknown

    def get_team_cc_score(self, champion_ids: List[int]) -> Dict:
        """
        Calculate total CC score for a team composition.

        Args:
            champion_ids: List of 5 champion IDs

        Returns:
            Dict with total_cc, avg_cc, max_cc, and per-champion breakdown
        """
        scores = [self.get_champion_cc_score(cid) for cid in champion_ids]
        return {
            'total_cc': sum(scores),
            'avg_cc': round(sum(scores) / len(scores), 2) if scores else 0,
            'max_cc': max(scores) if scores else 0,
            'min_cc': min(scores) if scores else 0,
            'breakdown': {cid: self.get_champion_cc_score(cid) for cid in champion_ids}
        }


# Singleton instance for easy access
_champion_data_instance: Optional[ChampionData] = None


def get_champion_data() -> ChampionData:
    """
    Get or create the singleton ChampionData instance.

    Returns:
        ChampionData instance with loaded data
    """
    global _champion_data_instance

    if _champion_data_instance is None:
        _champion_data_instance = ChampionData()
        _champion_data_instance.load()

    return _champion_data_instance


if __name__ == '__main__':
    # Test the module
    print("Testing ChampionData module...")

    cd = ChampionData()
    cd.load()

    # Test with some known champions
    test_champions = [
        (86, 'Garen'),      # Fighter/Tank - Physical
        (103, 'Ahri'),      # Mage/Assassin - Magic
        (51, 'Caitlyn'),    # Marksman - Physical
        (412, 'Thresh'),    # Support - Mixed
        (238, 'Zed'),       # Assassin - Physical
    ]

    print(f"\nLoaded {len(cd.champions)} champions (version {cd.version})")
    print("\nTest results:")

    for champ_id, expected_name in test_champions:
        champ = cd.get_champion_by_id(champ_id)
        if champ:
            print(f"\n{champ['name']} (ID: {champ_id}):")
            print(f"  Classes: {cd.get_champion_classes(champ_id)}")
            print(f"  Damage type: {cd.get_damage_type(champ_id)}")
            print(f"  Is tank: {cd.is_tank(champ_id)}")
            print(f"  Is assassin: {cd.is_assassin(champ_id)}")
        else:
            print(f"Champion {champ_id} not found!")
