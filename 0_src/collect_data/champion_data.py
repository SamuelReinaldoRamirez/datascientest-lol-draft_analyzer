"""
Champion Data Module

Fetches and manages champion metadata from Riot Data Dragon API and CommunityDragon.
Provides champion classes, damage types, playstyle stats (CC, mobility, etc.) and base statistics.

Data Sources:
- Data Dragon: Basic champion info, stats, tags
- CommunityDragon: Playstyle info (crowdControl, damage, durability, mobility, utility)

Usage:
    from champion_data import ChampionData

    champion_data = ChampionData()
    champion_data.load()  # Fetches from APIs or loads from cache

    # Get champion info
    champion_data.get_champion_class(86)  # Returns ['Fighter', 'Tank'] for Garen
    champion_data.get_damage_type(86)     # Returns 'physical'
    champion_data.get_playstyle_info(111) # Returns {'crowdControl': 3, 'damage': 1, ...}
"""

import json
import os
import requests
from pathlib import Path
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed


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
# CommunityDragon API for playstyle data
# ============================================================
CDRAGON_CHAMPION_URL = "https://raw.communitydragon.org/latest/plugins/rcp-be-lol-game-data/global/default/v1/champions/{champion_id}.json"

# Playstyle info scale (from CommunityDragon):
# 1 = Low, 2 = Medium, 3 = High
# Available stats: crowdControl, damage, durability, mobility, utility


class ChampionData:
    """
    Manages champion metadata from Riot Data Dragon and CommunityDragon.

    Features:
    - Fetches champion data from Data Dragon API (basic info, stats)
    - Fetches playstyle data from CommunityDragon (CC, mobility, damage, etc.)
    - Caches data locally to avoid repeated API calls
    - Provides champion classes (Assassin, Fighter, Mage, etc.)
    - Determines primary damage type (physical, magic, mixed)
    - Official playstyle ratings (crowdControl, damage, durability, mobility, utility)
    """

    # Data Dragon API endpoints
    VERSIONS_URL = "https://ddragon.leagueoflegends.com/api/versions.json"
    CHAMPIONS_URL = "https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json"
    CHAMPION_DETAIL_URL = "https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion/{champion}.json"

    # CommunityDragon API endpoint
    CDRAGON_CHAMPION_URL = "https://raw.communitydragon.org/latest/plugins/rcp-be-lol-game-data/global/default/v1/champions/{champion_id}.json"

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

            # Fetch playstyle data from CommunityDragon
            print("Fetching playstyle data from CommunityDragon...")
            self._fetch_playstyle_data()

            return True

        except Exception as e:
            print(f"Error fetching from Data Dragon: {e}")
            return False

    def _fetch_single_champion_playstyle(self, champion_id: int) -> Optional[dict]:
        """Fetch playstyle data for a single champion from CommunityDragon."""
        try:
            url = self.CDRAGON_CHAMPION_URL.format(champion_id=champion_id)
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            result = {
                'playstyleInfo': data.get('playstyleInfo', {}),
                'tacticalInfo': data.get('tacticalInfo', {}),
                'roles': data.get('roles', []),
            }

            # Extract champion tags if available
            tag_info = data.get('championTagInfo', {})
            if tag_info:
                result['primaryTag'] = tag_info.get('championTagPrimary', '')
                result['secondaryTag'] = tag_info.get('championTagSecondary', '')

            return result
        except Exception:
            return None

    def _fetch_playstyle_data(self):
        """Fetch playstyle data for all champions from CommunityDragon (parallel)."""
        champion_ids = list(self.champions.keys())
        success_count = 0

        # Use ThreadPoolExecutor for parallel fetching
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_id = {
                executor.submit(self._fetch_single_champion_playstyle, cid): cid
                for cid in champion_ids
            }

            for future in as_completed(future_to_id):
                champion_id = future_to_id[future]
                try:
                    result = future.result()
                    if result and champion_id in self.champions:
                        self.champions[champion_id].update(result)
                        success_count += 1
                except Exception:
                    pass

        print(f"  Fetched playstyle data for {success_count}/{len(champion_ids)} champions")

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

    # ================================================================
    # Playstyle Info Methods (from CommunityDragon)
    # ================================================================

    def get_playstyle_info(self, champion_id: int) -> Dict:
        """
        Get official playstyle ratings from CommunityDragon.

        Args:
            champion_id: Riot champion ID

        Returns:
            Dict with crowdControl, damage, durability, mobility, utility (1-3 scale)
        """
        champ = self.champions.get(champion_id)
        if champ:
            return champ.get('playstyleInfo', {
                'crowdControl': 2,
                'damage': 2,
                'durability': 2,
                'mobility': 2,
                'utility': 2
            })
        return {
            'crowdControl': 2,
            'damage': 2,
            'durability': 2,
            'mobility': 2,
            'utility': 2
        }

    def get_tactical_info(self, champion_id: int) -> Dict:
        """
        Get tactical info from CommunityDragon.

        Args:
            champion_id: Riot champion ID

        Returns:
            Dict with style, difficulty, damageType, attackType
        """
        champ = self.champions.get(champion_id)
        if champ:
            return champ.get('tacticalInfo', {})
        return {}

    def get_champion_roles(self, champion_id: int) -> List[str]:
        """
        Get champion roles from CommunityDragon (e.g., ['tank', 'support']).

        Args:
            champion_id: Riot champion ID

        Returns:
            List of role strings
        """
        champ = self.champions.get(champion_id)
        if champ:
            return champ.get('roles', [])
        return []

    def get_champion_cc_score(self, champion_id: int) -> int:
        """
        Get CC capability score for a champion (1-3 scale from CommunityDragon).

        Args:
            champion_id: Riot champion ID

        Returns:
            CC score (1=low, 2=medium, 3=high)
        """
        playstyle = self.get_playstyle_info(champion_id)
        return playstyle.get('crowdControl', 2)

    def get_champion_damage_score(self, champion_id: int) -> int:
        """
        Get damage rating for a champion (1-3 scale).

        Args:
            champion_id: Riot champion ID

        Returns:
            Damage score (1=low, 2=medium, 3=high)
        """
        playstyle = self.get_playstyle_info(champion_id)
        return playstyle.get('damage', 2)

    def get_champion_durability_score(self, champion_id: int) -> int:
        """
        Get durability rating for a champion (1-3 scale).

        Args:
            champion_id: Riot champion ID

        Returns:
            Durability score (1=low, 2=medium, 3=high)
        """
        playstyle = self.get_playstyle_info(champion_id)
        return playstyle.get('durability', 2)

    def get_champion_mobility_score(self, champion_id: int) -> int:
        """
        Get mobility rating for a champion (1-3 scale).

        Args:
            champion_id: Riot champion ID

        Returns:
            Mobility score (1=low, 2=medium, 3=high)
        """
        playstyle = self.get_playstyle_info(champion_id)
        return playstyle.get('mobility', 2)

    def get_champion_utility_score(self, champion_id: int) -> int:
        """
        Get utility rating for a champion (1-3 scale).

        Args:
            champion_id: Riot champion ID

        Returns:
            Utility score (1=low, 2=medium, 3=high)
        """
        playstyle = self.get_playstyle_info(champion_id)
        return playstyle.get('utility', 2)

    # ================================================================
    # Team Composition Analysis
    # ================================================================

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

    def get_team_playstyle_scores(self, champion_ids: List[int]) -> Dict:
        """
        Calculate aggregated playstyle scores for a team composition.

        Args:
            champion_ids: List of 5 champion IDs

        Returns:
            Dict with total and avg for each playstyle stat
        """
        stats = ['crowdControl', 'damage', 'durability', 'mobility', 'utility']
        result = {}

        for stat in stats:
            scores = []
            for cid in champion_ids:
                playstyle = self.get_playstyle_info(cid)
                scores.append(playstyle.get(stat, 2))

            result[f'total_{stat}'] = sum(scores)
            result[f'avg_{stat}'] = round(sum(scores) / len(scores), 2) if scores else 0
            result[f'max_{stat}'] = max(scores) if scores else 0

        return result

    def get_team_damage_profile(self, champion_ids: List[int]) -> Dict:
        """
        Analyze team damage composition (physical vs magic).

        Args:
            champion_ids: List of champion IDs

        Returns:
            Dict with physical_count, magic_count, mixed_count
        """
        profile = {'physical': 0, 'magic': 0, 'mixed': 0}
        for cid in champion_ids:
            damage_type = self.get_damage_type(cid)
            profile[damage_type] += 1

        # Also get official damage type from tacticalInfo if available
        official_types = {'kPhysical': 0, 'kMagic': 0, 'kMixed': 0}
        for cid in champion_ids:
            tactical = self.get_tactical_info(cid)
            dtype = tactical.get('damageType', '')
            if dtype in official_types:
                official_types[dtype] += 1

        return {
            'physical_count': profile['physical'],
            'magic_count': profile['magic'],
            'mixed_count': profile['mixed'],
            'official_physical': official_types['kPhysical'],
            'official_magic': official_types['kMagic'],
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
    print("=" * 60)

    cd = ChampionData()
    cd.load(force_refresh=True)  # Force refresh to get CommunityDragon data

    # Test with some known champions
    test_champions = [
        (111, 'Nautilus'),  # Tank/Support - Heavy CC
        (238, 'Zed'),       # Assassin - Low CC, High Damage
        (86, 'Garen'),      # Fighter/Tank - Physical
        (103, 'Ahri'),      # Mage/Assassin - Magic
        (67, 'Vayne'),      # Marksman - Physical
    ]

    print(f"\nLoaded {len(cd.champions)} champions (version {cd.version})")
    print("\n" + "=" * 60)
    print("Individual Champion Stats:")
    print("=" * 60)

    for champ_id, expected_name in test_champions:
        champ = cd.get_champion_by_id(champ_id)
        if champ:
            playstyle = cd.get_playstyle_info(champ_id)
            print(f"\n{champ['name']} (ID: {champ_id}):")
            print(f"  Tags: {cd.get_champion_classes(champ_id)}")
            print(f"  Roles: {cd.get_champion_roles(champ_id)}")
            print(f"  Damage type: {cd.get_damage_type(champ_id)}")
            print(f"  Playstyle Info (1-3 scale):")
            print(f"    - Crowd Control: {playstyle.get('crowdControl', 'N/A')}")
            print(f"    - Damage: {playstyle.get('damage', 'N/A')}")
            print(f"    - Durability: {playstyle.get('durability', 'N/A')}")
            print(f"    - Mobility: {playstyle.get('mobility', 'N/A')}")
            print(f"    - Utility: {playstyle.get('utility', 'N/A')}")
        else:
            print(f"Champion {champ_id} not found!")

    # Test team composition analysis
    print("\n" + "=" * 60)
    print("Team Composition Analysis:")
    print("=" * 60)

    # Example team: Nautilus, Sejuani, Orianna, Jinx, Thresh
    team_ids = [111, 113, 61, 222, 412]
    team_names = [cd.get_champion_name(cid) for cid in team_ids]
    print(f"\nTeam: {', '.join(team_names)}")

    team_scores = cd.get_team_playstyle_scores(team_ids)
    print(f"\nTeam Playstyle Scores:")
    for stat in ['crowdControl', 'damage', 'durability', 'mobility', 'utility']:
        total = team_scores.get(f'total_{stat}', 0)
        avg = team_scores.get(f'avg_{stat}', 0)
        print(f"  {stat}: total={total}, avg={avg}")

    damage_profile = cd.get_team_damage_profile(team_ids)
    print(f"\nDamage Profile:")
    print(f"  Physical: {damage_profile['physical_count']}")
    print(f"  Magic: {damage_profile['magic_count']}")
    print(f"  Mixed: {damage_profile['mixed_count']}")
