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
