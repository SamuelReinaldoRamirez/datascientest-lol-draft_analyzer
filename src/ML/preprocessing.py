"""
Data Preparation Script for ML Training

This script prepares the collected match data for machine learning:
1. Loads data from SQLite database
2. Removes ID columns that shouldn't influence predictions
3. Encodes categorical features (champions) using One-Hot encoding
4. Splits data into train/validation/test sets
5. Saves prepared data in Parquet format for fast loading

Usage:
    python src/prepare_data.py
    python src/prepare_data.py --encoding onehot --test-size 0.15 --val-size 0.15
"""

import os
import sys
import argparse
import json
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from database import MatchDatabase
from champion_data import ChampionData


# =============================================================================
# OP.GG DATA PROVIDER - External data source for champion stats
# =============================================================================

class OPGGDataProvider:
    """
    Provides champion statistics, matchups, and synergies from OP.GG data.

    This uses external data scraped from OP.GG instead of calculating from
    historical match data, providing more accurate and up-to-date statistics.
    """

    def __init__(self, opgg_data_dir: str = 'data/opgg'):
        """
        Initialize the OP.GG data provider.

        Args:
            opgg_data_dir: Directory containing OP.GG scraped data files
        """
        self.opgg_data_dir = Path(opgg_data_dir)
        self.champion_stats = None
        self.matchups = None
        self.synergies = None
        self._loaded = False

    def load_data(self) -> bool:
        """Load all OP.GG data files."""
        if self._loaded:
            return True

        try:
            # Load champion stats
            stats_path = self.opgg_data_dir / 'champion_stats.parquet'
            if stats_path.exists():
                self.champion_stats = pd.read_parquet(stats_path)
                print(f"  Loaded {len(self.champion_stats)} champion stat entries from OP.GG")
            else:
                print(f"  Warning: {stats_path} not found")
                return False

            # Load matchups
            matchups_path = self.opgg_data_dir / 'matchups.parquet'
            if matchups_path.exists():
                self.matchups = pd.read_parquet(matchups_path)
                print(f"  Loaded {len(self.matchups)} matchup entries from OP.GG")

            # Load synergies
            synergies_path = self.opgg_data_dir / 'synergies.parquet'
            if synergies_path.exists():
                self.synergies = pd.read_parquet(synergies_path)
                print(f"  Loaded {len(self.synergies)} synergy entries from OP.GG")

            self._loaded = True
            return True

        except Exception as e:
            print(f"  Error loading OP.GG data: {e}")
            return False

    def get_champion_winrate(self, champion_id: int, position: str = None) -> float:
        """
        Get win rate for a champion from OP.GG data.

        Args:
            champion_id: Champion ID
            position: Position (top, jungle, mid, adc, support) or None for overall

        Returns:
            Win rate (0.0 to 1.0), defaults to 0.5 if not found
        """
        if self.champion_stats is None:
            return 0.5

        # Try position-specific first
        if position:
            mask = (self.champion_stats['champion_id'] == champion_id) & \
                   (self.champion_stats['position'] == position.lower())
            result = self.champion_stats[mask]
            if not result.empty:
                return result.iloc[0]['opgg_winrate']

        # Fall back to 'all' position
        mask = (self.champion_stats['champion_id'] == champion_id) & \
               (self.champion_stats['position'] == 'all')
        result = self.champion_stats[mask]
        if not result.empty:
            return result.iloc[0]['opgg_winrate']

        return 0.5

    def get_champion_tier(self, champion_id: int, position: str = None) -> int:
        """Get tier for a champion (1=best, 5=worst)."""
        if self.champion_stats is None:
            return 3

        if position:
            mask = (self.champion_stats['champion_id'] == champion_id) & \
                   (self.champion_stats['position'] == position.lower())
            result = self.champion_stats[mask]
            if not result.empty:
                return int(result.iloc[0]['opgg_tier'])

        mask = (self.champion_stats['champion_id'] == champion_id) & \
               (self.champion_stats['position'] == 'all')
        result = self.champion_stats[mask]
        if not result.empty:
            return int(result.iloc[0]['opgg_tier'])

        return 3

    def get_champion_pickrate(self, champion_id: int, position: str = None) -> float:
        """Get pick rate for a champion."""
        if self.champion_stats is None:
            return 0.0

        if position:
            mask = (self.champion_stats['champion_id'] == champion_id) & \
                   (self.champion_stats['position'] == position.lower())
            result = self.champion_stats[mask]
            if not result.empty:
                return result.iloc[0]['opgg_pickrate']

        mask = (self.champion_stats['champion_id'] == champion_id) & \
               (self.champion_stats['position'] == 'all')
        result = self.champion_stats[mask]
        if not result.empty:
            return result.iloc[0]['opgg_pickrate']

        return 0.0

    def get_matchup_winrate(self, champion_id: int, opponent_id: int, position: str) -> float:
        """
        Get matchup win rate for champion vs opponent in a position.

        Args:
            champion_id: Champion ID
            opponent_id: Opponent champion ID
            position: Lane position

        Returns:
            Win rate for champion against opponent (0.0 to 1.0)
        """
        if self.matchups is None:
            return 0.5

        # Direct lookup
        mask = (self.matchups['champion_id'] == champion_id) & \
               (self.matchups['opponent_id'] == opponent_id) & \
               (self.matchups['position'] == position.lower())
        result = self.matchups[mask]
        if not result.empty:
            return result.iloc[0]['matchup_winrate']

        # Try reverse lookup (opponent's perspective)
        mask = (self.matchups['champion_id'] == opponent_id) & \
               (self.matchups['opponent_id'] == champion_id) & \
               (self.matchups['position'] == position.lower())
        result = self.matchups[mask]
        if not result.empty:
            return 1.0 - result.iloc[0]['matchup_winrate']

        return 0.5

    def get_synergy_winrate(self, champion_id: int, ally_id: int,
                           champion_pos: str = None, ally_pos: str = None) -> float:
        """
        Get synergy win rate for two champions playing together.

        Args:
            champion_id: First champion ID
            ally_id: Second champion ID
            champion_pos: Position of first champion
            ally_pos: Position of second champion

        Returns:
            Win rate when playing together (0.0 to 1.0)
        """
        if self.synergies is None:
            return 0.5

        # Try direct lookup with positions
        if champion_pos and ally_pos:
            mask = (self.synergies['champion_id'] == champion_id) & \
                   (self.synergies['ally_id'] == ally_id) & \
                   (self.synergies['position'] == champion_pos.lower()) & \
                   (self.synergies['ally_position'] == ally_pos.lower())
            result = self.synergies[mask]
            if not result.empty:
                return result.iloc[0]['synergy_winrate']

            # Try reverse
            mask = (self.synergies['champion_id'] == ally_id) & \
                   (self.synergies['ally_id'] == champion_id) & \
                   (self.synergies['position'] == ally_pos.lower()) & \
                   (self.synergies['ally_position'] == champion_pos.lower())
            result = self.synergies[mask]
            if not result.empty:
                return result.iloc[0]['synergy_winrate']

        # Without positions, search any combination
        mask = ((self.synergies['champion_id'] == champion_id) & \
                (self.synergies['ally_id'] == ally_id)) | \
               ((self.synergies['champion_id'] == ally_id) & \
                (self.synergies['ally_id'] == champion_id))
        result = self.synergies[mask]
        if not result.empty:
            return result.iloc[0]['synergy_winrate']

        return 0.5

    def get_team_average_tier(self, champion_ids: list, positions: list) -> float:
        """Calculate average tier for a team (lower is better)."""
        tiers = []
        for champ_id, pos in zip(champion_ids, positions):
            if pd.notna(champ_id):
                tier = self.get_champion_tier(int(champ_id), pos)
                tiers.append(tier)
        return np.mean(tiers) if tiers else 3.0


# Global OP.GG data provider instance
_opgg_provider = None


def get_opgg_provider(data_dir: str = 'data/opgg') -> OPGGDataProvider:
    """Get or create the global OP.GG data provider."""
    global _opgg_provider
    if _opgg_provider is None:
        _opgg_provider = OPGGDataProvider(data_dir)
        _opgg_provider.load_data()
    return _opgg_provider


# =============================================================================
# DPM.LOL DATA PROVIDER - External data source for champion stats
# =============================================================================

class DPMLOLDataProvider:
    """
    Provides champion statistics from DPM.LOL data.

    DPM.LOL provides unique features not available in OP.GG:
    - tierScore: A composite score combining winrate, pickrate, banrate
    - winrateVariance: How much the champion's winrate is changing (trend)
    - More granular lane-specific data
    """

    # Lane mapping from database positions to DPM.LOL lanes
    LANE_MAP = {
        'top': 'TOP',
        'jungle': 'JUNGLE',
        'mid': 'MIDDLE',
        'middle': 'MIDDLE',
        'adc': 'BOTTOM',
        'bot': 'BOTTOM',
        'bottom': 'BOTTOM',
        'support': 'UTILITY',
        'utility': 'UTILITY',
    }

    def __init__(self, dpmlol_data_dir: str = 'data/dpmlol'):
        """
        Initialize the DPM.LOL data provider.

        Args:
            dpmlol_data_dir: Directory containing DPM.LOL scraped data files
        """
        self.dpmlol_data_dir = Path(dpmlol_data_dir)
        self.tierlist = None
        self._loaded = False
        self._champion_cache = {}  # Cache for fast lookup

    def load_data(self) -> bool:
        """Load DPM.LOL data files."""
        if self._loaded:
            return True

        try:
            tierlist_path = self.dpmlol_data_dir / 'dpmlol_tierlist.parquet'
            if tierlist_path.exists():
                self.tierlist = pd.read_parquet(tierlist_path)
                print(f"  Loaded {len(self.tierlist)} champion entries from DPM.LOL")

                # Build champion lookup cache for faster access
                self._build_cache()
                self._loaded = True
                return True
            else:
                print(f"  Warning: {tierlist_path} not found")
                return False

        except Exception as e:
            print(f"  Error loading DPM.LOL data: {e}")
            return False

    def _build_cache(self):
        """Build a cache for fast champion lookups."""
        if self.tierlist is None:
            return

        # Create a dict: {(champion_id, lane): row_data}
        for _, row in self.tierlist.iterrows():
            key = (int(row['championId']), row['lane'])
            self._champion_cache[key] = {
                'winrate': row['winrate'],
                'pickrate': row['pickrate'],
                'banrate': row['banrate'],
                'tierScore': row['tierScore'],
                'winrateVariance': row['winrateVariance'],
                'count': row['count'],
            }

    def _normalize_lane(self, position: str) -> str:
        """Convert position to DPM.LOL lane format."""
        if position is None:
            return None
        return self.LANE_MAP.get(position.lower(), position.upper())

    def _get_champion_data(self, champion_id: int, position: str = None) -> dict:
        """
        Get all data for a champion in a specific position.

        Args:
            champion_id: Champion ID
            position: Position (top, jungle, mid, adc, support)

        Returns:
            Dict with champion data or default values
        """
        defaults = {
            'winrate': 50.0,
            'pickrate': 0.0,
            'banrate': 0.0,
            'tierScore': 0.0,
            'winrateVariance': 0.0,
            'count': 0,
        }

        if not self._loaded:
            return defaults

        lane = self._normalize_lane(position)
        if lane:
            key = (int(champion_id), lane)
            if key in self._champion_cache:
                return self._champion_cache[key]

        # Try to find any lane data for this champion
        for cached_key, data in self._champion_cache.items():
            if cached_key[0] == int(champion_id):
                return data

        return defaults

    def get_champion_winrate(self, champion_id: int, position: str = None) -> float:
        """Get win rate for a champion (0-100 scale)."""
        return self._get_champion_data(champion_id, position)['winrate']

    def get_champion_pickrate(self, champion_id: int, position: str = None) -> float:
        """Get pick rate for a champion (0-100 scale)."""
        return self._get_champion_data(champion_id, position)['pickrate']

    def get_champion_banrate(self, champion_id: int, position: str = None) -> float:
        """Get ban rate for a champion (0-100 scale)."""
        return self._get_champion_data(champion_id, position)['banrate']

    def get_champion_tier_score(self, champion_id: int, position: str = None) -> float:
        """
        Get tier score for a champion.

        tierScore is a composite metric from DPM.LOL that combines
        winrate, pickrate, and banrate into a single score.
        Higher = better champion in the current meta.
        """
        return self._get_champion_data(champion_id, position)['tierScore']

    def get_champion_winrate_variance(self, champion_id: int, position: str = None) -> float:
        """
        Get win rate variance for a champion.

        Positive = winrate is increasing (champion getting stronger)
        Negative = winrate is decreasing (champion getting weaker)
        """
        return self._get_champion_data(champion_id, position)['winrateVariance']

    def get_team_tier_score(self, champion_ids: list, positions: list = None) -> dict:
        """
        Calculate team tier score statistics.

        Args:
            champion_ids: List of 5 champion IDs
            positions: Optional list of 5 positions

        Returns:
            Dict with total, avg, min, max tier scores
        """
        scores = []
        if positions is None:
            positions = [None] * len(champion_ids)

        for champ_id, pos in zip(champion_ids, positions):
            if champ_id and pd.notna(champ_id):
                scores.append(self.get_champion_tier_score(int(champ_id), pos))

        if not scores:
            return {'total': 0.0, 'avg': 0.0, 'min': 0.0, 'max': 0.0}

        return {
            'total': sum(scores),
            'avg': np.mean(scores),
            'min': min(scores),
            'max': max(scores),
        }


# Global DPM.LOL data provider instance
_dpmlol_provider = None


def get_dpmlol_provider(data_dir: str = 'data/dpmlol') -> DPMLOLDataProvider:
    """Get or create the global DPM.LOL data provider."""
    global _dpmlol_provider
    if _dpmlol_provider is None:
        _dpmlol_provider = DPMLOLDataProvider(data_dir)
        _dpmlol_provider.load_data()
    return _dpmlol_provider


class ChampionStatsCalculator:
    """
    Calculates champion statistics from match data.

    Computes win rates, pick rates, and matchup statistics
    that can be used as features for the ML model.
    """

    def __init__(self, df: pd.DataFrame):
        """
        Initialize with match data.

        Args:
            df: DataFrame with match data (before post-game column removal)
        """
        self.df = df
        self.champion_winrates = {}  # {champion_id: {role: winrate}}
        self.champion_pickrates = {}  # {champion_id: pickrate}
        self.matchup_winrates = {}  # {(champ1, champ2, role): winrate}

    def calculate_champion_winrates(self) -> dict:
        """
        Calculate win rate for each champion per role.

        Returns:
            dict: {champion_id: {'global': winrate, 'top': winrate, ...}}
        """
        print("  Calculating champion win rates...")

        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        winrates = {}

        for pos in positions:
            # Team 100 champion and win
            col_100 = f'team_100_{pos}_champion_id'
            col_200 = f'team_200_{pos}_champion_id'

            if col_100 not in self.df.columns:
                continue

            # Calculate for team 100
            for _, row in self.df.iterrows():
                champ_100 = row.get(col_100)
                champ_200 = row.get(col_200)
                team_100_win = row.get('team_100_win', False)

                if pd.notna(champ_100):
                    champ_100 = int(champ_100)
                    if champ_100 not in winrates:
                        winrates[champ_100] = {'wins': 0, 'games': 0, 'by_role': {}}
                    if pos not in winrates[champ_100]['by_role']:
                        winrates[champ_100]['by_role'][pos] = {'wins': 0, 'games': 0}

                    winrates[champ_100]['games'] += 1
                    winrates[champ_100]['by_role'][pos]['games'] += 1
                    if team_100_win:
                        winrates[champ_100]['wins'] += 1
                        winrates[champ_100]['by_role'][pos]['wins'] += 1

                if pd.notna(champ_200):
                    champ_200 = int(champ_200)
                    if champ_200 not in winrates:
                        winrates[champ_200] = {'wins': 0, 'games': 0, 'by_role': {}}
                    if pos not in winrates[champ_200]['by_role']:
                        winrates[champ_200]['by_role'][pos] = {'wins': 0, 'games': 0}

                    winrates[champ_200]['games'] += 1
                    winrates[champ_200]['by_role'][pos]['games'] += 1
                    if not team_100_win:
                        winrates[champ_200]['wins'] += 1
                        winrates[champ_200]['by_role'][pos]['wins'] += 1

        # Convert to winrates
        for champ_id, data in winrates.items():
            if data['games'] > 0:
                data['global_winrate'] = data['wins'] / data['games']
            else:
                data['global_winrate'] = 0.5

            for role, role_data in data['by_role'].items():
                if role_data['games'] > 0:
                    role_data['winrate'] = role_data['wins'] / role_data['games']
                else:
                    role_data['winrate'] = 0.5

        self.champion_winrates = winrates
        print(f"    Calculated win rates for {len(winrates)} champions")
        return winrates

    def calculate_matchup_winrates(self, min_games: int = 3) -> dict:
        """
        Calculate win rates for lane matchups.

        Args:
            min_games: Minimum games for a matchup to be included

        Returns:
            dict: {(champ1, champ2, role): winrate_for_champ1}
        """
        print("  Calculating matchup win rates...")

        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        matchups = {}

        for pos in positions:
            col_100 = f'team_100_{pos}_champion_id'
            col_200 = f'team_200_{pos}_champion_id'

            if col_100 not in self.df.columns:
                continue

            for _, row in self.df.iterrows():
                champ_100 = row.get(col_100)
                champ_200 = row.get(col_200)
                team_100_win = row.get('team_100_win', False)

                if pd.notna(champ_100) and pd.notna(champ_200):
                    champ_100 = int(champ_100)
                    champ_200 = int(champ_200)
                    key = (champ_100, champ_200, pos)

                    if key not in matchups:
                        matchups[key] = {'wins': 0, 'games': 0}

                    matchups[key]['games'] += 1
                    if team_100_win:
                        matchups[key]['wins'] += 1

        # Convert to winrates, filter by min_games
        filtered_matchups = {}
        for key, data in matchups.items():
            if data['games'] >= min_games:
                filtered_matchups[key] = data['wins'] / data['games']

        self.matchup_winrates = filtered_matchups
        print(f"    Calculated {len(filtered_matchups)} matchup win rates")
        return filtered_matchups

    def get_champion_winrate(self, champion_id: int, role: str = None) -> float:
        """Get win rate for a champion, optionally by role."""
        if champion_id not in self.champion_winrates:
            return 0.5

        data = self.champion_winrates[champion_id]
        if role and role in data.get('by_role', {}):
            return data['by_role'][role].get('winrate', 0.5)
        return data.get('global_winrate', 0.5)

    def get_matchup_winrate(self, champ1: int, champ2: int, role: str) -> float:
        """Get win rate for champ1 vs champ2 in a specific role."""
        key = (champ1, champ2, role)
        if key in self.matchup_winrates:
            return self.matchup_winrates[key]

        # Try reverse matchup
        reverse_key = (champ2, champ1, role)
        if reverse_key in self.matchup_winrates:
            return 1.0 - self.matchup_winrates[reverse_key]

        return 0.5  # Default to 50% if no data


# =============================================================================
# MATCHUP DETECTION - Known Counter Picks
# =============================================================================
# Format: (champion_countered_id, counter_pick_id, position): severity (0.0 to 1.0)
# Higher severity = stronger counter

KNOWN_COUNTERS = {
    # ===== TOP LANE =====
    # Garen (86) counters
    (86, 17, 'top'): 0.9,      # Garen countered by Teemo (blind + kite)
    (86, 85, 'top'): 0.85,     # Garen countered by Kennen (ranged + stun)
    (86, 8, 'top'): 0.8,       # Garen countered by Vladimir (sustain + poke)
    (86, 150, 'top'): 0.8,     # Garen countered by Gnar (ranged + kite)

    # Darius (122) counters
    (122, 85, 'top'): 0.85,    # Darius countered by Kennen
    (122, 17, 'top'): 0.85,    # Darius countered by Teemo
    (122, 150, 'top'): 0.8,    # Darius countered by Gnar
    (122, 69, 'top'): 0.8,     # Darius countered by Cassiopeia

    # Yasuo (157) counters
    (157, 58, 'top'): 0.9,     # Yasuo countered by Renekton
    (157, 80, 'top'): 0.85,    # Yasuo countered by Pantheon
    (157, 90, 'top'): 0.85,    # Yasuo countered by Malzahar

    # Yone (777) counters
    (777, 58, 'top'): 0.9,     # Yone countered by Renekton
    (777, 80, 'top'): 0.85,    # Yone countered by Pantheon

    # Riven (92) counters
    (92, 58, 'top'): 0.85,     # Riven countered by Renekton
    (92, 80, 'top'): 0.8,      # Riven countered by Pantheon
    (92, 78, 'top'): 0.8,      # Riven countered by Poppy

    # Fiora (114) counters
    (114, 78, 'top'): 0.85,    # Fiora countered by Poppy (W blocks vitals)
    (114, 90, 'top'): 0.8,     # Fiora countered by Malzahar

    # Irelia (39) counters
    (39, 80, 'top'): 0.85,     # Irelia countered by Pantheon
    (39, 58, 'top'): 0.8,      # Irelia countered by Renekton
    (39, 24, 'top'): 0.8,      # Irelia countered by Jax

    # ===== MID LANE =====
    # Zed (238) counters
    (238, 90, 'mid'): 0.9,     # Zed countered by Malzahar (passive + R)
    (238, 127, 'mid'): 0.85,   # Zed countered by Lissandra
    (238, 245, 'mid'): 0.8,    # Zed countered by Ekko

    # Katarina (55) counters
    (55, 90, 'mid'): 0.9,      # Katarina countered by Malzahar
    (55, 127, 'mid'): 0.85,    # Katarina countered by Lissandra
    (55, 1, 'mid'): 0.8,       # Katarina countered by Annie

    # Yasuo (157) counters - mid
    (157, 90, 'mid'): 0.9,     # Yasuo countered by Malzahar
    (157, 80, 'mid'): 0.85,    # Yasuo countered by Pantheon
    (157, 127, 'mid'): 0.85,   # Yasuo countered by Lissandra

    # Yone (777) counters - mid
    (777, 90, 'mid'): 0.9,     # Yone countered by Malzahar
    (777, 127, 'mid'): 0.85,   # Yone countered by Lissandra

    # LeBlanc (7) counters
    (7, 55, 'mid'): 0.85,      # LeBlanc countered by Kassadin
    (7, 90, 'mid'): 0.8,       # LeBlanc countered by Malzahar

    # Akali (84) counters
    (84, 90, 'mid'): 0.85,     # Akali countered by Malzahar
    (84, 1, 'mid'): 0.8,       # Akali countered by Annie

    # Sylas (517) counters
    (517, 69, 'mid'): 0.85,    # Sylas countered by Cassiopeia
    (517, 90, 'mid'): 0.8,     # Sylas countered by Malzahar

    # ===== JUNGLE =====
    # Lee Sin (64) counters
    (64, 107, 'jungle'): 0.8,  # Lee Sin countered by Rengar (bush control)
    (64, 121, 'jungle'): 0.8,  # Lee Sin countered by Kha'Zix

    # Nidalee (76) counters
    (76, 107, 'jungle'): 0.85, # Nidalee countered by Rengar
    (76, 121, 'jungle'): 0.8,  # Nidalee countered by Kha'Zix

    # ===== ADC =====
    # Draven (119) counters
    (119, 51, 'adc'): 0.85,    # Draven countered by Caitlyn (range)
    (119, 222, 'adc'): 0.8,    # Draven countered by Jinx (outscales)

    # Vayne (67) counters
    (67, 119, 'adc'): 0.85,    # Vayne countered by Draven (early bully)
    (67, 51, 'adc'): 0.8,      # Vayne countered by Caitlyn

    # Kog'Maw (96) counters
    (96, 119, 'adc'): 0.85,    # Kog'Maw countered by Draven
    (96, 51, 'adc'): 0.8,      # Kog'Maw countered by Caitlyn

    # ===== SUPPORT =====
    # Yuumi (350) counters
    (350, 111, 'support'): 0.9,  # Yuumi countered by Nautilus (hook + lock)
    (350, 89, 'support'): 0.85,  # Yuumi countered by Leona
    (350, 53, 'support'): 0.85,  # Yuumi countered by Blitzcrank

    # Sona (37) counters
    (37, 111, 'support'): 0.85,  # Sona countered by Nautilus
    (37, 89, 'support'): 0.85,   # Sona countered by Leona
    (37, 53, 'support'): 0.8,    # Sona countered by Blitzcrank

    # Soraka (16) counters
    (16, 111, 'support'): 0.85,  # Soraka countered by Nautilus
    (16, 89, 'support'): 0.8,    # Soraka countered by Leona

    # Lux (99) counters - support
    (99, 53, 'support'): 0.85,   # Lux countered by Blitzcrank
    (99, 111, 'support'): 0.8,   # Lux countered by Nautilus
}


# =============================================================================
# LANE SYNERGIES - Bot Lane and Jungle Roaming
# =============================================================================
# Format: (champion_1_id, champion_2_id): synergy_score (0.0 to 1.0)

BOT_LANE_SYNERGIES = {
    # ===== KILL LANE (High early aggression, all-in potential) =====
    'kill_lane': {
        # Draven (119) combos - early game terror
        (119, 89): 0.95,     # Draven + Leona - level 2 all-in
        (119, 111): 0.9,     # Draven + Nautilus - hook + axes
        (119, 412): 0.9,     # Draven + Thresh - lantern engage
        (119, 53): 0.85,     # Draven + Blitzcrank - pull = death

        # Samira (360) combos - dash + style
        (360, 89): 0.95,     # Samira + Leona - all-in combo
        (360, 111): 0.95,    # Samira + Nautilus - engage chain
        (360, 412): 0.9,     # Samira + Thresh - flay + dash
        (360, 497): 0.9,     # Samira + Rakan - knock-up combo

        # Lucian (236) combos - burst trades
        (236, 89): 0.9,      # Lucian + Leona - short trade
        (236, 111): 0.85,    # Lucian + Nautilus
        (236, 555): 0.9,     # Lucian + Pyke - execute combo

        # Kalista (429) combos - R synergy
        (429, 89): 0.9,      # Kalista + Leona - ult into engage
        (429, 412): 0.9,     # Kalista + Thresh - double knock-up
        (429, 111): 0.85,    # Kalista + Nautilus

        # Tristana (18) combos - burst all-in
        (18, 89): 0.9,       # Tristana + Leona
        (18, 111): 0.85,     # Tristana + Nautilus

        # Miss Fortune (21) combos
        (21, 89): 0.85,      # MF + Leona - ult setup
        (21, 111): 0.85,     # MF + Nautilus
    },

    # ===== POKE LANE (Range advantage, chip damage) =====
    'poke_lane': {
        # Caitlyn (51) combos - trap control
        (51, 267): 0.9,      # Caitlyn + Nami - bubble + trap
        (51, 99): 0.9,       # Caitlyn + Lux - binding + trap
        (51, 25): 0.85,      # Caitlyn + Morgana - snare combo
        (51, 143): 0.85,     # Caitlyn + Zyra - zone control
        (51, 63): 0.85,      # Caitlyn + Brand - poke + burn

        # Ezreal (81) combos - safe poke
        (81, 37): 0.85,      # Ezreal + Sona - sustain poke
        (81, 267): 0.85,     # Ezreal + Nami - heal + poke
        (81, 117): 0.8,      # Ezreal + Lulu - speed + poke
        (81, 43): 0.85,      # Ezreal + Karma - double Q poke

        # Jhin (202) combos - root setup
        (202, 25): 0.9,      # Jhin + Morgana - double snare
        (202, 99): 0.9,      # Jhin + Lux - binding chain
        (202, 143): 0.85,    # Jhin + Zyra - root + trap

        # Varus (110) combos
        (110, 25): 0.9,      # Varus + Morgana - chain CC
        (110, 99): 0.85,     # Varus + Lux

        # Ashe (22) combos
        (22, 143): 0.85,     # Ashe + Zyra - slows + roots
        (22, 99): 0.85,      # Ashe + Lux - arrow + bind
    },

    # ===== PROTECT THE CARRY (Hypercarry + Enchanter) =====
    'protect_carry': {
        # Kog'Maw (96) combos - THE hypercarry
        (96, 117): 0.95,     # Kog'Maw + Lulu - THE protect comp
        (96, 40): 0.9,       # Kog'Maw + Janna - peel queen
        (96, 350): 0.9,      # Kog'Maw + Yuumi - infinite sustain
        (96, 267): 0.85,     # Kog'Maw + Nami - heal + AS

        # Jinx (222) combos - late game monster
        (222, 117): 0.9,     # Jinx + Lulu - speed + shield
        (222, 40): 0.85,     # Jinx + Janna - tornado peel
        (222, 350): 0.85,    # Jinx + Yuumi - late scaling

        # Twitch (29) combos - stealth + hypercarry
        (29, 117): 0.95,     # Twitch + Lulu - ICONIC duo
        (29, 350): 0.9,      # Twitch + Yuumi - invisible cat
        (29, 40): 0.85,      # Twitch + Janna

        # Vayne (67) combos - scaling
        (67, 117): 0.9,      # Vayne + Lulu - condemn + polymorph
        (67, 40): 0.9,       # Vayne + Janna - peel for days
        (67, 267): 0.85,     # Vayne + Nami

        # Aphelios (523) combos
        (523, 117): 0.9,     # Aphelios + Lulu
        (523, 350): 0.85,    # Aphelios + Yuumi
        (523, 412): 0.85,    # Aphelios + Thresh - lantern saves

        # Zeri (221) combos
        (221, 117): 0.9,     # Zeri + Lulu - speed machine
        (221, 350): 0.85,    # Zeri + Yuumi
    },

    # ===== ENGAGE/WOMBO LANE (AoE ult synergy) =====
    'engage_lane': {
        # Miss Fortune (21) - ult follow-up
        (21, 497): 0.95,     # MF + Rakan - grand entrance + bullet time
        (21, 412): 0.9,      # MF + Thresh - box + ult
        (21, 89): 0.85,      # MF + Leona - ult zone

        # Xayah (498) combos - Rakan soulmate
        (498, 497): 0.95,    # Xayah + Rakan - THE couple
        (498, 89): 0.85,     # Xayah + Leona

        # Sivir (15) combos - engage support
        (15, 497): 0.9,      # Sivir + Rakan - ult synergy
        (15, 89): 0.85,      # Sivir + Leona - ult engage

        # Kai'Sa (145) combos
        (145, 111): 0.9,     # Kai'Sa + Nautilus - passive proc
        (145, 89): 0.9,      # Kai'Sa + Leona - follow R
        (145, 412): 0.85,    # Kai'Sa + Thresh
    },
}

JUNGLE_ROAM_SYNERGIES = {
    # ===== JUNGLE + MID (Gank synergy) =====
    'jungle_mid': {
        # Lee Sin (64) + mid assassins
        (64, 238): 0.9,      # Lee Sin + Zed - double dive
        (64, 91): 0.9,       # Lee Sin + Talon - roam kings
        (64, 245): 0.85,     # Lee Sin + Ekko - timing plays

        # Elise (60) + CC mid
        (60, 134): 0.9,      # Elise + Syndra - stun chain
        (60, 61): 0.85,      # Elise + Orianna - cocoon + ult
        (60, 112): 0.85,     # Elise + Viktor - cage combo

        # Jarvan IV (59) + wombo mid
        (59, 61): 0.95,      # J4 + Orianna - ICONIC wombo
        (59, 112): 0.9,      # J4 + Viktor - cage in cataclysm
        (59, 134): 0.85,     # J4 + Syndra

        # Rek'Sai (421) + burst mid
        (421, 7): 0.85,      # Rek'Sai + LeBlanc - tunnel + burst
        (421, 238): 0.85,    # Rek'Sai + Zed - knock-up combo

        # Nocturne (56) + dive mid
        (56, 238): 0.9,      # Nocturne + Zed - double darkness
        (56, 91): 0.85,      # Nocturne + Talon - no vision needed

        # Zac (154) + follow-up mid
        (154, 61): 0.9,      # Zac + Orianna - E in with ball
        (154, 134): 0.85,    # Zac + Syndra - CC chain

        # Nidalee (76) + poke/dive mid
        (76, 238): 0.85,     # Nidalee + Zed - poke + all-in
        (76, 7): 0.85,       # Nidalee + LeBlanc

        # Kha'Zix (121) + assassin mid
        (121, 238): 0.9,     # Kha'Zix + Zed - isolation + shadows
        (121, 91): 0.85,     # Kha'Zix + Talon

        # Vi (254) + burst mid
        (254, 134): 0.9,     # Vi + Syndra - ult + stun lock
        (254, 7): 0.85,      # Vi + LeBlanc - Q + chain
    },

    # ===== JUNGLE + TOP (Dive/gank synergy) =====
    'jungle_top': {
        # Sejuani (113) + CC top
        (113, 516): 0.95,    # Sejuani + Ornn - double knockup
        (113, 57): 0.9,      # Sejuani + Maokai - CC chain
        (113, 54): 0.85,     # Sejuani + Malphite

        # Zac (154) + engage top
        (154, 516): 0.9,     # Zac + Ornn - wombo
        (154, 57): 0.85,     # Zac + Maokai - double engage

        # Jarvan IV (59) + dive top
        (59, 58): 0.9,       # J4 + Renekton - early dive
        (59, 240): 0.85,     # J4 + Kled - double all-in
        (59, 164): 0.85,     # J4 + Camille - cataclysm + R

        # Rek'Sai (421) + bruiser top
        (421, 58): 0.85,     # Rek'Sai + Renekton - stun chain
        (421, 164): 0.85,    # Rek'Sai + Camille

        # Warwick (19) + all-in top
        (19, 58): 0.9,       # Warwick + Renekton - sustain dive
        (19, 240): 0.85,     # Warwick + Kled - run down

        # Elise (60) + dive top
        (60, 58): 0.85,      # Elise + Renekton - cocoon dive
        (60, 164): 0.85,     # Elise + Camille

        # Vi (254) + lockdown top
        (254, 164): 0.9,     # Vi + Camille - double lockdown
        (254, 516): 0.85,    # Vi + Ornn - ult chain

        # Hecarim (120) + dive top
        (120, 58): 0.85,     # Hecarim + Renekton - run at them
        (120, 164): 0.85,    # Hecarim + Camille

        # Poppy (78) + wall stun top
        (78, 164): 0.85,     # Poppy + Camille - double wall CC
        (78, 240): 0.85,     # Poppy + Kled - charge + stun
    },
}

# Encoding for synergy types
SYNERGY_TYPE_ENCODING = {
    'none': 0,
    'kill_lane': 1,
    'poke_lane': 2,
    'protect_carry': 3,
    'engage_lane': 4,
}


class MatchupAnalyzer:
    """
    Analyzes lane matchups and detects unfavorable/counter matchups.

    Combines:
    - Known counter picks (KNOWN_COUNTERS dictionary)
    - Data-driven matchup win rates from collected matches
    """

    # Matchup severity thresholds
    FAVORABLE_THRESHOLD = 0.52      # > 52% = favorable
    NEUTRAL_HIGH = 0.52             # 48-52% = neutral
    NEUTRAL_LOW = 0.48
    UNFAVORABLE_THRESHOLD = 0.45    # 45-48% = unfavorable
    COUNTER_THRESHOLD = 0.45        # < 45% = counter

    def __init__(self, df: pd.DataFrame = None, min_games: int = 10):
        """
        Initialize matchup analyzer.

        Args:
            df: DataFrame with match data
            min_games: Minimum games for reliable matchup data
        """
        self.df = df
        self.min_games = min_games
        self.matchups = {}  # {(champ_a, champ_b, position): stats}
        self.champion_names = {}  # {champion_id: name}

        if df is not None:
            self._build_champion_names()
            self.calculate_all_matchups()

    def _build_champion_names(self):
        """Build champion ID to name mapping from data."""
        if self.df is None:
            return

        for team in ['team_100', 'team_200']:
            for pos in ['top', 'jungle', 'mid', 'adc', 'support']:
                id_col = f'{team}_{pos}_champion_id'
                name_col = f'{team}_{pos}_champion_name'
                if id_col in self.df.columns and name_col in self.df.columns:
                    for _, row in self.df[[id_col, name_col]].drop_duplicates().dropna().iterrows():
                        self.champion_names[int(row[id_col])] = row[name_col]

    def get_champion_name(self, champion_id: int) -> str:
        """Get champion name from ID."""
        return self.champion_names.get(champion_id, str(champion_id))

    def calculate_all_matchups(self) -> dict:
        """
        Calculate all lane matchups with detailed statistics.

        Returns:
            dict: {(champ_a, champ_b, position): {
                'games': int,
                'wins_for_a': int,
                'winrate': float,
                'gold_diff_avg': float (optional),
                'cs_diff_avg': float (optional)
            }}
        """
        if self.df is None:
            return {}

        print("  Calculating lane matchups...")
        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        matchups = {}

        for _, row in self.df.iterrows():
            team_100_win = row.get('team_100_win', False)

            for pos in positions:
                col_100 = f'team_100_{pos}_champion_id'
                col_200 = f'team_200_{pos}_champion_id'

                if col_100 not in self.df.columns or col_200 not in self.df.columns:
                    continue

                champ_100 = row.get(col_100)
                champ_200 = row.get(col_200)

                if pd.isna(champ_100) or pd.isna(champ_200):
                    continue

                champ_100 = int(champ_100)
                champ_200 = int(champ_200)
                key = (champ_100, champ_200, pos)

                if key not in matchups:
                    matchups[key] = {'games': 0, 'wins_for_a': 0}

                matchups[key]['games'] += 1
                if team_100_win:
                    matchups[key]['wins_for_a'] += 1

        # Calculate winrates and filter by min_games
        for key, stats in matchups.items():
            if stats['games'] >= self.min_games:
                stats['winrate'] = stats['wins_for_a'] / stats['games']
            else:
                stats['winrate'] = None  # Not enough data

        self.matchups = matchups
        print(f"    Found {len([k for k, v in matchups.items() if v['winrate'] is not None])} matchups with >= {self.min_games} games")
        return matchups

    def get_matchup_winrate(self, champ_a: int, champ_b: int, position: str) -> float:
        """
        Get win rate for champ_a vs champ_b in a specific position.

        Args:
            champ_a: Champion ID for team 100 side
            champ_b: Champion ID for team 200 side
            position: Lane position

        Returns:
            Win rate for champ_a (0.0 to 1.0), or 0.5 if no data
        """
        key = (champ_a, champ_b, position)
        if key in self.matchups and self.matchups[key]['winrate'] is not None:
            return self.matchups[key]['winrate']

        # Try reverse matchup
        reverse_key = (champ_b, champ_a, position)
        if reverse_key in self.matchups and self.matchups[reverse_key]['winrate'] is not None:
            return 1.0 - self.matchups[reverse_key]['winrate']

        # Check known counters
        if (champ_a, champ_b, position) in KNOWN_COUNTERS:
            # champ_a is countered by champ_b
            severity = KNOWN_COUNTERS[(champ_a, champ_b, position)]
            return 0.5 - (severity * 0.2)  # Convert severity to winrate estimate

        if (champ_b, champ_a, position) in KNOWN_COUNTERS:
            # champ_b is countered by champ_a
            severity = KNOWN_COUNTERS[(champ_b, champ_a, position)]
            return 0.5 + (severity * 0.2)

        return 0.5  # No data

    def get_matchup_severity(self, champ_a: int, champ_b: int, position: str) -> str:
        """
        Classify matchup severity for champ_a.

        Returns:
            'favorable': > 52% winrate
            'neutral': 48-52% winrate
            'unfavorable': 45-48% winrate
            'counter': < 45% winrate
        """
        winrate = self.get_matchup_winrate(champ_a, champ_b, position)

        if winrate > self.FAVORABLE_THRESHOLD:
            return 'favorable'
        elif winrate >= self.NEUTRAL_LOW:
            return 'neutral'
        elif winrate >= self.COUNTER_THRESHOLD:
            return 'unfavorable'
        else:
            return 'counter'

    def get_unfavorable_matchups(self, threshold: float = 0.45) -> list:
        """
        Get all matchups with winrate below threshold.

        Args:
            threshold: Win rate threshold (default: 0.45 = 45%)

        Returns:
            List of tuples: [(champ_a_name, champ_b_name, position, winrate, games), ...]
            Sorted by winrate ascending (worst first)
        """
        unfavorable = []

        for key, stats in self.matchups.items():
            if stats['winrate'] is not None and stats['winrate'] < threshold:
                champ_a, champ_b, pos = key
                unfavorable.append((
                    self.get_champion_name(champ_a),
                    self.get_champion_name(champ_b),
                    pos,
                    stats['winrate'],
                    stats['games']
                ))

        # Sort by winrate (worst first)
        unfavorable.sort(key=lambda x: x[3])
        return unfavorable

    def get_counter_matchups(self, threshold: float = 0.40) -> list:
        """
        Get severe counter matchups (very low winrate).

        Args:
            threshold: Win rate threshold (default: 0.40 = 40%)

        Returns:
            List of counter matchups
        """
        return self.get_unfavorable_matchups(threshold)

    def analyze_draft_matchups(self, team_100_comp: dict, team_200_comp: dict) -> dict:
        """
        Analyze matchups for a specific draft.

        Args:
            team_100_comp: {'top': champ_id, 'jungle': champ_id, ...}
            team_200_comp: {'top': champ_id, 'jungle': champ_id, ...}

        Returns:
            {
                'team_100_warnings': [{'position', 'your_champ', 'enemy_champ', 'winrate', 'severity'}, ...],
                'team_200_warnings': [...],
                'team_100_matchup_score': float,  # Average matchup winrate
                'team_200_matchup_score': float,
                'worst_matchup': {...}
            }
        """
        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        team_100_warnings = []
        team_200_warnings = []
        team_100_winrates = []
        team_200_winrates = []

        for pos in positions:
            champ_100 = team_100_comp.get(pos)
            champ_200 = team_200_comp.get(pos)

            if champ_100 is None or champ_200 is None:
                continue

            # Get matchup from team 100's perspective
            winrate_100 = self.get_matchup_winrate(champ_100, champ_200, pos)
            severity_100 = self.get_matchup_severity(champ_100, champ_200, pos)
            team_100_winrates.append(winrate_100)
            team_200_winrates.append(1.0 - winrate_100)

            # Check for warnings
            if severity_100 in ['unfavorable', 'counter']:
                team_100_warnings.append({
                    'position': pos,
                    'your_champ': self.get_champion_name(champ_100),
                    'enemy_champ': self.get_champion_name(champ_200),
                    'winrate': winrate_100,
                    'severity': severity_100
                })

            if (1.0 - winrate_100) < self.UNFAVORABLE_THRESHOLD:
                team_200_warnings.append({
                    'position': pos,
                    'your_champ': self.get_champion_name(champ_200),
                    'enemy_champ': self.get_champion_name(champ_100),
                    'winrate': 1.0 - winrate_100,
                    'severity': self.get_matchup_severity(champ_200, champ_100, pos)
                })

        # Calculate average scores
        team_100_score = np.mean(team_100_winrates) if team_100_winrates else 0.5
        team_200_score = np.mean(team_200_winrates) if team_200_winrates else 0.5

        # Find worst matchup
        worst_matchup = None
        if team_100_warnings:
            worst = min(team_100_warnings, key=lambda x: x['winrate'])
            worst_matchup = {'team': 'team_100', **worst}
        if team_200_warnings:
            worst_200 = min(team_200_warnings, key=lambda x: x['winrate'])
            if worst_matchup is None or worst_200['winrate'] < worst_matchup['winrate']:
                worst_matchup = {'team': 'team_200', **worst_200}

        return {
            'team_100_warnings': team_100_warnings,
            'team_200_warnings': team_200_warnings,
            'team_100_matchup_score': team_100_score,
            'team_200_matchup_score': team_200_score,
            'worst_matchup': worst_matchup,
            'num_counters_team_100': len([w for w in team_100_warnings if w['severity'] == 'counter']),
            'num_counters_team_200': len([w for w in team_200_warnings if w['severity'] == 'counter'])
        }

    def get_matchup_features(self, row: pd.Series) -> dict:
        """
        Extract matchup features for a single match row.

        Features:
        - worst_matchup_winrate: Minimum matchup winrate across all lanes
        - num_unfavorable_matchups: Count of matchups < 45%
        - num_counter_matchups: Count of matchups < 40%
        - avg_matchup_advantage: Average matchup winrate - 0.5
        - matchup_variance: Variance in matchup winrates

        Returns:
            dict with matchup features
        """
        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        matchup_winrates = []

        for pos in positions:
            col_100 = f'team_100_{pos}_champion_id'
            col_200 = f'team_200_{pos}_champion_id'

            champ_100 = row.get(col_100)
            champ_200 = row.get(col_200)

            if pd.notna(champ_100) and pd.notna(champ_200):
                wr = self.get_matchup_winrate(int(champ_100), int(champ_200), pos)
                matchup_winrates.append(wr)
            else:
                matchup_winrates.append(0.5)

        # Calculate features
        worst_wr = min(matchup_winrates) if matchup_winrates else 0.5
        num_unfavorable = sum(1 for wr in matchup_winrates if wr < 0.45)
        num_counter = sum(1 for wr in matchup_winrates if wr < 0.40)
        avg_advantage = np.mean(matchup_winrates) - 0.5 if matchup_winrates else 0.0
        variance = np.var(matchup_winrates) if len(matchup_winrates) > 1 else 0.0

        return {
            'worst_matchup_winrate': worst_wr,
            'num_unfavorable_matchups': num_unfavorable,
            'num_counter_matchups': num_counter,
            'avg_matchup_advantage': avg_advantage,
            'matchup_variance': variance
        }


class ChampionSynergyCalculator:
    """
    Calculates champion synergy scores based on known powerful combinations.

    Detects synergies like:
    - Yasuo + knockup champions (Malphite, Alistar, Gragas, etc.)
    - Wombo combo potential (Orianna + engage, Miss Fortune + AoE CC)
    - Bot lane synergies (ADC + Support pairs)
    - Poke compositions, engage compositions, protect-the-carry
    """

    # Known synergy pairs with their synergy strength (0.0 to 1.0)
    # Format: (champion_id_1, champion_id_2): synergy_score
    # Champion IDs from Riot API
    SYNERGY_PAIRS = {
        # Yasuo (157) synergies with knockup champions
        (157, 54): 0.9,    # Yasuo + Malphite
        (157, 12): 0.85,   # Yasuo + Alistar
        (157, 79): 0.8,    # Yasuo + Gragas
        (157, 113): 0.8,   # Yasuo + Sejuani
        (157, 516): 0.85,  # Yasuo + Ornn
        (157, 497): 0.8,   # Yasuo + Rakan
        (157, 154): 0.75,  # Yasuo + Zac
        (157, 57): 0.7,    # Yasuo + Maokai
        (157, 111): 0.8,   # Yasuo + Nautilus
        (157, 53): 0.7,    # Yasuo + Blitzcrank

        # Yone (777) similar synergies
        (777, 54): 0.85,   # Yone + Malphite
        (777, 12): 0.8,    # Yone + Alistar
        (777, 516): 0.8,   # Yone + Ornn

        # Orianna (61) wombo combos
        (61, 54): 0.9,     # Orianna + Malphite
        (61, 59): 0.85,    # Orianna + Jarvan IV
        (61, 154): 0.85,   # Orianna + Zac
        (61, 79): 0.8,     # Orianna + Gragas
        (61, 19): 0.75,    # Orianna + Warwick

        # Miss Fortune (21) AoE combos
        (21, 12): 0.85,    # MF + Alistar
        (21, 89): 0.9,     # MF + Leona
        (21, 32): 0.85,    # MF + Amumu
        (21, 111): 0.8,    # MF + Nautilus
        (21, 497): 0.8,    # MF + Rakan

        # Samira (360) engage support synergies
        (360, 111): 0.9,   # Samira + Nautilus
        (360, 89): 0.9,    # Samira + Leona
        (360, 12): 0.85,   # Samira + Alistar
        (360, 412): 0.85,  # Samira + Thresh
        (360, 497): 0.85,  # Samira + Rakan

        # Kai'Sa (145) engage support synergies
        (145, 111): 0.85,  # Kai'Sa + Nautilus
        (145, 89): 0.85,   # Kai'Sa + Leona
        (145, 412): 0.8,   # Kai'Sa + Thresh

        # Kog'Maw (96) protect-the-carry
        (96, 117): 0.9,    # Kog'Maw + Lulu
        (96, 40): 0.85,    # Kog'Maw + Janna
        (96, 267): 0.85,   # Kog'Maw + Nami
        (96, 16): 0.8,     # Kog'Maw + Soraka

        # Twitch (29) + enchanter
        (29, 117): 0.85,   # Twitch + Lulu
        (29, 16): 0.8,     # Twitch + Soraka
        (29, 40): 0.75,    # Twitch + Janna

        # Jinx (222) + peel supports
        (222, 412): 0.8,   # Jinx + Thresh
        (222, 117): 0.85,  # Jinx + Lulu
        (222, 40): 0.8,    # Jinx + Janna
        (222, 267): 0.8,   # Jinx + Nami

        # Draven (119) + engage/kill lane
        (119, 89): 0.85,   # Draven + Leona
        (119, 111): 0.85,  # Draven + Nautilus
        (119, 412): 0.8,   # Draven + Thresh

        # Vayne (67) + peel
        (67, 40): 0.85,    # Vayne + Janna
        (67, 117): 0.85,   # Vayne + Lulu
        (67, 16): 0.8,     # Vayne + Soraka

        # Lucian (236) + Nami (strong poke lane)
        (236, 267): 0.9,   # Lucian + Nami
        (236, 350): 0.85,  # Lucian + Yuumi
        (236, 63): 0.8,    # Lucian + Brand

        # Senna (235) synergies
        (235, 223): 0.85,  # Senna + Tahm Kench (fasting Senna)
        (235, 14): 0.8,    # Senna + Sion

        # Rengar (107) + Ivern (107 needs bushes)
        (107, 427): 0.9,   # Rengar + Ivern

        # Kled (240) + engage comps
        (240, 54): 0.8,    # Kled + Malphite (double engage)
        (240, 59): 0.8,    # Kled + Jarvan IV

        # J4 (59) combos
        (59, 61): 0.85,    # J4 + Orianna
        (59, 136): 0.8,    # J4 + Aurelion Sol
        (59, 21): 0.8,     # J4 + Miss Fortune

        # Malphite (54) AoE combos
        (54, 61): 0.9,     # Malphite + Orianna
        (54, 157): 0.9,    # Malphite + Yasuo
        (54, 21): 0.85,    # Malphite + Miss Fortune

        # Seraphine (147) synergies
        (147, 21): 0.85,   # Seraphine + MF
        (147, 222): 0.8,   # Seraphine + Jinx
        (147, 96): 0.85,   # Seraphine + Kog'Maw

        # Zilean (26) + hypercarry
        (26, 67): 0.85,    # Zilean + Vayne
        (26, 96): 0.85,    # Zilean + Kog'Maw
        (26, 29): 0.85,    # Zilean + Twitch

        # Shen (98) global synergy
        (98, 119): 0.8,    # Shen + Draven
        (98, 67): 0.8,     # Shen + Vayne
        (98, 96): 0.85,    # Shen + Kog'Maw
    }

    # Champion categories for composition detection
    KNOCKUP_CHAMPIONS = {54, 12, 79, 113, 516, 497, 154, 57, 111, 53, 59, 164, 3}
    ENGAGE_CHAMPIONS = {54, 12, 89, 111, 497, 59, 113, 516, 240, 79, 57, 154, 32}
    POKE_CHAMPIONS = {101, 99, 202, 115, 126, 161, 143, 63, 43}
    HYPERCARRY_CHAMPIONS = {67, 96, 29, 222, 145, 498}
    ENCHANTER_SUPPORTS = {117, 40, 267, 16, 37, 350, 497, 147}

    def __init__(self, df: pd.DataFrame = None):
        """
        Initialize synergy calculator.

        Args:
            df: Optional DataFrame to calculate data-driven synergies
        """
        self.df = df
        self.data_synergies = {}  # Will store synergies calculated from data

    def calculate_data_driven_synergies(self, min_games: int = 5) -> dict:
        """
        Calculate synergies from actual match data.

        Looks for champion pairs that win more often when together
        vs their individual win rates.

        Args:
            min_games: Minimum games for a pair to be included

        Returns:
            dict: {(champ1, champ2): synergy_bonus}
        """
        if self.df is None:
            return {}

        print("  Calculating data-driven champion synergies...")

        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        pair_stats = {}  # {(champ1, champ2): {'wins': 0, 'games': 0}}

        for _, row in self.df.iterrows():
            team_100_win = row.get('team_100_win', False)

            for team, win in [('team_100', team_100_win), ('team_200', not team_100_win)]:
                # Get all champions in team
                team_champs = []
                for pos in positions:
                    col = f'{team}_{pos}_champion_id'
                    if col in self.df.columns and pd.notna(row.get(col)):
                        team_champs.append(int(row[col]))

                # Record all pairs
                for i, champ1 in enumerate(team_champs):
                    for champ2 in team_champs[i+1:]:
                        # Sort to make key consistent
                        key = tuple(sorted([champ1, champ2]))
                        if key not in pair_stats:
                            pair_stats[key] = {'wins': 0, 'games': 0}
                        pair_stats[key]['games'] += 1
                        if win:
                            pair_stats[key]['wins'] += 1

        # Calculate synergy as win rate deviation from 50%
        synergies = {}
        for pair, stats in pair_stats.items():
            if stats['games'] >= min_games:
                winrate = stats['wins'] / stats['games']
                # Synergy bonus: how much above 50% this pair wins
                synergy_bonus = winrate - 0.5
                if abs(synergy_bonus) > 0.05:  # Only keep meaningful synergies
                    synergies[pair] = synergy_bonus

        self.data_synergies = synergies
        print(f"    Found {len(synergies)} significant champion pair synergies")
        return synergies

    def get_pair_synergy(self, champ1: int, champ2: int) -> float:
        """
        Get synergy score for a champion pair.

        Combines known synergies with data-driven synergies.

        Args:
            champ1: First champion ID
            champ2: Second champion ID

        Returns:
            Synergy score (0.0 to 1.0 for known, -0.5 to 0.5 for data-driven)
        """
        # Check known synergies first
        if (champ1, champ2) in self.SYNERGY_PAIRS:
            return self.SYNERGY_PAIRS[(champ1, champ2)]
        if (champ2, champ1) in self.SYNERGY_PAIRS:
            return self.SYNERGY_PAIRS[(champ2, champ1)]

        # Check data-driven synergies
        key = tuple(sorted([champ1, champ2]))
        if key in self.data_synergies:
            # Convert to 0-1 scale (data synergy is -0.5 to 0.5)
            return 0.5 + self.data_synergies[key]

        return 0.0  # No known synergy

    def calculate_team_synergy_score(self, champion_ids: list) -> dict:
        """
        Calculate synergy features for a team.

        Args:
            champion_ids: List of 5 champion IDs

        Returns:
            dict with synergy features
        """
        valid_ids = [int(c) for c in champion_ids if pd.notna(c)]

        # Total synergy score (sum of all pair synergies)
        total_synergy = 0.0
        synergy_pairs_count = 0
        best_synergy = 0.0

        for i, champ1 in enumerate(valid_ids):
            for champ2 in valid_ids[i+1:]:
                pair_synergy = self.get_pair_synergy(champ1, champ2)
                if pair_synergy > 0:
                    total_synergy += pair_synergy
                    synergy_pairs_count += 1
                    best_synergy = max(best_synergy, pair_synergy)

        # Composition type features
        has_knockup = any(c in self.KNOCKUP_CHAMPIONS for c in valid_ids)
        has_yasuo_yone = 157 in valid_ids or 777 in valid_ids
        knockup_synergy = 1 if (has_knockup and has_yasuo_yone) else 0

        has_engage = sum(1 for c in valid_ids if c in self.ENGAGE_CHAMPIONS)
        has_poke = sum(1 for c in valid_ids if c in self.POKE_CHAMPIONS)
        has_hypercarry = any(c in self.HYPERCARRY_CHAMPIONS for c in valid_ids)
        has_enchanter = any(c in self.ENCHANTER_SUPPORTS for c in valid_ids)

        # Protect-the-carry composition
        protect_comp = 1 if (has_hypercarry and has_enchanter) else 0

        return {
            'total_synergy_score': total_synergy,
            'synergy_pairs_count': synergy_pairs_count,
            'best_synergy': best_synergy,
            'has_knockup_synergy': knockup_synergy,
            'engage_count': has_engage,
            'poke_count': has_poke,
            'protect_the_carry': protect_comp,
        }


class TeamCompositionFeatures:
    """
    Generates team composition features.

    Creates features based on champion classes, damage types,
    and team synergies.
    """

    def __init__(self, champion_data: ChampionData = None):
        """
        Initialize with champion metadata.

        Args:
            champion_data: ChampionData instance (loads automatically if None)
        """
        if champion_data is None:
            self.champion_data = ChampionData()
            self.champion_data.load()
        else:
            self.champion_data = champion_data

    def get_team_damage_profile(self, champion_ids: list) -> dict:
        """
        Calculate team damage type distribution.

        Args:
            champion_ids: List of 5 champion IDs

        Returns:
            dict with physical_ratio, magic_ratio, mixed_ratio
        """
        physical = 0
        magic = 0
        mixed = 0

        for champ_id in champion_ids:
            if pd.isna(champ_id):
                mixed += 1
                continue

            damage_type = self.champion_data.get_damage_type(int(champ_id))
            if damage_type == 'physical':
                physical += 1
            elif damage_type == 'magic':
                magic += 1
            else:
                mixed += 1

        total = max(len(champion_ids), 1)
        return {
            'physical_ratio': physical / total,
            'magic_ratio': magic / total,
            'mixed_ratio': mixed / total,
            'is_balanced': 1 if (physical >= 2 and magic >= 2) else 0
        }

    def get_team_class_composition(self, champion_ids: list) -> dict:
        """
        Count champion classes in a team.

        Args:
            champion_ids: List of 5 champion IDs

        Returns:
            dict with counts for each class
        """
        counts = {
            'tanks': 0,
            'assassins': 0,
            'mages': 0,
            'marksmen': 0,
            'supports': 0,
            'fighters': 0
        }

        for champ_id in champion_ids:
            if pd.isna(champ_id):
                continue

            champ_id = int(champ_id)
            if self.champion_data.is_tank(champ_id):
                counts['tanks'] += 1
            if self.champion_data.is_assassin(champ_id):
                counts['assassins'] += 1
            if self.champion_data.is_mage(champ_id):
                counts['mages'] += 1
            if self.champion_data.is_marksman(champ_id):
                counts['marksmen'] += 1
            if self.champion_data.is_support(champ_id):
                counts['supports'] += 1
            if self.champion_data.is_fighter(champ_id):
                counts['fighters'] += 1

        return counts

    def get_team_playstyle_features(self, champion_ids: list) -> dict:
        """
        Calculate team playstyle features from CommunityDragon data.

        Args:
            champion_ids: List of 5 champion IDs

        Returns:
            dict with total and average scores for:
                - crowdControl: CC and lockdown capability
                - damage: Damage output potential
                - durability: Tankiness and survivability
                - mobility: Movement and mobility
                - utility: Support and utility
        """
        playstyle_stats = ['crowdControl', 'damage', 'durability', 'mobility', 'utility']
        features = {}

        # Calculate totals and averages for each stat
        for stat in playstyle_stats:
            scores = []
            for champ_id in champion_ids:
                if pd.notna(champ_id):
                    playstyle = self.champion_data.get_playstyle_info(int(champ_id))
                    scores.append(playstyle.get(stat, 2))  # Default to 2 (medium)
                else:
                    scores.append(2)  # Default for missing champions

            features[f'total_{stat}'] = sum(scores)
            features[f'avg_{stat}'] = round(sum(scores) / len(scores), 2) if scores else 2.0

        return features

    def calculate_team_features(self, row: pd.Series, team: str) -> dict:
        """
        Calculate all team composition features for one team.

        Args:
            row: DataFrame row with champion data
            team: 'team_100' or 'team_200'

        Returns:
            dict with all team features
        """
        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        champion_ids = []

        for pos in positions:
            col = f'{team}_{pos}_champion_id'
            if col in row.index:
                champion_ids.append(row[col])
            else:
                champion_ids.append(None)

        # Get damage profile
        damage = self.get_team_damage_profile(champion_ids)

        # Get class composition
        classes = self.get_team_class_composition(champion_ids)

        # Combine features with team prefix
        features = {}
        for key, value in damage.items():
            features[f'{team}_{key}'] = value
        for key, value in classes.items():
            features[f'{team}_{key}'] = value

        return features


class LaneSynergyCalculator:
    """
    Calculates lane-specific synergies for bot lane and jungle roaming.

    Types of synergies:
    - Bot Lane (ADC + Support): kill_lane, poke_lane, protect_carry, engage_lane
    - Jungle Roaming: jungle_mid, jungle_top
    """

    def __init__(self):
        """Initialize with synergy dictionaries."""
        self.bot_synergies = BOT_LANE_SYNERGIES
        self.jungle_synergies = JUNGLE_ROAM_SYNERGIES
        self.type_encoding = SYNERGY_TYPE_ENCODING

    def get_bot_lane_synergy(self, adc_id: int, support_id: int) -> dict:
        """
        Get bot lane synergy between ADC and Support.

        Args:
            adc_id: Champion ID of ADC
            support_id: Champion ID of Support

        Returns:
            dict: {
                'score': float (0.0 to 1.0),
                'type': str (synergy type name),
                'type_encoded': int,
                'strength': str ('none', 'weak', 'moderate', 'strong')
            }
        """
        if pd.isna(adc_id) or pd.isna(support_id):
            return {
                'score': 0.0,
                'type': 'none',
                'type_encoded': 0,
                'strength': 'none'
            }

        adc_id = int(adc_id)
        support_id = int(support_id)
        pair = (adc_id, support_id)

        # Search in all synergy types
        for synergy_type, pairs in self.bot_synergies.items():
            if pair in pairs:
                score = pairs[pair]
                return {
                    'score': score,
                    'type': synergy_type,
                    'type_encoded': self.type_encoding.get(synergy_type, 0),
                    'strength': self._score_to_strength(score)
                }

        # No known synergy found
        return {
            'score': 0.0,
            'type': 'none',
            'type_encoded': 0,
            'strength': 'none'
        }

    def get_jungle_lane_synergy(self, jungle_id: int, lane_champ_id: int,
                                 lane_type: str = 'mid') -> dict:
        """
        Get jungle-lane roaming synergy.

        Args:
            jungle_id: Champion ID of jungler
            lane_champ_id: Champion ID of laner
            lane_type: 'mid' or 'top'

        Returns:
            dict: {
                'score': float,
                'strength': str
            }
        """
        if pd.isna(jungle_id) or pd.isna(lane_champ_id):
            return {'score': 0.0, 'strength': 'none'}

        jungle_id = int(jungle_id)
        lane_champ_id = int(lane_champ_id)
        pair = (jungle_id, lane_champ_id)

        synergy_key = f'jungle_{lane_type}'
        if synergy_key in self.jungle_synergies:
            pairs = self.jungle_synergies[synergy_key]
            if pair in pairs:
                score = pairs[pair]
                return {
                    'score': score,
                    'strength': self._score_to_strength(score)
                }

        return {'score': 0.0, 'strength': 'none'}

    def _score_to_strength(self, score: float) -> str:
        """Convert numeric score to strength category."""
        if score >= 0.9:
            return 'strong'
        elif score >= 0.8:
            return 'moderate'
        elif score > 0:
            return 'weak'
        return 'none'

    def calculate_lane_synergy_features(self, row: pd.Series) -> dict:
        """
        Calculate all lane synergy features for a match row.

        Args:
            row: DataFrame row with champion IDs

        Returns:
            dict: All lane synergy features for both teams
        """
        features = {}

        # ===== TEAM 100 =====
        # Bot lane synergy
        t100_bot = self.get_bot_lane_synergy(
            row.get('team_100_adc_champion_id'),
            row.get('team_100_support_champion_id')
        )
        features['team_100_bot_synergy_score'] = t100_bot['score']
        features['team_100_bot_synergy_type'] = t100_bot['type_encoded']

        # Jungle-Mid synergy
        t100_jg_mid = self.get_jungle_lane_synergy(
            row.get('team_100_jungle_champion_id'),
            row.get('team_100_mid_champion_id'),
            'mid'
        )
        features['team_100_jungle_mid_synergy'] = t100_jg_mid['score']

        # Jungle-Top synergy
        t100_jg_top = self.get_jungle_lane_synergy(
            row.get('team_100_jungle_champion_id'),
            row.get('team_100_top_champion_id'),
            'top'
        )
        features['team_100_jungle_top_synergy'] = t100_jg_top['score']

        # Total lane synergy for team 100
        features['team_100_total_lane_synergy'] = (
            t100_bot['score'] +
            t100_jg_mid['score'] +
            t100_jg_top['score']
        )

        # ===== TEAM 200 =====
        # Bot lane synergy
        t200_bot = self.get_bot_lane_synergy(
            row.get('team_200_adc_champion_id'),
            row.get('team_200_support_champion_id')
        )
        features['team_200_bot_synergy_score'] = t200_bot['score']
        features['team_200_bot_synergy_type'] = t200_bot['type_encoded']

        # Jungle-Mid synergy
        t200_jg_mid = self.get_jungle_lane_synergy(
            row.get('team_200_jungle_champion_id'),
            row.get('team_200_mid_champion_id'),
            'mid'
        )
        features['team_200_jungle_mid_synergy'] = t200_jg_mid['score']

        # Jungle-Top synergy
        t200_jg_top = self.get_jungle_lane_synergy(
            row.get('team_200_jungle_champion_id'),
            row.get('team_200_top_champion_id'),
            'top'
        )
        features['team_200_jungle_top_synergy'] = t200_jg_top['score']

        # Total lane synergy for team 200
        features['team_200_total_lane_synergy'] = (
            t200_bot['score'] +
            t200_jg_mid['score'] +
            t200_jg_top['score']
        )

        # ===== DIFFERENTIAL FEATURES =====
        # Bot lane advantage
        features['bot_synergy_diff'] = (
            t100_bot['score'] - t200_bot['score']
        )

        # Jungle roaming advantage
        features['jungle_roam_diff'] = (
            (t100_jg_mid['score'] + t100_jg_top['score']) -
            (t200_jg_mid['score'] + t200_jg_top['score'])
        )

        # Total lane synergy advantage
        features['lane_synergy_advantage'] = (
            features['team_100_total_lane_synergy'] -
            features['team_200_total_lane_synergy']
        )

        return features


class DataPreparer:
    """
    Prepares match data for machine learning.

    Features:
    - Automatic ID column removal
    - Champion One-Hot encoding with frequency threshold
    - Train/Val/Test split with stratification
    - Parquet output for fast loading
    """

    # Post-game statistics that reveal match outcome (data leakage)
    # These should be excluded for draft-only prediction
    POST_GAME_COLUMNS = [
        # Player performance stats
        'kills', 'deaths', 'assists', 'kda',
        'gold',  # catches all gold-related columns (gold, goldEarned, etc.)
        'totalMinionsKilled', 'minions', 'cs',
        'vision',  # catches visionScore, vision_score, etc.
        'wardsPlaced', 'wardsKilled', 'wards',
        'totalDamageDealt', 'damageDealt', 'damage',
        'level',  # champion level at end of game
        # Team objective stats
        'tower_kills', 'inhibitor_kills', 'baron_kills', 'dragon_kills',
        'rift_herald_kills', 'horde_kills',
        'towers', 'inhibitors', 'barons', 'dragons', 'heralds',
        # First objectives (happen during game)
        'first_blood', 'first_tower', 'first_inhibitor',
        'first_baron', 'first_dragon', 'first_rift_herald',
        # Game outcome indicators
        'teamEarlySurrendered', 'gameEndedInSurrender', 'surrender',
        # Duration reveals stomps vs close games
        'game_duration', 'gameDuration', 'duration',
    ]

    def __init__(self, db_path: str = 'data/lol_matches.db'):
        self.db = MatchDatabase(db_path)
        self.champion_mapping = {}
        self.feature_columns = []
        self.target_column = 'team_100_win'

    def load_data(self) -> pd.DataFrame:
        """Load data from SQLite database"""
        print("Loading data from database...")
        df = self.db.export_to_dataframe()
        print(f"Loaded {len(df)} matches with {len(df.columns)} columns")
        return df

    def remove_id_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove columns that shouldn't influence ML predictions.

        These include:
        - match_id: unique identifier, no predictive value
        - game_version: version string, not useful for prediction
        - Any *_name columns (we use IDs instead)
        """
        print("Removing ID and non-predictive columns...")

        # Columns to explicitly remove
        columns_to_remove = [
            'match_id',
            'game_version',
        ]

        # Pattern-based removal: champion names (we keep IDs)
        name_columns = [col for col in df.columns if 'champion_name' in col.lower()]
        columns_to_remove.extend(name_columns)

        # Remove columns that exist
        columns_to_drop = [col for col in columns_to_remove if col in df.columns]
        df = df.drop(columns=columns_to_drop)

        print(f"Removed {len(columns_to_drop)} columns: {columns_to_drop[:5]}...")
        print(f"Remaining columns: {len(df.columns)}")

        return df

    def remove_post_game_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove post-game statistics to prevent data leakage.

        For draft prediction, we can only use information available at draft time:
        - Champion picks (encoded)
        - Champion bans (if available)

        We must exclude all in-game and post-game stats like kills, gold, towers, etc.
        """
        print("Removing post-game statistics (preventing data leakage)...")

        columns_to_remove = []

        for col in df.columns:
            col_lower = col.lower()
            # Check if any post-game keyword appears in the column name
            for keyword in self.POST_GAME_COLUMNS:
                if keyword.lower() in col_lower:
                    columns_to_remove.append(col)
                    break

        # Remove columns that exist
        columns_to_drop = [col for col in columns_to_remove if col in df.columns]
        df = df.drop(columns=columns_to_drop)

        print(f"  Removed {len(columns_to_drop)} post-game columns")
        if columns_to_drop:
            print(f"  Examples: {columns_to_drop[:10]}...")
        print(f"  Remaining columns: {len(df.columns)}")

        return df

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in the dataset"""
        print("Handling missing values...")

        # Count missing values
        missing_counts = df.isnull().sum()
        columns_with_missing = missing_counts[missing_counts > 0]

        if len(columns_with_missing) > 0:
            print(f"  Found {len(columns_with_missing)} columns with missing values")

            # Fill numeric columns with 0
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            for col in numeric_cols:
                df[col] = df[col].fillna(0)

            # Fill boolean columns with False
            bool_cols = df.select_dtypes(include=['bool']).columns.tolist()
            for col in bool_cols:
                df[col] = df[col].fillna(False)

        print(f"  Missing values handled")
        return df

    def encode_champions(self, df: pd.DataFrame, min_appearances: int = 5) -> pd.DataFrame:
        """
        One-Hot encode champion IDs and ban IDs.

        Args:
            df: DataFrame with champion_id and ban columns
            min_appearances: Minimum times a champion must appear to get its own column.
                           Less frequent champions are grouped into 'other' category.

        Returns:
            DataFrame with One-Hot encoded champion and ban columns
        """
        print(f"Encoding champions and bans (One-Hot, min_appearances={min_appearances})...")

        # Find all champion ID columns (picks)
        champion_columns = [col for col in df.columns if 'champion_id' in col.lower()]
        print(f"  Found {len(champion_columns)} champion pick columns")

        # Find all ban columns (team_100_ban_1, team_200_ban_2, etc.)
        ban_columns = [col for col in df.columns if '_ban_' in col.lower() and 'champion' not in col.lower()]
        print(f"  Found {len(ban_columns)} ban columns")

        # Combine all columns that need encoding
        all_champion_columns = champion_columns + ban_columns

        if not all_champion_columns:
            print("  No champion or ban columns found!")
            return df

        # Collect all champion IDs and their frequencies (from both picks and bans)
        all_champion_ids = []
        for col in all_champion_columns:
            all_champion_ids.extend(df[col].dropna().astype(int).tolist())

        champion_counts = pd.Series(all_champion_ids).value_counts()
        frequent_champions = set(champion_counts[champion_counts >= min_appearances].index)

        print(f"  Found {len(frequent_champions)} frequent champions (appearing >= {min_appearances} times)")
        print(f"  Rare champions will be grouped as 'other'")

        # Create One-Hot encoding for each champion/ban column
        encoded_dfs = []

        for col in all_champion_columns:
            # Replace rare champions with -1 (will become 'other')
            col_data = df[col].fillna(-1).astype(int)
            col_data = col_data.apply(lambda x: x if x in frequent_champions else -1)

            # Get unique values for this column
            unique_vals = sorted(col_data.unique())

            # Create One-Hot columns
            for val in unique_vals:
                if val == -1:
                    new_col_name = f"{col}_other"
                else:
                    new_col_name = f"{col}_{int(val)}"

                encoded_dfs.append(pd.DataFrame({
                    new_col_name: (col_data == val).astype(int)
                }))

        # Combine all One-Hot columns
        if encoded_dfs:
            encoded_df = pd.concat(encoded_dfs, axis=1)
            print(f"  Created {len(encoded_df.columns)} One-Hot columns")

            # Remove original champion and ban columns, add encoded ones
            df = df.drop(columns=all_champion_columns)
            df = pd.concat([df, encoded_df], axis=1)

        print(f"  Final column count: {len(df.columns)}")
        return df

    def encode_champions_label(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Label encode champion IDs (simpler alternative to One-Hot).

        This keeps the original numeric IDs but ensures they're standardized.
        Use this for tree-based models that can handle categorical features.
        """
        print("Encoding champions (Label encoding)...")

        champion_columns = [col for col in df.columns if 'champion_id' in col.lower()]

        for col in champion_columns:
            df[col] = df[col].fillna(-1).astype(int)

        print(f"  Encoded {len(champion_columns)} champion columns")
        return df

    def add_champion_winrate_features(self, df: pd.DataFrame, use_opgg: bool = True) -> pd.DataFrame:
        """
        Add champion win rate features to the dataset.

        Features added:
        - Win rate for each champion in their role (from OP.GG or calculated)
        - Average team win rate
        - Win rate difference between teams
        - Matchup win rates for lanes
        - OP.GG specific: tier, pickrate, banrate

        Args:
            df: Input DataFrame
            use_opgg: If True, use OP.GG data; otherwise calculate from match history
        """
        print("Adding champion win rate features...")

        # Try to use OP.GG data if available
        opgg = None
        if use_opgg:
            opgg = get_opgg_provider()
            if opgg._loaded:
                print("  Using OP.GG data for win rates and matchups")
            else:
                print("  OP.GG data not available, falling back to calculated stats")
                opgg = None

        # Fallback: calculate from match data
        stats_calc = None
        if not opgg:
            stats_calc = ChampionStatsCalculator(df)
            stats_calc.calculate_champion_winrates()
            stats_calc.calculate_matchup_winrates(min_games=3)

        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        new_features = []

        for idx, row in df.iterrows():
            features = {}

            for team in ['team_100', 'team_200']:
                team_winrates = []
                team_tiers = []
                team_pickrates = []

                for pos in positions:
                    col = f'{team}_{pos}_champion_id'
                    if col in df.columns and pd.notna(row.get(col)):
                        champ_id = int(row[col])

                        if opgg:
                            # Use OP.GG data
                            winrate = opgg.get_champion_winrate(champ_id, pos)
                            tier = opgg.get_champion_tier(champ_id, pos)
                            pickrate = opgg.get_champion_pickrate(champ_id, pos)
                            features[f'{team}_{pos}_opgg_tier'] = tier
                            features[f'{team}_{pos}_opgg_pickrate'] = pickrate
                            team_tiers.append(tier)
                            team_pickrates.append(pickrate)
                        else:
                            # Use calculated data
                            winrate = stats_calc.get_champion_winrate(champ_id, pos)

                        features[f'{team}_{pos}_winrate'] = winrate
                        team_winrates.append(winrate)
                    else:
                        features[f'{team}_{pos}_winrate'] = 0.5
                        team_winrates.append(0.5)
                        if opgg:
                            features[f'{team}_{pos}_opgg_tier'] = 3
                            features[f'{team}_{pos}_opgg_pickrate'] = 0.0
                            team_tiers.append(3)
                            team_pickrates.append(0.0)

                # Average team win rate
                features[f'{team}_avg_winrate'] = np.mean(team_winrates)

                # OP.GG specific aggregations
                if opgg:
                    features[f'{team}_avg_tier'] = np.mean(team_tiers)
                    features[f'{team}_avg_pickrate'] = np.mean(team_pickrates)

            # Win rate difference
            features['winrate_diff'] = features['team_100_avg_winrate'] - features['team_200_avg_winrate']

            # Tier difference (lower is better, so team_200 - team_100)
            if opgg:
                features['tier_diff'] = features['team_200_avg_tier'] - features['team_100_avg_tier']

            # Matchup win rates for each lane
            for pos in positions:
                col_100 = f'team_100_{pos}_champion_id'
                col_200 = f'team_200_{pos}_champion_id'

                if col_100 in df.columns and col_200 in df.columns:
                    champ_100 = row.get(col_100)
                    champ_200 = row.get(col_200)

                    if pd.notna(champ_100) and pd.notna(champ_200):
                        if opgg:
                            matchup_wr = opgg.get_matchup_winrate(
                                int(champ_100), int(champ_200), pos
                            )
                        else:
                            matchup_wr = stats_calc.get_matchup_winrate(
                                int(champ_100), int(champ_200), pos
                            )
                        features[f'matchup_{pos}_winrate'] = matchup_wr
                    else:
                        features[f'matchup_{pos}_winrate'] = 0.5
                else:
                    features[f'matchup_{pos}_winrate'] = 0.5

            # Average matchup winrate (relative to 0.5)
            matchup_wrs = [features[f'matchup_{pos}_winrate'] for pos in positions]
            features['avg_lane_matchup_winrate'] = np.mean(matchup_wrs) - 0.5

            new_features.append(features)

        # Add features to dataframe
        features_df = pd.DataFrame(new_features, index=df.index)
        df = pd.concat([df, features_df], axis=1)

        print(f"  Added {len(features_df.columns)} win rate features")
        return df

    def add_team_composition_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add team composition features (damage types, champion classes).

        Features added:
        - Physical/magic/mixed damage ratio per team
        - Is team damage balanced (1 if both physical and magic)
        - Count of tanks, assassins, mages, marksmen, supports, fighters
        - Difference features between teams
        """
        print("Adding team composition features...")

        # Initialize team composition calculator
        comp_features = TeamCompositionFeatures()

        new_features = []

        for idx, row in df.iterrows():
            features = {}

            # Get features for both teams
            team_100_features = comp_features.calculate_team_features(row, 'team_100')
            team_200_features = comp_features.calculate_team_features(row, 'team_200')

            features.update(team_100_features)
            features.update(team_200_features)

            # Add difference features
            features['tanks_diff'] = team_100_features.get('team_100_tanks', 0) - team_200_features.get('team_200_tanks', 0)
            features['assassins_diff'] = team_100_features.get('team_100_assassins', 0) - team_200_features.get('team_200_assassins', 0)
            features['damage_balance_diff'] = team_100_features.get('team_100_is_balanced', 0) - team_200_features.get('team_200_is_balanced', 0)

            new_features.append(features)

        # Add features to dataframe
        features_df = pd.DataFrame(new_features, index=df.index)
        df = pd.concat([df, features_df], axis=1)

        print(f"  Added {len(features_df.columns)} team composition features")
        return df

    def add_playstyle_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add champion playstyle features from CommunityDragon data.

        Features added per team:
        - Total crowdControl (CC capability)
        - Average crowdControl
        - Total damage (Damage output)
        - Average damage
        - Total durability (Tankiness)
        - Average durability
        - Total mobility (Movement capability)
        - Average mobility
        - Total utility (Support/utility capability)
        - Average utility

        Also adds difference features between teams for each stat.
        """
        print("Adding playstyle features from CommunityDragon...")

        # Initialize team composition calculator (has champion_data with playstyle info)
        comp_features = TeamCompositionFeatures()

        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        new_features = []

        for idx, row in df.iterrows():
            features = {}

            for team in ['team_100', 'team_200']:
                # Get all champion IDs for this team
                champion_ids = []
                for pos in positions:
                    col = f'{team}_{pos}_champion_id'
                    if col in df.columns:
                        champion_ids.append(row.get(col))
                    else:
                        champion_ids.append(None)

                # Get playstyle features for this team
                playstyle = comp_features.get_team_playstyle_features(champion_ids)

                # Add with team prefix
                for key, value in playstyle.items():
                    features[f'{team}_{key}'] = value

            # Add difference features between teams
            playstyle_stats = ['crowdControl', 'damage', 'durability', 'mobility', 'utility']
            for stat in playstyle_stats:
                features[f'{stat}_diff'] = (
                    features.get(f'team_100_total_{stat}', 0) -
                    features.get(f'team_200_total_{stat}', 0)
                )
                features[f'avg_{stat}_diff'] = round(
                    features.get(f'team_100_avg_{stat}', 2.0) -
                    features.get(f'team_200_avg_{stat}', 2.0),
                    2
                )

            new_features.append(features)

        # Add features to dataframe
        features_df = pd.DataFrame(new_features, index=df.index)
        df = pd.concat([df, features_df], axis=1)

        print(f"  Added {len(features_df.columns)} playstyle features")
        return df

    def add_synergy_features(self, df: pd.DataFrame, use_opgg: bool = True) -> pd.DataFrame:
        """
        Add champion synergy features to the dataset.

        Features added per team:
        - Total synergy score (sum of all pair synergies)
        - Number of synergy pairs
        - Best synergy score
        - Has knockup synergy (Yasuo/Yone + knockup champion)
        - Engage count, poke count
        - Protect-the-carry composition indicator
        - OP.GG synergy winrates (if available)

        Also adds difference features between teams.

        Args:
            df: Input DataFrame
            use_opgg: If True, use OP.GG synergy data
        """
        print("Adding champion synergy features...")

        # Try to use OP.GG data if available
        opgg = None
        if use_opgg:
            opgg = get_opgg_provider()
            if opgg._loaded and opgg.synergies is not None:
                print("  Using OP.GG data for synergies")
            else:
                opgg = None

        # Initialize synergy calculator with data for data-driven synergies (fallback)
        synergy_calc = ChampionSynergyCalculator(df)
        synergy_calc.calculate_data_driven_synergies(min_games=5)

        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        new_features = []

        for idx, row in df.iterrows():
            features = {}

            for team in ['team_100', 'team_200']:
                # Get all champions in team with positions
                team_champs = []
                team_positions = []
                for pos in positions:
                    col = f'{team}_{pos}_champion_id'
                    if col in df.columns:
                        team_champs.append(row.get(col))
                        team_positions.append(pos)
                    else:
                        team_champs.append(None)
                        team_positions.append(pos)

                # Calculate synergy features using hardcoded knowledge
                synergy_features = synergy_calc.calculate_team_synergy_score(team_champs)

                # Add with team prefix
                for key, value in synergy_features.items():
                    features[f'{team}_{key}'] = value

                # Add OP.GG synergy winrates
                if opgg:
                    opgg_synergy_wrs = []
                    # Check all pairs of champions
                    for i in range(len(team_champs)):
                        for j in range(i + 1, len(team_champs)):
                            if pd.notna(team_champs[i]) and pd.notna(team_champs[j]):
                                syn_wr = opgg.get_synergy_winrate(
                                    int(team_champs[i]), int(team_champs[j]),
                                    team_positions[i], team_positions[j]
                                )
                                if syn_wr != 0.5:  # Only count if we have data
                                    opgg_synergy_wrs.append(syn_wr)

                    if opgg_synergy_wrs:
                        features[f'{team}_opgg_avg_synergy_wr'] = np.mean(opgg_synergy_wrs)
                        features[f'{team}_opgg_best_synergy_wr'] = max(opgg_synergy_wrs)
                        features[f'{team}_opgg_worst_synergy_wr'] = min(opgg_synergy_wrs)
                        features[f'{team}_opgg_synergy_pairs_found'] = len(opgg_synergy_wrs)
                    else:
                        features[f'{team}_opgg_avg_synergy_wr'] = 0.5
                        features[f'{team}_opgg_best_synergy_wr'] = 0.5
                        features[f'{team}_opgg_worst_synergy_wr'] = 0.5
                        features[f'{team}_opgg_synergy_pairs_found'] = 0

            # Add difference features
            features['synergy_score_diff'] = (
                features.get('team_100_total_synergy_score', 0) -
                features.get('team_200_total_synergy_score', 0)
            )
            features['synergy_pairs_diff'] = (
                features.get('team_100_synergy_pairs_count', 0) -
                features.get('team_200_synergy_pairs_count', 0)
            )
            features['engage_diff'] = (
                features.get('team_100_engage_count', 0) -
                features.get('team_200_engage_count', 0)
            )
            features['poke_diff'] = (
                features.get('team_100_poke_count', 0) -
                features.get('team_200_poke_count', 0)
            )

            # OP.GG synergy differences
            if opgg:
                features['opgg_synergy_wr_diff'] = (
                    features.get('team_100_opgg_avg_synergy_wr', 0.5) -
                    features.get('team_200_opgg_avg_synergy_wr', 0.5)
                )

            new_features.append(features)

        # Add features to dataframe
        features_df = pd.DataFrame(new_features, index=df.index)
        df = pd.concat([df, features_df], axis=1)

        print(f"  Added {len(features_df.columns)} synergy features")
        return df

    def add_dpmlol_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add champion statistics features from DPM.LOL data.

        DPM.LOL provides unique features:
        - tierScore: Composite metric of champion strength in current meta
        - winrateVariance: Trend indicator (is champion getting stronger/weaker?)
        - More granular lane-specific statistics

        Features added per team:
        - dpmlol_avg_tier_score: Average tier score of team composition
        - dpmlol_total_tier_score: Sum of tier scores
        - dpmlol_min_tier_score: Lowest tier score (weakest champion)
        - dpmlol_max_tier_score: Highest tier score (strongest champion)
        - dpmlol_avg_winrate: Average DPM.LOL winrate
        - dpmlol_avg_winrate_variance: Average winrate trend (positive = getting stronger)
        - dpmlol_avg_pickrate: Average pick rate (meta popularity)
        - dpmlol_avg_banrate: Average ban rate (perceived strength)

        Also adds difference features between teams.
        """
        print("Adding DPM.LOL features...")

        dpmlol = get_dpmlol_provider()
        if not dpmlol._loaded:
            print("  Warning: DPM.LOL data not available, skipping features")
            return df

        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        new_features = []

        for idx, row in df.iterrows():
            features = {}

            for team in ['team_100', 'team_200']:
                tier_scores = []
                winrates = []
                winrate_variances = []
                pickrates = []
                banrates = []

                for pos in positions:
                    champ_col = f'{team}_{pos}_champion_id'
                    if champ_col in row and pd.notna(row[champ_col]):
                        champ_id = int(row[champ_col])

                        # Get DPM.LOL stats for this champion in this position
                        tier_score = dpmlol.get_champion_tier_score(champ_id, pos)
                        winrate = dpmlol.get_champion_winrate(champ_id, pos)
                        wr_variance = dpmlol.get_champion_winrate_variance(champ_id, pos)
                        pickrate = dpmlol.get_champion_pickrate(champ_id, pos)
                        banrate = dpmlol.get_champion_banrate(champ_id, pos)

                        tier_scores.append(tier_score)
                        winrates.append(winrate)
                        winrate_variances.append(wr_variance)
                        pickrates.append(pickrate)
                        banrates.append(banrate)

                        # Per-position features
                        features[f'{team}_{pos}_dpmlol_tier_score'] = tier_score
                        features[f'{team}_{pos}_dpmlol_winrate'] = winrate
                        features[f'{team}_{pos}_dpmlol_wr_variance'] = wr_variance

                # Team aggregate features
                if tier_scores:
                    features[f'{team}_dpmlol_avg_tier_score'] = np.mean(tier_scores)
                    features[f'{team}_dpmlol_total_tier_score'] = sum(tier_scores)
                    features[f'{team}_dpmlol_min_tier_score'] = min(tier_scores)
                    features[f'{team}_dpmlol_max_tier_score'] = max(tier_scores)
                    features[f'{team}_dpmlol_tier_score_std'] = np.std(tier_scores) if len(tier_scores) > 1 else 0
                else:
                    features[f'{team}_dpmlol_avg_tier_score'] = 0.0
                    features[f'{team}_dpmlol_total_tier_score'] = 0.0
                    features[f'{team}_dpmlol_min_tier_score'] = 0.0
                    features[f'{team}_dpmlol_max_tier_score'] = 0.0
                    features[f'{team}_dpmlol_tier_score_std'] = 0.0

                if winrates:
                    features[f'{team}_dpmlol_avg_winrate'] = np.mean(winrates)
                else:
                    features[f'{team}_dpmlol_avg_winrate'] = 50.0

                if winrate_variances:
                    features[f'{team}_dpmlol_avg_wr_variance'] = np.mean(winrate_variances)
                    # Count how many champions are trending up vs down
                    features[f'{team}_dpmlol_trending_up_count'] = sum(1 for v in winrate_variances if v > 0)
                    features[f'{team}_dpmlol_trending_down_count'] = sum(1 for v in winrate_variances if v < 0)
                else:
                    features[f'{team}_dpmlol_avg_wr_variance'] = 0.0
                    features[f'{team}_dpmlol_trending_up_count'] = 0
                    features[f'{team}_dpmlol_trending_down_count'] = 0

                if pickrates:
                    features[f'{team}_dpmlol_avg_pickrate'] = np.mean(pickrates)
                    features[f'{team}_dpmlol_total_pickrate'] = sum(pickrates)
                else:
                    features[f'{team}_dpmlol_avg_pickrate'] = 0.0
                    features[f'{team}_dpmlol_total_pickrate'] = 0.0

                if banrates:
                    features[f'{team}_dpmlol_avg_banrate'] = np.mean(banrates)
                else:
                    features[f'{team}_dpmlol_avg_banrate'] = 0.0

            # Difference features (team_100 - team_200)
            features['dpmlol_tier_score_diff'] = (
                features.get('team_100_dpmlol_avg_tier_score', 0) -
                features.get('team_200_dpmlol_avg_tier_score', 0)
            )
            features['dpmlol_total_tier_score_diff'] = (
                features.get('team_100_dpmlol_total_tier_score', 0) -
                features.get('team_200_dpmlol_total_tier_score', 0)
            )
            features['dpmlol_winrate_diff'] = (
                features.get('team_100_dpmlol_avg_winrate', 50) -
                features.get('team_200_dpmlol_avg_winrate', 50)
            )
            features['dpmlol_wr_variance_diff'] = (
                features.get('team_100_dpmlol_avg_wr_variance', 0) -
                features.get('team_200_dpmlol_avg_wr_variance', 0)
            )
            features['dpmlol_pickrate_diff'] = (
                features.get('team_100_dpmlol_avg_pickrate', 0) -
                features.get('team_200_dpmlol_avg_pickrate', 0)
            )
            features['dpmlol_banrate_diff'] = (
                features.get('team_100_dpmlol_avg_banrate', 0) -
                features.get('team_200_dpmlol_avg_banrate', 0)
            )

            # Meta strength indicator: team with more trending-up champions
            features['dpmlol_trending_advantage'] = (
                features.get('team_100_dpmlol_trending_up_count', 0) -
                features.get('team_200_dpmlol_trending_up_count', 0)
            )

            new_features.append(features)

        # Add features to dataframe
        features_df = pd.DataFrame(new_features, index=df.index)
        df = pd.concat([df, features_df], axis=1)

        print(f"  Added {len(features_df.columns)} DPM.LOL features")
        return df

    def add_matchup_detection_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add matchup detection features to identify unfavorable lane matchups.

        Features added:
        - worst_matchup_winrate: Lowest matchup winrate across all 5 lanes
        - num_unfavorable_matchups: Count of lanes with < 45% winrate
        - num_counter_matchups: Count of lanes with < 40% winrate (severe)
        - matchup_advantage_score: Average matchup advantage (mean - 0.5)
        - matchup_variance: How spread out the matchup winrates are
        """
        print("Adding matchup detection features...")

        # Initialize matchup analyzer
        matchup_analyzer = MatchupAnalyzer(df, min_games=10)

        new_features = []

        for idx, row in df.iterrows():
            features = matchup_analyzer.get_matchup_features(row)
            new_features.append(features)

        # Add features to dataframe
        features_df = pd.DataFrame(new_features, index=df.index)
        df = pd.concat([df, features_df], axis=1)

        print(f"  Added {len(features_df.columns)} matchup detection features")
        return df

    def add_lane_synergy_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add lane-specific synergy features for bot lane and jungle roaming.

        Features added:
        - team_100/200_bot_synergy_score: Bot lane synergy strength (0-1)
        - team_100/200_bot_synergy_type: Type (kill/poke/protect/engage)
        - team_100/200_jungle_mid_synergy: Jungle-mid roaming synergy
        - team_100/200_jungle_top_synergy: Jungle-top roaming synergy
        - team_100/200_total_lane_synergy: Sum of all lane synergies
        - bot_synergy_diff: Bot lane synergy advantage
        - jungle_roam_diff: Jungle roaming advantage
        - lane_synergy_advantage: Total lane synergy advantage
        """
        print("Adding lane synergy features...")

        # Initialize lane synergy calculator
        lane_synergy_calc = LaneSynergyCalculator()

        new_features = []

        for idx, row in df.iterrows():
            features = lane_synergy_calc.calculate_lane_synergy_features(row)
            new_features.append(features)

        # Add features to dataframe
        features_df = pd.DataFrame(new_features, index=df.index)
        df = pd.concat([df, features_df], axis=1)

        # Count how many matches have known synergies
        bot_synergies_count = (features_df['team_100_bot_synergy_score'] > 0).sum() + \
                             (features_df['team_200_bot_synergy_score'] > 0).sum()
        jg_synergies_count = (features_df['team_100_jungle_mid_synergy'] > 0).sum() + \
                            (features_df['team_100_jungle_top_synergy'] > 0).sum() + \
                            (features_df['team_200_jungle_mid_synergy'] > 0).sum() + \
                            (features_df['team_200_jungle_top_synergy'] > 0).sum()

        print(f"  Added {len(features_df.columns)} lane synergy features")
        print(f"  Found {bot_synergies_count} bot lane synergies")
        print(f"  Found {jg_synergies_count} jungle roaming synergies")
        return df

    def add_player_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add player-level features based on historical performance (vectorized).

        Features added per player (10 players per match):
        - player_wr_on_champ: Historical winrate on this champion
        - player_games_on_champ: Number of games played (experience)

        Team aggregate features:
        - team_X_avg_player_wr: Average player winrate on their champions
        - team_X_min_player_wr: Lowest player winrate (weak link)
        - team_X_max_player_wr: Highest player winrate (carry)
        - team_X_avg_experience: Average games played on champions

        Differential features:
        - player_wr_diff: Team 100 vs Team 200 average winrate
        - experience_diff: Team 100 vs Team 200 average experience
        """
        print("Adding player features (vectorized)...")

        # Get player-champion stats as DataFrame
        stats_df = self._calculate_player_champion_stats_df()
        print(f"  Calculated stats for {len(stats_df)} player-champion pairs")

        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        result_df = df.copy()

        # For each position and team, merge player stats
        for team in ['team_100', 'team_200']:
            wr_cols = []
            games_cols = []

            for pos in positions:
                puuid_col = f'{team}_{pos}_puuid'
                champ_col = f'{team}_{pos}_champion_id'
                wr_out = f'{team}_{pos}_player_wr'
                games_out = f'{team}_{pos}_player_games'

                # Skip if columns don't exist
                if puuid_col not in result_df.columns or champ_col not in result_df.columns:
                    result_df[wr_out] = 0.5
                    result_df[games_out] = 0
                    wr_cols.append(wr_out)
                    games_cols.append(games_out)
                    continue

                # Create temporary merge keys
                temp_df = result_df[[puuid_col, champ_col]].copy()
                temp_df.columns = ['puuid', 'champion_id']
                temp_df['champion_id'] = temp_df['champion_id'].astype('Int64')

                # Merge with stats
                merged = temp_df.merge(stats_df, on=['puuid', 'champion_id'], how='left')

                # Fill NaN with defaults and add to result
                result_df[wr_out] = merged['winrate'].fillna(0.5).values
                result_df[games_out] = merged['games'].fillna(0).clip(upper=100).values

                wr_cols.append(wr_out)
                games_cols.append(games_out)

            # Team aggregate features
            wr_matrix = result_df[wr_cols].values
            games_matrix = result_df[games_cols].values

            result_df[f'{team}_avg_player_wr'] = np.nanmean(wr_matrix, axis=1)
            result_df[f'{team}_min_player_wr'] = np.nanmin(wr_matrix, axis=1)
            result_df[f'{team}_max_player_wr'] = np.nanmax(wr_matrix, axis=1)
            result_df[f'{team}_player_wr_std'] = np.nanstd(wr_matrix, axis=1)
            result_df[f'{team}_avg_experience'] = np.nanmean(games_matrix, axis=1)
            result_df[f'{team}_total_experience'] = np.nansum(games_matrix, axis=1)

        # Differential features
        result_df['player_wr_diff'] = (
            result_df['team_100_avg_player_wr'] - result_df['team_200_avg_player_wr']
        )
        result_df['experience_diff'] = (
            result_df['team_100_avg_experience'] - result_df['team_200_avg_experience']
        )
        result_df['min_player_wr_diff'] = (
            result_df['team_100_min_player_wr'] - result_df['team_200_min_player_wr']
        )
        result_df['max_player_wr_diff'] = (
            result_df['team_100_max_player_wr'] - result_df['team_200_max_player_wr']
        )

        # Count new features
        player_cols = [c for c in result_df.columns if 'player_wr' in c or 'experience' in c]
        print(f"  Added {len(player_cols)} player features")

        # Show some stats
        avg_wr_100 = result_df['team_100_avg_player_wr'].mean()
        avg_wr_200 = result_df['team_200_avg_player_wr'].mean()
        print(f"  Avg player winrate: Team 100 = {avg_wr_100:.3f}, Team 200 = {avg_wr_200:.3f}")

        return result_df

    def _calculate_player_champion_stats_df(self, before_timestamp: int = None) -> pd.DataFrame:
        """
        Calculate historical winrate per player-champion pair from database.

        Args:
            before_timestamp: If provided, only count matches before this timestamp
                             to avoid data leakage in temporal splits.

        Returns:
            DataFrame with columns: puuid, champion_id, games, winrate
        """
        with self.db.get_connection() as conn:
            # Add temporal filter if specified
            time_filter = ""
            if before_timestamp is not None:
                time_filter = f"AND m.game_creation < {before_timestamp}"

            query = f'''
                SELECT
                    ps.puuid,
                    ps.champion_id,
                    COUNT(*) as games,
                    SUM(CASE WHEN
                        (ps.team_id = 100 AND m.team_100_win = 1) OR
                        (ps.team_id = 200 AND m.team_100_win = 0)
                    THEN 1 ELSE 0 END) as wins
                FROM player_stats ps
                JOIN matches m ON ps.match_id = m.match_id
                WHERE ps.puuid IS NOT NULL AND ps.champion_id IS NOT NULL
                {time_filter}
                GROUP BY ps.puuid, ps.champion_id
                HAVING COUNT(*) >= 3
            '''
            result_df = pd.read_sql_query(query, conn)

        # Calculate winrate
        result_df['winrate'] = result_df['wins'] / result_df['games']
        result_df['winrate'] = result_df['winrate'].fillna(0.5)

        # Convert champion_id to Int64 for proper merge
        result_df['champion_id'] = result_df['champion_id'].astype('Int64')

        return result_df[['puuid', 'champion_id', 'games', 'winrate']]

    def add_player_features_temporal(self, df_train: pd.DataFrame, df_test: pd.DataFrame) -> tuple:
        """
        Add player features with temporal correctness (no data leakage).

        Stats are calculated only from train set matches, then applied to both sets.

        Args:
            df_train: Training DataFrame (earlier matches)
            df_test: Test DataFrame (later matches)

        Returns:
            tuple: (df_train_with_features, df_test_with_features)
        """
        print("Adding player features (temporal correctness)...")

        # Get the max timestamp from training set
        if 'game_creation' not in df_train.columns:
            print("  Warning: game_creation not in columns, using regular player features")
            return self.add_player_features(df_train), self.add_player_features(df_test)

        max_train_timestamp = df_train['game_creation'].max()
        print(f"  Training cutoff: matches before {max_train_timestamp}")

        # Calculate stats only from matches before the test set
        stats_df = self._calculate_player_champion_stats_df(before_timestamp=max_train_timestamp)
        print(f"  Calculated stats for {len(stats_df)} player-champion pairs (train only)")

        # Apply to both train and test
        df_train_result = self._apply_player_stats(df_train, stats_df)
        df_test_result = self._apply_player_stats(df_test, stats_df)

        return df_train_result, df_test_result

    def _apply_player_stats(self, df: pd.DataFrame, stats_df: pd.DataFrame) -> pd.DataFrame:
        """Apply pre-calculated player stats to a DataFrame."""
        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        result_df = df.copy()

        for team in ['team_100', 'team_200']:
            wr_cols = []
            games_cols = []

            for pos in positions:
                puuid_col = f'{team}_{pos}_puuid'
                champ_col = f'{team}_{pos}_champion_id'
                wr_out = f'{team}_{pos}_player_wr'
                games_out = f'{team}_{pos}_player_games'

                if puuid_col not in result_df.columns or champ_col not in result_df.columns:
                    result_df[wr_out] = 0.5
                    result_df[games_out] = 0
                    wr_cols.append(wr_out)
                    games_cols.append(games_out)
                    continue

                temp_df = result_df[[puuid_col, champ_col]].copy()
                temp_df.columns = ['puuid', 'champion_id']
                temp_df['champion_id'] = temp_df['champion_id'].astype('Int64')

                merged = temp_df.merge(stats_df, on=['puuid', 'champion_id'], how='left')

                result_df[wr_out] = merged['winrate'].fillna(0.5).values
                result_df[games_out] = merged['games'].fillna(0).clip(upper=100).values

                wr_cols.append(wr_out)
                games_cols.append(games_out)

            wr_matrix = result_df[wr_cols].values
            games_matrix = result_df[games_cols].values

            result_df[f'{team}_avg_player_wr'] = np.nanmean(wr_matrix, axis=1)
            result_df[f'{team}_min_player_wr'] = np.nanmin(wr_matrix, axis=1)
            result_df[f'{team}_max_player_wr'] = np.nanmax(wr_matrix, axis=1)
            result_df[f'{team}_player_wr_std'] = np.nanstd(wr_matrix, axis=1)
            result_df[f'{team}_avg_experience'] = np.nanmean(games_matrix, axis=1)
            result_df[f'{team}_total_experience'] = np.nansum(games_matrix, axis=1)

        result_df['player_wr_diff'] = (
            result_df['team_100_avg_player_wr'] - result_df['team_200_avg_player_wr']
        )
        result_df['experience_diff'] = (
            result_df['team_100_avg_experience'] - result_df['team_200_avg_experience']
        )
        result_df['min_player_wr_diff'] = (
            result_df['team_100_min_player_wr'] - result_df['team_200_min_player_wr']
        )
        result_df['max_player_wr_diff'] = (
            result_df['team_100_max_player_wr'] - result_df['team_200_max_player_wr']
        )

        return result_df

    def add_summoner_stats_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add summoner-level features (role stats, streaks, recent performance).

        Features added per player:
        - {team}_{pos}_role_pct: % of games in this role (specialization)
        - {team}_{pos}_role_winrate: Winrate in this role
        - {team}_{pos}_mastery_points: Champion mastery (log-scaled)
        - {team}_{pos}_streak_type: -1 (loss) or +1 (win)
        - {team}_{pos}_streak_length: Length of current streak
        - {team}_{pos}_role_kda: KDA average in this role
        - {team}_{pos}_role_vision: Vision score average in this role
        - {team}_{pos}_champ_recent_wr: Recent winrate on this champion

        Team aggregate features:
        - {team}_avg_role_specialization: Average role specialization
        - {team}_min_role_specialization: Minimum (autofill indicator)
        - {team}_avg_role_winrate: Average winrate in roles
        - {team}_streak_momentum: Sum of (streak_type * streak_length)
        - {team}_total_mastery_points: Total mastery on picked champions

        Differential features:
        - role_specialization_diff
        - role_winrate_diff
        - streak_momentum_diff
        - mastery_diff
        """
        print("Adding summoner stats features...")

        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        result_df = df.copy()

        # Collect all unique puuids from the dataframe
        all_puuids = set()
        for team in ['team_100', 'team_200']:
            for pos in positions:
                puuid_col = f'{team}_{pos}_puuid'
                if puuid_col in result_df.columns:
                    all_puuids.update(result_df[puuid_col].dropna().unique())

        print(f"  Fetching stats for {len(all_puuids)} unique players...")

        # Batch fetch all stats (without time filter for non-temporal version)
        all_stats = self.db.get_all_summoner_stats_batch(list(all_puuids), last_n_games=20)
        print(f"  Retrieved stats for {len(all_stats)} players")

        # Get mastery data as dict for quick lookup
        mastery_cache = {}

        for team in ['team_100', 'team_200']:
            role_spec_cols = []
            role_wr_cols = []
            streak_cols = []
            mastery_cols = []
            kda_cols = []
            vision_cols = []

            for pos in positions:
                puuid_col = f'{team}_{pos}_puuid'
                champ_col = f'{team}_{pos}_champion_id'

                # Feature column names
                role_pct_col = f'{team}_{pos}_role_pct'
                role_wr_col = f'{team}_{pos}_role_winrate'
                mastery_col = f'{team}_{pos}_mastery_points'
                streak_type_col = f'{team}_{pos}_streak_type'
                streak_len_col = f'{team}_{pos}_streak_length'
                kda_col = f'{team}_{pos}_role_kda'
                vision_col = f'{team}_{pos}_role_vision'
                champ_wr_col = f'{team}_{pos}_champ_recent_wr'

                # Initialize with defaults
                result_df[role_pct_col] = 0.2  # 20% = no specialization
                result_df[role_wr_col] = 0.5
                result_df[mastery_col] = 0.0
                result_df[streak_type_col] = 0  # Neutral
                result_df[streak_len_col] = 0
                result_df[kda_col] = 2.0
                result_df[vision_col] = 20.0
                result_df[champ_wr_col] = 0.5

                if puuid_col not in result_df.columns:
                    role_spec_cols.append(role_pct_col)
                    role_wr_cols.append(role_wr_col)
                    streak_cols.append(streak_type_col)
                    mastery_cols.append(mastery_col)
                    kda_cols.append(kda_col)
                    vision_cols.append(vision_col)
                    continue

                # Apply stats for each row
                for idx in result_df.index:
                    puuid = result_df.at[idx, puuid_col]
                    if pd.isna(puuid) or puuid not in all_stats:
                        continue

                    stats = all_stats[puuid]

                    # Role stats
                    role_stats = stats.get('role_stats', {})
                    role_dist = role_stats.get('role_distribution', {})
                    role_wrs = role_stats.get('role_winrates', {})
                    result_df.at[idx, role_pct_col] = role_dist.get(pos, 0.2)
                    result_df.at[idx, role_wr_col] = role_wrs.get(pos, 0.5)

                    # Performance stats
                    perf = stats.get('performance', {})
                    result_df.at[idx, kda_col] = perf.get('role_kda', {}).get(pos, 2.0)
                    result_df.at[idx, vision_col] = perf.get('role_vision_score', {}).get(pos, 20.0)

                    # Streak stats
                    streaks = stats.get('streaks', {})
                    streak_type = streaks.get('current_streak_type', 'none')
                    streak_len = streaks.get('current_streak_length', 0)
                    result_df.at[idx, streak_type_col] = 1 if streak_type == 'win' else (-1 if streak_type == 'loss' else 0)
                    result_df.at[idx, streak_len_col] = streak_len

                    # Champion recent winrate
                    recent_champs = stats.get('recent_champions', [])
                    champ_id = result_df.at[idx, champ_col] if champ_col in result_df.columns else None
                    if champ_id and not pd.isna(champ_id):
                        for champ_stat in recent_champs:
                            if champ_stat.get('champion_id') == champ_id:
                                result_df.at[idx, champ_wr_col] = champ_stat.get('winrate', 0.5)
                                break

                    # Mastery points (from champion_mastery table)
                    if champ_id and not pd.isna(champ_id):
                        mastery_key = (puuid, int(champ_id))
                        if mastery_key not in mastery_cache:
                            mastery_data = self.db.get_mastery_for_champion(puuid, int(champ_id))
                            mastery_cache[mastery_key] = mastery_data.get('champion_points', 0) if mastery_data else 0
                        mastery_points = mastery_cache[mastery_key]
                        # Log scale to reduce impact of extreme values
                        result_df.at[idx, mastery_col] = np.log1p(mastery_points) if mastery_points > 0 else 0

                role_spec_cols.append(role_pct_col)
                role_wr_cols.append(role_wr_col)
                streak_cols.append(streak_type_col)
                mastery_cols.append(mastery_col)
                kda_cols.append(kda_col)
                vision_cols.append(vision_col)

            # Team aggregate features
            spec_matrix = result_df[role_spec_cols].values
            wr_matrix = result_df[role_wr_cols].values
            mastery_matrix = result_df[mastery_cols].values

            result_df[f'{team}_avg_role_specialization'] = np.nanmean(spec_matrix, axis=1)
            result_df[f'{team}_min_role_specialization'] = np.nanmin(spec_matrix, axis=1)
            result_df[f'{team}_avg_role_winrate'] = np.nanmean(wr_matrix, axis=1)
            result_df[f'{team}_total_mastery_points'] = np.nansum(mastery_matrix, axis=1)

            # Streak momentum
            streak_momentum = 0
            for pos in positions:
                streak_type = result_df[f'{team}_{pos}_streak_type']
                streak_len = result_df[f'{team}_{pos}_streak_length']
                streak_momentum = streak_momentum + (streak_type * streak_len)
            result_df[f'{team}_streak_momentum'] = streak_momentum

        # Differential features
        result_df['role_specialization_diff'] = (
            result_df['team_100_avg_role_specialization'] - result_df['team_200_avg_role_specialization']
        )
        result_df['role_winrate_diff'] = (
            result_df['team_100_avg_role_winrate'] - result_df['team_200_avg_role_winrate']
        )
        result_df['streak_momentum_diff'] = (
            result_df['team_100_streak_momentum'] - result_df['team_200_streak_momentum']
        )
        result_df['mastery_diff'] = (
            result_df['team_100_total_mastery_points'] - result_df['team_200_total_mastery_points']
        )

        # Count features added
        summoner_cols = [c for c in result_df.columns if any(x in c for x in
                        ['role_pct', 'role_winrate', 'mastery', 'streak', 'role_kda', 'role_vision', 'champ_recent_wr',
                         'specialization', 'momentum'])]
        print(f"  Added {len(summoner_cols)} summoner stats features")

        return result_df

    def add_summoner_stats_features_temporal(self, df_train: pd.DataFrame,
                                              df_test: pd.DataFrame) -> tuple:
        """
        Add summoner stats features with temporal correctness (no data leakage).

        Stats are calculated only from matches before each match's timestamp.

        Args:
            df_train: Training DataFrame (earlier matches)
            df_test: Test DataFrame (later matches)

        Returns:
            tuple: (df_train_with_features, df_test_with_features)
        """
        print("Adding summoner stats features (temporal correctness)...")

        positions = ['top', 'jungle', 'mid', 'adc', 'support']

        # Get the max timestamp from training set
        if 'game_creation' not in df_train.columns:
            print("  Warning: game_creation not in columns, using regular features")
            return self.add_summoner_stats_features(df_train), self.add_summoner_stats_features(df_test)

        max_train_timestamp = df_train['game_creation'].max()
        print(f"  Training cutoff: matches before {max_train_timestamp}")

        # For temporal correctness, we calculate stats using only training data
        # Then apply those stats to both train and test sets

        # Collect all unique puuids
        all_puuids = set()
        for df in [df_train, df_test]:
            for team in ['team_100', 'team_200']:
                for pos in positions:
                    puuid_col = f'{team}_{pos}_puuid'
                    if puuid_col in df.columns:
                        all_puuids.update(df[puuid_col].dropna().unique())

        print(f"  Fetching stats for {len(all_puuids)} unique players (before training cutoff)...")

        # Fetch stats only using data before training cutoff
        all_stats = self.db.get_all_summoner_stats_batch(
            list(all_puuids),
            before_timestamp=int(max_train_timestamp),
            last_n_games=20
        )
        print(f"  Retrieved stats for {len(all_stats)} players")

        # Apply to both datasets using the same stats
        df_train_result = self._apply_summoner_stats(df_train, all_stats)
        df_test_result = self._apply_summoner_stats(df_test, all_stats)

        return df_train_result, df_test_result

    def _apply_summoner_stats(self, df: pd.DataFrame, all_stats: Dict) -> pd.DataFrame:
        """
        Apply pre-calculated summoner stats to a DataFrame.

        Args:
            df: DataFrame to add features to
            all_stats: Dict mapping puuid -> stats dict

        Returns:
            DataFrame with summoner features added
        """
        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        result_df = df.copy()

        mastery_cache = {}

        for team in ['team_100', 'team_200']:
            role_spec_cols = []
            role_wr_cols = []
            streak_cols = []
            mastery_cols = []

            for pos in positions:
                puuid_col = f'{team}_{pos}_puuid'
                champ_col = f'{team}_{pos}_champion_id'

                # Feature column names
                role_pct_col = f'{team}_{pos}_role_pct'
                role_wr_col = f'{team}_{pos}_role_winrate'
                mastery_col = f'{team}_{pos}_mastery_points'
                streak_type_col = f'{team}_{pos}_streak_type'
                streak_len_col = f'{team}_{pos}_streak_length'
                kda_col = f'{team}_{pos}_role_kda'
                vision_col = f'{team}_{pos}_role_vision'
                champ_wr_col = f'{team}_{pos}_champ_recent_wr'

                # Initialize with defaults
                result_df[role_pct_col] = 0.2
                result_df[role_wr_col] = 0.5
                result_df[mastery_col] = 0.0
                result_df[streak_type_col] = 0
                result_df[streak_len_col] = 0
                result_df[kda_col] = 2.0
                result_df[vision_col] = 20.0
                result_df[champ_wr_col] = 0.5

                if puuid_col not in result_df.columns:
                    role_spec_cols.append(role_pct_col)
                    role_wr_cols.append(role_wr_col)
                    streak_cols.append(streak_type_col)
                    mastery_cols.append(mastery_col)
                    continue

                # Vectorized application where possible
                puuids = result_df[puuid_col].values
                champ_ids = result_df[champ_col].values if champ_col in result_df.columns else [None] * len(result_df)

                role_pcts = []
                role_wrs = []
                kdas = []
                visions = []
                streak_types = []
                streak_lens = []
                champ_wrs = []
                masteries = []

                for i, (puuid, champ_id) in enumerate(zip(puuids, champ_ids)):
                    if pd.isna(puuid) or puuid not in all_stats:
                        role_pcts.append(0.2)
                        role_wrs.append(0.5)
                        kdas.append(2.0)
                        visions.append(20.0)
                        streak_types.append(0)
                        streak_lens.append(0)
                        champ_wrs.append(0.5)
                        masteries.append(0.0)
                        continue

                    stats = all_stats[puuid]

                    # Role stats
                    role_stats = stats.get('role_stats', {})
                    role_pcts.append(role_stats.get('role_distribution', {}).get(pos, 0.2))
                    role_wrs.append(role_stats.get('role_winrates', {}).get(pos, 0.5))

                    # Performance
                    perf = stats.get('performance', {})
                    kdas.append(perf.get('role_kda', {}).get(pos, 2.0))
                    visions.append(perf.get('role_vision_score', {}).get(pos, 20.0))

                    # Streaks
                    streaks = stats.get('streaks', {})
                    st = streaks.get('current_streak_type', 'none')
                    streak_types.append(1 if st == 'win' else (-1 if st == 'loss' else 0))
                    streak_lens.append(streaks.get('current_streak_length', 0))

                    # Champion winrate
                    recent_champs = stats.get('recent_champions', [])
                    champ_wr = 0.5
                    if champ_id and not pd.isna(champ_id):
                        for champ_stat in recent_champs:
                            if champ_stat.get('champion_id') == int(champ_id):
                                champ_wr = champ_stat.get('winrate', 0.5)
                                break
                    champ_wrs.append(champ_wr)

                    # Mastery
                    mastery_val = 0.0
                    if champ_id and not pd.isna(champ_id):
                        mastery_key = (puuid, int(champ_id))
                        if mastery_key not in mastery_cache:
                            mastery_data = self.db.get_mastery_for_champion(puuid, int(champ_id))
                            mastery_cache[mastery_key] = mastery_data.get('champion_points', 0) if mastery_data else 0
                        mp = mastery_cache[mastery_key]
                        mastery_val = np.log1p(mp) if mp > 0 else 0
                    masteries.append(mastery_val)

                result_df[role_pct_col] = role_pcts
                result_df[role_wr_col] = role_wrs
                result_df[kda_col] = kdas
                result_df[vision_col] = visions
                result_df[streak_type_col] = streak_types
                result_df[streak_len_col] = streak_lens
                result_df[champ_wr_col] = champ_wrs
                result_df[mastery_col] = masteries

                role_spec_cols.append(role_pct_col)
                role_wr_cols.append(role_wr_col)
                streak_cols.append(streak_type_col)
                mastery_cols.append(mastery_col)

            # Team aggregates
            spec_matrix = result_df[role_spec_cols].values
            wr_matrix = result_df[role_wr_cols].values
            mastery_matrix = result_df[mastery_cols].values

            result_df[f'{team}_avg_role_specialization'] = np.nanmean(spec_matrix, axis=1)
            result_df[f'{team}_min_role_specialization'] = np.nanmin(spec_matrix, axis=1)
            result_df[f'{team}_avg_role_winrate'] = np.nanmean(wr_matrix, axis=1)
            result_df[f'{team}_total_mastery_points'] = np.nansum(mastery_matrix, axis=1)

            # Streak momentum
            streak_momentum = 0
            for pos in positions:
                streak_type = result_df[f'{team}_{pos}_streak_type']
                streak_len = result_df[f'{team}_{pos}_streak_length']
                streak_momentum = streak_momentum + (streak_type * streak_len)
            result_df[f'{team}_streak_momentum'] = streak_momentum

        # Differentials
        result_df['role_specialization_diff'] = (
            result_df['team_100_avg_role_specialization'] - result_df['team_200_avg_role_specialization']
        )
        result_df['role_winrate_diff'] = (
            result_df['team_100_avg_role_winrate'] - result_df['team_200_avg_role_winrate']
        )
        result_df['streak_momentum_diff'] = (
            result_df['team_100_streak_momentum'] - result_df['team_200_streak_momentum']
        )
        result_df['mastery_diff'] = (
            result_df['team_100_total_mastery_points'] - result_df['team_200_total_mastery_points']
        )

        return result_df

    def add_early_game_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add early game differential features from @10min data.

        Features added:
        - Gold diff @10min (total and per position)
        - CS diff @10min
        - First blood/dragon indicators (if available)
        - Early lead score (composite)

        Args:
            df: DataFrame with timeline @10min columns

        Returns:
            DataFrame with early game features added
        """
        print("Adding early game features (@10min)...")
        result_df = df.copy()
        positions = ['top', 'jungle', 'mid', 'adc', 'support']

        # Check if timeline data is available
        has_timeline = 'team_100_gold_at_10' in result_df.columns
        has_cs_10 = 'team_100_top_cs_at_10' in result_df.columns

        if not has_timeline:
            print("  Warning: No timeline @10min data found. Skipping gold features.")
        else:
            # Gold diff @10min (already in export, but ensure it exists)
            if 'gold_diff_at_10' not in result_df.columns:
                result_df['gold_diff_at_10'] = (
                    result_df['team_100_gold_at_10'].fillna(0) -
                    result_df['team_200_gold_at_10'].fillna(0)
                )

            # Gold diff per position @10min
            for pos in positions:
                col_100 = f'team_100_{pos}_gold_at_10'
                col_200 = f'team_200_{pos}_gold_at_10'
                if col_100 in result_df.columns and col_200 in result_df.columns:
                    result_df[f'{pos}_gold_diff_at_10'] = (
                        result_df[col_100].fillna(0) - result_df[col_200].fillna(0)
                    )

            # Calculate gold percentage advantage
            total_gold = (
                result_df['team_100_gold_at_10'].fillna(0) +
                result_df['team_200_gold_at_10'].fillna(0)
            ).replace(0, 1)  # Avoid division by zero
            result_df['gold_advantage_pct_at_10'] = (
                result_df['gold_diff_at_10'] / total_gold * 100
            )

            print(f"  Added gold features @10min")

        if not has_cs_10:
            print("  Warning: No CS @10min data found. Skipping CS features.")
        else:
            # CS diff @10min (total)
            cs_100_total = 0
            cs_200_total = 0
            for pos in positions:
                col_100 = f'team_100_{pos}_cs_at_10'
                col_200 = f'team_200_{pos}_cs_at_10'
                if col_100 in result_df.columns:
                    cs_100_total = cs_100_total + result_df[col_100].fillna(0)
                if col_200 in result_df.columns:
                    cs_200_total = cs_200_total + result_df[col_200].fillna(0)

            result_df['team_100_total_cs_at_10'] = cs_100_total
            result_df['team_200_total_cs_at_10'] = cs_200_total
            result_df['cs_diff_at_10'] = cs_100_total - cs_200_total

            # CS diff per position @10min
            for pos in positions:
                col_100 = f'team_100_{pos}_cs_at_10'
                col_200 = f'team_200_{pos}_cs_at_10'
                if col_100 in result_df.columns and col_200 in result_df.columns:
                    result_df[f'{pos}_cs_diff_at_10'] = (
                        result_df[col_100].fillna(0) - result_df[col_200].fillna(0)
                    )

            print(f"  Added CS features @10min")

        # First blood indicator (from team_stats - may not have timestamp)
        if 'team_100_first_blood' in result_df.columns:
            result_df['first_blood_team_100'] = result_df['team_100_first_blood'].astype(int)
        elif 'team_200_first_blood' in result_df.columns:
            result_df['first_blood_team_100'] = 1 - result_df['team_200_first_blood'].astype(int)

        # First dragon indicator
        if 'team_100_first_dragon' in result_df.columns:
            result_df['first_dragon_team_100'] = result_df['team_100_first_dragon'].astype(int)

        # First tower indicator
        if 'team_100_first_tower' in result_df.columns:
            result_df['first_tower_team_100'] = result_df['team_100_first_tower'].astype(int)

        # Calculate early lead score (composite feature)
        early_score = pd.Series(0.0, index=result_df.index)

        if has_timeline:
            # Gold lead contributes most
            early_score += result_df['gold_diff_at_10'].fillna(0) / 1000

        if 'first_blood_team_100' in result_df.columns:
            # First blood is worth ~0.5 points
            early_score += (result_df['first_blood_team_100'] - 0.5) * 1.0

        if 'first_dragon_team_100' in result_df.columns:
            # First dragon is worth ~0.3 points
            early_score += (result_df['first_dragon_team_100'] - 0.5) * 0.6

        result_df['early_lead_score'] = early_score

        # Count matches with timeline data
        if has_timeline:
            timeline_count = result_df['team_100_gold_at_10'].notna().sum()
            print(f"  Matches with timeline @10min: {timeline_count}/{len(result_df)} ({100*timeline_count/len(result_df):.1f}%)")

        return result_df

    def _calculate_player_champion_stats(self) -> dict:
        """
        Calculate historical winrate per player-champion pair from database.
        (Legacy dict version for compatibility)

        Returns:
            dict: {(puuid, champion_id): {'games': N, 'winrate': float}}
        """
        df = self._calculate_player_champion_stats_df()
        stats = {}
        for _, row in df.iterrows():
            key = (row['puuid'], int(row['champion_id']))
            stats[key] = {
                'games': row['games'],
                'winrate': row['winrate']
            }
        return stats

    def prepare_features(self, df: pd.DataFrame) -> tuple:
        """
        Prepare features (X) and target (y).

        Returns:
            tuple: (X, y, feature_names)
        """
        print("Preparing features and target...")

        # Ensure target column exists
        if self.target_column not in df.columns:
            raise ValueError(f"Target column '{self.target_column}' not found in data")

        # Separate features and target
        y = df[self.target_column].astype(int)
        X = df.drop(columns=[self.target_column])

        # Convert all columns to numeric
        for col in X.columns:
            col_dtype = X[col].dtype
            if col_dtype == 'bool':
                X[col] = X[col].astype(int)
            elif col_dtype == 'object':
                # Try to convert to numeric, fill with 0 if fails
                X[col] = pd.to_numeric(X[col], errors='coerce').fillna(0)

        self.feature_columns = list(X.columns)
        print(f"  Features: {len(self.feature_columns)} columns")
        print(f"  Target: {self.target_column} (win rate: {y.mean():.2%})")

        return X, y, self.feature_columns

    def split_data(self, X: pd.DataFrame, y: pd.Series,
                   test_size: float = 0.15, val_size: float = 0.15,
                   random_state: int = 42) -> dict:
        """
        Split data into train/validation/test sets.

        Args:
            X: Features DataFrame
            y: Target Series
            test_size: Proportion for test set (default: 0.15)
            val_size: Proportion for validation set (default: 0.15)
            random_state: Random seed for reproducibility

        Returns:
            dict with X_train, y_train, X_val, y_val, X_test, y_test
        """
        print(f"Splitting data (train: {1-test_size-val_size:.0%}, val: {val_size:.0%}, test: {test_size:.0%})...")

        # First split: separate test set
        X_temp, X_test, y_temp, y_test = train_test_split(
            X, y,
            test_size=test_size,
            random_state=random_state,
            stratify=y
        )

        # Second split: separate validation from training
        val_ratio = val_size / (1 - test_size)
        X_train, X_val, y_train, y_val = train_test_split(
            X_temp, y_temp,
            test_size=val_ratio,
            random_state=random_state,
            stratify=y_temp
        )

        print(f"  Train: {len(X_train)} samples")
        print(f"  Validation: {len(X_val)} samples")
        print(f"  Test: {len(X_test)} samples")

        return {
            'X_train': X_train, 'y_train': y_train,
            'X_val': X_val, 'y_val': y_val,
            'X_test': X_test, 'y_test': y_test
        }

    def save_to_parquet(self, splits: dict, output_dir: str = 'data/prepared'):
        """
        Save prepared data to Parquet format.

        Args:
            splits: Dictionary with X_train, y_train, etc.
            output_dir: Output directory path
        """
        print(f"Saving data to Parquet format in {output_dir}/...")

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Save each split
        for name, data in splits.items():
            filepath = os.path.join(output_dir, f'{name}.parquet')

            if isinstance(data, pd.DataFrame):
                data.to_parquet(filepath, engine='pyarrow', compression='snappy')
            else:  # Series (y targets)
                pd.DataFrame({name: data}).to_parquet(filepath, engine='pyarrow', compression='snappy')

            print(f"  Saved {name}: {filepath}")

        # Save metadata
        metadata = {
            'feature_columns': self.feature_columns,
            'target_column': self.target_column,
            'n_features': len(self.feature_columns),
            'n_train': len(splits['X_train']),
            'n_val': len(splits['X_val']),
            'n_test': len(splits['X_test']),
            'train_win_rate': float(splits['y_train'].mean()),
            'test_win_rate': float(splits['y_test'].mean()),
        }

        metadata_path = os.path.join(output_dir, 'metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        print(f"  Saved metadata: {metadata_path}")

        print(f"\nData saved successfully!")
        print(f"Total files: {len(splits) + 1}")

    def prepare(self, encoding: str = 'onehot',
                test_size: float = 0.15, val_size: float = 0.15,
                min_champion_appearances: int = 5,
                output_dir: str = 'data/prepared',
                draft_only: bool = True,
                add_advanced_features: bool = True) -> dict:
        """
        Full data preparation pipeline.

        Args:
            encoding: 'onehot' or 'label' for champion encoding
            test_size: Test set proportion
            val_size: Validation set proportion
            min_champion_appearances: Minimum appearances for One-Hot encoding
            output_dir: Output directory for Parquet files
            draft_only: If True, remove post-game stats for pure draft prediction
            add_advanced_features: If True, add win rate and composition features

        Returns:
            dict with data splits
        """
        print("=" * 60)
        print("Data Preparation Pipeline")
        print(f"Mode: {'DRAFT-ONLY (no post-game stats)' if draft_only else 'FULL (includes post-game stats)'}")
        print(f"Advanced features: {'ENABLED' if add_advanced_features else 'DISABLED'}")
        print("=" * 60)

        # Step 1: Load data
        df = self.load_data()

        if df.empty:
            raise ValueError("No data found in database. Run data collection first.")

        # Step 2: Remove ID columns
        df = self.remove_id_columns(df)

        # Step 3: Add advanced features BEFORE removing post-game columns
        # (need champion IDs for calculations)
        if add_advanced_features:
            print("\n--- Adding Advanced Features ---")
            df = self.add_champion_winrate_features(df)
            df = self.add_team_composition_features(df)
            df = self.add_playstyle_features(df)
            df = self.add_synergy_features(df)
            df = self.add_dpmlol_features(df)
            df = self.add_matchup_detection_features(df)
            df = self.add_lane_synergy_features(df)
            df = self.add_player_features(df)

        # Step 4: Remove post-game columns (if draft-only mode)
        if draft_only:
            df = self.remove_post_game_columns(df)

        # Step 5: Handle missing values
        df = self.handle_missing_values(df)

        # Step 6: Encode champions
        if encoding == 'onehot':
            df = self.encode_champions(df, min_appearances=min_champion_appearances)
        else:
            df = self.encode_champions_label(df)

        # Step 7: Prepare features
        X, y, feature_names = self.prepare_features(df)

        # Step 6: Split data
        splits = self.split_data(X, y, test_size=test_size, val_size=val_size)

        # Step 7: Save to Parquet
        self.save_to_parquet(splits, output_dir)

        print("\n" + "=" * 60)
        print("Preparation Complete!")
        print("=" * 60)
        print(f"\nOutput directory: {output_dir}/")
        print(f"Features: {len(feature_names)}")
        print(f"Train samples: {len(splits['X_train'])}")
        print(f"Validation samples: {len(splits['X_val'])}")
        print(f"Test samples: {len(splits['X_test'])}")

        print("\nNext step: Train the model")
        print("  python src/draft_predictor.py")

        return splits


# =============================================================================
# TIME-BASED FEATURE ENGINEERING - Features for @X min predictions
# =============================================================================

def get_features_for_minute(minute: int) -> dict:
    """
    Get the list of features available for a specific game minute.

    Features vary by minute because:
    - CS data is only available at 10 minutes (from player_stats.lane_minions_first_10_min)
    - Gold data is available at all minutes (from match_timeline)

    Args:
        minute: Game minute (e.g., 5, 10, 15, 20)

    Returns:
        Dict with 'gold_features', 'cs_features' (only for minute 10), and 'objective_features'
    """
    positions = ['top', 'jungle', 'mid', 'adc', 'support']

    # Gold features available at all minutes
    gold_features = [
        f'gold_diff_at_{minute}',
        f'gold_advantage_pct_at_{minute}',
        f'early_lead_score_at_{minute}',
    ]

    # Position-specific gold diffs
    for pos in positions:
        gold_features.append(f'{pos}_gold_diff_at_{minute}')

    # CS features only available at minute 10
    cs_features = []
    if minute == 10:
        cs_features = [
            'cs_diff_at_10',
        ]
        for pos in positions:
            cs_features.append(f'{pos}_cs_diff_at_10')

    # Objective features (same for all minutes, from team_stats)
    objective_features = [
        'first_blood_team_100',
        'first_tower_team_100',
        'first_dragon_team_100',
        'first_herald_team_100',
    ]

    return {
        'gold_features': gold_features,
        'cs_features': cs_features,
        'objective_features': objective_features,
        'all_features': gold_features + cs_features + objective_features,
    }


def add_variable_minute_features(df: pd.DataFrame, minute: int) -> pd.DataFrame:
    """
    Add computed features for a specific game minute.

    This function takes raw timeline data and creates derived features like:
    - gold_advantage_pct: Percentage advantage in gold
    - position gold diffs: Gold difference per lane
    - CS diffs: CS difference per lane (only at 10 min)
    - early_lead_score: Composite score

    Args:
        df: DataFrame with timeline data (from export_with_timeline_at_minutes)
        minute: Game minute to compute features for

    Returns:
        DataFrame with added feature columns
    """
    df = df.copy()
    positions = ['top', 'jungle', 'mid', 'adc', 'support']

    # Check if we have data for this minute
    gold_diff_col = f'gold_diff_at_{minute}'
    if gold_diff_col not in df.columns:
        print(f"Warning: No data for minute {minute}")
        return df

    # Gold advantage percentage
    team_100_gold = df.get(f'team_100_gold_at_{minute}', pd.Series(0, index=df.index))
    team_200_gold = df.get(f'team_200_gold_at_{minute}', pd.Series(0, index=df.index))
    total_gold = team_100_gold + team_200_gold
    df[f'gold_advantage_pct_at_{minute}'] = np.where(
        total_gold > 0,
        (team_100_gold - team_200_gold) / total_gold,
        0
    )

    # Position-specific gold diffs
    for pos in positions:
        t100_col = f'team_100_{pos}_gold_at_{minute}'
        t200_col = f'team_200_{pos}_gold_at_{minute}'
        if t100_col in df.columns and t200_col in df.columns:
            df[f'{pos}_gold_diff_at_{minute}'] = df[t100_col].fillna(0) - df[t200_col].fillna(0)
        else:
            df[f'{pos}_gold_diff_at_{minute}'] = 0

    # CS diffs (only for minute 10)
    if minute == 10:
        # Total CS diff
        cs_cols_100 = [f'team_100_{pos}_cs_at_10' for pos in positions]
        cs_cols_200 = [f'team_200_{pos}_cs_at_10' for pos in positions]

        available_100 = [c for c in cs_cols_100 if c in df.columns]
        available_200 = [c for c in cs_cols_200 if c in df.columns]

        if available_100 and available_200:
            df['cs_diff_at_10'] = df[available_100].fillna(0).sum(axis=1) - df[available_200].fillna(0).sum(axis=1)

            # Position-specific CS diffs
            for pos in positions:
                t100_cs = f'team_100_{pos}_cs_at_10'
                t200_cs = f'team_200_{pos}_cs_at_10'
                if t100_cs in df.columns and t200_cs in df.columns:
                    df[f'{pos}_cs_diff_at_10'] = df[t100_cs].fillna(0) - df[t200_cs].fillna(0)
                else:
                    df[f'{pos}_cs_diff_at_10'] = 0
        else:
            df['cs_diff_at_10'] = 0
            for pos in positions:
                df[f'{pos}_cs_diff_at_10'] = 0

    # Early lead score - composite metric
    # Normalize gold diff (typical range: -5000 to +5000 at 10 min)
    gold_diff_normalized = df[gold_diff_col].fillna(0) / 5000.0

    # Add objective bonuses
    first_blood = df.get('first_blood_team_100', pd.Series(0, index=df.index)).fillna(0).astype(float)
    first_tower = df.get('first_tower_team_100', pd.Series(0, index=df.index)).fillna(0).astype(float)
    first_dragon = df.get('first_dragon_team_100', pd.Series(0, index=df.index)).fillna(0).astype(float)
    first_herald = df.get('first_herald_team_100', pd.Series(0, index=df.index)).fillna(0).astype(float)

    # Composite score: gold diff + objective bonuses
    df[f'early_lead_score_at_{minute}'] = (
        gold_diff_normalized * 0.6 +
        (first_blood * 2 - 1) * 0.15 +  # Convert 0/1 to -1/+1, then weight
        (first_tower * 2 - 1) * 0.1 +
        (first_dragon * 2 - 1) * 0.1 +
        (first_herald * 2 - 1) * 0.05
    )

    return df


def prepare_features_for_minute(df: pd.DataFrame, minute: int) -> tuple:
    """
    Prepare feature matrix X and target y for a specific minute.

    Args:
        df: DataFrame with timeline data (after add_variable_minute_features)
        minute: Game minute

    Returns:
        Tuple of (X, y, feature_names) where:
        - X: Feature matrix (DataFrame)
        - y: Target vector (Series)
        - feature_names: List of feature column names
    """
    # Add computed features
    df = add_variable_minute_features(df, minute)

    # Get feature definitions
    feature_def = get_features_for_minute(minute)
    feature_cols = feature_def['all_features']

    # Filter to available columns
    available_cols = [c for c in feature_cols if c in df.columns]
    missing_cols = [c for c in feature_cols if c not in df.columns]

    if missing_cols:
        print(f"Note: Missing {len(missing_cols)} features for minute {minute}: {missing_cols[:3]}...")

    # Filter to rows with timeline data for this minute
    gold_diff_col = f'gold_diff_at_{minute}'
    if gold_diff_col in df.columns:
        df_filtered = df[df[gold_diff_col].notna()].copy()
    else:
        df_filtered = df.copy()

    print(f"Matches with @{minute}min data: {len(df_filtered)}")

    if len(df_filtered) == 0:
        return None, None, []

    # Prepare X and y
    X = df_filtered[available_cols].fillna(0)
    y = df_filtered['team_100_win'].astype(int)

    return X, y, available_cols


def main():
    parser = argparse.ArgumentParser(description='Prepare LoL match data for ML training')
    parser.add_argument('--db', default='data/lol_matches.db',
                       help='SQLite database path (default: data/lol_matches.db)')
    parser.add_argument('--encoding', choices=['onehot', 'label'], default='onehot',
                       help='Champion encoding method (default: onehot)')
    parser.add_argument('--test-size', type=float, default=0.15,
                       help='Test set proportion (default: 0.15)')
    parser.add_argument('--val-size', type=float, default=0.15,
                       help='Validation set proportion (default: 0.15)')
    parser.add_argument('--min-appearances', type=int, default=5,
                       help='Min champion appearances for One-Hot encoding (default: 5)')
    parser.add_argument('--output', default='data/processed',
                       help='Output directory (default: data/processed)')
    parser.add_argument('--include-postgame', action='store_true',
                       help='Include post-game stats (kills, gold, etc). Default: draft-only mode')
    parser.add_argument('--no-advanced-features', action='store_true',
                       help='Disable advanced features (win rates, team composition)')

    args = parser.parse_args()

    # Change to project root (src/ML/preprocessing.py -> src/ML -> src -> root)
    project_root = Path(__file__).parent.parent.parent
    os.chdir(project_root)

    # Run preparation
    preparer = DataPreparer(args.db)
    preparer.prepare(
        encoding=args.encoding,
        test_size=args.test_size,
        val_size=args.val_size,
        min_champion_appearances=args.min_appearances,
        output_dir=args.output,
        draft_only=not args.include_postgame,
        add_advanced_features=not args.no_advanced_features
    )


if __name__ == '__main__':
    main()
