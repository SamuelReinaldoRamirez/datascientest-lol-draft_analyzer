"""
OP.GG Data Scraper

Fetches champion statistics, matchups (counters), and synergies from OP.GG's API.
This provides external data for ML features instead of calculating from historical match data.

API Endpoints:
- Champion Stats: https://lol-api-champion.op.gg/api/global/champions/ranked
- Champion Details: https://lol-api-champion.op.gg/api/global/champions/ranked/{champion_id}/{position}
- Synergies: https://lol-api-champion.op.gg/api/global/champions/ranked/{champion_id}/{position}/synergies
"""

import requests
import json
import time
import logging
from pathlib import Path
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)


class OPGGScraper:
    """Scraper for OP.GG champion statistics, matchups, and synergies."""

    BASE_URL = "https://lol-api-champion.op.gg/api"

    # Position mapping
    POSITIONS = ['top', 'jungle', 'mid', 'adc', 'support']
    POSITION_MAP = {
        'TOP': 'top',
        'JUNGLE': 'jungle',
        'MID': 'mid',
        'ADC': 'adc',
        'SUPPORT': 'support'
    }

    def __init__(self, tier: str = "emerald_plus", region: str = "global", delay: float = 0.5):
        """
        Initialize the scraper.

        Args:
            tier: Rank tier filter (default: emerald_plus)
            region: Region (global, KR, EUW, NA, etc.)
            delay: Delay between requests in seconds
        """
        self.tier = tier
        self.region = region
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'application/json',
        })

        # Cache
        self._champions_cache = None
        self._champion_details_cache = {}

    def _request(self, endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
        """Make a request to the API with rate limiting."""
        url = f"{self.BASE_URL}/{self.region}/{endpoint}"
        params = params or {}
        params['tier'] = self.tier

        try:
            time.sleep(self.delay)
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None

    def get_all_champions(self, force_refresh: bool = False) -> Optional[list]:
        """
        Get statistics for all champions.

        Returns:
            List of champion data with stats per position
        """
        if self._champions_cache and not force_refresh:
            return self._champions_cache

        logger.info("Fetching all champion stats from OP.GG...")
        data = self._request("champions/ranked")

        if data and 'data' in data:
            self._champions_cache = data['data']
            logger.info(f"Fetched stats for {len(self._champions_cache)} champions")
            return self._champions_cache
        return None

    def get_champion_details(self, champion_id: int, position: str) -> Optional[dict]:
        """
        Get detailed stats for a champion in a specific position.
        Includes counters, builds, runes, etc.

        Args:
            champion_id: Champion ID
            position: Position (top, jungle, mid, adc, support)

        Returns:
            Champion detail data including counters
        """
        cache_key = (champion_id, position.lower())
        if cache_key in self._champion_details_cache:
            return self._champion_details_cache[cache_key]

        data = self._request(f"champions/ranked/{champion_id}/{position.lower()}")

        if data and 'data' in data:
            self._champion_details_cache[cache_key] = data['data']
            return data['data']
        return None

    def get_synergies(self, champion_id: int, position: str) -> Optional[list]:
        """
        Get synergy data for a champion in a specific position.

        Args:
            champion_id: Champion ID
            position: Position (top, jungle, mid, adc, support)

        Returns:
            List of synergy data with win rates
        """
        data = self._request(f"champions/ranked/{champion_id}/{position.lower()}/synergies")

        if data and 'data' in data:
            return data['data']
        return None

    def build_champion_stats_df(self) -> pd.DataFrame:
        """
        Build a DataFrame with champion stats by position.

        Returns:
            DataFrame with columns:
            - champion_id, position
            - opgg_winrate, opgg_pickrate, opgg_banrate
            - opgg_tier, opgg_rank
            - opgg_kda
        """
        champions = self.get_all_champions()
        if not champions:
            return pd.DataFrame()

        rows = []
        for champ in champions:
            champion_id = champ['id']

            # Overall stats
            avg = champ.get('average_stats', {})
            rows.append({
                'champion_id': champion_id,
                'position': 'all',
                'opgg_winrate': avg.get('win_rate', 0),
                'opgg_pickrate': avg.get('pick_rate', 0),
                'opgg_banrate': avg.get('ban_rate', 0),
                'opgg_tier': avg.get('tier', 5),
                'opgg_rank': avg.get('rank', 999),
                'opgg_kda': avg.get('kda', 0),
                'opgg_play_count': avg.get('play', 0),
            })

            # Position-specific stats
            for pos_data in champ.get('positions', []):
                pos_name = self.POSITION_MAP.get(pos_data['name'], pos_data['name'].lower())
                stats = pos_data.get('stats', {})
                tier_data = stats.get('tier_data', {})

                rows.append({
                    'champion_id': champion_id,
                    'position': pos_name,
                    'opgg_winrate': stats.get('win_rate', 0),
                    'opgg_pickrate': stats.get('pick_rate', 0),
                    'opgg_banrate': stats.get('ban_rate', 0),
                    'opgg_tier': tier_data.get('tier', 5),
                    'opgg_rank': tier_data.get('rank', 999),
                    'opgg_kda': stats.get('kda', 0),
                    'opgg_play_count': stats.get('play', 0),
                    'opgg_role_rate': stats.get('role_rate', 0),
                })

        df = pd.DataFrame(rows)
        logger.info(f"Built champion stats DataFrame: {len(df)} rows")
        return df

    def build_matchups_df(self, min_games: int = 100) -> pd.DataFrame:
        """
        Build a DataFrame with matchup (counter) data.

        Args:
            min_games: Minimum games threshold for reliable stats

        Returns:
            DataFrame with columns:
            - champion_id, position
            - opponent_id
            - matchup_winrate, matchup_games
        """
        champions = self.get_all_champions()
        if not champions:
            return pd.DataFrame()

        rows = []
        total = len(champions)

        for i, champ in enumerate(champions):
            champion_id = champ['id']

            for pos_data in champ.get('positions', []):
                pos_name = self.POSITION_MAP.get(pos_data['name'], pos_data['name'].lower())

                # Get counters from the summary data first (faster)
                counters = pos_data.get('counters', [])

                for counter in counters:
                    opponent_id = counter['champion_id']
                    games = counter.get('play', 0)
                    wins = counter.get('win', 0)

                    if games >= min_games:
                        winrate = wins / games if games > 0 else 0.5
                        rows.append({
                            'champion_id': champion_id,
                            'position': pos_name,
                            'opponent_id': opponent_id,
                            'matchup_winrate': winrate,
                            'matchup_games': games,
                        })

            if (i + 1) % 20 == 0:
                logger.info(f"Processing matchups: {i + 1}/{total} champions")

        df = pd.DataFrame(rows)
        logger.info(f"Built matchups DataFrame: {len(df)} rows")
        return df

    def build_synergies_df(self, min_games: int = 100) -> pd.DataFrame:
        """
        Build a DataFrame with synergy data.

        Args:
            min_games: Minimum games threshold for reliable stats

        Returns:
            DataFrame with columns:
            - champion_id, position
            - ally_id, ally_position
            - synergy_winrate, synergy_games
        """
        champions = self.get_all_champions()
        if not champions:
            return pd.DataFrame()

        rows = []
        total = len(champions)

        for i, champ in enumerate(champions):
            champion_id = champ['id']

            for pos_data in champ.get('positions', []):
                pos_name = self.POSITION_MAP.get(pos_data['name'], pos_data['name'].lower())

                # Fetch synergy data (requires separate API call)
                synergies = self.get_synergies(champion_id, pos_name)

                if synergies:
                    for syn in synergies:
                        games = syn.get('play', 0)

                        if games >= min_games:
                            ally_pos = self.POSITION_MAP.get(
                                syn.get('synergy_position', ''),
                                syn.get('synergy_position', '').lower()
                            )
                            rows.append({
                                'champion_id': champion_id,
                                'position': pos_name,
                                'ally_id': syn.get('synergy_champion_id'),
                                'ally_position': ally_pos,
                                'synergy_winrate': syn.get('win_rate', 0.5),
                                'synergy_games': games,
                            })

            if (i + 1) % 10 == 0:
                logger.info(f"Processing synergies: {i + 1}/{total} champions")

        df = pd.DataFrame(rows)
        logger.info(f"Built synergies DataFrame: {len(df)} rows")
        return df

    def scrape_all(self, output_dir: str = "data/opgg", min_games: int = 100) -> dict:
        """
        Scrape all data and save to Parquet files.

        Args:
            output_dir: Directory to save the data
            min_games: Minimum games threshold

        Returns:
            Dict with DataFrames: {champion_stats, matchups, synergies}
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info("=" * 60)
        logger.info("Starting OP.GG data scraping...")
        logger.info(f"Tier: {self.tier}, Region: {self.region}")
        logger.info("=" * 60)

        # 1. Champion Stats
        logger.info("\n[1/3] Scraping champion stats...")
        champion_stats = self.build_champion_stats_df()
        if not champion_stats.empty:
            champion_stats.to_parquet(output_path / "champion_stats.parquet", index=False)
            logger.info(f"Saved champion_stats.parquet ({len(champion_stats)} rows)")

        # 2. Matchups
        logger.info("\n[2/3] Scraping matchups...")
        matchups = self.build_matchups_df(min_games=min_games)
        if not matchups.empty:
            matchups.to_parquet(output_path / "matchups.parquet", index=False)
            logger.info(f"Saved matchups.parquet ({len(matchups)} rows)")

        # 3. Synergies
        logger.info("\n[3/3] Scraping synergies...")
        synergies = self.build_synergies_df(min_games=min_games)
        if not synergies.empty:
            synergies.to_parquet(output_path / "synergies.parquet", index=False)
            logger.info(f"Saved synergies.parquet ({len(synergies)} rows)")

        # Save metadata
        meta = self._request("champions/ranked")
        if meta and 'meta' in meta:
            with open(output_path / "metadata.json", 'w') as f:
                json.dump({
                    'tier': self.tier,
                    'region': self.region,
                    'version': meta['meta'].get('version'),
                    'match_count': meta['meta'].get('match_count'),
                    'analyzed_at': meta['meta'].get('analyzed_at'),
                    'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                }, f, indent=2)

        logger.info("\n" + "=" * 60)
        logger.info("OP.GG scraping complete!")
        logger.info(f"Data saved to: {output_path}")
        logger.info("=" * 60)

        return {
            'champion_stats': champion_stats,
            'matchups': matchups,
            'synergies': synergies,
        }


def main():
    """Main function to run the scraper."""
    import argparse

    parser = argparse.ArgumentParser(description='Scrape OP.GG champion data')
    parser.add_argument('--tier', default='emerald_plus',
                        help='Rank tier (default: emerald_plus)')
    parser.add_argument('--region', default='global',
                        help='Region (default: global)')
    parser.add_argument('--output', default='data/opgg',
                        help='Output directory (default: data/opgg)')
    parser.add_argument('--min-games', type=int, default=100,
                        help='Minimum games for matchup/synergy stats (default: 100)')
    parser.add_argument('--delay', type=float, default=0.3,
                        help='Delay between API requests in seconds (default: 0.3)')

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    scraper = OPGGScraper(tier=args.tier, region=args.region, delay=args.delay)
    scraper.scrape_all(output_dir=args.output, min_games=args.min_games)


if __name__ == "__main__":
    main()
