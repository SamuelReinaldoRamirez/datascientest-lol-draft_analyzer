"""
DPM.LOL Data Scraper

Fetches champion statistics from DPM.LOL using Playwright browser automation.
The site uses Cloudflare protection, so we need a real browser session.

Data collected:
- Champion tierlist (winrate, pickrate, banrate, tierScore)
- Lane distribution per champion
- SoloQ Leaderboards (top players by region)
- Champion matchups (counter picks, synergies)

Usage:
    from collect_data.dpmlol_scraper import DPMLOLScraper

    scraper = DPMLOLScraper()
    tierlist = scraper.get_tierlist(tier="emerald_plus")
    leaderboard = scraper.get_leaderboard(platform="euw1")
    matchups = scraper.get_champion_matchups("jinx", "bottom")
    scraper.save_data()


    On va completer ce scrapping pour avoir winrate, pickrate, banrate par champion... global, par elo, par seveur, par role, par patch, par side, par duo, trio, quatuor, par matchup, par champ ennemi (en distngant les sides ?)
"""

import json
import logging
import time
from pathlib import Path
from typing import Optional, Dict, List, Any
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


class DPMLOLScraper:
    """Scraper for DPM.LOL champion statistics using Playwright."""

    BASE_URL = "https://dpm.lol"

    # Available tiers
    TIERS = [
        "all",
        "iron", "bronze", "silver", "gold",
        "platinum", "emerald", "diamond",
        "master", "grandmaster", "challenger",
        "emerald_plus", "diamond_plus", "master_plus"
    ]

    # Lane mapping
    LANES = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]

    # Lane URL mapping
    LANE_URL_MAP = {
        "top": "top",
        "jungle": "jungle",
        "mid": "middle",
        "middle": "middle",
        "bot": "bottom",
        "bottom": "bottom",
        "adc": "bottom",
        "support": "utility",
        "utility": "utility"
    }

    # Available platforms/regions for leaderboards
    PLATFORMS = ["kr", "euw1", "na1", "eun1", "br1", "jp1", "la1", "la2", "oc1", "tr1", "ru", "ph2", "sg2", "th2", "tw2", "vn2"]

    def __init__(self, data_dir: Optional[str] = None, headless: bool = True):
        """
        Initialize the scraper.

        Args:
            data_dir: Directory to save data (default: data/dpmlol/)
            headless: Run browser in headless mode
        """
        self.headless = headless
        self.data_dir = Path(data_dir) if data_dir else Path(__file__).parent.parent.parent / "data" / "dpmlol"
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Data storage
        self._tierlist_data: Dict[str, Any] = {}
        self._champion_data: Dict[int, Dict] = {}
        self._matchups_data: List[Dict] = []
        self._leaderboard_data: Dict[str, Any] = {}

        # Browser state
        self._browser = None
        self._context = None
        self._page = None
        self._playwright = None

    def _init_browser(self):
        """Initialize Playwright browser with anti-detection measures."""
        if self._browser is not None:
            return

        from playwright.sync_api import sync_playwright

        logger.info("Initializing browser...")
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox'
            ]
        )

        self._context = self._browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US"
        )

        self._page = self._context.new_page()
        self._cookies_accepted = False
        logger.info("Browser initialized")

    def _close_browser(self):
        """Close browser and cleanup."""
        if self._browser:
            self._browser.close()
            self._browser = None
        if self._playwright:
            self._playwright.stop()
            self._playwright = None

    def _capture_api_response(self, url_pattern: str, navigate_to: str, timeout: int = 60000) -> Optional[Dict]:
        """
        Navigate to a page and capture the API response matching the pattern.

        Args:
            url_pattern: Part of URL to match (e.g., '/v1/tierlist')
            navigate_to: Page URL to navigate to
            timeout: Timeout in milliseconds

        Returns:
            JSON response data or None
        """
        # Use a list to capture data (mutable, works with closures)
        captured_data = [None]

        def handle_response(response):
            if url_pattern in response.url and response.status == 200:
                try:
                    data = response.json()
                    captured_data[0] = data
                    logger.info(f"Captured API response from: {response.url}")
                except Exception as e:
                    logger.warning(f"Failed to parse response: {e}")

        # Add listener before navigation
        self._page.on("response", handle_response)

        try:
            self._page.goto(navigate_to, wait_until="domcontentloaded", timeout=timeout)
            self._page.wait_for_timeout(5000)  # Wait for API calls to complete
        except Exception as e:
            logger.error(f"Navigation error: {e}")

        # Note: Don't remove listener here - it might still be processing

        return captured_data[0]

    def get_tierlist(self, tier: str = "emerald_plus", timeframe: str = "15.24",
                     game_mode: str = "ranked") -> Optional[pd.DataFrame]:
        """
        Get champion tierlist data.

        Args:
            tier: Rank tier filter (emerald_plus, diamond_plus, master_plus, etc.)
            timeframe: Patch version (e.g., "15.24")
            game_mode: Game mode (ranked, aram)

        Returns:
            DataFrame with champion stats
        """
        self._init_browser()

        logger.info(f"Fetching tierlist for tier={tier}, timeframe={timeframe}")

        # Set up response capture BEFORE any navigation
        captured_data = [None]

        def handle_response(response):
            if '/v1/tierlist' in response.url and response.status == 200:
                try:
                    captured_data[0] = response.json()
                    logger.info(f"Captured tierlist data: {len(captured_data[0].get('champions', []))} champions")
                except Exception as e:
                    logger.warning(f"Failed to parse tierlist response: {e}")

        self._page.on("response", handle_response)

        # First navigate to home to accept cookies if not done
        if not self._cookies_accepted:
            logger.info("First visit - accepting cookies...")
            try:
                self._page.goto(f"{self.BASE_URL}/", wait_until="domcontentloaded", timeout=60000)
                self._page.wait_for_timeout(3000)
                agree_btn = self._page.query_selector("button:has-text('Agree')")
                if agree_btn:
                    agree_btn.click()
                    self._page.wait_for_timeout(2000)
                    logger.info("Cookies accepted")
                self._cookies_accepted = True
            except Exception as e:
                logger.warning(f"Cookie acceptance issue: {e}")
                self._cookies_accepted = True  # Continue anyway

        # Navigate to tierlist page - the API is called automatically
        url = f"{self.BASE_URL}/tierlist?tier={tier}&patch={timeframe}"
        try:
            logger.info(f"Navigating to {url}")
            self._page.goto(url, wait_until="domcontentloaded", timeout=60000)
            self._page.wait_for_timeout(5000)  # Wait for API to respond
        except Exception as e:
            logger.error(f"Navigation error: {e}")

        data = captured_data[0]

        if not data or 'champions' not in data:
            logger.error("Failed to get tierlist data")
            # Take screenshot for debugging
            try:
                self._page.screenshot(path=str(self.data_dir / "debug_screenshot.png"))
                logger.info(f"Debug screenshot saved to {self.data_dir / 'debug_screenshot.png'}")
            except:
                pass
            return None

        champions = data['champions']
        logger.info(f"Got {len(champions)} champion entries")

        # Store raw data
        self._tierlist_data[f"{tier}_{timeframe}"] = data

        # Convert to DataFrame
        df = pd.DataFrame(champions)

        # Expand lanesPickrate into separate columns
        if 'lanesPickrate' in df.columns:
            lanes_df = df['lanesPickrate'].apply(pd.Series)
            lanes_df.columns = [f"lane_pickrate_{col.lower()}" for col in lanes_df.columns]
            df = pd.concat([df.drop('lanesPickrate', axis=1), lanes_df], axis=1)

        # Add metadata
        df['tier'] = tier
        df['timeframe'] = timeframe
        df['scraped_at'] = datetime.now().isoformat()

        return df

    def get_all_tierlists(self, tiers: List[str] = None,
                          timeframe: str = "15.24") -> pd.DataFrame:
        """
        Get tierlist data for multiple rank tiers.

        Args:
            tiers: List of tiers to fetch (default: emerald_plus, diamond_plus, master_plus)
            timeframe: Patch version

        Returns:
            Combined DataFrame with all tierlist data
        """
        if tiers is None:
            tiers = ["emerald_plus", "diamond_plus", "master_plus"]

        all_data = []

        for tier in tiers:
            logger.info(f"Fetching tierlist for {tier}...")
            df = self.get_tierlist(tier=tier, timeframe=timeframe)
            if df is not None:
                all_data.append(df)
            time.sleep(2)  # Be nice to the server

        if not all_data:
            return pd.DataFrame()

        return pd.concat(all_data, ignore_index=True)

    # =========================================================================
    # LEADERBOARDS
    # =========================================================================

    def get_leaderboards_from_home(self) -> pd.DataFrame:
        """
        Get leaderboard data from home page (loads EUW, NA, KR automatically).

        The home page loads leaderboard data for major regions automatically,
        avoiding Cloudflare challenges on the dedicated leaderboards page.

        Returns:
            DataFrame with leaderboard data for all regions loaded on home page
        """
        self._init_browser()

        logger.info("Fetching leaderboards from home page...")

        captured_leaderboards = {}
        cutoffs = {}

        def handle_response(response):
            if '/v1/leaderboards/soloq' in response.url and response.status == 200:
                try:
                    data = response.json()
                    # Extract platform from URL
                    import re
                    match = re.search(r'platform=(\w+)', response.url)
                    if match:
                        platform = match.group(1)
                        captured_leaderboards[platform] = data
                        cutoffs[platform] = {
                            'challenger': data.get('challengerCutoff'),
                            'grandmaster': data.get('grandmasterCutoff')
                        }
                        logger.info(f"Captured {platform} leaderboard: {len(data.get('players', []))} players")
                except Exception as e:
                    logger.warning(f"Failed to parse leaderboard response: {e}")

        self._page.on("response", handle_response)

        # Navigate to home page - leaderboards are loaded automatically
        try:
            self._page.goto(f"{self.BASE_URL}/", wait_until="domcontentloaded", timeout=60000)
            self._page.wait_for_timeout(5000)

            # Accept cookies
            try:
                agree_btn = self._page.query_selector("button:has-text('Agree')")
                if agree_btn:
                    agree_btn.click()
                    self._page.wait_for_timeout(2000)
                self._cookies_accepted = True
            except:
                pass

        except Exception as e:
            logger.error(f"Navigation error: {e}")
            return pd.DataFrame()

        if not captured_leaderboards:
            logger.error("No leaderboard data captured from home page")
            return pd.DataFrame()

        # Process all captured leaderboards
        all_players = []

        for platform, data in captured_leaderboards.items():
            if 'players' not in data:
                continue

            platform_cutoffs = cutoffs.get(platform, {})

            for player in data['players']:
                rank_data = player.get('rank', {})
                flat_player = {
                    'platform': platform,
                    'leaderboard_position': player.get('leaderboardPosition'),
                    'puuid': player.get('puuid'),
                    'game_name': player.get('gameName'),
                    'tag_line': player.get('tagLine'),
                    'display_name': player.get('displayName'),
                    'team': player.get('team'),
                    'role': player.get('role'),
                    'main_lane': player.get('lane'),
                    'tier': rank_data.get('tier'),
                    'rank': rank_data.get('rank'),
                    'lp': rank_data.get('leaguePoints'),
                    'wins': rank_data.get('wins'),
                    'losses': rank_data.get('losses'),
                    'winrate': rank_data.get('wins', 0) / max(1, rank_data.get('wins', 0) + rank_data.get('losses', 0)) * 100,
                    'kda': player.get('kda'),
                    'champion_ids': player.get('championIds', []),
                    'is_live': player.get('isLive', False),
                    'challenger_cutoff_lp': platform_cutoffs.get('challenger'),
                    'grandmaster_cutoff_lp': platform_cutoffs.get('grandmaster'),
                }
                all_players.append(flat_player)

            # Store for saving
            self._leaderboard_data[platform] = {
                'players': [p for p in all_players if p['platform'] == platform],
                'cutoffs': platform_cutoffs,
                'total': len(data['players'])
            }

        df = pd.DataFrame(all_players)
        df['scraped_at'] = datetime.now().isoformat()

        logger.info(f"Got {len(df)} total players from {len(captured_leaderboards)} regions")
        return df

    def get_leaderboard(self, platform: str = "euw1", pages: int = 1,
                        is_pro: bool = False) -> Optional[pd.DataFrame]:
        """
        Get SoloQ leaderboard data for a specific region.

        Note: For EUW, NA, KR use get_leaderboards_from_home() which avoids Cloudflare.

        Args:
            platform: Region code (kr, euw1, na1, eun1, br1, etc.)
            pages: Number of pages to fetch (50 players per page)
            is_pro: Filter for pro players only

        Returns:
            DataFrame with leaderboard data
        """
        # For major regions, use home page method (avoids Cloudflare)
        if platform in ['euw1', 'na1', 'kr'] and pages == 1 and not is_pro:
            logger.info(f"Using home page method for {platform} (avoids Cloudflare)")
            df = self.get_leaderboards_from_home()
            if not df.empty:
                return df[df['platform'] == platform]

        # For other regions/options, try direct navigation (may hit Cloudflare)
        self._init_browser()
        self._ensure_cookies_accepted()

        logger.info(f"Fetching leaderboard for {platform} (pages={pages}, is_pro={is_pro})")
        logger.warning("Direct leaderboard access may trigger Cloudflare challenge")

        all_players = []
        cutoffs = {'challenger': None, 'grandmaster': None}

        for page_num in range(1, pages + 1):
            captured_data = [None]

            def handle_response(response):
                if '/v1/leaderboards/soloq' in response.url and response.status == 200:
                    try:
                        data = response.json()
                        captured_data[0] = data
                        if 'challengerCutoff' in data:
                            cutoffs['challenger'] = data['challengerCutoff']
                        if 'grandmasterCutoff' in data:
                            cutoffs['grandmaster'] = data['grandmasterCutoff']
                    except:
                        pass

            self._page.on("response", handle_response)

            url = f"{self.BASE_URL}/leaderboards?platform={platform}&page={page_num}&isPro={str(is_pro).lower()}"

            try:
                self._page.goto(url, wait_until="domcontentloaded", timeout=60000)
                self._page.wait_for_timeout(4000)

                if captured_data[0] and 'players' in captured_data[0]:
                    for player in captured_data[0]['players']:
                        rank_data = player.get('rank', {})
                        flat_player = {
                            'platform': platform,
                            'leaderboard_position': player.get('leaderboardPosition'),
                            'puuid': player.get('puuid'),
                            'game_name': player.get('gameName'),
                            'tag_line': player.get('tagLine'),
                            'display_name': player.get('displayName'),
                            'team': player.get('team'),
                            'role': player.get('role'),
                            'main_lane': player.get('lane'),
                            'tier': rank_data.get('tier'),
                            'rank': rank_data.get('rank'),
                            'lp': rank_data.get('leaguePoints'),
                            'wins': rank_data.get('wins'),
                            'losses': rank_data.get('losses'),
                            'winrate': rank_data.get('wins', 0) / max(1, rank_data.get('wins', 0) + rank_data.get('losses', 0)) * 100,
                            'kda': player.get('kda'),
                            'champion_ids': player.get('championIds', []),
                            'is_live': player.get('isLive', False),
                        }
                        all_players.append(flat_player)
            except Exception as e:
                logger.error(f"Error fetching page {page_num}: {e}")

            if page_num < pages:
                time.sleep(1)

        if not all_players:
            logger.error("Failed to get leaderboard data (Cloudflare blocked?)")
            return None

        df = pd.DataFrame(all_players)
        df['challenger_cutoff_lp'] = cutoffs['challenger']
        df['grandmaster_cutoff_lp'] = cutoffs['grandmaster']
        df['scraped_at'] = datetime.now().isoformat()

        self._leaderboard_data[platform] = {
            'players': all_players,
            'cutoffs': cutoffs,
            'total': len(all_players)
        }

        logger.info(f"Got {len(df)} players from {platform} leaderboard")
        return df

    def get_all_leaderboards(self, platforms: List[str] = None,
                             pages_per_platform: int = 2) -> pd.DataFrame:
        """
        Get leaderboards from multiple regions.

        Args:
            platforms: List of platform codes (default: kr, euw1, na1)
            pages_per_platform: Pages to fetch per region

        Returns:
            Combined DataFrame with all leaderboard data
        """
        if platforms is None:
            platforms = ["kr", "euw1", "na1"]

        all_data = []

        for platform in platforms:
            logger.info(f"Fetching {platform} leaderboard...")
            df = self.get_leaderboard(platform=platform, pages=pages_per_platform)
            if df is not None:
                all_data.append(df)
            time.sleep(2)

        if not all_data:
            return pd.DataFrame()

        return pd.concat(all_data, ignore_index=True)

    # =========================================================================
    # CHAMPION MATCHUPS
    # =========================================================================

    def _ensure_cookies_accepted(self):
        """Ensure cookies are accepted before making requests."""
        if not self._cookies_accepted:
            logger.info("First visit - accepting cookies...")
            try:
                self._page.goto(f"{self.BASE_URL}/", wait_until="domcontentloaded", timeout=60000)
                self._page.wait_for_timeout(3000)
                agree_btn = self._page.query_selector("button:has-text('Agree')")
                if agree_btn:
                    agree_btn.click()
                    self._page.wait_for_timeout(2000)
                    logger.info("Cookies accepted")
                self._cookies_accepted = True
            except Exception as e:
                logger.warning(f"Cookie acceptance issue: {e}")
                self._cookies_accepted = True

    def get_champion_matchups(self, champion_name: str, lane: str,
                              tier: str = "emerald_plus") -> Optional[pd.DataFrame]:
        """
        Get matchup data for a specific champion.

        Note: This may trigger Cloudflare challenge on some requests.

        Args:
            champion_name: Champion name (e.g., "jinx", "leesin", "missfortune")
            lane: Lane (top, jungle, mid, adc/bottom, support/utility)
            tier: Rank tier filter

        Returns:
            DataFrame with matchup data including:
            - opponent champion
            - winrate against opponent
            - games played
            - gold diff at 15
        """
        self._init_browser()
        self._ensure_cookies_accepted()

        lane_url = self.LANE_URL_MAP.get(lane.lower(), lane.lower())

        # Normalize champion name (remove spaces, lowercase)
        champ_normalized = champion_name.lower().replace(" ", "").replace("'", "")

        url = f"{self.BASE_URL}/champions/{champ_normalized}/{lane_url}/matchups?tier={tier}"
        logger.info(f"Fetching matchups for {champion_name} ({lane})...")

        # Set up response capture for matchup data
        captured_data = [None]

        def handle_response(response):
            url_lower = response.url.lower()
            if '/v1/champions/' in url_lower and '/matchups' in url_lower and response.status == 200:
                try:
                    data = response.json()
                    captured_data[0] = data
                    logger.info(f"Captured matchup data from: {response.url}")
                except Exception as e:
                    logger.warning(f"Failed to parse matchup response: {e}")

        self._page.on("response", handle_response)

        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=60000)
            self._page.wait_for_timeout(5000)
        except Exception as e:
            logger.error(f"Navigation error: {e}")

        data = captured_data[0]

        if not data:
            logger.warning(f"No matchup data for {champion_name} (may be Cloudflare blocked)")
            # Take screenshot for debugging
            try:
                self._page.screenshot(path=str(self.data_dir / f"debug_matchups_{champ_normalized}.png"))
            except:
                pass
            return None

        # Process matchup data - handle different response formats
        matchups_list = []

        if isinstance(data, dict):
            # Check for different possible keys
            if 'matchups' in data:
                matchups_raw = data['matchups']
            elif 'counters' in data:
                matchups_raw = data['counters']
            elif 'data' in data:
                matchups_raw = data['data']
            else:
                matchups_raw = data
        elif isinstance(data, list):
            matchups_raw = data
        else:
            logger.warning(f"Unexpected matchup data format: {type(data)}")
            return None

        # Normalize matchup data
        if isinstance(matchups_raw, list):
            for m in matchups_raw:
                if isinstance(m, dict):
                    matchup = {
                        'champion': champion_name,
                        'champion_normalized': champ_normalized,
                        'lane': lane,
                        'opponent_id': m.get('championId') or m.get('opponentId') or m.get('id'),
                        'opponent_name': m.get('championName') or m.get('opponentName') or m.get('name'),
                        'matchup_winrate': m.get('winrate') or m.get('winRate') or m.get('wr'),
                        'matchup_games': m.get('count') or m.get('games') or m.get('matches'),
                        'gold_diff_15': m.get('goldDiff15') or m.get('goldDiff') or m.get('gd15'),
                        'kills_diff': m.get('killsDiff') or m.get('kd'),
                        'tier': tier,
                    }
                    matchups_list.append(matchup)

        if not matchups_list:
            logger.warning(f"No matchups parsed for {champion_name}")
            return None

        df = pd.DataFrame(matchups_list)
        df['scraped_at'] = datetime.now().isoformat()

        # Store for later saving
        self._matchups_data.extend(matchups_list)

        logger.info(f"Got {len(df)} matchups for {champion_name} ({lane})")
        return df

    def get_all_matchups(self, champions: List[Dict[str, str]] = None,
                         tier: str = "emerald_plus",
                         delay: float = 3.0) -> pd.DataFrame:
        """
        Get matchups for multiple champions.

        Args:
            champions: List of dicts with 'name' and 'lane' keys
                      e.g., [{'name': 'jinx', 'lane': 'bottom'}, ...]
                      If None, uses top 20 champions from tierlist
            tier: Rank tier filter
            delay: Delay between requests (be nice to the server)

        Returns:
            Combined DataFrame with all matchup data
        """
        self._init_browser()
        self._ensure_cookies_accepted()

        # If no champions specified, get top champions from tierlist
        if champions is None:
            logger.info("No champions specified, fetching top 20 from tierlist...")
            tierlist_df = self.get_tierlist(tier=tier)
            if tierlist_df is not None:
                top_champs = tierlist_df.nlargest(20, 'tierScore')
                champions = [
                    {'name': row['championName'], 'lane': row['lane'].lower()}
                    for _, row in top_champs.iterrows()
                ]
            else:
                logger.error("Could not get tierlist for champion selection")
                return pd.DataFrame()

        all_matchups = []
        success_count = 0
        fail_count = 0

        for i, champ in enumerate(champions):
            name = champ['name']
            lane = champ['lane']

            logger.info(f"[{i+1}/{len(champions)}] Fetching matchups for {name} ({lane})...")

            df = self.get_champion_matchups(name, lane, tier=tier)

            if df is not None:
                all_matchups.append(df)
                success_count += 1
            else:
                fail_count += 1

            if i < len(champions) - 1:
                time.sleep(delay)

        logger.info(f"Matchup scraping complete: {success_count} success, {fail_count} failed")

        if not all_matchups:
            return pd.DataFrame()

        return pd.concat(all_matchups, ignore_index=True)

    def save_data(self, filename_prefix: str = "dpmlol") -> Dict[str, Path]:
        """
        Save collected data to parquet files.

        Args:
            filename_prefix: Prefix for output files

        Returns:
            Dict mapping data type to file path
        """
        saved_files = {}

        # Save tierlist data
        if self._tierlist_data:
            all_tierlist = []
            for key, data in self._tierlist_data.items():
                if 'champions' in data:
                    df = pd.DataFrame(data['champions'])
                    tier, timeframe = key.rsplit('_', 1) if '_' in key else (key, 'unknown')
                    df['tier'] = tier
                    df['timeframe'] = timeframe
                    all_tierlist.append(df)

            if all_tierlist:
                combined = pd.concat(all_tierlist, ignore_index=True)
                path = self.data_dir / f"{filename_prefix}_tierlist.parquet"
                combined.to_parquet(path, index=False)
                saved_files['tierlist'] = path
                logger.info(f"Saved tierlist to {path}")

        # Save leaderboard data
        if self._leaderboard_data:
            all_players = []
            for platform, data in self._leaderboard_data.items():
                if 'players' in data:
                    for player in data['players']:
                        player['platform'] = platform
                        all_players.append(player)

            if all_players:
                df = pd.DataFrame(all_players)
                path = self.data_dir / f"{filename_prefix}_leaderboards.parquet"
                df.to_parquet(path, index=False)
                saved_files['leaderboards'] = path
                logger.info(f"Saved leaderboards to {path}")

        # Save matchups data
        if self._matchups_data:
            df = pd.DataFrame(self._matchups_data)
            df['scraped_at'] = datetime.now().isoformat()
            path = self.data_dir / f"{filename_prefix}_matchups.parquet"
            df.to_parquet(path, index=False)
            saved_files['matchups'] = path
            logger.info(f"Saved matchups to {path}")

        # Save metadata
        metadata = {
            "source": "dpm.lol",
            "scraped_at": datetime.now().isoformat(),
            "tiers_collected": list(self._tierlist_data.keys()),
            "platforms_collected": list(self._leaderboard_data.keys()),
            "total_champions": sum(len(d.get('champions', [])) for d in self._tierlist_data.values()),
            "total_players": sum(len(d.get('players', [])) for d in self._leaderboard_data.values()),
            "total_matchups": len(self._matchups_data),
        }

        metadata_path = self.data_dir / "metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        saved_files['metadata'] = metadata_path

        return saved_files

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup browser."""
        self._close_browser()


def main():
    """Example usage of the scraper."""
    import logging
    logging.basicConfig(level=logging.INFO)

    with DPMLOLScraper(headless=True) as scraper:
        # Get tierlist for emerald+
        print("Fetching tierlist data...")
        df = scraper.get_tierlist(tier="emerald_plus")

        if df is not None:
            print(f"\nGot {len(df)} champion entries")
            print("\nTop 10 champions by tier score:")
            print(df.nlargest(10, 'tierScore')[['championName', 'lane', 'winrate', 'pickrate', 'tierScore']])

            # Save data
            files = scraper.save_data()
            print(f"\nSaved to: {files}")
        else:
            print("Failed to get data")


if __name__ == "__main__":
    main()
