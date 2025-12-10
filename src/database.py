"""
SQLite Database Interface for LoL Match Data Collection

This module provides a robust database interface for storing and retrieving
League of Legends match data. It replaces the previous JSON/CSV storage
for better reliability during long-running data collection.
"""

import sqlite3
import json
import pandas as pd
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, Set, Dict, Any, List


class MatchDatabase:
    """
    SQLite database interface for LoL match data.

    Features:
    - Atomic transactions for data integrity
    - Automatic deduplication via PRIMARY KEY
    - Progress tracking for resumable collection
    - Export to pandas DataFrame for ML
    """

    def __init__(self, db_path: str = 'lol_matches.db'):
        self.db_path = db_path
        self._init_db()
        self._migrate_schema()
        self._enable_wal()

    @contextmanager
    def get_connection(self):
        """Context manager for database connections with auto-commit"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        """Initialize database schema"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Table 1: Core match information
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS matches (
                    match_id TEXT PRIMARY KEY,
                    game_creation INTEGER,
                    game_duration INTEGER,
                    game_version TEXT,
                    queue_id INTEGER DEFAULT 420,
                    map_id INTEGER,
                    game_mode TEXT,
                    game_type TEXT,
                    team_100_win BOOLEAN,
                    team_100_early_surrendered BOOLEAN DEFAULT FALSE,
                    team_200_early_surrendered BOOLEAN DEFAULT FALSE,
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Table 2: Team-level objectives and bans
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS team_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id TEXT NOT NULL,
                    team_id INTEGER NOT NULL,
                    -- Objectives (first)
                    first_blood BOOLEAN,
                    first_tower BOOLEAN,
                    first_inhibitor BOOLEAN,
                    first_dragon BOOLEAN,
                    first_rift_herald BOOLEAN,
                    first_baron BOOLEAN,
                    -- Objective kills
                    dragon_kills INTEGER DEFAULT 0,
                    baron_kills INTEGER DEFAULT 0,
                    tower_kills INTEGER DEFAULT 0,
                    inhibitor_kills INTEGER DEFAULT 0,
                    rift_herald_kills INTEGER DEFAULT 0,
                    -- Bans (champion IDs)
                    ban_1_champion_id INTEGER,
                    ban_2_champion_id INTEGER,
                    ban_3_champion_id INTEGER,
                    ban_4_champion_id INTEGER,
                    ban_5_champion_id INTEGER,
                    FOREIGN KEY (match_id) REFERENCES matches(match_id),
                    UNIQUE(match_id, team_id)
                )
            ''')

            # Table 3: Individual player performance
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS player_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id TEXT NOT NULL,
                    team_id INTEGER NOT NULL,
                    position TEXT NOT NULL,
                    -- Player identity (NEW)
                    puuid TEXT,
                    riot_id_name TEXT,
                    riot_id_tagline TEXT,
                    -- Champion info
                    champion_id INTEGER NOT NULL,
                    champion_name TEXT,
                    champ_level INTEGER,
                    -- Summoner spells
                    summoner_1_id INTEGER,
                    summoner_2_id INTEGER,
                    -- KDA
                    kills INTEGER,
                    deaths INTEGER,
                    assists INTEGER,
                    -- Damage stats
                    total_damage_dealt INTEGER,
                    total_damage_to_champions INTEGER,
                    total_damage_taken INTEGER,
                    true_damage_dealt INTEGER,
                    physical_damage_dealt INTEGER,
                    magic_damage_dealt INTEGER,
                    -- Economy
                    gold_earned INTEGER,
                    total_minions_killed INTEGER,
                    neutral_minions_killed INTEGER,
                    -- Vision
                    vision_score INTEGER,
                    wards_placed INTEGER,
                    wards_killed INTEGER,
                    vision_wards_bought INTEGER,
                    -- Combat
                    enemy_champion_immobilizations INTEGER DEFAULT 0,
                    first_blood_kill BOOLEAN,
                    first_tower_kill BOOLEAN,
                    turret_kills INTEGER,
                    inhibitor_kills INTEGER,
                    largest_killing_spree INTEGER,
                    largest_multi_kill INTEGER,
                    killing_sprees INTEGER,
                    double_kills INTEGER,
                    triple_kills INTEGER,
                    quadra_kills INTEGER,
                    penta_kills INTEGER,
                    -- Advanced metrics (from challenges)
                    damage_per_minute REAL,
                    damage_taken_percentage REAL,
                    gold_per_minute REAL,
                    team_damage_percentage REAL,
                    kill_participation REAL,
                    kda REAL,
                    lane_minions_first_10_min INTEGER,
                    turret_plates_taken INTEGER,
                    solo_kills INTEGER,
                    FOREIGN KEY (match_id) REFERENCES matches(match_id),
                    UNIQUE(match_id, team_id, position)
                )
            ''')

            # Table 4: Collection progress tracking
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS collection_progress (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    puuid TEXT UNIQUE,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Table 5: Collection statistics
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS collection_stats (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')

            # Table 6: Summoners (player profiles)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS summoners (
                    puuid TEXT PRIMARY KEY,
                    riot_id_name TEXT,
                    riot_id_tagline TEXT,
                    -- Current rank info
                    current_tier TEXT,
                    current_rank TEXT,
                    current_lp INTEGER,
                    -- Stats
                    total_games_tracked INTEGER DEFAULT 0,
                    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Table 7: Summoner elo history (elo par patch)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS summoner_elo_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    puuid TEXT NOT NULL,
                    patch TEXT NOT NULL,
                    tier TEXT,
                    rank TEXT,
                    lp INTEGER,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (puuid) REFERENCES summoners(puuid),
                    UNIQUE(puuid, patch)
                )
            ''')

            # Table 8: Champion mastery per summoner
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS champion_mastery (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    puuid TEXT NOT NULL,
                    champion_id INTEGER NOT NULL,
                    champion_level INTEGER,
                    champion_points INTEGER,
                    last_play_time INTEGER,
                    tokens_earned INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (puuid) REFERENCES summoners(puuid),
                    UNIQUE(puuid, champion_id)
                )
            ''')

            # Table 9: Patches history
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patches (
                    patch TEXT PRIMARY KEY,
                    release_date DATE,
                    data_dragon_version TEXT,
                    notes_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Table 10: Champion stats per patch (winrate, pickrate, banrate)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS champion_patch_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    champion_id INTEGER NOT NULL,
                    patch TEXT NOT NULL,
                    -- Calculated from our data
                    games_played INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    bans INTEGER DEFAULT 0,
                    -- Calculated rates
                    winrate REAL,
                    pickrate REAL,
                    banrate REAL,
                    -- Per role stats
                    top_games INTEGER DEFAULT 0,
                    jungle_games INTEGER DEFAULT 0,
                    mid_games INTEGER DEFAULT 0,
                    adc_games INTEGER DEFAULT 0,
                    support_games INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(champion_id, patch)
                )
            ''')

            # Table 11: Match timeline snapshots (gold per minute)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS match_timeline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id TEXT NOT NULL,
                    minute INTEGER NOT NULL,
                    -- Team gold totals
                    team_100_gold INTEGER,
                    team_200_gold INTEGER,
                    gold_diff INTEGER,
                    -- Per position gold (team 100)
                    team_100_top_gold INTEGER,
                    team_100_jungle_gold INTEGER,
                    team_100_mid_gold INTEGER,
                    team_100_adc_gold INTEGER,
                    team_100_support_gold INTEGER,
                    -- Per position gold (team 200)
                    team_200_top_gold INTEGER,
                    team_200_jungle_gold INTEGER,
                    team_200_mid_gold INTEGER,
                    team_200_adc_gold INTEGER,
                    team_200_support_gold INTEGER,
                    FOREIGN KEY (match_id) REFERENCES matches(match_id),
                    UNIQUE(match_id, minute)
                )
            ''')

            # Create indices for common queries (only for new tables, player_stats index created in migration)
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_matches_game_creation ON matches(game_creation)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_stats_champion ON player_stats(champion_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_stats_match ON player_stats(match_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_team_stats_match ON team_stats(match_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_champion_mastery_puuid ON champion_mastery(puuid)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_summoner_elo_puuid ON summoner_elo_history(puuid)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_timeline_match ON match_timeline(match_id)')

    def _migrate_schema(self):
        """
        Migrate existing database to new schema.
        Adds new columns to existing tables without losing data.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Check existing columns in player_stats
            cursor.execute('PRAGMA table_info(player_stats)')
            existing_cols = {row[1] for row in cursor.fetchall()}

            # Add new columns to player_stats if they don't exist
            new_player_cols = [
                ('puuid', 'TEXT'),
                ('riot_id_name', 'TEXT'),
                ('riot_id_tagline', 'TEXT')
            ]

            for col_name, col_type in new_player_cols:
                if col_name not in existing_cols:
                    try:
                        cursor.execute(f'ALTER TABLE player_stats ADD COLUMN {col_name} {col_type}')
                        print(f"  Added column {col_name} to player_stats")
                    except Exception as e:
                        pass  # Column might already exist

            # Create indices for new columns (ignore if exists)
            try:
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_stats_puuid ON player_stats(puuid)')
            except:
                pass

    def _enable_wal(self):
        """
        Enable WAL (Write-Ahead Logging) mode for better concurrent access.
        This allows multiple processes to read/write simultaneously.
        """
        with self.get_connection() as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA busy_timeout=30000')  # 30 second timeout for locked DB

    def match_exists(self, match_id: str) -> bool:
        """Check if match already exists in database"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM matches WHERE match_id = ?', (match_id,))
            return cursor.fetchone() is not None

    def get_collected_match_ids(self) -> Set[str]:
        """Get set of all collected match IDs"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT match_id FROM matches')
            return {row[0] for row in cursor.fetchall()}

    def insert_match(self, match_data: Dict[str, Any]) -> bool:
        """
        Insert complete match with all related data.
        Uses transaction for atomic insert.

        Args:
            match_data: Raw match data from Riot API

        Returns:
            True if inserted, False if already exists
        """
        info = match_data.get("info", {})
        metadata = match_data.get("metadata", {})
        match_id = metadata.get("matchId")

        if not match_id:
            return False

        # Check if already exists
        if self.match_exists(match_id):
            return False

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Insert match info
            teams = info.get("teams", [])
            team_100_win = None
            team_100_early_surrendered = False
            team_200_early_surrendered = False

            for team in teams:
                if team.get("teamId") == 100:
                    team_100_win = team.get("win")
                    team_100_early_surrendered = team.get("teamEarlySurrendered", False)
                elif team.get("teamId") == 200:
                    team_200_early_surrendered = team.get("teamEarlySurrendered", False)

            cursor.execute('''
                INSERT INTO matches (
                    match_id, game_creation, game_duration, game_version,
                    queue_id, map_id, game_mode, game_type,
                    team_100_win, team_100_early_surrendered, team_200_early_surrendered
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                match_id,
                info.get("gameCreation"),
                info.get("gameDuration"),
                info.get("gameVersion"),
                info.get("queueId"),
                info.get("mapId"),
                info.get("gameMode"),
                info.get("gameType"),
                team_100_win,
                team_100_early_surrendered,
                team_200_early_surrendered
            ))

            # Insert team stats
            for team in teams:
                team_id = team.get("teamId")
                objectives = team.get("objectives", {})
                bans = team.get("bans", [])

                # Prepare ban champion IDs
                ban_ids = [None] * 5
                for i, ban in enumerate(bans[:5]):
                    ban_ids[i] = ban.get("championId")

                cursor.execute('''
                    INSERT INTO team_stats (
                        match_id, team_id,
                        first_blood, first_tower, first_inhibitor,
                        first_dragon, first_rift_herald, first_baron,
                        dragon_kills, baron_kills, tower_kills,
                        inhibitor_kills, rift_herald_kills,
                        ban_1_champion_id, ban_2_champion_id, ban_3_champion_id,
                        ban_4_champion_id, ban_5_champion_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match_id, team_id,
                    objectives.get("champion", {}).get("first", False),
                    objectives.get("tower", {}).get("first", False),
                    objectives.get("inhibitor", {}).get("first", False),
                    objectives.get("dragon", {}).get("first", False),
                    objectives.get("riftHerald", {}).get("first", False),
                    objectives.get("baron", {}).get("first", False),
                    objectives.get("dragon", {}).get("kills", 0),
                    objectives.get("baron", {}).get("kills", 0),
                    objectives.get("tower", {}).get("kills", 0),
                    objectives.get("inhibitor", {}).get("kills", 0),
                    objectives.get("riftHerald", {}).get("kills", 0),
                    ban_ids[0], ban_ids[1], ban_ids[2], ban_ids[3], ban_ids[4]
                ))

            # Insert player stats
            position_map = {
                "TOP": "top",
                "JUNGLE": "jungle",
                "MIDDLE": "mid",
                "BOTTOM": "adc",
                "UTILITY": "support"
            }

            for participant in info.get("participants", []):
                team_id = participant.get("teamId")
                position = position_map.get(
                    participant.get("teamPosition"),
                    participant.get("teamPosition", "unknown")
                )
                challenges = participant.get("challenges", {})

                cursor.execute('''
                    INSERT INTO player_stats (
                        match_id, team_id, position,
                        puuid, riot_id_name, riot_id_tagline,
                        champion_id, champion_name, champ_level,
                        summoner_1_id, summoner_2_id,
                        kills, deaths, assists,
                        total_damage_dealt, total_damage_to_champions, total_damage_taken,
                        true_damage_dealt, physical_damage_dealt, magic_damage_dealt,
                        gold_earned, total_minions_killed, neutral_minions_killed,
                        vision_score, wards_placed, wards_killed, vision_wards_bought,
                        enemy_champion_immobilizations,
                        first_blood_kill, first_tower_kill,
                        turret_kills, inhibitor_kills,
                        largest_killing_spree, largest_multi_kill, killing_sprees,
                        double_kills, triple_kills, quadra_kills, penta_kills,
                        damage_per_minute, damage_taken_percentage, gold_per_minute,
                        team_damage_percentage, kill_participation, kda,
                        lane_minions_first_10_min, turret_plates_taken, solo_kills
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match_id, team_id, position,
                    participant.get("puuid"),
                    participant.get("riotIdGameName"),
                    participant.get("riotIdTagline"),
                    participant.get("championId"),
                    participant.get("championName"),
                    participant.get("champLevel"),
                    participant.get("summoner1Id"),
                    participant.get("summoner2Id"),
                    participant.get("kills"),
                    participant.get("deaths"),
                    participant.get("assists"),
                    participant.get("totalDamageDealt"),
                    participant.get("totalDamageDealtToChampions"),
                    participant.get("totalDamageTaken"),
                    participant.get("trueDamageDealt"),
                    participant.get("physicalDamageDealt"),
                    participant.get("magicDamageDealt"),
                    participant.get("goldEarned"),
                    participant.get("totalMinionsKilled"),
                    participant.get("neutralMinionsKilled"),
                    participant.get("visionScore"),
                    participant.get("wardsPlaced"),
                    participant.get("wardsKilled"),
                    participant.get("visionWardsBoughtInGame"),
                    participant.get("enemyChampionImmobilizations", 0),
                    participant.get("firstBloodKill"),
                    participant.get("firstTowerKill"),
                    participant.get("turretKills"),
                    participant.get("inhibitorKills"),
                    participant.get("largestKillingSpree"),
                    participant.get("largestMultiKill"),
                    participant.get("killingSprees"),
                    participant.get("doubleKills"),
                    participant.get("tripleKills"),
                    participant.get("quadraKills"),
                    participant.get("pentaKills"),
                    challenges.get("damagePerMinute"),
                    challenges.get("damageTakenOnTeamPercentage"),
                    challenges.get("goldPerMinute"),
                    challenges.get("teamDamagePercentage"),
                    challenges.get("killParticipation"),
                    challenges.get("kda"),
                    challenges.get("laneMinionsFirst10Minutes"),
                    challenges.get("turretPlatesTaken"),
                    challenges.get("soloKills")
                ))

            return True

    def save_player_progress(self, puuid: str):
        """Mark player as processed (updates timestamp if already exists)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO collection_progress (puuid, processed_at)
                VALUES (?, CURRENT_TIMESTAMP)
                ON CONFLICT(puuid) DO UPDATE SET processed_at = CURRENT_TIMESTAMP
            ''', (puuid,))

    def is_player_processed(self, puuid: str, refresh_hours: int = 24) -> bool:
        """
        Check if player was recently processed.

        Args:
            puuid: Player's PUUID
            refresh_hours: Hours after which a player can be re-processed (default: 24h)
                          Set to 0 to never re-process (old behavior)

        Returns:
            True if player was processed within refresh_hours, False otherwise
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if refresh_hours > 0:
                # Check if processed within the last refresh_hours
                cursor.execute('''
                    SELECT 1 FROM collection_progress
                    WHERE puuid = ?
                    AND processed_at > datetime('now', ?)
                ''', (puuid, f'-{refresh_hours} hours'))
            else:
                # Old behavior: check if ever processed
                cursor.execute(
                    'SELECT 1 FROM collection_progress WHERE puuid = ?',
                    (puuid,)
                )
            return cursor.fetchone() is not None

    def get_processed_players(self) -> List[str]:
        """Get list of all processed player PUUIDs"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT puuid FROM collection_progress')
            return [row[0] for row in cursor.fetchall()]

    def clear_processed_players(self):
        """Clear all processed players to allow re-fetching new matches"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM collection_progress')
            return cursor.rowcount

    def clear_processed_players_by_prefix(self, prefix: str):
        """Clear processed players with a specific prefix (e.g., 'sid_' for Master+ players)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM collection_progress WHERE puuid LIKE ?', (f'{prefix}%',))
            return cursor.rowcount

    def update_stat(self, key: str, value: Any):
        """Update a collection statistic"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO collection_stats (key, value)
                VALUES (?, ?)
            ''', (key, json.dumps(value)))

    def get_stat(self, key: str, default: Any = None) -> Any:
        """Get a collection statistic"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT value FROM collection_stats WHERE key = ?',
                (key,)
            )
            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
            return default

    def get_stats(self) -> Dict[str, Any]:
        """Get all collection statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Count matches
            cursor.execute('SELECT COUNT(*) FROM matches')
            match_count = cursor.fetchone()[0]

            # Count players
            cursor.execute('SELECT COUNT(*) FROM collection_progress')
            player_count = cursor.fetchone()[0]

            # Get stored stats
            stats = {
                'total_matches': match_count,
                'processed_players': player_count,
                'total_requests': self.get_stat('total_requests', 0),
                'successful_requests': self.get_stat('successful_requests', 0),
                'rate_limit_errors': self.get_stat('rate_limit_errors', 0),
                'other_errors': self.get_stat('other_errors', 0),
                'last_page': self.get_stat('last_page', 1),
                'last_player_index': self.get_stat('last_player_index', 0)
            }

            return stats

    def increment_stat(self, key: str, amount: int = 1):
        """Increment a numeric statistic"""
        current = self.get_stat(key, 0)
        self.update_stat(key, current + amount)

    # ================================================================
    # Summoner Methods (NEW)
    # ================================================================

    def upsert_summoner(self, puuid: str, riot_id_name: str = None, riot_id_tagline: str = None,
                        tier: str = None, rank: str = None, lp: int = None):
        """Insert or update summoner info"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO summoners (puuid, riot_id_name, riot_id_tagline, current_tier, current_rank, current_lp, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(puuid) DO UPDATE SET
                    riot_id_name = COALESCE(excluded.riot_id_name, riot_id_name),
                    riot_id_tagline = COALESCE(excluded.riot_id_tagline, riot_id_tagline),
                    current_tier = COALESCE(excluded.current_tier, current_tier),
                    current_rank = COALESCE(excluded.current_rank, current_rank),
                    current_lp = COALESCE(excluded.current_lp, current_lp),
                    total_games_tracked = total_games_tracked + 1,
                    last_seen_at = CURRENT_TIMESTAMP,
                    last_updated_at = CURRENT_TIMESTAMP
            ''', (puuid, riot_id_name, riot_id_tagline, tier, rank, lp))

    def get_summoner(self, puuid: str) -> Optional[Dict]:
        """Get summoner info by PUUID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM summoners WHERE puuid = ?', (puuid,))
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None

    def record_elo_history(self, puuid: str, patch: str, tier: str, rank: str, lp: int):
        """Record summoner's elo for a specific patch"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO summoner_elo_history (puuid, patch, tier, rank, lp)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(puuid, patch) DO UPDATE SET
                    tier = excluded.tier,
                    rank = excluded.rank,
                    lp = excluded.lp,
                    recorded_at = CURRENT_TIMESTAMP
            ''', (puuid, patch, tier, rank, lp))

    # ================================================================
    # Champion Mastery Methods (NEW)
    # ================================================================

    def upsert_champion_mastery(self, puuid: str, champion_id: int, champion_level: int,
                                 champion_points: int, last_play_time: int = None, tokens_earned: int = 0):
        """Insert or update champion mastery"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO champion_mastery (puuid, champion_id, champion_level, champion_points, last_play_time, tokens_earned)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(puuid, champion_id) DO UPDATE SET
                    champion_level = excluded.champion_level,
                    champion_points = excluded.champion_points,
                    last_play_time = excluded.last_play_time,
                    tokens_earned = excluded.tokens_earned,
                    updated_at = CURRENT_TIMESTAMP
            ''', (puuid, champion_id, champion_level, champion_points, last_play_time, tokens_earned))

    def get_summoner_mastery(self, puuid: str, limit: int = None) -> List[Dict]:
        """Get champion mastery for a summoner, sorted by points"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = 'SELECT * FROM champion_mastery WHERE puuid = ? ORDER BY champion_points DESC'
            if limit:
                query += f' LIMIT {limit}'
            cursor.execute(query, (puuid,))
            return [dict(row) for row in cursor.fetchall()]

    def get_mastery_for_champion(self, puuid: str, champion_id: int) -> Optional[Dict]:
        """Get mastery for a specific champion"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT * FROM champion_mastery WHERE puuid = ? AND champion_id = ?',
                (puuid, champion_id)
            )
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None

    # ================================================================
    # Patch Methods (NEW)
    # ================================================================

    def upsert_patch(self, patch: str, release_date: str = None, data_dragon_version: str = None):
        """Insert or update patch info"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO patches (patch, release_date, data_dragon_version)
                VALUES (?, ?, ?)
                ON CONFLICT(patch) DO UPDATE SET
                    release_date = COALESCE(excluded.release_date, release_date),
                    data_dragon_version = COALESCE(excluded.data_dragon_version, data_dragon_version)
            ''', (patch, release_date, data_dragon_version))

    def get_patches(self) -> List[Dict]:
        """Get all patches"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM patches ORDER BY patch DESC')
            return [dict(row) for row in cursor.fetchall()]

    # ================================================================
    # Champion Patch Stats Methods (NEW)
    # ================================================================

    def update_champion_patch_stats(self, champion_id: int, patch: str, win: bool, position: str):
        """Update champion stats for a patch (called after each match insert)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Get position column name
            position_col = f'{position}_games' if position in ['top', 'jungle', 'mid', 'adc', 'support'] else None

            # Upsert champion stats
            cursor.execute('''
                INSERT INTO champion_patch_stats (champion_id, patch, games_played, wins)
                VALUES (?, ?, 1, ?)
                ON CONFLICT(champion_id, patch) DO UPDATE SET
                    games_played = games_played + 1,
                    wins = wins + ?,
                    updated_at = CURRENT_TIMESTAMP
            ''', (champion_id, patch, 1 if win else 0, 1 if win else 0))

            # Update position-specific count if valid position
            if position_col:
                cursor.execute(f'''
                    UPDATE champion_patch_stats
                    SET {position_col} = {position_col} + 1
                    WHERE champion_id = ? AND patch = ?
                ''', (champion_id, patch))

    def recalculate_champion_rates(self, patch: str):
        """Recalculate winrate/pickrate/banrate for a patch"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Get total games for this patch
            cursor.execute('''
                SELECT COUNT(*) FROM matches WHERE game_version LIKE ?
            ''', (f'{patch}%',))
            total_games = cursor.fetchone()[0]

            if total_games == 0:
                return

            # Update rates
            cursor.execute('''
                UPDATE champion_patch_stats
                SET winrate = CAST(wins AS REAL) / NULLIF(games_played, 0),
                    pickrate = CAST(games_played AS REAL) / ? / 10,
                    banrate = CAST(bans AS REAL) / ? / 10
                WHERE patch = ?
            ''', (total_games, total_games, patch))

    def get_champion_stats_for_patch(self, patch: str) -> List[Dict]:
        """Get all champion stats for a patch"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM champion_patch_stats
                WHERE patch = ?
                ORDER BY games_played DESC
            ''', (patch,))
            return [dict(row) for row in cursor.fetchall()]

    # ================================================================
    # Match Timeline Methods (NEW)
    # ================================================================

    def insert_timeline_frame(self, match_id: str, minute: int, team_gold: Dict, position_gold: Dict):
        """Insert a timeline frame (gold at minute M)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO match_timeline (
                    match_id, minute,
                    team_100_gold, team_200_gold, gold_diff,
                    team_100_top_gold, team_100_jungle_gold, team_100_mid_gold, team_100_adc_gold, team_100_support_gold,
                    team_200_top_gold, team_200_jungle_gold, team_200_mid_gold, team_200_adc_gold, team_200_support_gold
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                match_id, minute,
                team_gold.get('team_100', 0), team_gold.get('team_200', 0),
                team_gold.get('team_100', 0) - team_gold.get('team_200', 0),
                position_gold.get('team_100_top', 0), position_gold.get('team_100_jungle', 0),
                position_gold.get('team_100_mid', 0), position_gold.get('team_100_adc', 0),
                position_gold.get('team_100_support', 0),
                position_gold.get('team_200_top', 0), position_gold.get('team_200_jungle', 0),
                position_gold.get('team_200_mid', 0), position_gold.get('team_200_adc', 0),
                position_gold.get('team_200_support', 0)
            ))

    def get_match_timeline(self, match_id: str) -> List[Dict]:
        """Get timeline for a match"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM match_timeline
                WHERE match_id = ?
                ORDER BY minute
            ''', (match_id,))
            return [dict(row) for row in cursor.fetchall()]

    def get_gold_at_minute(self, match_id: str, minute: int) -> Optional[Dict]:
        """Get gold snapshot at a specific minute"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM match_timeline
                WHERE match_id = ? AND minute = ?
            ''', (match_id, minute))
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None

    # ================================================================
    # Teammates Analysis (NEW)
    # ================================================================

    def get_common_teammates(self, puuid: str, limit: int = 10) -> List[Dict]:
        """Get most frequent teammates for a summoner"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT
                    p2.puuid as teammate_puuid,
                    p2.riot_id_name as teammate_name,
                    COUNT(*) as games_together,
                    SUM(CASE WHEN m.team_100_win = (p1.team_id = 100) THEN 1 ELSE 0 END) as wins_together
                FROM player_stats p1
                JOIN player_stats p2 ON p1.match_id = p2.match_id AND p1.team_id = p2.team_id AND p1.puuid != p2.puuid
                JOIN matches m ON p1.match_id = m.match_id
                WHERE p1.puuid = ?
                GROUP BY p2.puuid, p2.riot_id_name
                ORDER BY games_together DESC
                LIMIT ?
            ''', (puuid, limit))
            return [dict(row) for row in cursor.fetchall()]

    def export_to_dataframe(self) -> pd.DataFrame:
        """
        Export database to pandas DataFrame for ML.
        Returns a flattened view with one row per match.
        """
        with self.get_connection() as conn:
            # Build the query with pivoted player stats
            query = '''
            SELECT
                m.match_id,
                m.game_duration,
                m.game_version,
                m.team_100_win,
                m.team_100_early_surrendered,
                m.team_200_early_surrendered,

                -- Team 100 stats
                t100.first_blood as team_100_first_blood,
                t100.first_tower as team_100_first_tower,
                t100.first_inhibitor as team_100_first_inhibitor,
                t100.first_dragon as team_100_first_dragon,
                t100.first_rift_herald as team_100_first_rift_herald,
                t100.first_baron as team_100_first_baron,
                t100.dragon_kills as team_100_dragon_kills,
                t100.baron_kills as team_100_baron_kills,
                t100.tower_kills as team_100_tower_kills,
                t100.inhibitor_kills as team_100_inhibitor_kills,
                t100.rift_herald_kills as team_100_rift_herald_kills,
                t100.ban_1_champion_id as team_100_ban_1,
                t100.ban_2_champion_id as team_100_ban_2,
                t100.ban_3_champion_id as team_100_ban_3,
                t100.ban_4_champion_id as team_100_ban_4,
                t100.ban_5_champion_id as team_100_ban_5,

                -- Team 200 stats
                t200.first_blood as team_200_first_blood,
                t200.first_tower as team_200_first_tower,
                t200.first_inhibitor as team_200_first_inhibitor,
                t200.first_dragon as team_200_first_dragon,
                t200.first_rift_herald as team_200_first_rift_herald,
                t200.first_baron as team_200_first_baron,
                t200.dragon_kills as team_200_dragon_kills,
                t200.baron_kills as team_200_baron_kills,
                t200.tower_kills as team_200_tower_kills,
                t200.inhibitor_kills as team_200_inhibitor_kills,
                t200.rift_herald_kills as team_200_rift_herald_kills,
                t200.ban_1_champion_id as team_200_ban_1,
                t200.ban_2_champion_id as team_200_ban_2,
                t200.ban_3_champion_id as team_200_ban_3,
                t200.ban_4_champion_id as team_200_ban_4,
                t200.ban_5_champion_id as team_200_ban_5,

                -- Team 100 players
                p100_top.champion_id as team_100_top_champion_id,
                p100_top.champion_name as team_100_top_champion_name,
                p100_top.kills as team_100_top_kills,
                p100_top.deaths as team_100_top_deaths,
                p100_top.assists as team_100_top_assists,
                p100_top.gold_earned as team_100_top_gold,
                p100_top.total_minions_killed as team_100_top_cs,
                p100_top.vision_score as team_100_top_vision,
                p100_top.total_damage_to_champions as team_100_top_damage,
                p100_top.kda as team_100_top_kda,

                p100_jg.champion_id as team_100_jungle_champion_id,
                p100_jg.champion_name as team_100_jungle_champion_name,
                p100_jg.kills as team_100_jungle_kills,
                p100_jg.deaths as team_100_jungle_deaths,
                p100_jg.assists as team_100_jungle_assists,
                p100_jg.gold_earned as team_100_jungle_gold,
                p100_jg.total_minions_killed as team_100_jungle_cs,
                p100_jg.vision_score as team_100_jungle_vision,
                p100_jg.total_damage_to_champions as team_100_jungle_damage,
                p100_jg.kda as team_100_jungle_kda,

                p100_mid.champion_id as team_100_mid_champion_id,
                p100_mid.champion_name as team_100_mid_champion_name,
                p100_mid.kills as team_100_mid_kills,
                p100_mid.deaths as team_100_mid_deaths,
                p100_mid.assists as team_100_mid_assists,
                p100_mid.gold_earned as team_100_mid_gold,
                p100_mid.total_minions_killed as team_100_mid_cs,
                p100_mid.vision_score as team_100_mid_vision,
                p100_mid.total_damage_to_champions as team_100_mid_damage,
                p100_mid.kda as team_100_mid_kda,

                p100_adc.champion_id as team_100_adc_champion_id,
                p100_adc.champion_name as team_100_adc_champion_name,
                p100_adc.kills as team_100_adc_kills,
                p100_adc.deaths as team_100_adc_deaths,
                p100_adc.assists as team_100_adc_assists,
                p100_adc.gold_earned as team_100_adc_gold,
                p100_adc.total_minions_killed as team_100_adc_cs,
                p100_adc.vision_score as team_100_adc_vision,
                p100_adc.total_damage_to_champions as team_100_adc_damage,
                p100_adc.kda as team_100_adc_kda,

                p100_sup.champion_id as team_100_support_champion_id,
                p100_sup.champion_name as team_100_support_champion_name,
                p100_sup.kills as team_100_support_kills,
                p100_sup.deaths as team_100_support_deaths,
                p100_sup.assists as team_100_support_assists,
                p100_sup.gold_earned as team_100_support_gold,
                p100_sup.total_minions_killed as team_100_support_cs,
                p100_sup.vision_score as team_100_support_vision,
                p100_sup.total_damage_to_champions as team_100_support_damage,
                p100_sup.kda as team_100_support_kda,

                -- Team 200 players
                p200_top.champion_id as team_200_top_champion_id,
                p200_top.champion_name as team_200_top_champion_name,
                p200_top.kills as team_200_top_kills,
                p200_top.deaths as team_200_top_deaths,
                p200_top.assists as team_200_top_assists,
                p200_top.gold_earned as team_200_top_gold,
                p200_top.total_minions_killed as team_200_top_cs,
                p200_top.vision_score as team_200_top_vision,
                p200_top.total_damage_to_champions as team_200_top_damage,
                p200_top.kda as team_200_top_kda,

                p200_jg.champion_id as team_200_jungle_champion_id,
                p200_jg.champion_name as team_200_jungle_champion_name,
                p200_jg.kills as team_200_jungle_kills,
                p200_jg.deaths as team_200_jungle_deaths,
                p200_jg.assists as team_200_jungle_assists,
                p200_jg.gold_earned as team_200_jungle_gold,
                p200_jg.total_minions_killed as team_200_jungle_cs,
                p200_jg.vision_score as team_200_jungle_vision,
                p200_jg.total_damage_to_champions as team_200_jungle_damage,
                p200_jg.kda as team_200_jungle_kda,

                p200_mid.champion_id as team_200_mid_champion_id,
                p200_mid.champion_name as team_200_mid_champion_name,
                p200_mid.kills as team_200_mid_kills,
                p200_mid.deaths as team_200_mid_deaths,
                p200_mid.assists as team_200_mid_assists,
                p200_mid.gold_earned as team_200_mid_gold,
                p200_mid.total_minions_killed as team_200_mid_cs,
                p200_mid.vision_score as team_200_mid_vision,
                p200_mid.total_damage_to_champions as team_200_mid_damage,
                p200_mid.kda as team_200_mid_kda,

                p200_adc.champion_id as team_200_adc_champion_id,
                p200_adc.champion_name as team_200_adc_champion_name,
                p200_adc.kills as team_200_adc_kills,
                p200_adc.deaths as team_200_adc_deaths,
                p200_adc.assists as team_200_adc_assists,
                p200_adc.gold_earned as team_200_adc_gold,
                p200_adc.total_minions_killed as team_200_adc_cs,
                p200_adc.vision_score as team_200_adc_vision,
                p200_adc.total_damage_to_champions as team_200_adc_damage,
                p200_adc.kda as team_200_adc_kda,

                p200_sup.champion_id as team_200_support_champion_id,
                p200_sup.champion_name as team_200_support_champion_name,
                p200_sup.kills as team_200_support_kills,
                p200_sup.deaths as team_200_support_deaths,
                p200_sup.assists as team_200_support_assists,
                p200_sup.gold_earned as team_200_support_gold,
                p200_sup.total_minions_killed as team_200_support_cs,
                p200_sup.vision_score as team_200_support_vision,
                p200_sup.total_damage_to_champions as team_200_support_damage,
                p200_sup.kda as team_200_support_kda

            FROM matches m
            LEFT JOIN team_stats t100 ON m.match_id = t100.match_id AND t100.team_id = 100
            LEFT JOIN team_stats t200 ON m.match_id = t200.match_id AND t200.team_id = 200
            LEFT JOIN player_stats p100_top ON m.match_id = p100_top.match_id AND p100_top.team_id = 100 AND p100_top.position = 'top'
            LEFT JOIN player_stats p100_jg ON m.match_id = p100_jg.match_id AND p100_jg.team_id = 100 AND p100_jg.position = 'jungle'
            LEFT JOIN player_stats p100_mid ON m.match_id = p100_mid.match_id AND p100_mid.team_id = 100 AND p100_mid.position = 'mid'
            LEFT JOIN player_stats p100_adc ON m.match_id = p100_adc.match_id AND p100_adc.team_id = 100 AND p100_adc.position = 'adc'
            LEFT JOIN player_stats p100_sup ON m.match_id = p100_sup.match_id AND p100_sup.team_id = 100 AND p100_sup.position = 'support'
            LEFT JOIN player_stats p200_top ON m.match_id = p200_top.match_id AND p200_top.team_id = 200 AND p200_top.position = 'top'
            LEFT JOIN player_stats p200_jg ON m.match_id = p200_jg.match_id AND p200_jg.team_id = 200 AND p200_jg.position = 'jungle'
            LEFT JOIN player_stats p200_mid ON m.match_id = p200_mid.match_id AND p200_mid.team_id = 200 AND p200_mid.position = 'mid'
            LEFT JOIN player_stats p200_adc ON m.match_id = p200_adc.match_id AND p200_adc.team_id = 200 AND p200_adc.position = 'adc'
            LEFT JOIN player_stats p200_sup ON m.match_id = p200_sup.match_id AND p200_sup.team_id = 200 AND p200_sup.position = 'support'
            '''

            return pd.read_sql_query(query, conn)

    def get_match_count(self) -> int:
        """Get total number of matches in database"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM matches')
            return cursor.fetchone()[0]


# Utility function for testing
def test_database():
    """Test database operations"""
    import os

    test_db = 'test_lol_matches.db'

    try:
        db = MatchDatabase(test_db)

        # Test with sample data
        sample_match = {
            "metadata": {"matchId": "TEST_123"},
            "info": {
                "gameCreation": 1234567890,
                "gameDuration": 1800,
                "gameVersion": "14.1",
                "queueId": 420,
                "mapId": 11,
                "gameMode": "CLASSIC",
                "gameType": "MATCHED_GAME",
                "teams": [
                    {
                        "teamId": 100,
                        "win": True,
                        "objectives": {
                            "champion": {"first": True, "kills": 0},
                            "dragon": {"first": True, "kills": 3},
                            "baron": {"first": False, "kills": 0},
                            "tower": {"first": True, "kills": 5},
                            "inhibitor": {"first": True, "kills": 1},
                            "riftHerald": {"first": True, "kills": 2}
                        },
                        "bans": [
                            {"championId": 1, "pickTurn": 1},
                            {"championId": 2, "pickTurn": 2}
                        ]
                    },
                    {
                        "teamId": 200,
                        "win": False,
                        "objectives": {
                            "champion": {"first": False, "kills": 0},
                            "dragon": {"first": False, "kills": 1},
                            "baron": {"first": False, "kills": 0},
                            "tower": {"first": False, "kills": 2},
                            "inhibitor": {"first": False, "kills": 0},
                            "riftHerald": {"first": False, "kills": 0}
                        },
                        "bans": []
                    }
                ],
                "participants": [
                    {
                        "teamId": 100,
                        "teamPosition": "TOP",
                        "championId": 86,
                        "championName": "Garen",
                        "kills": 5,
                        "deaths": 2,
                        "assists": 10,
                        "goldEarned": 12000,
                        "totalMinionsKilled": 200,
                        "visionScore": 30,
                        "challenges": {"kda": 7.5}
                    }
                ]
            }
        }

        # Test insert
        result = db.insert_match(sample_match)
        print(f"Insert result: {result}")

        # Test exists
        exists = db.match_exists("TEST_123")
        print(f"Match exists: {exists}")

        # Test duplicate
        result2 = db.insert_match(sample_match)
        print(f"Duplicate insert result: {result2}")

        # Test player progress
        db.save_player_progress("test_puuid_123")
        is_processed = db.is_player_processed("test_puuid_123")
        print(f"Player processed: {is_processed}")

        # Test stats
        db.increment_stat('total_requests', 5)
        stats = db.get_stats()
        print(f"Stats: {stats}")

        # Test export
        df = db.export_to_dataframe()
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)[:10]}...")

        print("\nAll tests passed!")

    finally:
        if os.path.exists(test_db):
            os.remove(test_db)


if __name__ == "__main__":
    test_database()
