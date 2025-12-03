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

            # Create indices for common queries
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_matches_game_creation ON matches(game_creation)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_stats_champion ON player_stats(champion_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_stats_match ON player_stats(match_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_team_stats_match ON team_stats(match_id)')

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
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match_id, team_id, position,
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
        """Mark player as processed"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO collection_progress (puuid)
                VALUES (?)
            ''', (puuid,))

    def is_player_processed(self, puuid: str) -> bool:
        """Check if player already processed"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
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
