"""
Migration Script: Transfer existing match data to SQLite database

This script migrates data from:
- match_details_extended.txt (raw JSON matches)
- collection_progress.json (player progress tracking)

To the new SQLite database (lol_matches.db)
"""

import json
import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from database import MatchDatabase
from extract_detailed_match_data import read_match_details_from_txt


def migrate_matches(db: MatchDatabase, txt_file: str) -> tuple:
    """
    Migrate matches from txt file to SQLite database.

    Returns:
        tuple: (migrated_count, skipped_count, error_count)
    """
    if not os.path.exists(txt_file):
        print(f"File not found: {txt_file}")
        return 0, 0, 0

    print(f"Reading matches from {txt_file}...")
    matches = read_match_details_from_txt(txt_file)
    print(f"Found {len(matches)} matches to migrate")

    migrated = 0
    skipped = 0
    errors = 0

    for i, match in enumerate(matches):
        try:
            match_id = match.get("metadata", {}).get("matchId", "Unknown")

            if db.insert_match(match):
                migrated += 1
                if migrated % 100 == 0:
                    print(f"  Migrated {migrated} matches...")
            else:
                skipped += 1  # Already exists

        except Exception as e:
            errors += 1
            print(f"  Error migrating match {i}: {e}")

    return migrated, skipped, errors


def migrate_progress(db: MatchDatabase, progress_file: str) -> tuple:
    """
    Migrate player progress from JSON file to SQLite database.

    Returns:
        tuple: (migrated_count, error_count)
    """
    if not os.path.exists(progress_file):
        print(f"Progress file not found: {progress_file}")
        return 0, 0

    print(f"Reading progress from {progress_file}...")

    try:
        with open(progress_file, 'r') as f:
            progress = json.load(f)
    except Exception as e:
        print(f"Error reading progress file: {e}")
        return 0, 0

    # Migrate processed players
    players = progress.get('processed_players', [])
    print(f"Found {len(players)} processed players to migrate")

    migrated = 0
    errors = 0

    for puuid in players:
        try:
            db.save_player_progress(puuid)
            migrated += 1
        except Exception as e:
            errors += 1
            print(f"  Error migrating player {puuid[:20]}...: {e}")

    # Migrate statistics
    stats = progress.get('stats', {})
    for key, value in stats.items():
        db.update_stat(key, value)

    # Migrate last page and index
    if 'last_page' in progress:
        db.update_stat('last_page', progress['last_page'])
    if 'last_player_index' in progress:
        db.update_stat('last_player_index', progress['last_player_index'])

    return migrated, errors


def main():
    """Main migration function"""
    import argparse

    parser = argparse.ArgumentParser(description='Migrate existing data to SQLite')
    parser.add_argument('--db', default='data/lol_matches.db',
                       help='SQLite database path (default: data/lol_matches.db)')
    parser.add_argument('--matches', default='data/raw/match_details_extended.txt',
                       help='Match details file (default: data/raw/match_details_extended.txt)')
    parser.add_argument('--progress', default='data/raw/collection_progress.json',
                       help='Progress file (default: data/raw/collection_progress.json)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be migrated without actually migrating')

    args = parser.parse_args()

    # Change to project root directory
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)

    print("=" * 60)
    print("LoL Match Data Migration to SQLite")
    print("=" * 60)
    print(f"\nProject root: {project_root}")
    print(f"Database: {args.db}")
    print(f"Matches file: {args.matches}")
    print(f"Progress file: {args.progress}")
    print()

    if args.dry_run:
        print("[DRY RUN MODE - No changes will be made]\n")

        # Check files
        if os.path.exists(args.matches):
            matches = read_match_details_from_txt(args.matches)
            print(f"Matches to migrate: {len(matches)}")
        else:
            print(f"Matches file not found: {args.matches}")

        if os.path.exists(args.progress):
            with open(args.progress) as f:
                progress = json.load(f)
            print(f"Players to migrate: {len(progress.get('processed_players', []))}")
            print(f"Stats to migrate: {list(progress.get('stats', {}).keys())}")
        else:
            print(f"Progress file not found: {args.progress}")

        return

    # Initialize database
    print("Initializing SQLite database...")
    db = MatchDatabase(args.db)

    # Check if database already has data
    existing_matches = db.get_match_count()
    if existing_matches > 0:
        print(f"\nWarning: Database already contains {existing_matches} matches.")
        response = input("Continue migration? Duplicates will be skipped. [y/N]: ")
        if response.lower() != 'y':
            print("Migration cancelled.")
            return

    # Migrate matches
    print("\n--- Migrating Matches ---")
    match_migrated, match_skipped, match_errors = migrate_matches(db, args.matches)

    # Migrate progress
    print("\n--- Migrating Progress ---")
    player_migrated, player_errors = migrate_progress(db, args.progress)

    # Summary
    print("\n" + "=" * 60)
    print("Migration Summary")
    print("=" * 60)
    print(f"\nMatches:")
    print(f"  - Migrated: {match_migrated}")
    print(f"  - Skipped (already exist): {match_skipped}")
    print(f"  - Errors: {match_errors}")

    print(f"\nPlayers:")
    print(f"  - Migrated: {player_migrated}")
    print(f"  - Errors: {player_errors}")

    # Verify
    print("\n--- Verification ---")
    stats = db.get_stats()
    print(f"Total matches in database: {stats['total_matches']}")
    print(f"Total players processed: {stats['processed_players']}")

    # Export test
    print("\n--- Testing Export ---")
    df = db.export_to_dataframe()
    print(f"DataFrame export: {df.shape[0]} rows x {df.shape[1]} columns")

    print("\n" + "=" * 60)
    print("Migration complete!")
    print("=" * 60)
    print(f"\nYou can now use the SQLite database: {args.db}")
    print("The old files (txt, json) can be kept as backup or deleted.")


if __name__ == "__main__":
    main()
