#!/usr/bin/env python3
"""
Backfill des timelines pour les matchs existants.

Récupère les données gold@minute depuis l'API Riot pour les ~177k matchs
qui n'ont pas encore de timeline.

Usage:
    python scripts/backfill_timelines.py [--limit N] [--batch-size N]

    # Lancer en production (tous les matchs):
    python scripts/backfill_timelines.py

    # Test avec 100 matchs:
    python scripts/backfill_timelines.py --limit 100
"""

import sys
import os
import argparse
import time
import logging

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'src', 'collect_data'))

from collect_data_safe import DataCollector

def main():
    parser = argparse.ArgumentParser(description='Backfill timeline data for existing matches')
    parser.add_argument('--db', type=str, default='data/lol_matches.db',
                       help='Path to database (default: data/lol_matches.db)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of matches to process (default: all)')
    parser.add_argument('--batch-size', type=int, default=None,
                       help='Override batch size (default: 2 * num_api_keys)')
    args = parser.parse_args()

    print("=" * 60)
    print("BACKFILL TIMELINES")
    print("=" * 60)
    print(f"Database: {args.db}")
    if args.limit:
        print(f"Limit: {args.limit} matchs")
    print("=" * 60)

    # Create collector with timeline collection enabled
    collector = DataCollector(
        db_path=args.db,
        collect_timelines=True
    )

    # Run backfill
    start = time.time()
    collector.backfill_timelines(limit=args.limit)
    elapsed = time.time() - start

    print(f"\nTerminé en {elapsed/60:.1f} minutes")

if __name__ == '__main__':
    main()
