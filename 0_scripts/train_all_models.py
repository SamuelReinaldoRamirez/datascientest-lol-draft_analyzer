#!/usr/bin/env python3
"""
Train All ML Models for LoL Match Prediction

This script trains all time-based prediction models at once:
- Early game @10min model (with CS data)
- Time-based models for minutes 5, 10, 15, 20

Usage:
    python scripts/train_all_models.py
    python scripts/train_all_models.py --minutes 5 10 15 20
    python scripts/train_all_models.py --db data/lol_matches.db --output models/

Output:
    models/
    ├── early_game_10min_model.pkl
    ├── time_based_5min_model.pkl
    ├── time_based_10min_model.pkl
    ├── time_based_15min_model.pkl
    └── time_based_20min_model.pkl
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add src directories to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src' / 'collect_data'))
sys.path.insert(0, str(project_root / 'src' / 'ML'))


def train_all_models(db_path: str, minutes: list, output_dir: str, test_size: float = 0.15):
    """
    Train all time-based models.

    Args:
        db_path: Path to SQLite database
        minutes: List of minutes to train models for
        output_dir: Output directory for models
        test_size: Test set proportion
    """
    from database import MatchDatabase
    from training import TimeBasedPredictor, train_early_game_model, save_early_game_model
    from preprocessing import add_variable_minute_features

    print("=" * 70)
    print("LoL ML Model Training Suite")
    print("=" * 70)
    print(f"\nDatabase: {db_path}")
    print(f"Output: {output_dir}")
    print(f"Minutes: {minutes}")
    print(f"Test size: {test_size}")

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Load database
    print(f"\nConnecting to database...")
    db = MatchDatabase(db_path)

    # Get match count
    match_count = db.get_match_count()
    print(f"Total matches in database: {match_count:,}")

    # Export data with timeline for all requested minutes
    print(f"\nExporting timeline data for minutes {minutes}...")
    df = db.export_with_timeline_at_minutes(minutes)
    print(f"Exported {len(df):,} matches with timeline data")

    # Check data availability per minute
    print("\nData availability per minute:")
    for minute in minutes:
        col = f'gold_diff_at_{minute}'
        if col in df.columns:
            available = df[col].notna().sum()
            pct = available / len(df) * 100
            print(f"  @{minute}min: {available:,} matches ({pct:.1f}%)")
        else:
            print(f"  @{minute}min: No data")

    # Results tracking
    results = {}

    # Train early game model @10min (if 10 is in minutes)
    if 10 in minutes:
        print("\n" + "=" * 70)
        print("Training Early Game Model (@10min with CS)")
        print("=" * 70)

        # Add features for minute 10
        df_10 = add_variable_minute_features(df.copy(), 10)

        # Define early game features (includes CS)
        early_features = [
            'gold_diff_at_10',
            'gold_advantage_pct_at_10',
            'top_gold_diff_at_10', 'jungle_gold_diff_at_10',
            'mid_gold_diff_at_10', 'adc_gold_diff_at_10', 'support_gold_diff_at_10',
            'cs_diff_at_10',
            'top_cs_diff_at_10', 'jungle_cs_diff_at_10',
            'mid_cs_diff_at_10', 'adc_cs_diff_at_10', 'support_cs_diff_at_10',
            'first_blood_team_100', 'first_dragon_team_100',
            'first_tower_team_100', 'first_herald_team_100',
            'early_lead_score_at_10',
        ]

        # Filter to available features
        available_early = [f for f in early_features if f in df_10.columns]
        print(f"Using {len(available_early)} early game features")

        # Train using the early game function
        model, scaler, accuracy, importance_df = train_early_game_model(
            df_10, target_col='team_100_win', test_size=test_size, show_importance=True
        )

        if model is not None:
            filepath = os.path.join(output_dir, 'early_game_10min_model.pkl')
            save_early_game_model(model, scaler, available_early, accuracy, filepath)
            results['early_game_10min'] = {
                'accuracy': accuracy,
                'path': filepath,
            }
        else:
            print("Failed to train early game model")
            results['early_game_10min'] = {'error': 'Training failed'}

    # Train time-based models for each minute
    for minute in minutes:
        print("\n" + "=" * 70)
        print(f"Training Time-Based Model (@{minute}min)")
        print("=" * 70)

        predictor = TimeBasedPredictor(minute=minute)
        result = predictor.train(df, test_size=test_size)

        if result.get('success'):
            filepath = os.path.join(output_dir, f'time_based_{minute}min_model.pkl')
            predictor.save_model(filepath)
            results[f'time_based_{minute}min'] = {
                'accuracy': result['accuracy'],
                'model_type': result['model_type'],
                'path': filepath,
            }
        else:
            print(f"Failed to train model for minute {minute}")
            results[f'time_based_{minute}min'] = {'error': result.get('error', 'Unknown error')}

    # Summary
    print("\n" + "=" * 70)
    print("Training Summary")
    print("=" * 70)

    print("\nResults:")
    for model_name, result in results.items():
        if 'accuracy' in result:
            print(f"  {model_name}: {result['accuracy']:.3f} accuracy")
        else:
            print(f"  {model_name}: {result.get('error', 'Failed')}")

    print(f"\nModels saved to: {output_dir}/")
    for model_name, result in results.items():
        if 'path' in result:
            print(f"  - {os.path.basename(result['path'])}")

    # Expected accuracy reference
    print("\nExpected Accuracy Reference:")
    print("  @5min:  55-60%")
    print("  @10min: 70-75%")
    print("  @15min: 75-80%")
    print("  @20min: 78-82%")

    return results


def main():
    parser = argparse.ArgumentParser(
        description='Train all LoL ML prediction models',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--db', default='data/lol_matches.db',
                        help='Path to SQLite database (default: data/lol_matches.db)')
    parser.add_argument('--minutes', nargs='+', type=int, default=[5, 10, 15, 20],
                        help='Minutes to train models for (default: 5 10 15 20)')
    parser.add_argument('--output', default='models',
                        help='Output directory for models (default: models)')
    parser.add_argument('--test-size', type=float, default=0.15,
                        help='Test set proportion (default: 0.15)')

    args = parser.parse_args()

    # Change to project root
    os.chdir(project_root)

    # Run training
    results = train_all_models(
        db_path=args.db,
        minutes=args.minutes,
        output_dir=args.output,
        test_size=args.test_size
    )

    # Return success if at least one model trained
    success_count = sum(1 for r in results.values() if 'accuracy' in r)
    return 0 if success_count > 0 else 1


if __name__ == '__main__':
    sys.exit(main())
