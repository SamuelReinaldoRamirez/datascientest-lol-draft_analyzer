"""
Model Inference Module for LoL Match Prediction

This module provides a unified interface for loading and using all trained models:
- Draft predictor model (full draft composition)
- Early game @10min model (with CS data)
- Time-based models (@5, 10, 15, 20 min gold-only)

Usage:
    from inference import predict_at_minute, get_available_models, load_model

    # List available models
    models = get_available_models()

    # Predict at a specific minute
    result = predict_at_minute(
        minute=15,
        gold_diff=2500,
        first_blood=True,
        first_dragon=True
    )

    # Load and use a model directly
    model_data = load_model('time_based', minute=10)
    prediction = model_data['model'].predict(features)
"""

import os
from pathlib import Path
from typing import Optional, Dict, List, Any
import joblib


# Default models directory
MODELS_DIR = Path(__file__).parent.parent.parent / 'models'


def get_available_models() -> Dict[str, List[Dict]]:
    """
    Get list of all available trained models.

    Returns:
        Dict with model categories and their available models:
        {
            'draft': [{'path': '...', 'metadata': {...}}],
            'early_game_10min': [{'path': '...', 'metadata': {...}}],
            'time_based': [
                {'minute': 5, 'path': '...', 'metadata': {...}},
                {'minute': 10, 'path': '...', 'metadata': {...}},
                ...
            ]
        }
    """
    models = {
        'draft': [],
        'early_game_10min': [],
        'time_based': [],
    }

    if not MODELS_DIR.exists():
        return models

    # Check for draft model
    draft_path = MODELS_DIR / 'draft_predictor_model.pkl'
    if draft_path.exists():
        try:
            data = joblib.load(draft_path)
            models['draft'].append({
                'path': str(draft_path),
                'metadata': data.get('metadata', {}),
            })
        except Exception:
            pass

    # Check for early game @10min model
    early_game_path = MODELS_DIR / 'early_game_10min_model.pkl'
    if early_game_path.exists():
        try:
            data = joblib.load(early_game_path)
            models['early_game_10min'].append({
                'path': str(early_game_path),
                'metadata': data.get('metadata', {}),
            })
        except Exception:
            pass

    # Check for time-based models
    for minute in [5, 10, 15, 20, 25]:
        time_path = MODELS_DIR / f'time_based_{minute}min_model.pkl'
        if time_path.exists():
            try:
                data = joblib.load(time_path)
                models['time_based'].append({
                    'minute': minute,
                    'path': str(time_path),
                    'metadata': data.get('metadata', {}),
                })
            except Exception:
                pass

    return models


def load_model(model_type: str, minute: Optional[int] = None) -> Dict[str, Any]:
    """
    Load a trained model by type.

    Args:
        model_type: One of 'draft', 'early_game_10min', 'time_based'
        minute: Required for 'time_based' models

    Returns:
        Dict with 'model', 'scaler', 'feature_columns', 'metadata'

    Raises:
        FileNotFoundError: If model file doesn't exist
        ValueError: If invalid model_type or missing minute
    """
    if model_type == 'draft':
        filepath = MODELS_DIR / 'draft_predictor_model.pkl'
    elif model_type == 'early_game_10min':
        filepath = MODELS_DIR / 'early_game_10min_model.pkl'
    elif model_type == 'time_based':
        if minute is None:
            raise ValueError("minute is required for time_based models")
        filepath = MODELS_DIR / f'time_based_{minute}min_model.pkl'
    else:
        raise ValueError(f"Unknown model_type: {model_type}. Use 'draft', 'early_game_10min', or 'time_based'")

    if not filepath.exists():
        raise FileNotFoundError(f"Model not found: {filepath}")

    return joblib.load(filepath)


def predict_at_minute(minute: int, gold_diff: float,
                      first_blood: bool = False,
                      first_tower: bool = False,
                      first_dragon: bool = False,
                      first_herald: bool = False,
                      top_gold_diff: float = 0,
                      jungle_gold_diff: float = 0,
                      mid_gold_diff: float = 0,
                      adc_gold_diff: float = 0,
                      support_gold_diff: float = 0,
                      cs_diff_at_10: Optional[float] = None,
                      use_early_game_model: bool = True) -> Dict[str, Any]:
    """
    Predict match outcome at a specific game minute.

    This is the main inference function that automatically selects the best
    available model for the given minute.

    Args:
        minute: Game minute (5, 10, 15, or 20)
        gold_diff: Total gold difference (positive = blue team advantage)
        first_blood: Blue team got first blood
        first_tower: Blue team got first tower
        first_dragon: Blue team got first dragon
        first_herald: Blue team got first herald
        top_gold_diff: Top lane gold difference
        jungle_gold_diff: Jungle gold difference
        mid_gold_diff: Mid lane gold difference
        adc_gold_diff: ADC gold difference
        support_gold_diff: Support gold difference
        cs_diff_at_10: CS difference at 10 min (only used if minute=10)
        use_early_game_model: If True and minute=10, try to use early_game model first

    Returns:
        Dict with prediction results:
        {
            'winner': 'Blue Team' or 'Red Team',
            'blue_win_probability': float,
            'red_win_probability': float,
            'confidence': float,
            'minute': int,
            'model_used': str,
        }
    """
    import numpy as np
    import pandas as pd

    # Try to find the best model
    model_data = None
    model_used = None

    # For minute 10, optionally try early_game model first (has CS data)
    if minute == 10 and use_early_game_model:
        try:
            model_data = load_model('early_game_10min')
            model_used = 'early_game_10min'
        except FileNotFoundError:
            pass

    # Try time-based model
    if model_data is None:
        try:
            model_data = load_model('time_based', minute=minute)
            model_used = f'time_based_{minute}min'
        except FileNotFoundError:
            pass

    # If no model found, return a default prediction
    if model_data is None:
        available = get_available_models()
        return {
            'winner': 'Unknown',
            'blue_win_probability': 0.5,
            'red_win_probability': 0.5,
            'confidence': 0.0,
            'minute': minute,
            'model_used': None,
            'error': f'No model available for minute {minute}',
            'available_models': available,
        }

    model = model_data['model']
    scaler = model_data['scaler']
    feature_columns = model_data.get('feature_columns', [])

    # Build feature dict
    features = {
        f'gold_diff_at_{minute}': gold_diff,
        'first_blood_team_100': 1 if first_blood else 0,
        'first_tower_team_100': 1 if first_tower else 0,
        'first_dragon_team_100': 1 if first_dragon else 0,
        'first_herald_team_100': 1 if first_herald else 0,
        f'top_gold_diff_at_{minute}': top_gold_diff,
        f'jungle_gold_diff_at_{minute}': jungle_gold_diff,
        f'mid_gold_diff_at_{minute}': mid_gold_diff,
        f'adc_gold_diff_at_{minute}': adc_gold_diff,
        f'support_gold_diff_at_{minute}': support_gold_diff,
    }

    # Add CS diff for minute 10
    if minute == 10 and cs_diff_at_10 is not None:
        features['cs_diff_at_10'] = cs_diff_at_10

    # Calculate derived features
    # Gold advantage percentage
    typical_gold_per_team = 20000 + (minute - 10) * 1500
    total_gold = 2 * typical_gold_per_team
    features[f'gold_advantage_pct_at_{minute}'] = gold_diff / total_gold if total_gold > 0 else 0

    # Early lead score
    gold_norm = gold_diff / 5000.0
    fb = 1 if first_blood else 0
    ft = 1 if first_tower else 0
    fd = 1 if first_dragon else 0
    fh = 1 if first_herald else 0
    features[f'early_lead_score_at_{minute}'] = (
        gold_norm * 0.6 +
        (fb * 2 - 1) * 0.15 +
        (ft * 2 - 1) * 0.1 +
        (fd * 2 - 1) * 0.1 +
        (fh * 2 - 1) * 0.05
    )

    # Handle legacy feature names (for early_game model)
    if model_used == 'early_game_10min':
        # Map new names to old names if needed
        if 'gold_diff_at_10' in feature_columns:
            pass  # Already correct
        elif 'gold_diff' in feature_columns:
            features['gold_diff'] = gold_diff

    # Create feature vector with only available columns
    available_features = {k: v for k, v in features.items() if k in feature_columns}

    # Fill missing features with 0
    for col in feature_columns:
        if col not in available_features:
            available_features[col] = 0

    # Build DataFrame in correct column order
    X = pd.DataFrame([available_features])[feature_columns]
    X_scaled = scaler.transform(X)

    # Predict
    prediction = model.predict(X_scaled)[0]
    probability = model.predict_proba(X_scaled)[0]

    return {
        'winner': 'Blue Team' if prediction == 1 else 'Red Team',
        'blue_win_probability': float(probability[1]),
        'red_win_probability': float(probability[0]),
        'confidence': float(max(probability)),
        'minute': minute,
        'model_used': model_used,
    }


def get_model_info(model_type: str, minute: Optional[int] = None) -> Dict[str, Any]:
    """
    Get detailed information about a trained model.

    Args:
        model_type: One of 'draft', 'early_game_10min', 'time_based'
        minute: Required for 'time_based' models

    Returns:
        Dict with model information including accuracy, features, etc.
    """
    try:
        model_data = load_model(model_type, minute)
    except (FileNotFoundError, ValueError) as e:
        return {'error': str(e)}

    metadata = model_data.get('metadata', {})
    feature_columns = model_data.get('feature_columns', [])

    return {
        'model_type': model_type,
        'minute': minute,
        'accuracy': metadata.get('accuracy'),
        'trained_at': metadata.get('trained_at'),
        'n_features': len(feature_columns),
        'feature_columns': feature_columns,
        'sklearn_model_type': metadata.get('model_type'),
        'n_train_samples': metadata.get('n_train_samples'),
        'n_test_samples': metadata.get('n_test_samples'),
    }


# CLI interface for testing
def main():
    """Command-line interface for testing inference."""
    import argparse

    parser = argparse.ArgumentParser(description='LoL Match Prediction Inference')
    parser.add_argument('--list', action='store_true', help='List available models')
    parser.add_argument('--minute', type=int, default=10, help='Game minute for prediction')
    parser.add_argument('--gold-diff', type=float, default=0, help='Gold difference')
    parser.add_argument('--first-blood', action='store_true', help='Blue team got first blood')
    parser.add_argument('--first-dragon', action='store_true', help='Blue team got first dragon')
    parser.add_argument('--first-tower', action='store_true', help='Blue team got first tower')
    parser.add_argument('--first-herald', action='store_true', help='Blue team got first herald')

    args = parser.parse_args()

    if args.list:
        print("\n=== Available Models ===\n")
        models = get_available_models()
        for category, model_list in models.items():
            if model_list:
                print(f"{category}:")
                for m in model_list:
                    acc = m.get('metadata', {}).get('accuracy', 'N/A')
                    if 'minute' in m:
                        print(f"  - @{m['minute']}min: accuracy={acc:.3f}" if isinstance(acc, float) else f"  - @{m['minute']}min")
                    else:
                        print(f"  - accuracy={acc:.3f}" if isinstance(acc, float) else f"  - {m['path']}")
        return

    # Make prediction
    print(f"\n=== Prediction @{args.minute}min ===\n")
    print(f"Gold diff: {args.gold_diff}")
    print(f"First blood: {args.first_blood}")
    print(f"First dragon: {args.first_dragon}")
    print(f"First tower: {args.first_tower}")
    print(f"First herald: {args.first_herald}")

    result = predict_at_minute(
        minute=args.minute,
        gold_diff=args.gold_diff,
        first_blood=args.first_blood,
        first_dragon=args.first_dragon,
        first_tower=args.first_tower,
        first_herald=args.first_herald,
    )

    print(f"\n=== Result ===")
    print(f"Predicted winner: {result['winner']}")
    print(f"Blue team win probability: {result['blue_win_probability']:.1%}")
    print(f"Red team win probability: {result['red_win_probability']:.1%}")
    print(f"Confidence: {result['confidence']:.1%}")
    print(f"Model used: {result.get('model_used', 'N/A')}")

    if 'error' in result:
        print(f"\nError: {result['error']}")


if __name__ == '__main__':
    main()
