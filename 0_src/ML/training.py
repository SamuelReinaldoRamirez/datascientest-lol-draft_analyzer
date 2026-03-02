"""
League of Legends Draft AI Predictor

This script trains machine learning models to predict match outcomes
based on draft composition and game statistics.

It can load data from:
1. Prepared Parquet files (recommended, from prepare_data.py)
2. CSV files (legacy support)

Usage:
    python src/draft_predictor.py
    python src/draft_predictor.py --data-dir data/prepared
    python src/draft_predictor.py --csv draft_data_with_bans.csv  # legacy mode
"""

import os
import sys
import json
import argparse
from pathlib import Path

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import joblib
import warnings

# Try to import XGBoost and LightGBM (optional)
try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    print("Warning: XGBoost not installed. Install with: pip install xgboost")

try:
    import lightgbm as lgb
    HAS_LGB = True
except ImportError:
    HAS_LGB = False
    print("Warning: LightGBM not installed. Install with: pip install lightgbm")

warnings.filterwarnings('ignore')


class DraftPredictor:
    """
    ML-based draft outcome predictor.

    This class can:
    - Load pre-prepared data from Parquet (recommended)
    - Load and prepare data from CSV (legacy)
    - Train multiple models and select the best
    - Make predictions on new drafts
    - Save/load trained models
    """

    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = []
        self.metadata = {}

    def load_prepared_data(self, data_dir: str = 'data/prepared') -> tuple:
        """
        Load pre-prepared Parquet data from prepare_data.py.

        Args:
            data_dir: Directory containing prepared data files

        Returns:
            tuple: (X_train, y_train), (X_val, y_val), (X_test, y_test)
        """
        print(f"Loading prepared data from {data_dir}/...")

        # Load metadata
        metadata_path = os.path.join(data_dir, 'metadata.json')
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                self.metadata = json.load(f)
            self.feature_columns = self.metadata.get('feature_columns', [])
            print(f"  Features: {self.metadata.get('n_features', 'unknown')}")
            print(f"  Train samples: {self.metadata.get('n_train', 'unknown')}")

        # Load data splits
        X_train = pd.read_parquet(os.path.join(data_dir, 'X_train.parquet'))
        y_train = pd.read_parquet(os.path.join(data_dir, 'y_train.parquet'))['y_train']

        X_val = pd.read_parquet(os.path.join(data_dir, 'X_val.parquet'))
        y_val = pd.read_parquet(os.path.join(data_dir, 'y_val.parquet'))['y_val']

        X_test = pd.read_parquet(os.path.join(data_dir, 'X_test.parquet'))
        y_test = pd.read_parquet(os.path.join(data_dir, 'y_test.parquet'))['y_test']

        self.feature_columns = list(X_train.columns)

        print(f"  Loaded: Train={len(X_train)}, Val={len(X_val)}, Test={len(X_test)}")
        print(f"  Win rate: {y_train.mean():.1%} (train), {y_test.mean():.1%} (test)")

        return (X_train, y_train), (X_val, y_val), (X_test, y_test)

    def prepare_features_from_csv(self, data: pd.DataFrame) -> tuple:
        """
        Prepare features from CSV data (legacy mode).

        Args:
            data: DataFrame loaded from CSV

        Returns:
            tuple: (X, y)
        """
        print("Preparing features from CSV data...")

        features = []
        targets = []

        for _, row in data.iterrows():
            try:
                if pd.isna(row.get('team_100_win')):
                    continue

                feature_dict = {}

                # Basic match info
                feature_dict['gameDuration'] = row.get('gameDuration', 0)

                # Team composition features
                for team in ['team_100', 'team_200']:
                    for position in ['top', 'jungle', 'mid', 'adc', 'support']:
                        champ_id = row.get(f'{team}_{position}_championId', 0)
                        feature_dict[f'{team}_{position}_champion'] = champ_id

                        feature_dict[f'{team}_{position}_kills'] = row.get(f'{team}_{position}_kills', 0)
                        feature_dict[f'{team}_{position}_goldEarned'] = row.get(f'{team}_{position}_goldEarned', 0)
                        feature_dict[f'{team}_{position}_totalMinionsKilled'] = row.get(f'{team}_{position}_totalMinionsKilled', 0)
                        feature_dict[f'{team}_{position}_visionScore'] = row.get(f'{team}_{position}_visionScore', 0)
                        feature_dict[f'{team}_{position}_kda'] = row.get(f'{team}_{position}_kda', 0)

                # Team-level features
                for team in ['team_100', 'team_200']:
                    feature_dict[f'{team}_teamEarlySurrendered'] = 1 if row.get(f'{team}_teamEarlySurrendered', False) else 0
                    feature_dict[f'{team}_first_blood'] = 1 if row.get(f'{team}_first_blood', False) else 0
                    feature_dict[f'{team}_first_tower'] = 1 if row.get(f'{team}_first_tower', False) else 0
                    feature_dict[f'{team}_first_dragon'] = 1 if row.get(f'{team}_first_dragon', False) else 0
                    feature_dict[f'{team}_dragon_kills'] = row.get(f'{team}_dragon_kills', 0)
                    feature_dict[f'{team}_baron_kills'] = row.get(f'{team}_baron_kills', 0)
                    feature_dict[f'{team}_tower_kills'] = row.get(f'{team}_tower_kills', 0)

                # Aggregate differences
                team_100_gold = sum([row.get(f'team_100_{pos}_goldEarned', 0) or 0 for pos in ['top', 'jungle', 'mid', 'adc', 'support']])
                team_200_gold = sum([row.get(f'team_200_{pos}_goldEarned', 0) or 0 for pos in ['top', 'jungle', 'mid', 'adc', 'support']])
                feature_dict['gold_difference'] = team_100_gold - team_200_gold

                features.append(feature_dict)
                targets.append(1 if row['team_100_win'] else 0)

            except Exception as e:
                continue

        X = pd.DataFrame(features)
        y = np.array(targets)

        self.feature_columns = list(X.columns)

        print(f"Prepared {len(X)} samples with {len(X.columns)} features")
        print(f"Team 100 win rate: {np.mean(y):.1%}")

        return X, y

    def select_features(self, train_data: tuple, val_data: tuple = None,
                       n_features: int = 500, method: str = 'importance') -> tuple:
        """
        Select most important features to reduce dimensionality.

        Args:
            train_data: (X_train, y_train) tuple
            val_data: Optional (X_val, y_val) for validation
            n_features: Number of features to keep (default 500)
            method: Feature selection method ('importance', 'mutual_info', or 'both')

        Returns:
            tuple: (X_train_selected, y_train), (X_val_selected, y_val) if val_data provided
        """
        from sklearn.feature_selection import mutual_info_classif, SelectKBest

        print("\n" + "=" * 60)
        print("Feature Selection")
        print("=" * 60)

        X_train, y_train = train_data
        X_train = X_train.fillna(0)

        original_n_features = X_train.shape[1]
        n_features = min(n_features, original_n_features)

        print(f"Reducing from {original_n_features} to {n_features} features...")

        if method == 'importance':
            # Use tree-based feature importance
            print("  Using tree-based feature importance...")
            temp_model = RandomForestClassifier(
                n_estimators=100, max_depth=10, random_state=42, n_jobs=-1
            )
            temp_model.fit(X_train, y_train)

            # Get feature importance
            importance_df = pd.DataFrame({
                'feature': self.feature_columns,
                'importance': temp_model.feature_importances_
            }).sort_values('importance', ascending=False)

            # Select top N features
            selected_features = importance_df.head(n_features)['feature'].tolist()

        elif method == 'mutual_info':
            # Use mutual information
            print("  Using mutual information...")
            selector = SelectKBest(mutual_info_classif, k=n_features)
            selector.fit(X_train, y_train)

            # Get selected feature indices
            selected_indices = selector.get_support(indices=True)
            selected_features = [self.feature_columns[i] for i in selected_indices]

        elif method == 'both':
            # Combine both methods
            print("  Using combined approach (importance + mutual_info)...")

            # Tree-based importance
            temp_model = RandomForestClassifier(
                n_estimators=100, max_depth=10, random_state=42, n_jobs=-1
            )
            temp_model.fit(X_train, y_train)
            importance_df = pd.DataFrame({
                'feature': self.feature_columns,
                'importance': temp_model.feature_importances_
            }).sort_values('importance', ascending=False)
            top_importance = set(importance_df.head(n_features)['feature'].tolist())

            # Mutual information
            selector = SelectKBest(mutual_info_classif, k=n_features)
            selector.fit(X_train, y_train)
            selected_indices = selector.get_support(indices=True)
            top_mi = set([self.feature_columns[i] for i in selected_indices])

            # Take union and limit to n_features
            combined = list(top_importance | top_mi)
            if len(combined) > n_features:
                # Prioritize features that appear in both
                both = top_importance & top_mi
                only_importance = list(top_importance - top_mi)
                only_mi = list(top_mi - top_importance)
                selected_features = list(both) + only_importance + only_mi
                selected_features = selected_features[:n_features]
            else:
                selected_features = combined

        else:
            raise ValueError(f"Unknown method: {method}")

        # Update feature columns
        self.feature_columns = selected_features
        print(f"  Selected {len(selected_features)} features")

        # Filter datasets
        X_train_selected = X_train[selected_features]

        if val_data:
            X_val, y_val = val_data
            X_val = X_val.fillna(0)
            X_val_selected = X_val[selected_features]
            return (X_train_selected, y_train), (X_val_selected, y_val)
        else:
            return (X_train_selected, y_train), None

    def train(self, train_data: tuple, val_data: tuple = None, test_data: tuple = None) -> bool:
        """
        Train the machine learning model.

        Args:
            train_data: (X_train, y_train) tuple
            val_data: Optional (X_val, y_val) for validation
            test_data: Optional (X_test, y_test) for final evaluation

        Returns:
            bool: True if training successful
        """
        print("\n" + "=" * 60)
        print("Training AI Model")
        print("=" * 60)

        X_train, y_train = train_data

        # Handle missing values
        X_train = X_train.fillna(0)

        if len(X_train) < 20:
            print(f"Warning: Only {len(X_train)} samples. Need more data for reliable training.")
            return False

        # Use validation set if provided, otherwise split from train
        if val_data:
            X_val, y_val = val_data
            X_val = X_val.fillna(0)
        else:
            X_train, X_val, y_train, y_val = train_test_split(
                X_train, y_train, test_size=0.15, random_state=42, stratify=y_train
            )

        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_val_scaled = self.scaler.transform(X_val)

        # Models to try
        models = {
            'RandomForest': RandomForestClassifier(
                n_estimators=200, max_depth=12, min_samples_split=5,
                random_state=42, n_jobs=-1
            ),
            'GradientBoosting': GradientBoostingClassifier(
                n_estimators=150, max_depth=6, learning_rate=0.1,
                random_state=42
            ),
            'LogisticRegression': LogisticRegression(
                random_state=42, max_iter=1000, C=1.0
            )
        }

        # Add XGBoost if available
        if HAS_XGB:
            models['XGBoost'] = xgb.XGBClassifier(
                n_estimators=200,
                max_depth=6,
                learning_rate=0.1,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                n_jobs=-1,
                eval_metric='logloss'
            )

        # Add LightGBM if available
        if HAS_LGB:
            models['LightGBM'] = lgb.LGBMClassifier(
                n_estimators=200,
                max_depth=6,
                learning_rate=0.1,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                n_jobs=-1,
                verbose=-1
            )

        best_val_score = 0
        best_model_name = None

        print(f"\nTraining on {len(X_train)} samples, validating on {len(X_val)} samples...")
        print("-" * 40)

        for name, model in models.items():
            # Train
            model.fit(X_train_scaled, y_train)

            # Validation score
            val_score = model.score(X_val_scaled, y_val)

            # Cross-validation on training data
            cv_folds = min(5, len(X_train) // 10)
            if cv_folds >= 2:
                cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=cv_folds)
                cv_mean = cv_scores.mean()
                cv_std = cv_scores.std()
            else:
                cv_mean = val_score
                cv_std = 0

            print(f"\n{name}:")
            print(f"  Validation accuracy: {val_score:.3f}")
            print(f"  CV accuracy: {cv_mean:.3f} (+/- {cv_std * 2:.3f})")

            if val_score > best_val_score:
                best_val_score = val_score
                best_model_name = name
                self.model = model

        print("\n" + "-" * 40)
        print(f"Best Model: {best_model_name} (validation accuracy: {best_val_score:.3f})")

        # Final evaluation on test set if provided
        if test_data:
            X_test, y_test = test_data
            X_test = X_test.fillna(0)
            X_test_scaled = self.scaler.transform(X_test)

            y_pred = self.model.predict(X_test_scaled)
            test_accuracy = accuracy_score(y_test, y_pred)

            print(f"\nTest Set Performance:")
            print(f"  Accuracy: {test_accuracy:.3f}")
            print("\nClassification Report:")
            print(classification_report(y_test, y_pred, target_names=['Team 200 Win', 'Team 100 Win']))

        # Feature importance
        if hasattr(self.model, 'feature_importances_'):
            importance_df = pd.DataFrame({
                'feature': self.feature_columns,
                'importance': self.model.feature_importances_
            }).sort_values('importance', ascending=False)

            print("\nTop 15 Most Important Features:")
            print(importance_df.head(15).to_string(index=False))

        return True

    def predict_match(self, team_100_comp: dict, team_200_comp: dict) -> dict:
        """
        Predict outcome for new team compositions.

        Args:
            team_100_comp: {'top': champion_id, 'jungle': champion_id, ...}
            team_200_comp: {'top': champion_id, 'jungle': champion_id, ...}

        Returns:
            dict with prediction results
        """
        if self.model is None:
            raise ValueError("Model not trained yet!")

        # Create feature vector
        features = {}

        for pos, champ_id in team_100_comp.items():
            features[f'team_100_{pos}_champion'] = champ_id

        for pos, champ_id in team_200_comp.items():
            features[f'team_200_{pos}_champion'] = champ_id

        # Fill missing features with 0
        for feature in self.feature_columns:
            if feature not in features:
                features[feature] = 0

        # Predict
        X_pred = pd.DataFrame([features])[self.feature_columns]
        X_pred_scaled = self.scaler.transform(X_pred)

        prediction = self.model.predict(X_pred_scaled)[0]
        probability = self.model.predict_proba(X_pred_scaled)[0]

        return {
            'winner': 'Team 100 (Blue)' if prediction == 1 else 'Team 200 (Red)',
            'team_100_win_probability': float(probability[1]),
            'team_200_win_probability': float(probability[0]),
            'confidence': float(max(probability))
        }

    def save_model(self, filepath: str = 'draft_predictor_model.pkl'):
        """Save the trained model"""
        if self.model is None:
            print("No model to save!")
            return

        joblib.dump({
            'model': self.model,
            'scaler': self.scaler,
            'feature_columns': self.feature_columns,
            'metadata': self.metadata
        }, filepath)
        print(f"Model saved to {filepath}")

    def load_model(self, filepath: str = 'draft_predictor_model.pkl') -> bool:
        """Load a trained model"""
        try:
            saved_data = joblib.load(filepath)
            self.model = saved_data['model']
            self.scaler = saved_data['scaler']
            self.feature_columns = saved_data.get('feature_columns', [])
            self.metadata = saved_data.get('metadata', {})
            print(f"Model loaded from {filepath}")
            return True
        except FileNotFoundError:
            print(f"No saved model found at {filepath}")
            return False


def main():
    parser = argparse.ArgumentParser(description='Train LoL Draft AI Predictor')
    parser.add_argument('--data-dir', default='data/processed',
                       help='Directory with prepared Parquet data (default: data/processed)')
    parser.add_argument('--csv', type=str, default=None,
                       help='Path to CSV file (legacy mode)')
    parser.add_argument('--model-output', default='models/draft_predictor_model.pkl',
                       help='Output path for trained model')

    args = parser.parse_args()

    # Change to project root (src/ML/training.py -> src/ML -> src -> project_root)
    project_root = Path(__file__).parent.parent.parent
    os.chdir(project_root)

    print("=" * 60)
    print("League of Legends Draft AI Predictor")
    print("=" * 60)

    predictor = DraftPredictor()

    # Load data
    if args.csv:
        # Legacy CSV mode
        print(f"\nLoading CSV data from {args.csv}...")
        try:
            data = pd.read_csv(args.csv)
            print(f"Loaded {len(data)} matches")
        except FileNotFoundError:
            print(f"Error: {args.csv} not found!")
            print("Run data collection first: python src/collect_data_safe.py --continuous")
            return

        X, y = predictor.prepare_features_from_csv(data)

        if len(X) == 0:
            print("No valid data found for training!")
            return

        # Train with legacy data
        success = predictor.train((X, y))

    else:
        # Parquet mode (recommended)
        if not os.path.exists(args.data_dir):
            print(f"\nError: Data directory '{args.data_dir}' not found!")
            print("\nPlease run data preparation first:")
            print("  1. Collect data: python src/collect_data_safe.py --continuous")
            print("  2. Migrate to SQLite: python src/migrate_to_sqlite.py")
            print("  3. Prepare data: python src/prepare_data.py")
            return

        train_data, val_data, test_data = predictor.load_prepared_data(args.data_dir)
        success = predictor.train(train_data, val_data, test_data)

    if success:
        predictor.save_model(args.model_output)

        print("\n" + "=" * 60)
        print("AI Training Complete!")
        print("=" * 60)
        print(f"\nModel saved to: {args.model_output}")
        print("\nNext steps:")
        print("  - Make predictions: python src/predict_draft.py")
        print("  - Collect more data: python src/collect_data_safe.py --continuous")

        # Training insights
        if predictor.metadata:
            print(f"\n=== Training Data Insights ===")
            print(f"Total matches: {predictor.metadata.get('n_train', 0) + predictor.metadata.get('n_val', 0) + predictor.metadata.get('n_test', 0)}")
            print(f"Features used: {predictor.metadata.get('n_features', 'unknown')}")


def train_early_game_model(df: pd.DataFrame, target_col: str = 'team_100_win',
                           test_size: float = 0.15, show_importance: bool = True) -> tuple:
    """
    Train a model to predict match outcome from @10min stats.

    This is a standalone function for early game prediction experiments.

    Args:
        df: DataFrame with early game features (gold_diff_at_10, etc.)
        target_col: Target column name
        test_size: Proportion of data for test set
        show_importance: Whether to show feature importance

    Returns:
        tuple: (model, scaler, accuracy, feature_importance_df)
    """
    print("\n" + "=" * 60)
    print("Training Early Game Prediction Model (@10min)")
    print("=" * 60)

    # Define early game features
    early_features = [
        # Gold @10min
        'gold_diff_at_10',
        'top_gold_diff_at_10', 'jungle_gold_diff_at_10',
        'mid_gold_diff_at_10', 'adc_gold_diff_at_10', 'support_gold_diff_at_10',
        'gold_advantage_pct_at_10',

        # CS @10min
        'cs_diff_at_10',
        'top_cs_diff_at_10', 'jungle_cs_diff_at_10',
        'mid_cs_diff_at_10', 'adc_cs_diff_at_10', 'support_cs_diff_at_10',

        # First objectives
        'first_blood_team_100', 'first_dragon_team_100', 'first_tower_team_100',

        # Composite
        'early_lead_score',
    ]

    # Filter to features that exist
    available_features = [f for f in early_features if f in df.columns]
    missing_features = [f for f in early_features if f not in df.columns]

    if missing_features:
        print(f"\nNote: {len(missing_features)} features not available: {missing_features[:5]}...")

    print(f"\nUsing {len(available_features)} early game features")

    # Filter to rows with timeline data
    if 'gold_diff_at_10' in df.columns:
        df_filtered = df[df['gold_diff_at_10'].notna()].copy()
        print(f"Matches with timeline @10min: {len(df_filtered)} / {len(df)}")
    else:
        df_filtered = df.copy()

    if len(df_filtered) < 100:
        print("Error: Not enough matches with timeline data!")
        return None, None, 0, None

    # Prepare data
    X = df_filtered[available_features].fillna(0)
    y = df_filtered[target_col].astype(int)

    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=42, stratify=y
    )

    print(f"Train: {len(X_train)}, Test: {len(X_test)}")

    # Scale
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Models to try
    models = {
        'LogisticRegression': LogisticRegression(random_state=42, max_iter=1000),
        'GradientBoosting': GradientBoostingClassifier(
            n_estimators=100, max_depth=4, learning_rate=0.1, random_state=42
        ),
        'RandomForest': RandomForestClassifier(
            n_estimators=100, max_depth=6, random_state=42, n_jobs=-1
        ),
    }

    if HAS_XGB:
        models['XGBoost'] = xgb.XGBClassifier(
            n_estimators=100, max_depth=4, learning_rate=0.1,
            random_state=42, n_jobs=-1, eval_metric='logloss'
        )

    best_accuracy = 0
    best_model = None
    best_name = None

    print("\n" + "-" * 40)
    for name, model in models.items():
        model.fit(X_train_scaled, y_train)
        accuracy = model.score(X_test_scaled, y_test)
        print(f"{name}: {accuracy:.3f}")

        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_model = model
            best_name = name

    print("-" * 40)
    print(f"\nBest: {best_name} with {best_accuracy:.3f} accuracy")

    # Classification report
    y_pred = best_model.predict(X_test_scaled)
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=['Team 200 Win', 'Team 100 Win']))

    # Feature importance
    importance_df = None
    if show_importance and hasattr(best_model, 'feature_importances_'):
        importance_df = pd.DataFrame({
            'feature': available_features,
            'importance': best_model.feature_importances_
        }).sort_values('importance', ascending=False)

        print("\nFeature Importance:")
        print(importance_df.to_string(index=False))

    elif show_importance and hasattr(best_model, 'coef_'):
        importance_df = pd.DataFrame({
            'feature': available_features,
            'importance': np.abs(best_model.coef_[0])
        }).sort_values('importance', ascending=False)

        print("\nFeature Coefficients (absolute):")
        print(importance_df.to_string(index=False))

    return best_model, scaler, best_accuracy, importance_df


def save_early_game_model(model, scaler, feature_columns: list, accuracy: float,
                          filepath: str = 'models/early_game_10min_model.pkl'):
    """
    Save the trained early game model with metadata.

    Args:
        model: Trained sklearn model
        scaler: Fitted StandardScaler
        feature_columns: List of feature column names
        accuracy: Model accuracy on test set
        filepath: Output file path
    """
    from datetime import datetime

    # Ensure models directory exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    model_data = {
        'model': model,
        'scaler': scaler,
        'feature_columns': feature_columns,
        'metadata': {
            'model_type': type(model).__name__,
            'accuracy': accuracy,
            'minute': 10,
            'trained_at': datetime.now().isoformat(),
            'n_features': len(feature_columns),
        }
    }

    joblib.dump(model_data, filepath)
    print(f"\nModel saved to {filepath}")
    return filepath


# =============================================================================
# TIME-BASED PREDICTOR - Flexible model for any game minute
# =============================================================================

class TimeBasedPredictor:
    """
    ML predictor for match outcomes at any game minute.

    This class supports training models for different game times (5, 10, 15, 20 min)
    and includes proper feature engineering, model training, and persistence.

    Usage:
        predictor = TimeBasedPredictor(minute=15)
        predictor.train(df)
        predictor.save_model('models/time_based_15min_model.pkl')

        # Later:
        predictor = TimeBasedPredictor.load_model('models/time_based_15min_model.pkl')
        result = predictor.predict(features)
    """

    def __init__(self, minute: int = 10):
        """
        Initialize predictor for a specific game minute.

        Args:
            minute: Game minute to predict at (default: 10)
        """
        self.minute = minute
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = []
        self.metadata = {}

    def prepare_features(self, df: pd.DataFrame) -> tuple:
        """
        Prepare feature matrix for the configured minute.

        Args:
            df: DataFrame with timeline data

        Returns:
            Tuple of (X, y, feature_names)
        """
        # Import here to avoid circular imports
        from preprocessing import (
            add_variable_minute_features,
            get_features_for_minute,
            prepare_features_for_minute
        )

        return prepare_features_for_minute(df, self.minute)

    def train(self, df: pd.DataFrame, test_size: float = 0.15) -> dict:
        """
        Train the model on the provided data.

        Args:
            df: DataFrame with timeline data (from export_with_timeline_at_minutes)
            test_size: Proportion of data for test set

        Returns:
            Dict with training results including accuracy
        """
        from datetime import datetime

        print(f"\n{'=' * 60}")
        print(f"Training Time-Based Predictor (@{self.minute}min)")
        print('=' * 60)

        # Prepare features
        X, y, feature_names = self.prepare_features(df)

        if X is None or len(X) < 100:
            print(f"Error: Not enough data for minute {self.minute}")
            return {'success': False, 'error': 'Insufficient data'}

        self.feature_columns = feature_names

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42, stratify=y
        )

        print(f"Train: {len(X_train)}, Test: {len(X_test)}")
        print(f"Features: {len(feature_names)}")

        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        # Models to try
        models = {
            'LogisticRegression': LogisticRegression(random_state=42, max_iter=1000),
            'GradientBoosting': GradientBoostingClassifier(
                n_estimators=100, max_depth=4, learning_rate=0.1, random_state=42
            ),
            'RandomForest': RandomForestClassifier(
                n_estimators=100, max_depth=6, random_state=42, n_jobs=-1
            ),
        }

        if HAS_XGB:
            models['XGBoost'] = xgb.XGBClassifier(
                n_estimators=100, max_depth=4, learning_rate=0.1,
                random_state=42, n_jobs=-1, eval_metric='logloss'
            )

        best_accuracy = 0
        best_model = None
        best_name = None

        print("\n" + "-" * 40)
        for name, model in models.items():
            model.fit(X_train_scaled, y_train)
            accuracy = model.score(X_test_scaled, y_test)
            print(f"{name}: {accuracy:.3f}")

            if accuracy > best_accuracy:
                best_accuracy = accuracy
                best_model = model
                best_name = name

        print("-" * 40)
        print(f"\nBest: {best_name} with {best_accuracy:.3f} accuracy")

        self.model = best_model
        self.metadata = {
            'model_type': best_name,
            'accuracy': best_accuracy,
            'minute': self.minute,
            'trained_at': datetime.now().isoformat(),
            'n_features': len(feature_names),
            'n_train_samples': len(X_train),
            'n_test_samples': len(X_test),
        }

        # Feature importance
        importance_df = None
        if hasattr(best_model, 'feature_importances_'):
            importance_df = pd.DataFrame({
                'feature': feature_names,
                'importance': best_model.feature_importances_
            }).sort_values('importance', ascending=False)

            print("\nFeature Importance:")
            print(importance_df.head(10).to_string(index=False))

        return {
            'success': True,
            'accuracy': best_accuracy,
            'model_type': best_name,
            'feature_importance': importance_df,
        }

    def predict(self, features: dict) -> dict:
        """
        Predict match outcome from features.

        Args:
            features: Dict with feature values (gold_diff_at_X, first_blood, etc.)

        Returns:
            Dict with prediction results
        """
        if self.model is None:
            raise ValueError("Model not trained yet!")

        # Create feature vector
        X = pd.DataFrame([features])[self.feature_columns].fillna(0)
        X_scaled = self.scaler.transform(X)

        prediction = self.model.predict(X_scaled)[0]
        probability = self.model.predict_proba(X_scaled)[0]

        return {
            'winner': 'Blue Team' if prediction == 1 else 'Red Team',
            'blue_win_probability': float(probability[1]),
            'red_win_probability': float(probability[0]),
            'confidence': float(max(probability)),
            'minute': self.minute,
        }

    def predict_from_raw(self, gold_diff: float, first_blood: bool = False,
                         first_tower: bool = False, first_dragon: bool = False,
                         first_herald: bool = False, **position_diffs) -> dict:
        """
        Predict from raw game state values.

        Args:
            gold_diff: Total gold difference (positive = blue advantage)
            first_blood: Blue team got first blood
            first_tower: Blue team got first tower
            first_dragon: Blue team got first dragon
            first_herald: Blue team got first herald
            **position_diffs: Optional position-specific gold diffs

        Returns:
            Dict with prediction results
        """
        # Build feature dict
        features = {
            f'gold_diff_at_{self.minute}': gold_diff,
            'first_blood_team_100': 1 if first_blood else 0,
            'first_tower_team_100': 1 if first_tower else 0,
            'first_dragon_team_100': 1 if first_dragon else 0,
            'first_herald_team_100': 1 if first_herald else 0,
        }

        # Add position diffs if provided
        for pos in ['top', 'jungle', 'mid', 'adc', 'support']:
            key = f'{pos}_gold_diff'
            if key in position_diffs:
                features[f'{pos}_gold_diff_at_{self.minute}'] = position_diffs[key]
            else:
                features[f'{pos}_gold_diff_at_{self.minute}'] = 0

        # Calculate derived features
        # Gold advantage percentage (estimate based on typical total gold)
        typical_gold_per_team = 20000 + (self.minute - 10) * 1500  # Rough estimate
        total_gold = 2 * typical_gold_per_team
        features[f'gold_advantage_pct_at_{self.minute}'] = gold_diff / total_gold if total_gold > 0 else 0

        # Early lead score
        gold_norm = gold_diff / 5000.0
        fb = 1 if first_blood else 0
        ft = 1 if first_tower else 0
        fd = 1 if first_dragon else 0
        fh = 1 if first_herald else 0
        features[f'early_lead_score_at_{self.minute}'] = (
            gold_norm * 0.6 +
            (fb * 2 - 1) * 0.15 +
            (ft * 2 - 1) * 0.1 +
            (fd * 2 - 1) * 0.1 +
            (fh * 2 - 1) * 0.05
        )

        return self.predict(features)

    def save_model(self, filepath: str = None):
        """
        Save the trained model to disk.

        Args:
            filepath: Output file path (default: models/time_based_{minute}min_model.pkl)
        """
        if self.model is None:
            raise ValueError("No model to save!")

        if filepath is None:
            filepath = f'models/time_based_{self.minute}min_model.pkl'

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'feature_columns': self.feature_columns,
            'metadata': self.metadata,
        }

        joblib.dump(model_data, filepath)
        print(f"Model saved to {filepath}")
        return filepath

    @classmethod
    def load_model(cls, filepath: str) -> 'TimeBasedPredictor':
        """
        Load a trained model from disk.

        Args:
            filepath: Path to saved model

        Returns:
            TimeBasedPredictor instance with loaded model
        """
        model_data = joblib.load(filepath)

        minute = model_data.get('metadata', {}).get('minute', 10)
        predictor = cls(minute=minute)
        predictor.model = model_data['model']
        predictor.scaler = model_data['scaler']
        predictor.feature_columns = model_data.get('feature_columns', [])
        predictor.metadata = model_data.get('metadata', {})

        print(f"Loaded model for @{minute}min (accuracy: {predictor.metadata.get('accuracy', 'N/A'):.3f})")
        return predictor


if __name__ == "__main__":
    main()
