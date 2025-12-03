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

    # Change to project root
    project_root = Path(__file__).parent.parent
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


if __name__ == "__main__":
    main()
