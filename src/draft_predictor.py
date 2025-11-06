import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import joblib
import warnings
warnings.filterwarnings('ignore')

class DraftPredictor:
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.feature_encoders = {}
        
    def prepare_features(self, data):
        """
        Prepare features from the collected match data
        """
        print("Preparing features from match data...")
        
        # Create feature matrix
        features = []
        targets = []
        
        for _, row in data.iterrows():
            try:
                # Skip if missing critical data
                if pd.isna(row.get('team_100_win')):
                    continue
                    
                feature_dict = {}
                
                # Basic match info
                feature_dict['gameDuration'] = row.get('gameDuration', 0)
                
                # Team composition features
                for team in ['team_100', 'team_200']:
                    for position in ['top', 'jungle', 'mid', 'adc', 'support']:
                        # Champion ID
                        champ_id = row.get(f'{team}_{position}_championId', 0)
                        feature_dict[f'{team}_{position}_champion'] = champ_id
                        
                        # Player performance metrics (your new columns)
                        feature_dict[f'{team}_{position}_kills'] = row.get(f'{team}_{position}_kills', 0)
                        feature_dict[f'{team}_{position}_goldEarned'] = row.get(f'{team}_{position}_goldEarned', 0)
                        feature_dict[f'{team}_{position}_totalMinionsKilled'] = row.get(f'{team}_{position}_totalMinionsKilled', 0)
                        feature_dict[f'{team}_{position}_visionScore'] = row.get(f'{team}_{position}_visionScore', 0)
                        feature_dict[f'{team}_{position}_enemyChampionImmobilizations'] = row.get(f'{team}_{position}_enemyChampionImmobilizations', 0)
                        feature_dict[f'{team}_{position}_kda'] = row.get(f'{team}_{position}_kda', 0)
                
                # Team-level features
                for team in ['team_100', 'team_200']:
                    # Early surrender
                    feature_dict[f'{team}_teamEarlySurrendered'] = 1 if row.get(f'{team}_teamEarlySurrendered', False) else 0
                    
                    # Objectives
                    feature_dict[f'{team}_first_blood'] = 1 if row.get(f'{team}_first_blood', False) else 0
                    feature_dict[f'{team}_first_tower'] = 1 if row.get(f'{team}_first_tower', False) else 0
                    feature_dict[f'{team}_first_dragon'] = 1 if row.get(f'{team}_first_dragon', False) else 0
                    feature_dict[f'{team}_dragon_kills'] = row.get(f'{team}_dragon_kills', 0)
                    feature_dict[f'{team}_baron_kills'] = row.get(f'{team}_baron_kills', 0)
                    feature_dict[f'{team}_tower_kills'] = row.get(f'{team}_tower_kills', 0)
                
                # Aggregate team stats
                team_100_gold = sum([row.get(f'team_100_{pos}_goldEarned', 0) for pos in ['top', 'jungle', 'mid', 'adc', 'support']])
                team_200_gold = sum([row.get(f'team_200_{pos}_goldEarned', 0) for pos in ['top', 'jungle', 'mid', 'adc', 'support']])
                feature_dict['gold_difference'] = team_100_gold - team_200_gold
                
                team_100_cs = sum([row.get(f'team_100_{pos}_totalMinionsKilled', 0) for pos in ['top', 'jungle', 'mid', 'adc', 'support']])
                team_200_cs = sum([row.get(f'team_200_{pos}_totalMinionsKilled', 0) for pos in ['top', 'jungle', 'mid', 'adc', 'support']])
                feature_dict['cs_difference'] = team_100_cs - team_200_cs
                
                team_100_vision = sum([row.get(f'team_100_{pos}_visionScore', 0) for pos in ['top', 'jungle', 'mid', 'adc', 'support']])
                team_200_vision = sum([row.get(f'team_200_{pos}_visionScore', 0) for pos in ['top', 'jungle', 'mid', 'adc', 'support']])
                feature_dict['vision_difference'] = team_100_vision - team_200_vision
                
                features.append(feature_dict)
                targets.append(1 if row['team_100_win'] else 0)
                
            except Exception as e:
                print(f"Error processing row: {e}")
                continue
        
        # Convert to DataFrame
        X = pd.DataFrame(features)
        y = np.array(targets)
        
        print(f"Prepared {len(X)} samples with {len(X.columns)} features")
        print(f"Team 100 win rate: {np.mean(y):.1%}")
        
        return X, y
    
    def train(self, X, y):
        """
        Train the machine learning model
        """
        print("\n=== Training AI Model ===")
        
        # Handle missing values
        X = X.fillna(0)
        
        # Check if we have enough data
        if len(X) < 20:
            print(f"Warning: Only {len(X)} samples available. Need more data for reliable training.")
            return False
        
        # Split data
        test_size = min(0.2, max(0.1, len(X) * 0.2 / len(X)))
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Try different models
        models = {
            'RandomForest': RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42),
            'GradientBoosting': GradientBoostingClassifier(n_estimators=100, max_depth=6, random_state=42),
            'LogisticRegression': LogisticRegression(random_state=42, max_iter=1000)
        }
        
        best_score = 0
        best_model = None
        
        print(f"\nTraining on {len(X_train)} samples, testing on {len(X_test)} samples...")
        
        for name, model in models.items():
            # Cross-validation
            cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=min(5, len(X_train)//2))
            
            # Train on full training set
            model.fit(X_train_scaled, y_train)
            
            # Test performance
            test_score = model.score(X_test_scaled, y_test)
            y_pred = model.predict(X_test_scaled)
            
            print(f"\n{name}:")
            print(f"  Cross-validation: {cv_scores.mean():.3f} (+/- {cv_scores.std() * 2:.3f})")
            print(f"  Test accuracy: {test_score:.3f}")
            
            if cv_scores.mean() > best_score:
                best_score = cv_scores.mean()
                best_model = model
                self.model = model
        
        # Final evaluation
        y_pred = self.model.predict(X_test_scaled)
        
        print(f"\n=== Best Model: {type(self.model).__name__} ===")
        print(f"Accuracy: {accuracy_score(y_test, y_pred):.3f}")
        print("\nDetailed Classification Report:")
        print(classification_report(y_test, y_pred, target_names=['Team 200 Win', 'Team 100 Win']))
        
        # Feature importance
        if hasattr(self.model, 'feature_importances_'):
            importance_df = pd.DataFrame({
                'feature': X.columns,
                'importance': self.model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            print("\nTop 15 Most Important Features:")
            print(importance_df.head(15).to_string(index=False))
        
        return True
    
    def predict_match(self, team_100_comp, team_200_comp):
        """
        Predict outcome for new team compositions
        team_comp format: {'top': champion_id, 'jungle': champion_id, ...}
        """
        if self.model is None:
            raise ValueError("Model not trained yet!")
        
        # Create feature vector (simplified version)
        features = {}
        
        # Add champion IDs
        for pos, champ_id in team_100_comp.items():
            features[f'team_100_{pos}_champion'] = champ_id
        
        for pos, champ_id in team_200_comp.items():
            features[f'team_200_{pos}_champion'] = champ_id
        
        # Fill other features with defaults (you could enhance this)
        feature_names = self.scaler.feature_names_in_
        for feature in feature_names:
            if feature not in features:
                features[feature] = 0
        
        # Create DataFrame and predict
        X_pred = pd.DataFrame([features])
        X_pred_scaled = self.scaler.transform(X_pred)
        
        prediction = self.model.predict(X_pred_scaled)[0]
        probability = self.model.predict_proba(X_pred_scaled)[0]
        
        return {
            'winner': 'Team 100 (Blue)' if prediction == 1 else 'Team 200 (Red)',
            'team_100_win_probability': probability[1],
            'team_200_win_probability': probability[0],
            'confidence': max(probability)
        }
    
    def save_model(self, filepath='draft_predictor_model.pkl'):
        """Save the trained model"""
        if self.model is None:
            print("No model to save!")
            return
            
        joblib.dump({
            'model': self.model,
            'scaler': self.scaler,
            'feature_encoders': self.feature_encoders
        }, filepath)
        print(f"Model saved to {filepath}")
    
    def load_model(self, filepath='draft_predictor_model.pkl'):
        """Load a trained model"""
        try:
            saved_data = joblib.load(filepath)
            self.model = saved_data['model']
            self.scaler = saved_data['scaler']
            self.feature_encoders = saved_data.get('feature_encoders', {})
            print(f"Model loaded from {filepath}")
            return True
        except FileNotFoundError:
            print(f"No saved model found at {filepath}")
            return False

def main():
    print("=== League of Legends Draft AI Predictor ===\n")
    
    # Load data
    try:
        print("Loading match data...")
        data = pd.read_csv('draft_data_with_bans.csv')
        print(f"Loaded {len(data)} matches")
    except FileNotFoundError:
        print("Error: draft_data_with_bans.csv not found!")
        print("Run the data collection first: python src/collect_data_safe.py --continuous")
        return
    
    # Initialize predictor
    predictor = DraftPredictor()
    
    # Prepare features
    X, y = predictor.prepare_features(data)
    
    if len(X) == 0:
        print("No valid data found for training!")
        return
    
    # Train model
    success = predictor.train(X, y)
    
    if success:
        # Save model
        predictor.save_model()
        
        print("\n=== AI Training Complete! ===")
        print("The AI has learned from your match data and can now predict draft outcomes.")
        print("\nNext steps:")
        print("1. Collect more data: python src/collect_data_safe.py --continuous")
        print("2. Make predictions: python src/predict_draft.py")
        
        # Show data insights
        print(f"\n=== Training Data Insights ===")
        print(f"Total matches analyzed: {len(y)}")
        print(f"Team 100 (Blue side) wins: {np.sum(y)} ({np.mean(y):.1%})")
        print(f"Team 200 (Red side) wins: {len(y) - np.sum(y)} ({1 - np.mean(y):.1%})")
        
        if len(y) < 100:
            print(f"\n⚠️  Recommendation: Collect more data for better accuracy!")
            print(f"   Current: {len(y)} matches | Recommended: 500+ matches")

if __name__ == "__main__":
    main()