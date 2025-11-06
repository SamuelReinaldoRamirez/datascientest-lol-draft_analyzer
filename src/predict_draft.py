import pandas as pd
from draft_predictor import DraftPredictor

# Common champion mappings (you can expand this)
CHAMPIONS = {
    # Top laners
    'garen': 86, 'darius': 122, 'fiora': 114, 'jax': 24, 'irelia': 39,
    'shen': 98, 'malphite': 54, 'ornn': 516, 'aatrox': 266, 'riven': 92,
    
    # Junglers  
    'graves': 104, 'kindred': 203, 'xinzhao': 5, 'leesin': 64, 'hecarim': 120,
    'ekko': 245, 'karthus': 30, 'nocturne': 56, 'vi': 254, 'jarvan': 59,
    
    # Mid laners
    'yasuo': 157, 'zed': 238, 'akali': 84, 'orianna': 61, 'syndra': 134,
    'azir': 268, 'cassiopeia': 69, 'diana': 131, 'fizz': 105, 'katarina': 55,
    
    # ADCs
    'jinx': 222, 'caitlyn': 51, 'ezreal': 81, 'jhin': 202, 'kaisa': 145,
    'vayne': 67, 'ashe': 22, 'lucian': 236, 'sivir': 15, 'draven': 119,
    
    # Supports
    'thresh': 412, 'leona': 89, 'nami': 267, 'lulu': 117, 'janna': 40,
    'braum': 201, 'alistar': 12, 'soraka': 16, 'zyra': 143, 'bard': 432
}

def get_champion_id(name):
    """Convert champion name to ID"""
    name = name.lower().replace(' ', '').replace("'", "")
    return CHAMPIONS.get(name, None)

def input_team_composition(team_name):
    """Get team composition from user input"""
    positions = ['top', 'jungle', 'mid', 'adc', 'support']
    team_comp = {}
    
    print(f"\n--- {team_name} ---")
    for pos in positions:
        while True:
            champ_name = input(f"Enter {pos} champion: ").strip()
            champ_id = get_champion_id(champ_name)
            
            if champ_id:
                team_comp[pos] = champ_id
                print(f"  ✓ {champ_name.title()} selected")
                break
            else:
                print(f"  ❌ Champion '{champ_name}' not found.")
                print(f"  Available champions: {', '.join(list(CHAMPIONS.keys())[:10])}...")
    
    return team_comp

def display_prediction(result, team_100, team_200):
    """Display prediction results nicely"""
    print("\n" + "="*60)
    print("🤖 AI PREDICTION RESULTS")
    print("="*60)
    
    # Team compositions
    print("\n📋 TEAM COMPOSITIONS:")
    print(f"Team 100 (Blue): {format_team(team_100)}")
    print(f"Team 200 (Red):  {format_team(team_200)}")
    
    # Prediction
    winner = result['winner']
    confidence = result['confidence']
    
    print(f"\n🎯 PREDICTION:")
    print(f"Winner: {winner}")
    print(f"Confidence: {confidence:.1%}")
    
    # Probabilities
    print(f"\n📊 WIN PROBABILITIES:")
    print(f"Team 100 (Blue): {result['team_100_win_probability']:.1%}")
    print(f"Team 200 (Red):  {result['team_200_win_probability']:.1%}")
    
    # Confidence level
    if confidence > 0.7:
        confidence_level = "🟢 HIGH"
    elif confidence > 0.6:
        confidence_level = "🟡 MEDIUM"
    else:
        confidence_level = "🔴 LOW"
    
    print(f"\n🎲 CONFIDENCE LEVEL: {confidence_level}")
    
    if confidence < 0.6:
        print("⚠️  This prediction has low confidence. The match could go either way!")

def format_team(team_comp):
    """Format team composition for display"""
    champ_names = []
    for pos, champ_id in team_comp.items():
        # Find champion name by ID
        champ_name = next((name for name, id in CHAMPIONS.items() if id == champ_id), f"ID{champ_id}")
        champ_names.append(f"{champ_name.title()}({pos})")
    return " | ".join(champ_names)

def main():
    print("🎮 LEAGUE OF LEGENDS DRAFT PREDICTOR 🎮")
    print("="*50)
    
    # Load trained model
    predictor = DraftPredictor()
    if not predictor.load_model('draft_predictor_model.pkl'):
        print("❌ No trained model found!")
        print("\n🔧 Train the AI first:")
        print("   python src/draft_predictor.py")
        return
    
    print("✅ AI model loaded successfully!")
    print("\nEnter team compositions to get win predictions.")
    print("Available positions: top, jungle, mid, adc, support")
    
    while True:
        try:
            print("\n" + "="*50)
            
            # Get team compositions
            team_100 = input_team_composition("TEAM 100 (BLUE SIDE)")
            team_200 = input_team_composition("TEAM 200 (RED SIDE)")
            
            # Make prediction
            print("\n🤖 Analyzing draft compositions...")
            result = predictor.predict_match(team_100, team_200)
            
            # Display results
            display_prediction(result, team_100, team_200)
            
            # Continue?
            print("\n" + "="*60)
            continue_prediction = input("\n🔄 Predict another match? (y/n): ").lower().strip()
            if continue_prediction != 'y':
                break
                
        except KeyboardInterrupt:
            print("\n\n👋 Thanks for using the Draft Predictor!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            print("Please try again...")
    
    print("\n🎯 Remember: Predictions are based on historical data.")
    print("   Individual skill and strategy still matter most!")

if __name__ == "__main__":
    main()