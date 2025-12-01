import pandas as pd
import numpy as np
from collections import Counter

def analyze_dataset(csv_file='draft_data_with_bans.csv'):
    """
    Analyse complète du dataset avec distributions des valeurs
    """
    try:
        # Charger les données
        df = pd.read_csv(csv_file)
        print(f"Dataset chargé: {len(df)} matches, {len(df.columns)} colonnes")
        print("="*80)
        
        # Informations générales
        print("\n📊 INFORMATIONS GÉNÉRALES")
        print("="*50)
        print(f"Nombre de matches: {len(df)}")
        print(f"Nombre de colonnes: {len(df.columns)}")
        print(f"Période: {df['gameDuration'].min()//60:.0f}min à {df['gameDuration'].max()//60:.0f}min")
        
        # Statistiques des victoires
        print(f"\n🏆 STATISTIQUES DES VICTOIRES")
        print("="*40)
        team_100_wins = df['team_100_win'].sum()
        total_matches = len(df)
        print(f"Team 100 (Bleue) victoires: {team_100_wins} ({team_100_wins/total_matches:.1%})")
        print(f"Team 200 (Rouge) victoires: {total_matches - team_100_wins} ({(total_matches - team_100_wins)/total_matches:.1%})")
        
        # Séparer les colonnes par type
        categorical_cols = []
        numerical_cols = []
        boolean_cols = []
        
        for col in df.columns:
            if col.endswith('_win') or col.endswith('_first_blood') or col.endswith('_first_tower') or col.endswith('_first_inhibitor') or col.endswith('_first_dragon') or col.endswith('_first_riftHerald') or col.endswith('_first_baron') or col.endswith('_teamEarlySurrendered'):
                boolean_cols.append(col)
            elif 'championId' in col or 'ban' in col:
                categorical_cols.append(col)
            elif col in ['matchId']:
                continue  # Skip ID columns
            else:
                numerical_cols.append(col)
        
        print(f"\n📋 TYPES DE VARIABLES")
        print("="*30)
        print(f"Variables booléennes: {len(boolean_cols)}")
        print(f"Variables catégorielles: {len(categorical_cols)}")
        print(f"Variables numériques: {len(numerical_cols)}")
        
        # Analyse des variables booléennes
        print(f"\n🔘 VARIABLES BOOLÉENNES")
        print("="*40)
        for col in boolean_cols:
            if col in df.columns:
                value_counts = df[col].value_counts()
                true_count = value_counts.get(True, 0)
                false_count = value_counts.get(False, 0)
                total = true_count + false_count
                if total > 0:
                    print(f"{col}:")
                    print(f"  True: {true_count} ({true_count/total:.1%})")
                    print(f"  False: {false_count} ({false_count/total:.1%})")
        
        # Analyse des variables catégorielles (Champions)
        print(f"\n🎮 VARIABLES CATÉGORIELLES - CHAMPIONS")
        print("="*50)
        
        # Analyse des champions les plus joués
        all_champions = []
        for col in categorical_cols:
            if 'championId' in col and col in df.columns:
                champions = df[col].dropna().astype(int).tolist()
                all_champions.extend(champions)
        
        if all_champions:
            champion_counts = Counter(all_champions)
            print(f"Nombre total de picks de champions: {len(all_champions)}")
            print(f"Champions uniques: {len(champion_counts)}")
            print(f"\nTop 15 champions les plus joués:")
            for i, (champ_id, count) in enumerate(champion_counts.most_common(15), 1):
                print(f"  {i:2d}. Champion ID {champ_id}: {count} picks ({count/len(all_champions):.1%})")
        
        # Analyse des bans
        print(f"\n🚫 ANALYSE DES BANS")
        print("="*30)
        ban_cols = [col for col in categorical_cols if 'ban' in col and 'championId' in col]
        all_bans = []
        for col in ban_cols:
            if col in df.columns:
                bans = df[col].dropna().astype(int).tolist()
                all_bans.extend(bans)
        
        if all_bans:
            ban_counts = Counter(all_bans)
            print(f"Nombre total de bans: {len(all_bans)}")
            print(f"Champions bannis uniques: {len(ban_counts)}")
            print(f"\nTop 10 champions les plus bannis:")
            for i, (champ_id, count) in enumerate(ban_counts.most_common(10), 1):
                print(f"  {i:2d}. Champion ID {champ_id}: {count} bans ({count/len(all_bans):.1%})")
        
        # Analyse des variables numériques
        print(f"\n📈 VARIABLES NUMÉRIQUES - STATISTIQUES DESCRIPTIVES")
        print("="*60)
        
        # Grouper par catégorie
        performance_cols = [col for col in numerical_cols if any(metric in col for metric in ['kills', 'goldEarned', 'totalMinionsKilled', 'visionScore', 'kda', 'enemyChampionImmobilizations'])]
        objective_cols = [col for col in numerical_cols if any(obj in col for obj in ['dragon_kills', 'baron_kills', 'tower_kills', 'inhibitor_kills', 'riftHerald_kills'])]
        duration_cols = [col for col in numerical_cols if 'Duration' in col]
        
        # Statistiques de performance des joueurs
        print(f"\n🎯 PERFORMANCE DES JOUEURS")
        print("-" * 40)
        for col in performance_cols:
            if col in df.columns:
                stats = df[col].describe()
                print(f"\n{col}:")
                print(f"  Count: {stats['count']:.0f}")
                print(f"  Mean: {stats['mean']:.2f}")
                print(f"  Std: {stats['std']:.2f}")
                print(f"  Min: {stats['min']:.2f}")
                print(f"  Q1: {stats['25%']:.2f}")
                print(f"  Median: {stats['50%']:.2f}")
                print(f"  Q3: {stats['75%']:.2f}")
                print(f"  Max: {stats['max']:.2f}")
        
        # Statistiques d'objectifs d'équipe
        print(f"\n🏰 OBJECTIFS D'ÉQUIPE")
        print("-" * 30)
        for col in objective_cols:
            if col in df.columns:
                stats = df[col].describe()
                print(f"\n{col}:")
                print(f"  Count: {stats['count']:.0f}")
                print(f"  Mean: {stats['mean']:.2f}")
                print(f"  Std: {stats['std']:.2f}")
                print(f"  Min: {stats['min']:.0f}")
                print(f"  Median: {stats['50%']:.1f}")
                print(f"  Max: {stats['max']:.0f}")
        
        # Durée des matches
        print(f"\n⏱️ DURÉE DES MATCHES")
        print("-" * 25)
        if 'gameDuration' in df.columns:
            duration_minutes = df['gameDuration'] / 60
            stats = duration_minutes.describe()
            print(f"Durée en minutes:")
            print(f"  Count: {stats['count']:.0f}")
            print(f"  Mean: {stats['mean']:.1f} min")
            print(f"  Std: {stats['std']:.1f} min")
            print(f"  Min: {stats['min']:.1f} min")
            print(f"  Q1: {stats['25%']:.1f} min")
            print(f"  Median: {stats['50%']:.1f} min")
            print(f"  Q3: {stats['75%']:.1f} min")
            print(f"  Max: {stats['max']:.1f} min")
        
        # Analyse par position
        print(f"\n👥 ANALYSE PAR POSITION")
        print("="*35)
        positions = ['top', 'jungle', 'mid', 'adc', 'support']
        
        for pos in positions:
            print(f"\n🎲 POSITION: {pos.upper()}")
            print("-" * 20)
            
            # KDA moyen par position
            team_100_kda = f'team_100_{pos}_kda'
            team_200_kda = f'team_200_{pos}_kda'
            
            if team_100_kda in df.columns and team_200_kda in df.columns:
                avg_kda_100 = df[team_100_kda].mean()
                avg_kda_200 = df[team_200_kda].mean()
                print(f"KDA moyen Team 100: {avg_kda_100:.2f}")
                print(f"KDA moyen Team 200: {avg_kda_200:.2f}")
            
            # Gold moyen par position
            team_100_gold = f'team_100_{pos}_goldEarned'
            team_200_gold = f'team_200_{pos}_goldEarned'
            
            if team_100_gold in df.columns and team_200_gold in df.columns:
                avg_gold_100 = df[team_100_gold].mean()
                avg_gold_200 = df[team_200_gold].mean()
                print(f"Gold moyen Team 100: {avg_gold_100:.0f}")
                print(f"Gold moyen Team 200: {avg_gold_200:.0f}")
        
        # Résumé final
        print(f"\n📋 RÉSUMÉ DU DATASET")
        print("="*30)
        print(f"✅ Dataset analysé avec succès")
        print(f"📊 {len(df)} matches analysés")
        print(f"🎮 {len(champion_counts) if all_champions else 0} champions uniques")
        print(f"⚖️ Équilibre des victoires: {abs(0.5 - team_100_wins/total_matches)*200:.1f}% d'écart")
        print(f"⏱️ Durée moyenne des matches: {df['gameDuration'].mean()/60:.1f} minutes")
        
        return df
        
    except FileNotFoundError:
        print(f"❌ Fichier {csv_file} non trouvé!")
        print("Exécutez d'abord la collecte de données:")
        print("python src/collect_data_safe.py --continuous")
        return None
    except Exception as e:
        print(f"❌ Erreur lors de l'analyse: {e}")
        return None

def save_analysis_report(df, output_file='data_analysis_report.txt'):
    """
    Sauvegarde le rapport d'analyse dans un fichier
    """
    if df is None:
        return
    
    with open(output_file, 'w', encoding='utf-8') as f:
        # Rediriger la sortie vers le fichier
        import sys
        original_stdout = sys.stdout
        sys.stdout = f
        
        analyze_dataset()
        
        # Restaurer la sortie normale
        sys.stdout = original_stdout
    
    print(f"📄 Rapport d'analyse sauvegardé dans: {output_file}")

def main():
    print("🔍 ANALYSE COMPLÈTE DU DATASET LOL")
    print("="*50)
    
    # Analyser le dataset
    df = analyze_dataset('draft_data_with_bans.csv')
    
    if df is not None:
        # Sauvegarder le rapport
        save_analysis_report(df)
        
        print(f"\n✨ Analyse terminée!")
        print(f"📁 Fichiers générés:")
        print(f"  - Console: Statistiques affichées")
        print(f"  - data_analysis_report.txt: Rapport complet")

if __name__ == "__main__":
    main()