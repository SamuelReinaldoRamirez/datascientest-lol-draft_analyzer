import pandas as pd
import numpy as np
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    print("⚠️  scipy not installed - some advanced statistics will be skipped")
from collections import Counter

def comprehensive_statistics(data):
    """
    Calcule des statistiques complètes incluant:
    - Moyenne, Médiane, Mode
    - Écart-type, Variance
    - Min, Q1, Q2 (médiane), Q3, Max
    - Coefficient de variation
    - Skewness (asymétrie)
    - Kurtosis (aplatissement)
    - Intervalle interquartile (IQR)
    """
    result = {
        'count': len(data),
        'mean': np.mean(data),
        'std': np.std(data),
        'variance': np.var(data),
        'min': np.min(data),
        'q1': np.percentile(data, 25),
        'median': np.median(data),
        'q3': np.percentile(data, 75),
        'max': np.max(data),
        'iqr': np.percentile(data, 75) - np.percentile(data, 25),
        'cv': (np.std(data) / np.mean(data) * 100) if np.mean(data) != 0 else 0,  # Coefficient de variation
        'skewness': stats.skew(data) if SCIPY_AVAILABLE else np.nan,
        'kurtosis': stats.kurtosis(data) if SCIPY_AVAILABLE else np.nan,
        'range': np.max(data) - np.min(data)
    }
    
    # Mode (peut avoir plusieurs valeurs)
    if SCIPY_AVAILABLE:
        mode_result = stats.mode(data, keepdims=True)
        result['mode'] = mode_result.mode[0] if len(mode_result.mode) > 0 else None
        result['mode_count'] = mode_result.count[0] if len(mode_result.count) > 0 else 0
    else:
        # Calcul manuel du mode
        unique, counts = np.unique(data, return_counts=True)
        max_count = np.max(counts)
        mode_idx = np.argmax(counts)
        result['mode'] = unique[mode_idx]
        result['mode_count'] = max_count
    
    return result

def analyze_complete_statistics(csv_file='draft_data_with_bans.csv'):
    """
    Analyse statistique complète du dataset
    """
    try:
        # Charger les données
        df = pd.read_csv(csv_file)
        print(f"📊 ANALYSE STATISTIQUE COMPLÈTE - {len(df)} matches")
        print("="*100)
        
        # Variables numériques à analyser
        numerical_vars = {
            'Performance des joueurs': [
                'kills', 'goldEarned', 'totalMinionsKilled', 'visionScore', 'kda'
            ],
            'Objectifs d\'équipe': [
                'dragon_kills', 'baron_kills', 'tower_kills', 'inhibitor_kills', 'riftHerald_kills'
            ],
            'Durée': ['gameDuration']
        }
        
        # Analyser par catégorie
        for category, vars_list in numerical_vars.items():
            print(f"\n{'='*80}")
            print(f"📈 {category.upper()}")
            print(f"{'='*80}")
            
            for var_base in vars_list:
                # Trouver toutes les colonnes qui correspondent
                matching_cols = [col for col in df.columns if var_base in col and not 'championName' in col]
                
                if not matching_cols:
                    continue
                
                print(f"\n📊 Variable: {var_base}")
                print("-"*60)
                
                # Analyser globalement toutes les valeurs
                all_values = []
                for col in matching_cols:
                    values = df[col].dropna().values
                    all_values.extend(values)
                
                if len(all_values) > 0:
                    stats = comprehensive_statistics(all_values)
                    
                    print(f"N (observations):     {stats['count']}")
                    print(f"\n📐 TENDANCE CENTRALE:")
                    print(f"  Moyenne:            {stats['mean']:.3f}")
                    print(f"  Médiane (Q2):       {stats['median']:.3f}")
                    print(f"  Mode:               {stats['mode']:.3f} (apparaît {stats['mode_count']} fois)")
                    
                    print(f"\n📏 DISPERSION:")
                    print(f"  Écart-type:         {stats['std']:.3f}")
                    print(f"  Variance:           {stats['variance']:.3f}")
                    print(f"  Coefficient var:    {stats['cv']:.1f}%")
                    print(f"  Étendue:            {stats['range']:.3f}")
                    print(f"  IQR:                {stats['iqr']:.3f}")
                    
                    print(f"\n📊 DISTRIBUTION:")
                    print(f"  Minimum:            {stats['min']:.3f}")
                    print(f"  Q1 (25%):           {stats['q1']:.3f}")
                    print(f"  Q2 (50%, médiane):  {stats['median']:.3f}")
                    print(f"  Q3 (75%):           {stats['q3']:.3f}")
                    print(f"  Maximum:            {stats['max']:.3f}")
                    
                    print(f"\n📈 FORME DE LA DISTRIBUTION:")
                    print(f"  Asymétrie (skew):   {stats['skewness']:.3f}", end="")
                    if stats['skewness'] > 1:
                        print(" (fortement asymétrique à droite)")
                    elif stats['skewness'] < -1:
                        print(" (fortement asymétrique à gauche)")
                    elif abs(stats['skewness']) < 0.5:
                        print(" (distribution symétrique)")
                    else:
                        print(" (légèrement asymétrique)")
                    
                    print(f"  Kurtosis:           {stats['kurtosis']:.3f}", end="")
                    if stats['kurtosis'] > 3:
                        print(" (distribution leptokurtique - plus pointue)")
                    elif stats['kurtosis'] < -1:
                        print(" (distribution platykurtique - plus aplatie)")
                    else:
                        print(" (distribution normale)")
                
                # Analyser par équipe si applicable
                team_100_cols = [col for col in matching_cols if 'team_100' in col]
                team_200_cols = [col for col in matching_cols if 'team_200' in col]
                
                if team_100_cols and team_200_cols:
                    print(f"\n🔄 COMPARAISON PAR ÉQUIPE:")
                    
                    # Team 100
                    team_100_values = []
                    for col in team_100_cols:
                        team_100_values.extend(df[col].dropna().values)
                    
                    # Team 200
                    team_200_values = []
                    for col in team_200_cols:
                        team_200_values.extend(df[col].dropna().values)
                    
                    if len(team_100_values) > 0 and len(team_200_values) > 0:
                        mean_100 = np.mean(team_100_values)
                        mean_200 = np.mean(team_200_values)
                        std_100 = np.std(team_100_values)
                        std_200 = np.std(team_200_values)
                        
                        print(f"  Team 100 (Bleue):   μ={mean_100:.2f}, σ={std_100:.2f}")
                        print(f"  Team 200 (Rouge):   μ={mean_200:.2f}, σ={std_200:.2f}")
                        print(f"  Différence moyenne: {abs(mean_100 - mean_200):.2f} ({abs(mean_100 - mean_200)/max(mean_100, mean_200)*100:.1f}%)")
                        
                        # Test statistique de différence
                        if SCIPY_AVAILABLE and len(team_100_values) > 30 and len(team_200_values) > 30:
                            t_stat, p_value = stats.ttest_ind(team_100_values, team_200_values)
                            print(f"  Test t:             t={t_stat:.3f}, p={p_value:.4f}", end="")
                            if p_value < 0.05:
                                print(" (différence significative)")
                            else:
                                print(" (pas de différence significative)")
        
        # Analyse des corrélations
        print(f"\n{'='*80}")
        print(f"🔗 ANALYSE DES CORRÉLATIONS")
        print(f"{'='*80}")
        
        # Sélectionner les colonnes numériques clés
        key_metrics = []
        for team in ['team_100', 'team_200']:
            for pos in ['top', 'jungle', 'mid', 'adc', 'support']:
                for metric in ['goldEarned', 'kda', 'visionScore']:
                    col = f'{team}_{pos}_{metric}'
                    if col in df.columns:
                        key_metrics.append(col)
        
        # Ajouter les objectifs
        for team in ['team_100', 'team_200']:
            for obj in ['dragon_kills', 'baron_kills', 'tower_kills']:
                col = f'{team}_{obj}'
                if col in df.columns:
                    key_metrics.append(col)
        
        # Calculer la matrice de corrélation
        if len(key_metrics) > 0:
            corr_df = df[key_metrics].corr()
            
            # Trouver les corrélations les plus fortes
            strong_corr = []
            for i in range(len(corr_df)):
                for j in range(i+1, len(corr_df)):
                    corr_value = corr_df.iloc[i, j]
                    if abs(corr_value) > 0.5:  # Corrélation forte
                        strong_corr.append((corr_df.index[i], corr_df.columns[j], corr_value))
            
            if strong_corr:
                print("\nCorrélations fortes (|r| > 0.5):")
                for var1, var2, corr in sorted(strong_corr, key=lambda x: abs(x[2]), reverse=True)[:10]:
                    print(f"  {var1} ↔ {var2}: r={corr:.3f}")
        
        # Autres statistiques utiles
        print(f"\n{'='*80}")
        print(f"📊 AUTRES STATISTIQUES UTILES")
        print(f"{'='*80}")
        
        # Taux de victoire par côté
        print(f"\n🏆 ANALYSE DES VICTOIRES:")
        team_100_wins = df['team_100_win'].sum()
        total = len(df)
        print(f"  Taux de victoire Team 100 (Bleue): {team_100_wins/total:.3f}")
        print(f"  Taux de victoire Team 200 (Rouge): {(total-team_100_wins)/total:.3f}")
        
        # Test binomial pour équilibre des victoires
        if SCIPY_AVAILABLE:
            binom_test = stats.binom_test(team_100_wins, total, 0.5)
            print(f"  Test binomial (H0: p=0.5): p-value={binom_test:.4f}", end="")
            if binom_test < 0.05:
                print(" (déséquilibre significatif)")
            else:
                print(" (équilibré)")
        
        # Analyse des premières objectives
        print(f"\n🎯 IMPACT DES PREMIERS OBJECTIFS:")
        objectives = ['first_blood', 'first_tower', 'first_dragon', 'first_baron']
        
        for obj in objectives:
            col = f'team_100_{obj}'
            if col in df.columns:
                win_rate_with_obj = df[df[col] == True]['team_100_win'].mean()
                win_rate_without = df[df[col] == False]['team_100_win'].mean()
                print(f"  {obj}:")
                print(f"    Avec: {win_rate_with_obj:.3f} | Sans: {win_rate_without:.3f} | Δ={win_rate_with_obj-win_rate_without:.3f}")
        
        # Analyse par durée de partie
        print(f"\n⏱️ ANALYSE PAR DURÉE DE PARTIE:")
        if 'gameDuration' in df.columns:
            df['duration_category'] = pd.cut(df['gameDuration']/60, 
                                            bins=[0, 20, 25, 30, 35, 100],
                                            labels=['<20min', '20-25min', '25-30min', '30-35min', '>35min'])
            
            for cat in df['duration_category'].cat.categories:
                matches_in_cat = df[df['duration_category'] == cat]
                if len(matches_in_cat) > 0:
                    win_rate = matches_in_cat['team_100_win'].mean()
                    print(f"  {cat}: {len(matches_in_cat)} matches ({len(matches_in_cat)/len(df)*100:.1f}%), WR Team 100: {win_rate:.3f}")
        
        print(f"\n✅ Analyse statistique complète terminée!")
        
        return df
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return None

def main():
    print("🔬 ANALYSE STATISTIQUE APPROFONDIE - LEAGUE OF LEGENDS DRAFT DATA")
    print("="*100)
    
    # Lancer l'analyse
    df = analyze_complete_statistics('draft_data_with_bans.csv')
    
    if df is not None:
        # Sauvegarder les résultats
        with open('statistical_analysis_report.txt', 'w', encoding='utf-8') as f:
            import sys
            original_stdout = sys.stdout
            sys.stdout = f
            analyze_complete_statistics('draft_data_with_bans.csv')
            sys.stdout = original_stdout
        
        print(f"\n📄 Rapport complet sauvegardé dans: statistical_analysis_report.txt")

if __name__ == "__main__":
    main()