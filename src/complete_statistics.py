import pandas as pd
import numpy as np

def calculate_detailed_stats(data):
    """
    Calcule toutes les statistiques demandées
    """
    data = np.array(data)
    data = data[~np.isnan(data)]  # Enlever les NaN
    
    if len(data) == 0:
        return None
    
    stats = {
        # Taille de l'échantillon
        'n': len(data),
        
        # Mesures de tendance centrale
        'moyenne': np.mean(data),
        'mediane': np.median(data),
        
        # Mesures de dispersion
        'ecart_type': np.std(data, ddof=1),  # ddof=1 pour l'écart-type échantillon
        'variance': np.var(data, ddof=1),
        
        # Quartiles et extremes
        'minimum': np.min(data),
        'q1': np.percentile(data, 25),
        'q2_mediane': np.percentile(data, 50),
        'q3': np.percentile(data, 75),
        'maximum': np.max(data),
        
        # Autres statistiques utiles
        'etendue': np.max(data) - np.min(data),
        'iqr': np.percentile(data, 75) - np.percentile(data, 25),
        'coefficient_variation': np.std(data, ddof=1) / np.mean(data) * 100 if np.mean(data) != 0 else 0,
        
        # Percentiles additionnels
        'p5': np.percentile(data, 5),
        'p10': np.percentile(data, 10),
        'p90': np.percentile(data, 90),
        'p95': np.percentile(data, 95),
    }
    
    # Mode
    unique, counts = np.unique(data, return_counts=True)
    mode_idx = np.argmax(counts)
    stats['mode'] = unique[mode_idx]
    stats['mode_frequence'] = counts[mode_idx]
    
    return stats

def print_detailed_statistics():
    """
    Affiche les statistiques complètes du dataset
    """
    try:
        df = pd.read_csv('draft_data_with_bans.csv')
        print(f"📊 STATISTIQUES COMPLÈTES - {len(df)} matches")
        print("="*100)
        
        # Variables à analyser
        key_variables = {
            'DURÉE DES MATCHES (minutes)': {
                'col': 'gameDuration',
                'transform': lambda x: x/60
            },
            'KILLS PAR JOUEUR': {
                'cols': [f'team_{t}_{p}_kills' for t in ['100', '200'] for p in ['top', 'jungle', 'mid', 'adc', 'support']]
            },
            'GOLD GAGNÉ': {
                'cols': [f'team_{t}_{p}_goldEarned' for t in ['100', '200'] for p in ['top', 'jungle', 'mid', 'adc', 'support']]
            },
            'MINIONS TUÉS': {
                'cols': [f'team_{t}_{p}_totalMinionsKilled' for t in ['100', '200'] for p in ['top', 'jungle', 'mid', 'adc', 'support']]
            },
            'VISION SCORE': {
                'cols': [f'team_{t}_{p}_visionScore' for t in ['100', '200'] for p in ['top', 'jungle', 'mid', 'adc', 'support']]
            },
            'KDA': {
                'cols': [f'team_{t}_{p}_kda' for t in ['100', '200'] for p in ['top', 'jungle', 'mid', 'adc', 'support']]
            },
            'DRAGONS PAR ÉQUIPE': {
                'cols': ['team_100_dragon_kills', 'team_200_dragon_kills']
            },
            'BARONS PAR ÉQUIPE': {
                'cols': ['team_100_baron_kills', 'team_200_baron_kills']
            },
            'TOURS DÉTRUITES': {
                'cols': ['team_100_tower_kills', 'team_200_tower_kills']
            }
        }
        
        # Analyser chaque variable
        for var_name, config in key_variables.items():
            print(f"\n{'='*80}")
            print(f"📈 {var_name}")
            print(f"{'='*80}")
            
            # Collecter toutes les valeurs
            all_values = []
            
            if 'col' in config:
                # Une seule colonne
                values = df[config['col']].dropna().values
                if 'transform' in config:
                    values = config['transform'](values)
                all_values = values
            else:
                # Plusieurs colonnes
                for col in config.get('cols', []):
                    if col in df.columns:
                        values = df[col].dropna().values
                        all_values.extend(values)
            
            if len(all_values) > 0:
                stats = calculate_detailed_stats(all_values)
                
                if stats:
                    print(f"\n📊 STATISTIQUES DESCRIPTIVES:")
                    print(f"  N (taille échantillon): {stats['n']:,}")
                    print(f"\n  TENDANCE CENTRALE:")
                    print(f"    - Moyenne:           {stats['moyenne']:.3f}")
                    print(f"    - Médiane:           {stats['mediane']:.3f}")
                    print(f"    - Mode:              {stats['mode']:.3f} (fréquence: {stats['mode_frequence']})")
                    
                    print(f"\n  DISPERSION:")
                    print(f"    - Écart-type:        {stats['ecart_type']:.3f}")
                    print(f"    - Variance:          {stats['variance']:.3f}")
                    print(f"    - Coef. variation:   {stats['coefficient_variation']:.1f}%")
                    print(f"    - Étendue:           {stats['etendue']:.3f}")
                    print(f"    - IQR:               {stats['iqr']:.3f}")
                    
                    print(f"\n  DISTRIBUTION (Quartiles):")
                    print(f"    - Minimum:           {stats['minimum']:.3f}")
                    print(f"    - Q1 (25%):          {stats['q1']:.3f}")
                    print(f"    - Q2 (50%):          {stats['q2_mediane']:.3f}")
                    print(f"    - Q3 (75%):          {stats['q3']:.3f}")
                    print(f"    - Maximum:           {stats['maximum']:.3f}")
                    
                    print(f"\n  PERCENTILES:")
                    print(f"    - P5:                {stats['p5']:.3f}")
                    print(f"    - P10:               {stats['p10']:.3f}")
                    print(f"    - P90:               {stats['p90']:.3f}")
                    print(f"    - P95:               {stats['p95']:.3f}")
                    
                    # Box plot textuel
                    print(f"\n  📊 BOXPLOT (représentation textuelle):")
                    print(f"    |--[{stats['minimum']:.1f}]---◄{stats['q1']:.1f}|███{stats['mediane']:.1f}███|{stats['q3']:.1f}►---[{stats['maximum']:.1f}]--|")
                    
                    # Analyse par rôle si applicable
                    if 'cols' in config and len(config['cols']) > 5:
                        print(f"\n  📊 PAR RÔLE:")
                        for role in ['top', 'jungle', 'mid', 'adc', 'support']:
                            role_values = []
                            for col in config['cols']:
                                if role in col and col in df.columns:
                                    role_values.extend(df[col].dropna().values)
                            
                            if role_values:
                                mean = np.mean(role_values)
                                std = np.std(role_values, ddof=1)
                                print(f"    - {role.upper():8s}: μ={mean:7.2f}, σ={std:7.2f}")
        
        # Statistiques sur les victoires
        print(f"\n{'='*80}")
        print(f"🏆 ANALYSE DES VICTOIRES")
        print(f"{'='*80}")
        
        team_100_wins = df['team_100_win'].sum()
        total = len(df)
        team_100_wr = team_100_wins / total
        team_200_wr = 1 - team_100_wr
        
        print(f"\n  Team 100 (Bleue):")
        print(f"    - Victoires:         {team_100_wins}")
        print(f"    - Taux de victoire:  {team_100_wr:.3f} ({team_100_wr*100:.1f}%)")
        
        print(f"\n  Team 200 (Rouge):")
        print(f"    - Victoires:         {total - team_100_wins}")
        print(f"    - Taux de victoire:  {team_200_wr:.3f} ({team_200_wr*100:.1f}%)")
        
        print(f"\n  Différence absolue:    {abs(team_100_wr - 0.5)*100:.1f}%")
        
        # Intervalle de confiance pour le taux de victoire (approximation normale)
        se = np.sqrt(team_100_wr * (1 - team_100_wr) / total)
        ci_lower = team_100_wr - 1.96 * se
        ci_upper = team_100_wr + 1.96 * se
        print(f"  IC 95% Team 100:       [{ci_lower:.3f}, {ci_upper:.3f}]")
        
        # Analyse par durée de match
        print(f"\n  PAR DURÉE DE MATCH:")
        df['duration_min'] = df['gameDuration'] / 60
        duration_bins = [0, 20, 25, 30, 35, 100]
        duration_labels = ['<20min', '20-25min', '25-30min', '30-35min', '>35min']
        df['duration_cat'] = pd.cut(df['duration_min'], bins=duration_bins, labels=duration_labels)
        
        for cat in duration_labels:
            cat_matches = df[df['duration_cat'] == cat]
            if len(cat_matches) > 0:
                wr = cat_matches['team_100_win'].mean()
                print(f"    - {cat:10s}: {len(cat_matches):3d} matches ({len(cat_matches)/total*100:4.1f}%), WR={wr:.3f}")
        
        # Statistiques additionnelles utiles
        print(f"\n{'='*80}")
        print(f"📊 AUTRES STATISTIQUES UTILES")
        print(f"{'='*80}")
        
        print(f"\n  CORRÉLATIONS AVEC LA VICTOIRE:")
        # Calculer les corrélations simples avec team_100_win
        correlations = []
        for col in df.columns:
            if col.startswith('team_100_') and col not in ['team_100_win', 'team_100_teamEarlySurrendered']:
                if df[col].dtype in ['int64', 'float64']:
                    corr = df[col].corr(df['team_100_win'])
                    if not pd.isna(corr):
                        correlations.append((col, corr))
        
        # Top 10 corrélations positives
        correlations.sort(key=lambda x: x[1], reverse=True)
        print(f"\n  Top 10 corrélations positives:")
        for col, corr in correlations[:10]:
            print(f"    - {col:40s}: r={corr:+.3f}")
        
        # Top 10 corrélations négatives
        print(f"\n  Top 10 corrélations négatives:")
        for col, corr in correlations[-10:]:
            print(f"    - {col:40s}: r={corr:+.3f}")
        
        print(f"\n✅ Analyse statistique complète terminée!")
        
    except Exception as e:
        print(f"❌ Erreur: {e}")

if __name__ == "__main__":
    print_detailed_statistics()