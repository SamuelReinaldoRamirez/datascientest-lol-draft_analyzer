# Analyse des Améliorations - LoL Draft Predictor

## Objectif Initial

Atteindre **70%+ d'accuracy** pour la prédiction du résultat d'un match de League of Legends à partir du draft.

---

## Résultats Clés

### Avec Data Leakage (INCORRECT)

| Configuration | Accuracy | Note |
|--------------|----------|------|
| Player features (stats sur TOUS les matchs) | 83.90% | **DATA LEAKAGE** |
| Draft + résultats match (kills, objectives) | 99.09% | **DATA LEAKAGE** |

### Sans Data Leakage (CORRECT)

| Configuration | Accuracy | Features |
|--------------|----------|----------|
| Draft seul (OPGG + DPM.LOL + Playstyle) | **51.91%** | 129 |
| Player features (temporel correct) | **51.80%** | 26 |
| Combined draft + player (temporel) | ~52% | 155 |

---

## Analyse du Data Leakage

### Problème 1: Player Features

**Avant correction:**
```python
# Calcule stats sur TOUS les matchs (y compris futurs)
def _calculate_player_champion_stats():
    query = "SELECT puuid, champion_id, COUNT(*), wins FROM player_stats..."
```

**Après correction:**
```python
# Calcule stats UNIQUEMENT sur les matchs avant le test set
def _calculate_player_champion_stats_df(before_timestamp):
    time_filter = f"AND m.game_creation < {before_timestamp}"
```

**Impact:**
- Avant: Corrélation test set = 0.70 (artificiellement haute)
- Après: Corrélation test set = 0.03 (réaliste)

### Problème 2: Features de Résultat Match

Les colonnes de la base de données incluaient des résultats post-match:
- `team_100_first_blood`, `team_100_dragon_kills`, etc.
- `team_100_top_kills`, `team_100_top_deaths`, etc.

Ces features sont des **résultats** du match, pas des prédicteurs valides.

---

## Découvertes Importantes

### 1. Le Draft Seul Ne Prédit Pas le Résultat

Avec 129 features de draft (winrates, tier scores, playstyle):
- Accuracy: **51.91%** (à peine mieux que le hasard)
- ROC-AUC: **0.52**

**Conclusion:** Le draft influence le jeu mais ne détermine pas le gagnant.

### 2. Les Stats Joueur Historiques Ne Sont Pas Prédictives

Même avec les winrates historiques des joueurs sur leurs champions:
- Accuracy: **51.80%**
- Corrélation temporelle: **0.03**

**Raisons:**
- Les joueurs progressent/régressent
- La méta change entre les patchs
- L'exécution en jeu est plus importante

### 3. L'Objectif de 70% N'Est Pas Réaliste

Sans utiliser de données post-match (qui serait du data leakage), il est impossible d'atteindre 70% de précision.

---

## Features Ajoutées (Valides)

### Player Features Temporelles

Nouvelle méthode `add_player_features_temporal()`:

```python
# Usage correct (évite le data leakage)
preparer = DataPreparer('data/lol_matches.db')
df = preparer.load_data()
df = df.sort_values('game_creation')  # Tri temporel

train_size = int(len(df) * 0.85)
df_train, df_test = df.iloc[:train_size], df.iloc[train_size:]

df_train, df_test = preparer.add_player_features_temporal(df_train, df_test)
```

Features générées (26 au total):
- `team_X_pos_player_wr`: Winrate historique par position
- `team_X_avg_player_wr`: Moyenne équipe
- `player_wr_diff`: Différence entre équipes
- `experience_diff`: Différence de games joués

### Export Database Optimisé

- Filtre: Matchs avec tous les 10 puuids présents
- Ordre: `ORDER BY game_creation DESC`
- ~175k matchs high-elo (KR, EUW, EUNE)

---

## Performance Optimisée

### Vectorisation des Player Features

**Avant:** ~10 minutes pour 170k matchs (row iteration)
```python
for idx, row in df.iterrows():  # LENT
    features = lookup(row)
```

**Après:** ~35 secondes (pandas merge)
```python
merged = temp_df.merge(stats_df, on=['puuid', 'champion_id'])  # RAPIDE
```

---

## Recommandations

### Pour Un Projet Réaliste

1. **Accepter ~52-55% accuracy** comme baseline pour le draft seul
2. **Combiner avec des features live** (early game stats) pour atteindre 60-65%
3. **Utiliser des embeddings** (neural network) pour potentiellement 55-60%

### Features Potentielles Non-Leaking

- Forme récente du joueur (5 derniers matchs)
- Winrate sur le patch actuel
- Temps moyen de partie
- Préférence de lane/champion
- Side preference (blue/red)

### Ce Qui NE Marche PAS

- Stats agrégées sur tous les matchs (data leakage)
- Features post-game (kills, objectives, gold)
- Winrates "actuels" qui incluent le match à prédire

---

## Fichiers Modifiés

| Fichier | Modifications |
|---------|---------------|
| `src/ML/preprocessing.py` | `add_player_features()` vectorisé, `add_player_features_temporal()`, `_calculate_player_champion_stats_df(before_timestamp)` |
| `src/collect_data/database.py` | Filtre puuid dans export, ORDER BY game_creation |

---

## Conclusion

L'objectif de 70% n'est pas atteignable avec seulement les données de draft. Les résultats "impressionnants" (80%+) dans la littérature viennent généralement de data leakage (utilisation de données post-match).

**La réalité:** Le draft compte (~52%), mais l'exécution en jeu détermine le gagnant.

---

*Document généré le 2026-01-06*
