# Commandes de Collecte de Données LoL

## Collecte de Matchs

### Mode continu (recommandé)
```bash
# Collecte standard - tous les elos (Challenger/GM/Master/Diamond I)
python src/collect_data_safe.py --continuous

# Avec collecte des timelines (gold par minute)
python src/collect_data_safe.py --continuous --collect-timelines

# Collecte Diamond I uniquement
python src/collect_data_safe.py --continuous --elo diamond

# Collecte Master+ uniquement (Challenger/GM/Master)
python src/collect_data_safe.py --continuous --elo master
```

### Mode parallèle (2 terminaux)
```bash
# Terminal 1 - Diamond I avec clé API #0
python src/collect_data_safe.py --continuous --elo diamond --api-key-index 0

# Terminal 2 - Master+ avec clé API #1
python src/collect_data_safe.py --continuous --elo master --api-key-index 1
```

### Options de collecte
| Option | Description |
|--------|-------------|
| `--continuous` | Mode continu (boucle infinie) |
| `--players N` | Nombre de joueurs par batch (défaut: 50) |
| `--matches N` | Matchs par joueur (défaut: 20) |
| `--db PATH` | Chemin de la base SQLite (défaut: `data/lol_matches.db`) |
| `--elo [diamond\|master]` | Filtre par elo |
| `--api-key-index N` | Index de la clé API (0, 1, 2...) |
| `--refresh-hours N` | Re-fetch joueurs après N heures (défaut: 24, 0=jamais) |
| `--collect-timelines` | Collecter gold par minute (SRZ: "gold à la minute m") |
| `--high-elo-only` | Master/GM/Challenger uniquement |

---

## Backfill Timelines (matchs existants)

### Collecter les timelines pour tous les matchs existants
```bash
# Tous les matchs (~103k = ~17h avec 4 clés API)
python src/collect_data_safe.py --backfill-timelines

# Tester avec un échantillon
python src/collect_data_safe.py --backfill-timelines --limit 100
```

**Note:** Cette commande récupère les données "gold par minute" pour les matchs déjà collectés qui n'ont pas encore de timeline.

---

## Backfill Names (noms manquants)

### Remplir les noms de bans et summoner spells depuis les IDs
```bash
python src/collect_data_safe.py --backfill-names
```

Cette commande remplit:
- **ban_1_name...ban_5_name** depuis les champion IDs
- **summoner_1_name, summoner_2_name** depuis les spell IDs
- **source_elo** avec "UNKNOWN" si manquant

---

## Stats Champions

### Peupler les stats depuis les matchs existants
```bash
python src/collect_data_safe.py --populate-stats
```

### Recalculer winrate/pickrate/banrate
```bash
python src/collect_data_safe.py --recalculate-stats
```

### Workflow complet
```bash
# 1. Peupler les stats
python src/collect_data_safe.py --populate-stats

# 2. Recalculer les taux
python src/collect_data_safe.py --recalculate-stats
```

---

## Export et Reset

### Exporter en CSV
```bash
python src/collect_data_safe.py --export-csv
```
Génère:
- `match_data_from_db.csv` - Dataset complet
- `draft_data_from_db.csv` - Données de draft uniquement

### Reset de la progression
```bash
# Reset complet (garde les matchs)
python src/collect_data_safe.py --reset

# Reset Diamond I uniquement
python src/collect_data_safe.py --reset --elo diamond

# Reset Master+ uniquement
python src/collect_data_safe.py --reset --elo master
```

---

## Utilisation Python

### Accès aux données
```python
import sys
sys.path.insert(0, 'src')
from database import MatchDatabase

db = MatchDatabase('data/lol_matches.db')

# Stats générales
stats = db.get_stats()
print(f"Total matchs: {stats['total_matches']}")

# Export DataFrame
df = db.export_to_dataframe()

# Stats champions par patch
champ_df = db.get_champions_data(['15.23', '15.24'])
print(champ_df[['champion_name', 'winrate', 'pickrate']].head(10))

# Distribution par rôle (% supp, % mid, etc.)
role_df = db.get_champion_role_distribution()
print(role_df[['champion_name', 'top_pct', 'mid_pct', 'support_pct']].head(10))

# Invocateurs par patch
inv_df = db.get_invocateurs_data(['15.23'])

# Teammates fréquents
teammates = db.get_common_teammates('puuid_du_joueur', limit=10)
```

### Gold à la minute M (Timeline)
```python
# Récupérer la timeline complète d'un match
timeline = db.get_match_timeline('KR_12345678')
for frame in timeline:
    print(f"Minute {frame['minute']}: Team100={frame['team_100_gold']} vs Team200={frame['team_200_gold']} (diff: {frame['gold_diff']})")

# Gold à une minute spécifique
gold_min_10 = db.get_gold_at_minute('KR_12345678', 10)
if gold_min_10:
    print(f"Minute 10:")
    print(f"  Team 100: {gold_min_10['team_100_gold']} gold")
    print(f"  Team 200: {gold_min_10['team_200_gold']} gold")
    print(f"  Différence: {gold_min_10['gold_diff']} gold")
    print(f"  Top T100: {gold_min_10['team_100_top_gold']} gold")
    print(f"  Mid T100: {gold_min_10['team_100_mid_gold']} gold")
    print(f"  ADC T100: {gold_min_10['team_100_adc_gold']} gold")
```

### Champion Data
```python
from champion_data import get_champion_data, get_summoner_spell_name

cd = get_champion_data()

# Nom du champion
print(cd.get_champion_name(86))  # "Garen"

# Score CC (0-10)
print(cd.get_champion_cc_score(111))  # 10 (Nautilus)

# Score CC d'une équipe
team = [111, 412, 99, 51, 238]  # Naut, Thresh, Lux, Cait, Zed
print(cd.get_team_cc_score(team))
# {'total_cc': 28, 'avg_cc': 5.6, 'max_cc': 10, 'min_cc': 2, ...}

# Nom du sort d'invocateur
print(get_summoner_spell_name(4))   # "Flash"
print(get_summoner_spell_name(14))  # "Ignite"
```

---

## Préparation ML

### Préparer les données pour le modèle
```bash
python src/prepare_data.py
```

### Entraîner le modèle
```bash
python src/draft_predictor.py
```

---

## Base de données

### Tables principales
| Table | Description |
|-------|-------------|
| `matches` | Info match (durée, version, gagnant) |
| `team_stats` | Objectifs, bans par équipe |
| `player_stats` | Stats joueur (KDA, gold, vision, etc.) |
| `summoners` | Profils joueurs |
| `summoner_elo_history` | Historique elo par patch |
| `champion_mastery` | Maîtrise champion par joueur |
| `champion_patch_stats` | Winrate/pickrate/banrate par patch |
| `match_timeline` | Gold par minute |
| `collection_progress` | Suivi de collecte |

### Colonnes ajoutées (SRZ)
- `ban_1_name` ... `ban_5_name` - Noms des bans
- `summoner_1_name`, `summoner_2_name` - Noms des sorts d'invocateur
- `source_elo` - Elo d'origine du match
- `region` - Région du match

---

## Configuration

### Fichier `src/config.py`
```python
API_KEYS = [
    "RGAPI-xxx",  # Clé 1
    "RGAPI-yyy",  # Clé 2
    # Ajouter plus de clés pour collecte plus rapide
]

REGION = "kr"              # Région (euw1, na1, kr)
QUEUE = "RANKED_SOLO_5x5"
TIER = "DIAMOND"
DIVISION = "I"
```

---

## Exemples Complets

### Collecte rapide avec 4 clés API
```bash
# La collecte utilise automatiquement toutes les clés configurées
python src/collect_data_safe.py --continuous --players 100 --matches 30
```

### Collecter avec timelines + stats
```bash
# 1. Collecter les matchs avec timelines
python src/collect_data_safe.py --continuous --collect-timelines

# 2. Après collecte, peupler et calculer les stats
python src/collect_data_safe.py --populate-stats
python src/collect_data_safe.py --recalculate-stats

# 3. Exporter pour analyse
python src/collect_data_safe.py --export-csv
```
