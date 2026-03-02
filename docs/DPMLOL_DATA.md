# Documentation des Données DPM.LOL

## Vue d'ensemble

**DPM.LOL** (https://dpm.lol) est un site de statistiques League of Legends reconnu officiellement par Riot Games (statut RSO - Riot Sign-On). Il fournit des données de tierlist, builds, matchups et statistiques esport.

### Pourquoi DPM.LOL ?

| Source | Avantages | Données uniques |
|--------|-----------|-----------------|
| **OP.GG** | Matchups, synergies détaillées | KDA, tier ranking |
| **DPM.LOL** | Métriques composites, tendances | `tierScore`, `winrateVariance` |
| **CommunityDragon** | Données officielles Riot | Playstyle scores (CC, mobility, etc.) |

DPM.LOL complète OP.GG avec :
- **tierScore** : Score composite unique combinant winrate, pickrate et banrate
- **winrateVariance** : Indicateur de tendance (le champion monte ou descend dans la méta)
- **Données par lane** plus granulaires

---

## Structure des Données

### Fichier : `data/dpmlol/dpmlol_tierlist.parquet`

| Colonne | Type | Description | Exemple |
|---------|------|-------------|---------|
| `championName` | string | Nom du champion | "MissFortune" |
| `championId` | int64 | ID Riot du champion | 21 |
| `lane` | string | Lane/Rôle | "BOTTOM", "UTILITY", etc. |
| `winrate` | float64 | Taux de victoire (%) | 51.40 |
| `pickrate` | float64 | Taux de sélection (%) | 17.63 |
| `banrate` | float64 | Taux de ban (%) | 15.26 |
| `tierScore` | float64 | Score de tier composite | 69.60 |
| `winrateVariance` | float64 | Variation du winrate | -0.40 |
| `count` | int64 | Nombre de parties | 739835 |
| `lanesPickrate` | dict | Distribution par lane | {"BOTTOM": 98.46, ...} |
| `tier` | string | Tier ELO filtré | "emerald_plus" |
| `timeframe` | string | Patch version | "15.24" |

### Statistiques du Dataset

```
Total entries:     860
Champions uniques: 172
Lanes:            ['TOP', 'JUNGLE', 'MIDDLE', 'BOTTOM', 'UTILITY']
```

Chaque champion a une entrée par lane (172 × 5 = 860 entrées).

---

## Description des Métriques

### tierScore (Score de Tier)

Le `tierScore` est une métrique composite calculée par DPM.LOL qui combine :
- Winrate
- Pickrate
- Banrate

**Interprétation :**
- **> 50** : Champion très fort dans la méta actuelle (S/A tier)
- **20-50** : Champion viable (B tier)
- **0-20** : Champion moyen (C tier)
- **< 0** : Champion faible ou hors-méta (D tier)

```
Plage observée: -377.93 à 69.87
Moyenne: -29.27
```

### winrateVariance (Tendance)

Indique si le winrate du champion est en train de monter ou descendre.

**Interprétation :**
- **> 0** : Champion en hausse (buff récent, méta favorable)
- **= 0** : Champion stable
- **< 0** : Champion en baisse (nerf, méta défavorable)

```
Plage observée: -35.05 à 34.17
Moyenne: 0.15
```

### lanesPickrate (Distribution par Lane)

Dictionnaire montrant le % de fois où le champion est joué dans chaque lane.

**Exemple pour Miss Fortune :**
```python
{
    'BOTTOM': 98.46,   # ADC principal
    'UTILITY': 0.92,   # Support occasionnel
    'MIDDLE': 0.52,    # Mid rare
    'TOP': 0.09,       # Top très rare
    'JUNGLE': 0.01     # Jungle quasi jamais
}
```

---

## Top Champions (Patch 15.24, Emerald+)

| Champion | Lane | Winrate | Pickrate | Banrate | TierScore |
|----------|------|---------|----------|---------|-----------|
| Milio | UTILITY | 52.09% | 10.26% | 11.02% | 69.87 |
| Miss Fortune | BOTTOM | 51.40% | 17.63% | 15.26% | 69.60 |
| Taric | MIDDLE | 56.66% | 0.02% | 0.31% | 69.37 |
| Malphite | TOP | 52.41% | 7.66% | 31.26% | 66.93 |
| Nami | UTILITY | 51.94% | 15.82% | 3.07% | 62.88 |

---

## Utilisation du Scraper

### Installation

```bash
pip install playwright
python -m playwright install chromium
```

### Usage Basique

```python
from src.collect_data.dpmlol_scraper import DPMLOLScraper

# Scraper avec contexte manager (ferme automatiquement le navigateur)
with DPMLOLScraper(headless=True) as scraper:
    # Récupérer la tierlist Emerald+
    df = scraper.get_tierlist(tier="emerald_plus")

    # Sauvegarder les données
    scraper.save_data()
```

### Tiers Disponibles

| Tier | Description |
|------|-------------|
| `all` | Tous les rangs |
| `iron`, `bronze`, `silver`, `gold` | Rangs bas |
| `platinum`, `emerald`, `diamond` | Rangs moyens |
| `master`, `grandmaster`, `challenger` | Hauts rangs |
| `emerald_plus` | Emerald et au-dessus |
| `diamond_plus` | Diamond et au-dessus |
| `master_plus` | Master et au-dessus |

### Récupérer Plusieurs Tiers

```python
with DPMLOLScraper() as scraper:
    df = scraper.get_all_tierlists(
        tiers=["emerald_plus", "diamond_plus", "master_plus"],
        timeframe="15.24"
    )
    scraper.save_data()
```

---

## Intégration dans le Preprocessing

Les données DPM.LOL sont automatiquement intégrées dans le pipeline de preprocessing.

### Features Générées (61 au total)

#### Par Position (30 features)
```
team_100_top_dpmlol_tier_score
team_100_top_dpmlol_winrate
team_100_top_dpmlol_wr_variance
... (pour chaque position et équipe)
```

#### Par Équipe (24 features)
```
team_100_dpmlol_avg_tier_score      # Moyenne des tier scores
team_100_dpmlol_total_tier_score    # Somme des tier scores
team_100_dpmlol_min_tier_score      # Champion le plus faible
team_100_dpmlol_max_tier_score      # Champion le plus fort
team_100_dpmlol_tier_score_std      # Écart-type (équilibre de l'équipe)
team_100_dpmlol_avg_winrate         # Winrate moyen
team_100_dpmlol_avg_wr_variance     # Tendance moyenne
team_100_dpmlol_trending_up_count   # Nombre de champions en hausse
team_100_dpmlol_trending_down_count # Nombre de champions en baisse
team_100_dpmlol_avg_pickrate        # Popularité moyenne
team_100_dpmlol_total_pickrate      # Popularité totale
team_100_dpmlol_avg_banrate         # Banrate moyen
```

#### Différences Entre Équipes (7 features)
```
dpmlol_tier_score_diff        # Avantage tier score
dpmlol_total_tier_score_diff  # Avantage tier total
dpmlol_winrate_diff           # Avantage winrate
dpmlol_wr_variance_diff       # Avantage tendance
dpmlol_pickrate_diff          # Différence popularité
dpmlol_banrate_diff           # Différence banrate
dpmlol_trending_advantage     # Avantage tendance (count)
```

### Usage Manuel

```python
from src.ML.preprocessing import DataPreparer, get_dpmlol_provider

# Accès direct au provider
dpmlol = get_dpmlol_provider()
tier_score = dpmlol.get_champion_tier_score(21, 'adc')  # Miss Fortune ADC
winrate = dpmlol.get_champion_winrate(21, 'adc')
trend = dpmlol.get_champion_winrate_variance(21, 'adc')

# Intégration dans preprocessing
preparer = DataPreparer('data/lol_matches.db')
df = preparer.add_dpmlol_features(df)
```

---

## API DPM.LOL (Référence Technique)

### Endpoint Tierlist
```
GET https://dpm.lol/v1/tierlist
    ?tier=emerald_plus
    &timeframe=15.24
    &gameMode=ranked
```

### Réponse JSON
```json
{
  "champions": [
    {
      "championName": "Milio",
      "championId": 902,
      "lane": "UTILITY",
      "pickrate": 10.26,
      "count": 430670,
      "winrate": 52.09,
      "banrate": 11.02,
      "tierScore": 69.87,
      "winrateVariance": -0.007,
      "lanesPickrate": {
        "UTILITY": 99.81,
        "MIDDLE": 0.10,
        "TOP": 0.04,
        "BOTTOM": 0.03
      }
    },
    // ... autres champions
  ],
  "total": 860
}
```

### Protection Anti-Bot

Le site utilise Cloudflare pour protéger contre le scraping automatique. Le scraper utilise Playwright avec :
- User-Agent réaliste
- Acceptation des cookies GDPR
- Délais entre requêtes

**Limitations Cloudflare :**
- Les pages champion sont bloquées (matchups indisponibles)
- Seules les régions EUW, NA, KR sont disponibles pour les leaderboards via la home page
- Pour les matchups, utiliser les données OP.GG (`data/opgg/matchups.parquet`)

---

## Données Leaderboards

### Fichier : `data/dpmlol/dpmlol_leaderboard.parquet`

| Colonne | Type | Description | Exemple |
|---------|------|-------------|---------|
| `platform` | string | Région | "euw1", "na1", "kr" |
| `leaderboard_position` | int64 | Rang dans le classement | 1, 2, 3... |
| `game_name` | string | Nom du joueur | "Agurin" |
| `tag_line` | string | Tag Riot | "DND" |
| `display_name` | string | Nom d'affichage | "Agurin" |
| `team` | string | Équipe esport (si applicable) | "VIT", None |
| `role` | string | Rôle | "PRO", "STREAMER" |
| `main_lane` | dict | Lane principale + % | {"value": "JUNGLE", "percentage": 87} |
| `tier` | string | Rang actuel | "CHALLENGER" |
| `lp` | int64 | League Points | 2161 |
| `wins` | int64 | Victoires | 948 |
| `losses` | int64 | Défaites | 782 |
| `winrate` | float64 | Taux de victoire (%) | 54.80 |
| `kda` | float64 | KDA moyen | 2.94 |
| `champion_ids` | list | Champions les plus joués | [59, 60, 131, 121] |
| `challenger_cutoff_lp` | int64 | LP requis Challenger | 1183 |
| `grandmaster_cutoff_lp` | int64 | LP requis Grandmaster | 831 |

### Utilisation

```python
from src.collect_data.dpmlol_scraper import DPMLOLScraper

with DPMLOLScraper() as scraper:
    # Récupérer les leaderboards (EUW, NA, KR)
    df = scraper.get_leaderboards_from_home()

    # Filtrer par région
    euw_top = df[df['platform'] == 'euw1']

    # Voir les LP cutoffs
    cutoffs = df.groupby('platform')[['challenger_cutoff_lp', 'grandmaster_cutoff_lp']].first()
```

---

## Comparaison avec OP.GG

| Métrique | OP.GG | DPM.LOL |
|----------|-------|---------|
| Winrate | ✅ | ✅ |
| Pickrate | ✅ | ✅ |
| Banrate | ✅ | ✅ |
| KDA | ✅ | ❌ |
| Tier (1-5) | ✅ | ❌ |
| TierScore composite | ❌ | ✅ |
| Winrate Variance | ❌ | ✅ |
| Matchups détaillés | ✅ | ❌ (Cloudflare) |
| Synergies | ✅ | ❌ |
| Lane distribution | ❌ | ✅ |
| Leaderboards | ❌ | ✅ (EUW, NA, KR) |
| LP Cutoffs | ❌ | ✅ |

**Recommandation :** Utiliser les deux sources pour maximiser les features disponibles.
- **OP.GG** : Matchups, synergies, KDA
- **DPM.LOL** : TierScore, tendances (variance), leaderboards

---

## Fichiers

```
data/dpmlol/
├── dpmlol_tierlist.parquet    # Tierlist par champion/lane
├── dpmlol_leaderboard.parquet # Top players (EUW, NA, KR)
└── metadata.json              # Métadonnées du scraping

src/collect_data/
└── dpmlol_scraper.py          # Scraper Playwright

src/ML/
└── preprocessing.py           # DPMLOLDataProvider + add_dpmlol_features()
```

---

## Mise à Jour des Données

Pour mettre à jour les données avec le dernier patch :

```python
from src.collect_data.dpmlol_scraper import DPMLOLScraper

with DPMLOLScraper() as scraper:
    # Récupérer la tierlist
    tierlist = scraper.get_tierlist(tier="emerald_plus")

    # Récupérer les leaderboards (EUW, NA, KR)
    leaderboard = scraper.get_leaderboards_from_home()

    # Sauvegarder
    scraper.save_data()

print("Données mises à jour!")
```

**Fréquence recommandée :** Après chaque nouveau patch (toutes les 2 semaines).
