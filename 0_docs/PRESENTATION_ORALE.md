# LoL Draft Predictor — Guide de Presentation Orale

**Projet DataScientest — Machine Learning Engineer**
**Auteurs** : Aissam, Samuel, Guilhem | **Promotion** : MLE25 | **Date** : Mars 2026

---

## Table des matieres

1. [Introduction & Contexte](#1-introduction--contexte)
2. [Collecte de donnees](#2-collecte-de-donnees)
3. [Feature Engineering](#3-feature-engineering)
4. [Donnees externes (dpm.lol)](#4-donnees-externes-dpmlol)
5. [Audit des fuites de donnees (V1 → V2 → V3)](#5-audit-des-fuites-de-donnees-v1--v2--v3)
6. [Entrainement des modeles](#6-entrainement-des-modeles)
7. [Resultats](#7-resultats)
8. [Application Streamlit](#8-application-streamlit)
9. [Conclusions & Perspectives](#9-conclusions--perspectives)

---

## 1. Introduction & Contexte

### 1.1 League of Legends
- **MOBA** de Riot Games, 180M+ joueurs actifs, esport majeur mondial
- **2 equipes de 5 joueurs**, chacun controle 1 champion (160+ personnages)
- **5 roles** : Top, Jungle, Mid, ADC, Support
- **Objectif** : detruire le Nexus adverse (~25-35 min)

### 1.2 La phase de Draft
- **Avant chaque match** : 5 bans + 10 picks par equipe
- Choix strategique : synergies, counter-picks, matchups de lane, meta
- Represente **40-60%** de l'issue du match selon les pros

### 1.3 Problematique ML
> **Peut-on predire l'issue d'un match LoL a partir du draft et des donnees d'early game ?**

**Objectifs** :
1. Predire la victoire (classification binaire) a differents instants
2. Identifier les facteurs cles de victoire
3. Analyser synergies et counter-picks
4. Mesurer l'evolution de la precision selon le temps de jeu

**Defis** :
- 160+ champions → espace combinatoire enorme
- Meta evolutive (patch toutes les 2 semaines)
- Facteur humain non observe (skill individuel, communication)
- Donnees timeline pas toujours disponibles

---

## 2. Collecte de donnees

### 2.1 Sources
| Source | Donnees | Usage |
|--------|---------|-------|
| **Riot Games API** | Matchs, joueurs, timelines | Donnees principales |
| **dpm.lol** | Winrates, matchups, synergies | Features externes |
| **CommunityDragon** | Icons, metadata champions | Interface Streamlit |

### 2.2 Dataset
- **305,000+ matchs** collectes
- **164,000+ matchs** avec donnees timeline (gold/CS par minute)
- **Region** : EUW (Europe West)
- **Elo** : Diamond+ → Challenger
- **Saison 15 (2025)** — Ranked Solo/Duo

### 2.3 Base de donnees SQLite
Structure : 7 tables principales

| Table | Lignes | Description |
|-------|--------|-------------|
| `matches` | ~305k | Infos match (duree, creation, patch) |
| `player_stats` | ~3M | Stats joueur par match (10 par match) |
| `team_stats` | ~610k | Stats equipe (2 par match) |
| `match_timeline` | ~164k | Timeline gold/CS/XP par minute |
| `match_events` | ~16M | Events (kills, dragons, towers) |
| `summoners` | ~226k | Infos invocateurs |
| `champion_mastery` | — | (collecte separee) |

### 2.4 Pipeline de collecte
```
Riot API  →  RateLimiter (20/1s, 100/2min)  →  DataCollector  →  SQLite
                                                    ↓
                                           Sauvegarde incrementale
                                           Reprise apres interruption
                                           Backoff exponentiel
```

- 4 cles API en rotation
- Collecte incrementale (pas de duplicata)
- Gestion robuste des erreurs et timeouts

---

## 3. Feature Engineering

### 3.1 Pipeline global
```
┌──────────────┐     ┌────────────────────┐     ┌──────────────┐     ┌──────────────┐
│  Base SQLite  │ ──► │ Feature Engineering │ ──► │   Vecteur    │ ──► │  Modele ML   │
│  (305k matchs)│     │                    │     │  d'entree    │     │  (XGBoost/   │
│  7 tables    │     │ • Winrates externes │     │  (153-186    │     │   LightGBM)  │
│              │     │ • Matchups par lane │     │   features)  │     │              │
│  dpm.lol     │     │ • Synergies/Counter │     │              │     │  Prediction  │
│  (winrates)  │     │ • Summoner stats    │     │              │     │  Win/Loss    │
│              │     │ • Timeline gold/CS  │     │              │     │              │
└──────────────┘     └────────────────────┘     └──────────────┘     └──────────────┘
```

### 3.2 Approche multi-vecteurs (5 modeles)
| Vecteur | Nb features | Donnees utilisees |
|---------|-------------|-------------------|
| **Draft** | 153 | Winrates, matchups, synergies, summoner stats temporelles |
| **@5min** | 171 | Draft + gold par role @5min |
| **@10min** | 186 | Draft + gold/CS par role @10min |
| **@15min** | 186 | Draft + gold par role @15min |
| **@20min** | 186 | Draft + gold par role @20min |

### 3.3 Detail des features

#### A. Features Draft (153 features en V3)

**Winrates externes dpm.lol (36 features) :**
- `ext_wr_{team}_{pos}` : winrate du champion dans ce role (10)
- `ext_tier_{team}_{pos}` : tier encode S+=6, S=5, A=4, B=3, C=2, D=1 (10)
- `ext_pickrate_{team}_{pos}` : popularite du champion (10)
- `ext_avg_wr_{team}` : winrate moyen par equipe (2)
- `ext_avg_tier_{team}` : tier moyen par equipe (2)
- `ext_wr_diff` : ecart de winrate moyen entre equipes (1)
- `ext_tier_diff` : ecart de tier moyen (1)

**Matchups par lane (13 features) :**
- `matchup_wr_{pos}` : winrate du duel par lane (5)
- `matchup_advantage_{pos}` : WR - 0.5 (5)
- `avg_matchup_advantage` : avantage moyen sur 5 lanes (1)
- `max_matchup_advantage` : meilleur matchup (1)
- `min_matchup_advantage` : pire matchup (1)

**Synergies et Counters (7 features) :**
- `team_{t}_synergy_score` : synergie intra-equipe (2)
  - Calculees sur nos 305k matchs (min 30 games)
  - Paires de champions dans la meme equipe
- `team_{t}_counter_score` : counter inter-equipes (2)
  - Champions d'une equipe contre ceux de l'autre
- `synergy_diff`, `counter_diff` : ecarts (2)
- `draft_advantage` : synergy_diff + counter_diff (1)

**Synergies externes dpm.lol (3 features) :**
- `ext_synergy_{team}` : synergie depuis dpm.lol (2)
- `ext_synergy_diff` : ecart (1)

**Summoner stats temporelles V3 (93 features) :**

*Per-position (80 features = 8 × 5 positions × 2 equipes) :*
- `{team}_{pos}_role_pct` : % de games dans ce role (specialisation) (10)
- `{team}_{pos}_role_winrate` : winrate dans ce role (10)
- `{team}_{pos}_mastery_points` : points de maitrise (log1p) (10)
- `{team}_{pos}_streak_type` : serie en cours (+1 win, -1 loss, 0 none) (10)
- `{team}_{pos}_streak_length` : longueur de la serie (10)
- `{team}_{pos}_role_kda` : KDA moyen dans ce role (10)
- `{team}_{pos}_role_vision` : vision score moyen dans ce role (10)
- `{team}_{pos}_champ_recent_wr` : winrate recent sur ce champion (10)

*Agregats equipe (10 features = 5 × 2 equipes) :*
- `{team}_avg_role_specialization` : specialisation moyenne (2)
- `{team}_min_role_specialization` : specialisation min (indicateur autofill) (2)
- `{team}_avg_role_winrate` : winrate moyen de l'equipe (2)
- `{team}_total_mastery_points` : somme des points de maitrise (2)
- `{team}_streak_momentum` : somme (type × longueur) de series (2)

*Differentiels (4 features) :*
- `role_specialization_diff`, `role_winrate_diff`, `streak_momentum_diff`, `mastery_diff`

> **IMPORTANT** : ces stats sont calculees **temporellement** (seuls les matchs anterieurs
> au mois courant sont utilises), contrairement a la V1 qui les calculait sur tous les matchs
> (y compris le match courant → fuite de donnees).

#### B. Features Timeline (18-33 features supplementaires)

**Gold par role (10 features par timestamp) :**
- `team_{t}_{pos}_gold_at_{minute}` pour chaque equipe/role

**Gold diffs (6 features par timestamp) :**
- `gold_diff_at_{minute}` : diff totale
- `{pos}_gold_diff_at_{minute}` : diff par lane (5)

**CS a @10min+ (15 features supplementaires) :**
- `team_{t}_{pos}_cs_at_10` : CS par role (10)
- `{pos}_cs_diff_at_10` : diff CS par lane (5)

### 3.4 Standardisation
- `StandardScaler` applique sur toutes les features avant entrainement
- Necessaire pour la regularisation et la convergence

---

## 4. Donnees externes (dpm.lol)

### 4.1 Sources et volumes

| Donnee | Fichiers | Entries | Description |
|--------|----------|---------|-------------|
| **Winrates simples** | 1 CSV raffine | 212 | Champion+role → WR, tier, pickrate |
| **Matchups** | 220+ CSV | 41,145 | ChampA vs ChampB par lane → WR |
| **Synergies** | 220+ CSV | 34,314 | ChampA + ChampB par lane → WR |

### 4.2 Traitement des winrates simples

**Fichier** : `data/winrates/les winrates simples/données raffinées/df_Simple_WR_FULL.csv`

**Colonnes** : `elo, server, patch, name, role, tier, winrate, pickrate, games`

**Pipeline** :
1. Filtrer `elo == 'TOUT'` et `server == 'TOUT'` (tous rangs, tous serveurs)
2. Garder uniquement le **patch le plus recent** (16.3)
3. **Normaliser les noms** de champions :
   - CSV utilise `"Dr.Mundo"`, `"Aurelion Sol"`, `"Jarvan IV"`...
   - DB utilise `"DrMundo"`, `"AurelionSol"`, `"JarvanIV"`...
   - Table de correspondance de 17 champions problematiques
4. **Parser** les valeurs : `"51.6%"` → `0.516`, tier `"S+"` → `6`
5. **Stocker** dans un dictionnaire : `(champion_db, role)` → `{winrate, pickrate, tier_num}`
6. **Valeurs par defaut** si champion non trouve : WR=0.5, tier=0, pickrate=0.0

**Exemple** :
```python
wr_dict[("Jinx", "adc")] = {"winrate": 0.516, "pickrate": 0.12, "tier_num": 5}
# → ext_wr_100_adc = 0.516, ext_tier_100_adc = 5, ext_pickrate_100_adc = 0.12
```

### 4.3 Traitement des matchups

**Fichiers** : `data/winrates/les winrates matchups/`
- Racine : `*_top_matchup_TOUT_TOUT_16.3.csv` (top lane, patch recent)
- Sous-dossier : `TOUTTOUT15.24/matchups/` → 5 dossiers (top, jun, mid, adc, sup) × 44 CSV

**Format CSV** (wide) :
```
champion | matchup_top_1_name | matchup_top_1_winrate | matchup_top_2_name | ...
Darius   | Garen              | 55.97%                | Nasus              | ...
```

**Pipeline** :
1. Scanner tous les CSV dans chaque dossier de lane
2. Pour chaque ligne : extraire champion principal
3. Iterer sur les colonnes `matchup_{lane}_{i}_name` / `matchup_{lane}_{i}_winrate` (i=1..60)
4. Normaliser les noms, parser le winrate
5. Stocker : `(champA, champB, role)` → `winrate_champA`
6. **Symetrie** : si `(A, B, top)` = 55.97%, alors `(B, A, top)` = 1 - 0.5597 = 44.03%

**Exemple** :
```python
matchup_dict[("Darius", "Garen", "top")] = 0.5597
# Pour le match : matchup_wr_top = 0.5597, matchup_advantage_top = 0.0597
```

### 4.4 Traitement des synergies externes

**Fichiers** : `data/winrates/les winrates matchups/TOUTTOUT15.24/synergies/`
- 5 dossiers (top, jun, mid, adc, sup) × 44 CSV

**Format CSV** (wide) :
```
champion | synergy_adc_1_name | synergy_adc_1_winrate | synergy_adc_2_name | ...
Jinx     | Nami               | 54.2%                 | Lulu               | ...
```

**Pipeline** :
1. Scanner tous les CSV dans chaque dossier de lane
2. Extraire paires (champion, partenaire) avec winrate
3. **Cle triee** pour eviter doublons : `tuple(sorted([champA, champB]) + [role])`
4. Stocker : `(sorted_champA, sorted_champB, role)` → `synergy_winrate`

**Exemple** :
```python
synergy_dict[("Jinx", "Nami", "adc")] = 0.542
# → ext_synergy_100 = moyenne des synergies intra-equipe blue
```

### 4.5 Synergies/Counters internes (nos matchs)

**Calcul sur nos 305k matchs (train uniquement)** :
- **Synergies** : pour chaque paire de champions dans la meme equipe (combinaisons de 2 parmi 5)
  - Minimum 30 games ensemble pour etre inclus
  - `synergy_wr[(champA, champB)]` = winrate quand ils jouent ensemble
  - 13,569 paires de synergies
- **Counters** : pour chaque champion d'une equipe contre chaque champion adverse
  - `counter_wr[(champA, champB)]` = winrate de A quand B est en face
  - 28,912 paires de counters

**Stockage** : `data/features/champion_synergy_counter_v2.pkl`

---

## 5. Audit des fuites de donnees (V1 → V2 → V3)

### 5.1 Contexte V1

Le modele V1 utilisait 192 features incluant des **summoner stats** :
- `role_winrate_diff` : difference de winrate par role entre equipes
- `streak_momentum_diff` : momentum de series de victoires
- `{team}_{pos}_role_winrate` : winrate du joueur dans ce role (10 features)
- `{team}_{pos}_mastery_points` : points de maitrise (10 features)
- `{team}_{pos}_streak_*` : series de victoires (20 features)
- `{team}_{pos}_role_kda` : KDA par role (10 features)
- Et toutes les features derivees... (**93 features au total**)

**Resultats V1 apparents** : Draft = 83.9% → semblait excellent !

### 5.2 Detection de la fuite

**Diagnostic statistique** :
```python
# Correlation de role_winrate_diff avec la target (team_100_win)
Train : 0.81   # Corrélation MASSIVE
Test  : 0.02   # Quasi nulle !

# Correlation de streak_momentum_diff
Train : 0.37
Test  : 0.008
```

**Cause racine** : La requete SQL dans `database.py:get_all_summoner_stats_batch()`
calculait les winrates du joueur sur **TOUS les matchs**, y compris le match courant.
→ Le resultat du match etait encode dans les features d'entree.

**Preuve** : Non-zero correlation = 0.81 en train mais 0.02 en test
(le test set contient des matchs futurs dont les stats n'etaient pas encore calculees)

### 5.3 Autres problemes identifies

| # | Probleme | Impact |
|---|----------|--------|
| 1 | **Fuite role_winrate_diff** | corr=0.81 train, 0.02 test |
| 2 | **Fuite streak/mastery** | corr=0.37/0.008 |
| 3 | **Champion IDs ordinaux** | encodage sans sens (ID 1 ≠ meilleur que ID 100) |
| 4 | **Ban IDs, Spell IDs** | meme probleme ordinal |
| 5 | **Split random pour in-game** | pas de respect temporel |
| 6 | **Overfitting** | val/test gap de 32.2% pour draft |
| 7 | **Regularisation faible** | profondeur trop grande, lr trop eleve |

### 5.4 Corrections V2

1. **SUPPRESSION** des 93 features summoner stats
2. **SUPPRESSION** des champion/ban/spell IDs
3. **SPLIT TEMPOREL** pour tous les modeles (80% train / 20% test, tries par date)
4. **REGULARISATION renforcee** :
   - Draft : max_depth=4, lr=0.01, min_child_weight=20, reg_alpha=1.0, reg_lambda=5.0
   - In-game : max_depth=6, lr=0.03, min_child_weight=10, reg_alpha=0.5, reg_lambda=2.0
5. **EARLY STOPPING** (50 rounds) avec 15% validation temporelle
6. **FEATURES DERIVEES** ajoutees : gold_diff par role, CS diff par role
7. **SYNERGIES recalculees** sur train uniquement (pas de data leakage)

### 5.5 Solution V3 : Summoner Stats Temporelles

**Probleme** : les summoner stats apportent un signal reel (experience du joueur sur son role,
forme recente), mais etaient calculees de maniere circulaire en V1.

**Solution V3** : calcul **temporel par bucket mensuel**

1. Le train set est divise en buckets mensuels
2. Pour chaque bucket, les stats joueur ne sont calculees que sur les matchs **anterieurs au debut du mois**
3. Pour le test set, on utilise `max(train.game_creation)` comme cutoff
4. Script : `scripts/compute_temporal_summoner_stats.py`

**Verification** : le diagnostic montre que la correlation train/test est **coherente**
(pas de gap > 10x), confirmant l'absence de fuite.

```
Scripts V3 :
  1. python scripts/compute_temporal_summoner_stats.py   (~1h, calcul des stats temporelles)
  2. python scripts/train_with_winrates_v3.py             (~3min, entrainement des modeles)
```

### 5.6 Impact (V1 → V2 → V3)

| Metrique | V1 (avec fuite) | V2 (corrige) | V3 (temporel) |
|----------|-----------------|--------------|---------------|
| Draft accuracy | 83.9% | 53.6% | **54.0%** |
| Nb features (draft) | 192 | 59 | **153** |
| Val/Test gap (draft) | 32.2% | 0.5% | **6.1%** |
| Val/Test gap (in-game) | 5-10% | < 1% | **< 1%** |
| Summoner stats | ❌ Fuite | ❌ Supprimees | ✅ Temporelles |
| Confiance | ❌ Faux | ✅ Honnetes | ✅ Honnetes + enrichi |

**Message cle** : Mieux vaut 54.0% honnetes que 83.9% avec fuite.
Le gain V3 vs V2 est modeste (+0.4% draft) mais les 93 features enrichissent
le modele avec des signaux reels (experience du joueur, forme, specialisation).

---

## 6. Entrainement des modeles

### 6.1 Scripts principaux
- **Pre-requis** : `scripts/compute_temporal_summoner_stats.py` (calcul des summoner stats temporelles)
- **Entrainement** : `scripts/train_with_winrates_v3.py`

### 6.2 Pipeline d'entrainement

```
1. Calculer stats temp.   →  train_temporal_stats.parquet + test_temporal_stats.parquet
                              (buckets mensuels, stats only from prior matches)
2. Charger donnees temp.  →  train (224k matchs, 93 summoner feats) + test (56k)
                              + timeline.parquet (164k avec gold/CS)
3. Charger donnees ext.   →  winrates (212), matchups (41k), synergies (34k)
4. Ajouter features       →  ext_wr, matchup, synergy, counter, summoner stats, timeline
5. Selectionner features   →  153 (draft) ou 171-186 (in-game)
6. Split temporel          →  80% train / 20% test (tries par game_creation)
7. StandardScaler          →  normalisation des features
8. Entrainer               →  XGBoost ou LightGBM avec early stopping
9. Evaluer                 →  accuracy, AUC-ROC, classification report
10. Sauvegarder            →  models/model_*.pkl
```

### 6.3 Algorithmes

#### XGBoost (Draft)
```python
XGBClassifier(
    n_estimators=1000,          # max arbres
    max_depth=4,                # peu profond → anti-overfitting
    learning_rate=0.01,         # apprentissage lent
    min_child_weight=20,        # feuilles larges
    subsample=0.7,              # bagging
    colsample_bytree=0.7,       # feature sampling
    reg_alpha=1.0,              # regularisation L1
    reg_lambda=5.0,             # regularisation L2
    early_stopping_rounds=50,   # arret precoce
)
```
**Choix** : regularisation forte car le signal draft est faible (54.0%)

#### LightGBM / XGBoost (In-game @5-@20)
```python
# Configuration in-game (identique pour XGBoost et LightGBM)
n_estimators=1000,
max_depth=6,                # plus profond (signal plus fort)
learning_rate=0.03,
min_child_samples=10,       # (min_child_weight=10 pour XGBoost)
subsample=0.8,
colsample_bytree=0.8,
reg_alpha=0.5,
reg_lambda=2.0,
early_stopping_rounds=50,
```
**Choix V3** : XGBoost domine LightGBM pour @10, @15, @20min (meilleure gestion du
feature space elargi avec les summoner stats). LightGBM conserve un avantage a @5min.

### 6.4 Validation
- **15% du train** utilise comme validation (split temporel)
- **Early stopping** : arret si 50 rounds sans amelioration
- **Pas de cross-validation** : split temporel unique (plus realiste)

### 6.5 Selection de features

**Principe V3** : On selectionne les features numeriques par categorie :
- **Inclus** : winrates ext, matchups, synergies/counters, summoner stats temporelles, timeline
- **Exclus** : `champion_id`, `ban`, `spell` (encodage ordinal sans sens)
- **Exclus** : `match_id`, `game_creation`, `game_duration`, `team_100_win` (target)

### 6.6 Contenu des fichiers pickle

Chaque `model_*.pkl` contient :
```python
{
    "model": <XGBClassifier ou LGBMClassifier>,
    "scaler": <StandardScaler>,
    "features": ["ext_wr_100_top", "ext_tier_100_top", ...],
    "model_name": "XGBoost" ou "LightGBM",
    "test_accuracy": 0.540,
    "val_accuracy": 0.601,
    "auc_roc": 0.555,
    "external_data": {
        "wr_dict": {("Jinx", "adc"): {"winrate": 0.516, ...}, ...},
        "matchup_dict": {("Darius", "Garen", "top"): 0.5597, ...},
    },
    "synergy_data": {
        "synergy_wr": {(123, 456): 0.52, ...},
        "counter_wr": {(123, 789): 0.48, ...},
    },
}
```

Les dictionnaires externes sont embarques dans le pickle pour que
l'application Streamlit puisse recalculer les features a la volee
(prediction interactive).

---

## 7. Resultats

### 7.1 Performance des 5 modeles (V3)

| Modele | Features | Algo | Accuracy | AUC-ROC | Val/Test gap |
|--------|----------|------|----------|---------|-------------|
| **Draft** | 153 | XGBoost | **54.0%** | 0.555 | 6.1% |
| **@5min** | 171 | LightGBM | **65.4%** | 0.719 | 0.6% |
| **@10min** | 186 | XGBoost | **72.0%** | 0.797 | -0.6% |
| **@15min** | 186 | XGBoost | **78.0%** | 0.864 | -0.6% |
| **@20min** | 186 | XGBoost | **79.9%** | 0.885 | -0.3% |

### 7.1b Comparaison V2 vs V3

| Modele | V2 (sans summoner) | V3 (avec summoner temporal) | Delta |
|--------|-------------------|---------------------------|-------|
| Draft | 53.6% (59 feat) | **54.0%** (153 feat) | +0.4% |
| @5min | 65.6% (77 feat) | **65.4%** (171 feat) | -0.2% |
| @10min | 72.1% (92 feat) | **72.0%** (186 feat) | -0.1% |
| @15min | 77.8% (92 feat) | **78.0%** (186 feat) | +0.2% |
| @20min | 80.0% (92 feat) | **79.9%** (186 feat) | -0.1% |

**Interpretation** : Les summoner stats temporelles n'apportent pas de gain massif en accuracy
mais enrichissent le modele avec des signaux reels (forme du joueur, specialisation, mastery)
sans introduire de fuite. Le gain est limite par la contrainte temporelle
(on ne peut utiliser que les matchs anterieurs).

### 7.2 Analyse des resultats

**Draft seul (54.0%)** :
- Le draft ne suffit **pas** en solo queue (Diamond+)
- Le skill individuel et l'execution priment
- Mais `draft_advantage` (synergies + counters) reste la feature **la plus predictive**
- Les summoner stats enrichissent le modele mais le signal temporel est faible

**Progression temporelle** :
- @5min : **+11.4%** par rapport au draft → le gold early est tres predictif
- @10min : **+6.6%** supplementaire (avec CS en plus)
- @15min : **+6.0%** supplementaire
- @20min : **+1.9%** supplementaire (rendements decroissants)

**Bonne generalisation** :
- Ecart val/test < 1% sur tous les modeles in-game
- Gap de 6.1% pour le draft (attendu : signal faible + meta shift entre periodes)
- AUC-ROC de 0.885 a @20min → bonne discrimination
- XGBoost domine LightGBM pour @10-@20 en V3

### 7.3 Feature importance (Draft model, top 10)

| Rang | Feature | Importance |
|------|---------|------------|
| 1 | `draft_advantage` | ~14% |
| 2 | `counter_diff` | ~8% |
| 3 | `synergy_diff` | ~6% |
| 4 | `avg_matchup_advantage` | ~5% |
| 5 | `ext_wr_diff` | ~4% |
| 6 | `ext_tier_diff` | ~3.5% |
| 7 | `matchup_advantage_mid` | ~3% |
| 8 | `matchup_advantage_top` | ~2.8% |
| 9 | `ext_synergy_diff` | ~2.5% |
| 10 | `matchup_advantage_adc` | ~2.2% |

**Interpretation** : Les features aggregees (draft_advantage, counter_diff, synergy_diff)
dominent car elles capturent la dynamique globale du draft.

### 7.4 Feature importance (@20min model, top 5)

| Rang | Feature | Importance |
|------|---------|------------|
| 1 | `gold_diff_at_20` | ~15% |
| 2 | `mid_gold_diff_at_20` | ~8% |
| 3 | `jungle_gold_diff_at_20` | ~7% |
| 4 | `adc_gold_diff_at_20` | ~6% |
| 5 | `top_gold_diff_at_20` | ~5% |

**Interpretation** : Le gold diff total et par lane domine completement.
Les features draft deviennent marginales quand les donnees in-game sont disponibles.

---

## 8. Application Streamlit

### 8.1 Architecture

```
streamlit_app/
├── app.py                          # Page d'accueil + stats + navigation
├── config.py                       # Constantes, couleurs, chemins, benchmarks
├── pages/
│   ├── 1_📊_Presentation.py        # Contexte LoL, problematique, defis
│   ├── 2_📁_Donnees.py             # Exploration dataset, BDD, visualisations
│   ├── 3_⚙️_Traitement.py          # Pipeline, features, fuite V1→V2→V3, selecteur vecteur
│   ├── 4_🤖_Modeles.py             # Algorithmes, comparaison, feature importance
│   └── 5_🎯_Resultats.py           # Evaluation, metriques, prediction interactive
├── components/
│   ├── gold_chart.py               # Graphique timeline gold
│   └── prediction_gauge.py         # Jauge de probabilite de victoire
└── utils/
    ├── data_loader.py              # Chargement BDD SQLite + parquets
    ├── feature_builder.py          # Reconstruction features V3 a la volee (incl. summoner stats)
    └── model_loader.py             # Chargement modeles pkl, prediction
```

### 8.2 Description des 5 pages

#### Page 1 — Presentation
- Introduction a League of Legends (MOBA, 180M+ joueurs)
- Explication des 5 roles et de la phase de draft
- Problematique ML et objectifs
- Defis scientifiques
- Schema de l'approche multi-modeles
- Statistiques du dataset (dynamiques depuis la BDD)

#### Page 2 — Donnees
- **Tab Vue d'ensemble** : metriques globales (305k matchs, 164k timelines, blue WR, duree)
- Structure de la BDD SQLite (7 tables, nombre de lignes/colonnes)
- Fichiers parquet traites
- **Tab Exploration** : apercu, types, valeurs manquantes, statistiques descriptives
- **Tab Visualisations** : WR par cote, distribution duree, champions joues, correlation

#### Page 3 — Traitement
- Diagramme du pipeline complet (SQLite → Features → Vecteur → Modele)
- Detail du feature engineering (draft, matchup, synergy, timeline)
- Presentation des donnees externes dpm.lol (3 types, volumes)
- Features derivees avec formules
- **Section fuite de donnees** : detection V1, correction V2 (suppression), resolution V3 (temporel)
- Selecteur de vecteur d'entree (radio buttons, metriques, comparaison)

#### Page 4 — Modeles
- Presentation XGBoost vs LightGBM (parametres, choix justifie)
- Metriques du modele selectionne (accuracy, validation, nb features)
- Verification overfitting (ecart val/test < 5% = ok)
- Bar chart comparatif des 5 modeles (avec baseline 50%)
- Feature importance interactive (slider pour le nombre, graphique horizontal)

#### Page 5 — Resultats
- **Tab Resultats** : evaluation complete sur le jeu de test
  - Accuracy, precision, recall
  - Matrice de confusion (heatmap Plotly)
  - Courbe ROC avec AUC
  - Classification report detaille
  - Tableau comparatif des 5 modeles
  - Bar chart de progression d'accuracy
- **Tab Prediction interactive** :
  - Chargement aleatoire d'un match reel
  - Affichage des compositions d'equipe (noms de champions)
  - Matchups par lane (winrate externe)
  - Sliders pour ajuster le gold (modeles in-game)
  - Prediction avec probabilites
  - Jauge de probabilite (composant custom)
  - Comparaison prediction vs resultat reel

### 8.3 Aspects techniques

**Theme LoL** :
- Background sombre (#0a1428, #1e2328)
- Accents dores (#c8aa6e) style interface de jeu
- Couleurs equipes : bleu (#3498db) et rouge (#e74c3c)
- CSS custom injecte avec `unsafe_allow_html=True`

**Session state** :
- `vector_type` : persist entre pages (selecteur dans sidebar + page Traitement)
- `random_match_idx` : match courant pour prediction interactive

**Caching** :
- `@st.cache_data(ttl=3600)` sur toutes les fonctions de chargement
- 1 heure de cache pour eviter les requetes repetees a la BDD

**Feature builder** (`feature_builder.py`) :
- Reconstruit toutes les features V3 a la volee (y compris summoner stats depuis la BDD)
- Utilise les dictionnaires embarques dans les pickle
- Permet la prediction interactive sans preprocesser tout le dataset

### 8.4 Lancement
```bash
cd datascientest-lol-draft_analyzer
streamlit run streamlit_app/app.py --server.port 8501
```

---

## 9. Conclusions & Perspectives

### 9.1 Conclusions

1. **Le draft seul ne suffit pas** en solo queue Diamond+ (~54.0%)
   - Le facteur humain (skill, communication, fatigue) prime
   - Mais les synergies/counters sont des signaux mesurables
   - Les summoner stats temporelles (experience, forme) enrichissent mais ne revelent pas tout

2. **L'early game est tres predictif**
   - Des 5 minutes : 65.4% (+11.4% vs draft seul)
   - A 20 minutes : 79.9% (AUC 0.885)
   - Le gold diff par lane est la feature dominante

3. **L'audit des fuites est indispensable**
   - V1 : 83.9% → faux (fuite massive)
   - V2 : 53.6% → honnete (features supprimees)
   - V3 : 54.0% → honnete et enrichi (features temporelles)
   - La rigueur methodologique est non-negociable

4. **Les donnees externes enrichissent le modele**
   - dpm.lol apporte 52 features utiles (winrates, matchups, synergies)
   - Summoner stats temporelles (93 features) : specialisation, forme, experience
   - La combinaison de sources multiples donne les meilleurs resultats

### 9.2 Limites

- **Solo queue ≠ Pro play** : le draft compte plus en pro
- **Meta evolutive** : les modeles doivent etre reentraines regulierement
- **Region unique** (EUW) : pas de generalisation garantie
- **Summoner stats temporelles** : signal limite par la contrainte de ne pas utiliser le match courant
- **164k/305k** matchs seulement avec timeline

### 9.3 Perspectives

1. **Extension multi-regions** (NA, KR, CN)
2. **Reentrainement automatise** a chaque nouveau patch
3. **Assistant de draft en temps reel** (recommandation de picks/bans)
4. **Modeles pro play** (autres dynamiques, plus de donnees structurees)
5. **Deep learning** : embeddings de champions (Word2Vec-like)
6. **Features supplementaires** : objectifs (dragons, herauts), vision score
7. **Modele temporel continu** : prediction a chaque minute (pas juste 5/10/15/20)
8. **Summoner stats glissantes** : fenetre de 30 jours au lieu de buckets mensuels

---

## Annexe A — Fichiers cles du projet

| Fichier | Role |
|---------|------|
| `scripts/compute_temporal_summoner_stats.py` | Calcul des summoner stats temporelles (pre-requis V3) |
| `scripts/train_with_winrates_v3.py` | Script d'entrainement V3 (principal) |
| `scripts/train_with_winrates_v2.py` | Script d'entrainement V2 (reference) |
| `models/model_draft.pkl` | Modele draft V3 (XGBoost, 153 features) |
| `models/model_at5.pkl` | Modele @5min V3 (LightGBM, 171 features) |
| `models/model_at10.pkl` | Modele @10min V3 (XGBoost, 186 features) |
| `models/model_at15.pkl` | Modele @15min V3 (XGBoost, 186 features) |
| `models/model_at20.pkl` | Modele @20min V3 (XGBoost, 186 features) |
| `models/backup_v2/` | Backup des modeles V2 |
| `data/lol_matches.db` | Base SQLite (305k matchs) |
| `data/processed/train_temporal_stats.parquet` | Train set avec summoner stats temporelles |
| `data/processed/test_temporal_stats.parquet` | Test set avec summoner stats temporelles |
| `data/features/champion_synergy_counter_v3.pkl` | Synergies/counters V3 |
| `streamlit_app/app.py` | Point d'entree Streamlit |
| `streamlit_app/config.py` | Configuration (VECTOR_TYPES, MODEL_BENCHMARKS) |
| `streamlit_app/utils/feature_builder.py` | Reconstruction features V3 a la volee |
| `streamlit_app/utils/model_loader.py` | Chargement et prediction |
| `streamlit_app/utils/data_loader.py` | Acces BDD et parquets (V2 + V3) |

## Annexe B — Normalisation des noms de champions

| CSV (dpm.lol) | Base de donnees (Riot API) |
|---------------|--------------------------|
| Aurelion Sol | AurelionSol |
| Bel'Veth | Belveth |
| Cho'Gath | Chogath |
| Dr.Mundo | DrMundo |
| Jarvan IV | JarvanIV |
| K'Sante | KSante |
| Kai'Sa | Kaisa |
| Kha'Zix | Khazix |
| Kog'Maw | KogMaw |
| Master Yi | MasterYi |
| Miss Fortune | MissFortune |
| Rek'Sai | RekSai |
| Tahm Kench | TahmKench |
| Twisted Fate | TwistedFate |
| Vel'Koz | Velkoz |
| Wukong | MonkeyKing |
| Xin Zhao | XinZhao |

## Annexe C — Commandes utiles

```bash
# Lancer le Streamlit
streamlit run streamlit_app/app.py

# === Pipeline V3 (actuel) ===
# Etape 1 : Calculer les summoner stats temporelles (~1h)
python scripts/compute_temporal_summoner_stats.py

# Etape 2 : Entrainer les 5 modeles V3 (~3min)
python scripts/train_with_winrates_v3.py

# Entrainer sans recalculer les synergies
python scripts/train_with_winrates_v3.py --no-synergy-recompute

# === Pipeline V2 (reference) ===
python scripts/train_with_winrates_v2.py
```
