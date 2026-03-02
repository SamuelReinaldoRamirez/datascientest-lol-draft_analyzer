# scripts/

Scripts utilitaires d'entraînement, collecte et maintenance.

## Entraînement

| Script | Description |
|---|---|
| `train_with_winrates_v3.py` | Entraînement V3 (summoner stats temporelles) — **version actuelle** |
| `train_with_winrates_v2.py` | Entraînement V2 (corrections encodage + régularisation) |
| `train_with_winrates.py` | Entraînement V1 (premier pipeline avec winrates externes) |
| `train_all_models.py` | Entraînement batch de tous les modèles |
| `train_multi_timestamp.py` | Entraînement multi-timestamps (draft → @20min) |

## Collecte & données

| Script | Description |
|---|---|
| `collect_summoner_data.py` | Collecte des données summoner (level, XP, mastery) |
| `compute_temporal_summoner_stats.py` | Calcul des stats summoner par mois (pas de fuite temporelle) |
| `backfill_timelines_fast.py` | Backfill rapide des timelines de matchs |
| `backfill_level_xp.py` | Backfill du level et XP des summoners |

## Maintenance

| Script | Description |
|---|---|
| `wait_and_retrain.py` | Pipeline automatique : attente + réentraînement |
