# data/

Toutes les données du projet (brutes, traitées, features).

## Structure

| Dossier / Fichier | Description |
|---|---|
| `lol_matches.db` | Base SQLite principale (matches, player_stats, team_stats, timelines, summoners, mastery) |
| `processed/` | Parquets d'entraînement et de test (train/test splits temporels) |
| `features/` | Features pré-calculées (synergies, counters, summoner stats) |
| `winrates/` | Winrates externes scrappées (dpm.lol) |
| `dpmlol/` | Données brutes dpm.lol |
| `opgg/` | Données scrappées OP.GG |
| `processed_opgg/` | Données OP.GG traitées |
| `archive/` | Anciennes données archivées |
| `raw/` | Données brutes |
| `*.csv` | Fichiers CSV intermédiaires (champions, winrates, draft) |
| `*.csv.gz` | Exports compressés de la DB |
| `*.log` | Logs de collecte (backfill, summoner data) |

## Sources

- **Riot Games API** : matchs ranked Solo/Duo, EUW, Diamond+ → Challenger, Saison 15
- **dpm.lol** : winrates simples, matchups par lane, synergies
- **OP.GG** : données complémentaires
- **CommunityDragon** : métadonnées champions
