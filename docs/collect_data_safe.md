# Documentation : collect_data_safe.py

## Vue d'ensemble

`collect_data_safe.py` est un script Python conçu pour collecter des données de matchs League of Legends via l'API Riot Games de manière fiable et respectueuse des limites de taux.

## Fonctionnalités principales

- **Rate limiting avancé** avec fenêtre glissante
- **Reprise automatique** après interruption
- **Sauvegarde incrémentale** pour éviter les pertes de données
- **Extraction CSV** automatique des données collectées
- **Logging détaillé** pour le suivi et le débogage

---

## Architecture

### Classes

#### 1. RateLimiter

Gère les limites de taux de l'API Riot Games.

| Limite | Requêtes | Fenêtre |
|--------|----------|---------|
| Court terme | 20 | 1 seconde |
| Long terme | 100 | 2 minutes |

**Méthodes principales :**

| Méthode | Description |
|---------|-------------|
| `can_make_request(endpoint)` | Vérifie si une requête peut être effectuée |
| `record_request(endpoint)` | Enregistre une requête avec son timestamp |
| `handle_429_error(retry_after)` | Gère les erreurs de limitation avec backoff exponentiel |

#### 2. DataCollector

Orchestre la collecte et la sauvegarde des données.

**Attributs de progression :**

```json
{
  "processed_players": [],
  "collected_matches": {},
  "last_page": 1,
  "last_player_index": 0,
  "stats": {
    "total_requests": 0,
    "successful_requests": 0,
    "rate_limit_errors": 0,
    "other_errors": 0
  }
}
```

**Méthodes principales :**

| Méthode | Description |
|---------|-------------|
| `collect_matches(num_players, matches_per_player)` | Collecte les matchs pour N joueurs |
| `make_api_request(func, endpoint, *args)` | Exécute une requête API avec gestion d'erreurs |
| `save_matches_incremental(matches)` | Sauvegarde les matchs de manière incrémentale |
| `extract_to_csv()` | Extrait les données vers des fichiers CSV |
| `save_progress()` | Sauvegarde la progression courante |

---

## Utilisation

### Installation des dépendances

```bash
pip install -r requirements.txt
```

### Commandes

#### Mode simple (une exécution)

```bash
python src/collect_data_safe.py --players 50 --matches 20
```

#### Mode continu (boucle jusqu'à Ctrl+C)

```bash
python src/collect_data_safe.py --continuous --players 50 --matches 20
```

#### Reprendre une collecte interrompue

```bash
python src/collect_data_safe.py --resume --players 50 --matches 20
```

#### Réinitialiser la progression

```bash
python src/collect_data_safe.py --reset --players 50 --matches 20
```

### Options disponibles

| Option | Type | Défaut | Description |
|--------|------|--------|-------------|
| `--players` | int | 50 | Nombre de joueurs à traiter par batch |
| `--matches` | int | 20 | Nombre de matchs à collecter par joueur |
| `--resume` | flag | false | Reprendre depuis la dernière progression |
| `--reset` | flag | false | Réinitialiser la progression et recommencer |
| `--continuous` | flag | false | Exécuter en mode continu |

---

## Flux de données

```
┌─────────────────────────────────────────────────────────────────┐
│                         API Riot Games                          │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                         RateLimiter                             │
│  • Vérifie les limites avant chaque requête                     │
│  • Gère le backoff exponentiel en cas d'erreur 429              │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        DataCollector                            │
│  1. Récupère la liste des joueurs (get_entries)                 │
│  2. Pour chaque joueur :                                        │
│     • Récupère les infos du compte (get_account_by_puuid)       │
│     • Récupère la liste des matchs (get_matches_by_puuid)       │
│     • Pour chaque match :                                       │
│       - Récupère les détails (get_match_details)                │
│       - Filtre : queueId == 420 (Ranked Solo/Duo)               │
│  3. Sauvegarde incrémentale tous les 5 joueurs                  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Fichiers de sortie                           │
├─────────────────────────────────────────────────────────────────┤
│  • match_details_extended.txt    (données brutes JSON)          │
│  • collection_progress.json      (progression)                  │
│  • data_collection.log           (logs)                         │
│  • match_data_detailed.csv       (dataset complet)              │
│  • draft_data_with_bans.csv      (dataset simplifié)            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Fichiers générés

### match_details_extended.txt

Fichier texte contenant les données JSON brutes de chaque match.

### collection_progress.json

Fichier de progression permettant la reprise après interruption.

### data_collection.log

Fichier de log avec toutes les opérations effectuées.

### match_data_detailed.csv

Dataset complet avec toutes les statistiques des matchs :
- Informations générales du match
- Statistiques détaillées de chaque joueur
- Données de draft (champions, bans)

### draft_data_with_bans.csv

Dataset simplifié focalisé sur l'analyse de draft :
- Champions sélectionnés
- Bans
- Résultat (victoire/défaite)
- Statistiques clés (KDA, gold, CS, vision)

---

## Gestion des erreurs

### Erreur 429 (Rate Limit)

1. Le script détecte l'erreur 429
2. Récupère le header `Retry-After` si disponible
3. Sinon, applique un backoff exponentiel : 1s, 2s, 4s, 8s, 16s, 32s (max 60s)
4. Réessaie automatiquement la requête

### Autres erreurs

- Maximum 3 tentatives par requête
- Backoff exponentiel entre les tentatives
- L'erreur est loggée et le script continue avec le prochain élément

---

## Bonnes pratiques

1. **Commencer petit** : Tester avec `--players 5 --matches 5` avant de lancer une grosse collecte

2. **Utiliser le mode continu** pour les grandes collectes :
   ```bash
   python src/collect_data_safe.py --continuous --players 50 --matches 20
   ```

3. **Surveiller les logs** dans `data_collection.log`

4. **Ne pas supprimer** `collection_progress.json` si vous voulez reprendre la collecte

5. **Vérifier la progression** :
   ```bash
   cat collection_progress.json | python -m json.tool
   ```

---

## Étapes suivantes après la collecte

1. **Vérifier les données** :
   ```bash
   wc -l match_data_detailed.csv
   head -5 draft_data_with_bans.csv
   ```

2. **Entraîner le modèle de prédiction** :
   ```bash
   python src/draft_predictor.py
   ```

---

## Dépendances

- `riot_api.py` : Fonctions d'appel à l'API Riot
- `extract_detailed_match_data.py` : Fonctions d'extraction des données

---

## Exemple de sortie

```
2024-01-15 10:30:00 - INFO - Starting collection: 50 players, 20 matches each
2024-01-15 10:30:01 - INFO - Page 1 retrieved: 205 players
2024-01-15 10:30:02 - INFO - [1/50] Processing Player1#EUW
2024-01-15 10:30:05 - INFO -   ✓ Added 15 new matches from Player1#EUW
2024-01-15 10:30:06 - INFO - [2/50] Processing Player2#EUW
...
2024-01-15 10:45:00 - INFO -
=== Collection Statistics ===
2024-01-15 10:45:00 - INFO - Total API requests: 1250
2024-01-15 10:45:00 - INFO - Successful requests: 1248
2024-01-15 10:45:00 - INFO - Rate limit errors: 2
2024-01-15 10:45:00 - INFO - Other errors: 0
2024-01-15 10:45:00 - INFO - Total unique matches: 750
2024-01-15 10:45:00 - INFO - Players processed: 50
```
