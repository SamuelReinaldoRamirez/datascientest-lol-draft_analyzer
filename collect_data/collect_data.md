ce dossier rassemble les scripts, notebooks et autres fichiers qui permettent de collecter la donnée que ce soit par requêtage d'APIs, scrapping ou autre.

collect_champions_by_patch.ipynb sert à récolter les stats des champions par patch (leurs points d'attaque/defense ...). Il s'agit de requêter l'api officielle : Riot API.

collect_general_winrates.ipynb permet (sans tenir compte du side red/blue) de récolter les winrates (et le tier, pickrate...) généraux des champions par patch, serveur, elo. Il s'agit de scrapper le site "dpm lol".

collect_matchup_winrates.ipynb sert à récolter (sans tenir compte du side red/blue) les winrates des matchups et synergies (combinaisons de 2 champions dans la même équipe). Il s'agit de scrapper le site "dpm lol".

collect_pros_games.ipynb permet de traiter 2025_pro_games.csv pour le transformer sous forme de 1 ligne = 1 partie : games_pro_2025_very_light.csv


backfill_timelines.py permet de parcourir les parties jouées de draft_simple.csv et de récupérer des données in-game en faisant des requêtes à riot API. Cela sert pour les prédictions en cours de partie.
