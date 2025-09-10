# import time
# from riot_api import get_entries, get_league, get_matches_by_puuid, get_match_details

# def main():
#     print("=== Récupération des joueurs Diamond I ===")
#     page = 1
#     all_entries = []

#     # 1️⃣ Récupérer toutes les entrées Diamond I
#     while True:
#         entries = get_entries(page)
#         if not entries:
#             break
#         all_entries.extend(entries)
#         print(f"Page {page} récupérée : {len(entries)} joueurs")
#         page += 1

#     print(f"\nTotal de joueurs Diamond I : {len(all_entries)}")

#     # 2️⃣ Lister toutes les ligues uniques
#     league_ids = {entry["leagueId"] for entry in all_entries}
#     print(f"Nombre de ligues uniques : {len(league_ids)}\n")

#     for lid in league_ids:
#         league = get_league(lid)
#         print(f"Ligue : {league.get('name')}, Queue : {league.get('queue')}, Joueurs : {len(league.get('entries', []))}")

#     # 3️⃣ Récupérer les match IDs pour chaque joueur
#     print("\n=== Récupération des match IDs ===")
#     all_matches = {}
#     for entry in all_entries:
#         puuid = entry["puuid"]
#         match_ids = get_matches_by_puuid(puuid)
#         all_matches[puuid] = match_ids
#         print(f"{entry['summonerName']} → {len(match_ids)} matchs")
#         time.sleep(0.1)  # pour éviter le rate limit Riot

#     # 4️⃣ Récupérer les détails de chaque match
#     print("\n=== Récupération des détails des matchs ===")
#     for puuid, match_ids in all_matches.items():
#         print(f"\nDétails des matchs pour le joueur {puuid}:")
#         for mid in match_ids:
#             match_detail = get_match_details(mid)
#             print(f"Match ID : {mid}, Queue : {match_detail.get('info', {}).get('queueId')}, Durée : {match_detail.get('info', {}).get('gameDuration')} sec")
#             time.sleep(0.1)  # pour éviter le rate limit

# if __name__ == "__main__":
#     main()


import time
import csv
from riot_api import get_entries, get_league, get_matches_by_puuid, get_match_details

# Fichiers CSV
PLAYERS_CSV = "players.csv"
MATCHES_CSV = "matches.csv"

def main():
    print("=== Récupération des joueurs Diamond I ===")
    page = 1
    all_entries = []

    # # 1️⃣ Récupérer toutes les entrées Diamond I
    # while True:
    #     entries = get_entries(page)
    #     if not entries:
    #         break
    #     all_entries.extend(entries)
    #     print(f"Page {page} récupérée : {len(entries)} joueurs")
    #     page += 1

    # 1️⃣ Récupérer toutes les entrées Diamond I
    for i in range(1, 7):  # Limite à 100 pages
        entries = get_entries(i)
        if not entries:
            break
        all_entries.extend(entries)
        print(f"Page {i} récupérée : {len(entries)} joueurs")

    ###############################################################

    print(f"\nTotal de joueurs Diamond I : {len(all_entries)}")

    # 2️⃣ Lister toutes les ligues uniques
    league_ids = {entry["leagueId"] for entry in all_entries}
    print(f"Nombre de ligues uniques : {len(league_ids)}\n")

    # Export joueurs CSV
    with open(PLAYERS_CSV, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["SummonerName", "PUUID", "LeagueId", "LeaguePoints", "Wins", "Losses", "Rank"])
        for entry in all_entries:
            writer.writerow([
                entry.get("summonerName"),
                entry.get("puuid"),
                entry.get("leagueId"),
                entry.get("leaguePoints"),
                entry.get("wins"),
                entry.get("losses"),
                entry.get("rank")
            ])
    print(f"Joueurs exportés dans {PLAYERS_CSV}")

    # 3️⃣ Récupérer les matchs de chaque joueur et exporter
    with open(MATCHES_CSV, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["SummonerName", "PUUID", "MatchID", "QueueId", "GameDuration"])
        for entry in all_entries:
            puuid = entry.get("puuid")
            summoner_name = entry.get("summonerName")
            try:
                match_ids = get_matches_by_puuid(puuid, count=5)  # on récupère 5 derniers matchs
            except Exception as e:
                print(f"Erreur pour {summoner_name} : {e}")
                continue

            for mid in match_ids:
                try:
                    match_detail = get_match_details(mid)
                    info = match_detail.get("info", {})
                    print(f"Match ID : {mid}, infos : {info}")
                    writer.writerow([
                        summoner_name,
                        puuid,
                        mid,
                        info.get("queueId"),
                        info.get("gameDuration")
                    ])
                except Exception as e:
                    print(f"Erreur match {mid} : {e}")

                time.sleep(0.1)  # limiter le rate limit
            time.sleep(0.1)  # limiter le rate limit global

    print(f"Matchs exportés dans {MATCHES_CSV}")

if __name__ == "__main__":
    main()
