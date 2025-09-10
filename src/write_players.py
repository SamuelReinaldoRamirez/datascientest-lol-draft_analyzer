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
    for i in range(1, 2):  # Limite à 100 pages
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


if __name__ == "__main__":
    main()
