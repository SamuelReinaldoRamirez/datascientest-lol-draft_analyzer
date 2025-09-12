import csv
import json
from riot_api import get_matches_by_puuid, get_match_details, get_account_by_puuid, get_summoner_by_puuid


# {
#     "puuid": "kEfMDYjhzBDX1DGz4hneOzoQKZbYzzQQAM7uj9KODCJWwKA1wRLoozDvtsFyWm-ZK8tYkO2eVzIqEA",
#     "gameName": "Samk930",
#     "tagLine": "EUW"
# }


# Fichiers CSV
MATCH_SAM = "sam.txt"

def main():
    puuid = "kEfMDYjhzBDX1DGz4hneOzoQKZbYzzQQAM7uj9KODCJWwKA1wRLoozDvtsFyWm-ZK8tYkO2eVzIqEA"

    gameName = get_account_by_puuid(puuid).get("gameName")
    print(gameName)
    try:
        match_ids = get_matches_by_puuid(puuid, count=2)
    except Exception as e:
        print(f"[ERROR] Impossible de récupérer les matchs pour {summoner_name} : {e}")

    with open(MATCH_SAM, "w", encoding="utf-8") as f:
        for mid in match_ids:
            try:
                match_detail = get_match_details(mid)
                if not match_detail:
                    print(f"[WARN] Pas de données pour le match {mid}")
                    continue
                f.write(f"=== Détails du match {mid} ===\n")
                f.write(json.dumps(match_detail, indent=2, ensure_ascii=False))
                f.write("\n\n")  # Saut de ligne entre chaque match
            except Exception as e:
                print(f"[ERROR] Erreur sur le match {mid} : {e}")


if __name__ == "__main__":
    main()