import csv
import json
from riot_api import get_matches_by_puuid, get_match_details

# Fichiers CSV
PLAYERS_CSV = "players.csv"
MATCHES_CSV = "matches.csv"
MATCHES_TXT = "match_details.txt"

def flatten_dict(d, parent_key='', sep='.'):
    """
    Aplati un dictionnaire imbriqué pour avoir des colonnes plates dans le CSV.
    """
    items = []
    if isinstance(d, dict):
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, elem in enumerate(v):
                    list_key = f"{new_key}_{i}"
                    if isinstance(elem, (dict, list)):
                        items.extend(flatten_dict(elem, list_key, sep=sep).items())
                    else:
                        items.append((list_key, elem))
            else:
                items.append((new_key, v))
    else:
        items.append((parent_key, d))
    return dict(items)

def main():
    # 1️⃣ Lire les 10 premiers PUUID depuis players.csv
    players = []
    with open(PLAYERS_CSV, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 10:
                break
            if not row.get("PUUID"):
                print(f"[WARN] PUUID manquant pour : {row}")
                continue
            players.append({"SummonerName": row.get("SummonerName"), "PUUID": row.get("PUUID")})

    if not players:
        print("[ERROR] Aucun joueur valide trouvé dans players.csv.")
        return

    all_rows = []
    all_headers = set()

    # 2️⃣ Récupérer les 5 derniers matchs pour chaque joueur
    for player in players:
        summoner_name = player["SummonerName"]
        puuid = player["PUUID"]
        print(f"➡️  Récupération des matchs pour {summoner_name} ({puuid})")
        try:
            match_ids = get_matches_by_puuid(puuid, count=5)
        except Exception as e:
            print(f"[ERROR] Impossible de récupérer les matchs pour {summoner_name} : {e}")
            continue

        with open(MATCHES_TXT, "w", encoding="utf-8") as f:
            for mid in match_ids:
                try:
                    match_detail = get_match_details(mid)
                    if not match_detail:
                        print(f"[WARN] Pas de données pour le match {mid}")
                        continue

                    # # 🖨️ Affichage lisible du JSON avec indentation
                    # print(f"\n=== Détails du match {mid} ===")
                    # print(json.dumps(match_detail, indent=2, ensure_ascii=False))
                    # # //laaaaaaaaaaaaaaaaaaaaaaaaaaa

                     # 🖨️ Écrire dans le fichier avec indentation
                    f.write(f"=== Détails du match {mid} ===\n")
                    f.write(json.dumps(match_detail, indent=2, ensure_ascii=False))
                    f.write("\n\n")  # Saut de ligne entre chaque match

                    flat = flatten_dict(match_detail)
                    row = {
                        "SummonerName": summoner_name,
                        "PUUID": puuid,
                        "MatchID": mid,
                        **flat
                    }

                    all_headers.update(row.keys())
                    all_rows.append(row)

                except Exception as e:
                    print(f"[ERROR] Erreur sur le match {mid} : {e}")
        # for mid in match_ids:
        #     try:
        #         match_detail = get_match_details(mid)
        #         if not match_detail:
        #             print(f"[WARN] Pas de données pour le match {mid}")
        #             continue

        #          # 🖨️ Affichage lisible du JSON avec indentation
        #         print(f"\n=== Détails du match {mid} ===")
        #         print(json.dumps(match_detail, indent=2, ensure_ascii=False))
        #         # //laaaaaaaaaaaaaaaaaaaaaaaaaaa

        #         flat = flatten_dict(match_detail)
        #         row = {
        #             "SummonerName": summoner_name,
        #             "PUUID": puuid,
        #             "MatchID": mid,
        #             **flat
        #         }

        #         all_headers.update(row.keys())
        #         all_rows.append(row)

        #     except Exception as e:
        #         print(f"[ERROR] Erreur sur le match {mid} : {e}")

    # 3️⃣ Écrire toutes les infos dans matches.csv
    if not all_rows:
        print("[INFO] Aucun match récupéré, CSV non créé.")
        return

    with open(MATCHES_CSV, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(all_headers))
        writer.writeheader()
        for row in all_rows:
            safe_row = {k: ("" if v is None else v) for k, v in row.items()}
            writer.writerow(safe_row)

    print(f"[INFO] matches.csv créé avec {len(all_rows)} lignes et {len(all_headers)} colonnes.")

if __name__ == "__main__":
    main()
