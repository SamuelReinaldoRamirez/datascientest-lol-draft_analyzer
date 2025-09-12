import csv
import json
# from riot_api import get_matches_by_puuid, get_match_details, get_account_by_puuid, get_summoner_by_puuid


# {
#     "puuid": "kEfMDYjhzBDX1DGz4hneOzoQKZbYzzQQAM7uj9KODCJWwKA1wRLoozDvtsFyWm-ZK8tYkO2eVzIqEA",
#     "gameName": "Samk930",
#     "tagLine": "EUW"
# }


MATCH_SAM = "sam.txt"
KEYS_MATCH = "keys.txt"

def extract_keys(obj, parent_key=""):
    keys = []
    if isinstance(obj, dict):
        for k,v in obj.items():
            full_key = f"{parent_key}.{k}" if parent_key else k
            keys.append(full_key)
            keys.extend(extract_keys(v, full_key))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            keys.extend(extract_keys(item, parent_key))
    return keys


def main():
    with open(MATCH_SAM, mode="r", encoding="utf-8") as f:
        data = f.read()        
    try:
        obj = json.loads(data)
    except json.JSONDecodeError:
        obj = eval(data)
    all_keys = extract_keys(obj)
    with open(KEYS_MATCH, "w", encoding="utf-8") as w:
        for k in sorted(set(all_keys)):
            print(k)
            w.write(k)
            w.write("\n\n")


if __name__ == "__main__":
    main()