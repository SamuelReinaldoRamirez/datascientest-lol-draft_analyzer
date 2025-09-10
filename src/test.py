from riot_api import get_entries, get_summoner_by_puuid, get_summoner_by_name

# entries = get_entries(page=1)
# print(f"Nombre de joueurs récupérés sur la page 1 : {len(entries)}")
# print("Premier joueur :", entries[0])

# # print("Premier joueur :", entries[0]["summonerName"])


entries = get_entries(page=1)
print(f"Nombre de joueurs récupérés sur la page 1 : {len(entries)}")

print(entries[0])

# Récupérer le nom du premier joueur
first_puuid = entries[0]["puuid"]
summoner_info = get_summoner_by_puuid(first_puuid)
print("Premier joueur :", summoner_info)

info = get_summoner_by_name("Faker")
print(info)