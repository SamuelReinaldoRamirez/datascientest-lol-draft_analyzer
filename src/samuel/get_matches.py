import requests

url = "https://europe.api.riotgames.com/lor/match/v1/matches/by-puuid/kEfMDYjhzBDX1DGz4hneOzoQKZbYzzQQAM7uj9KODCJWwKA1wRLoozDvtsFyWm-ZK8tYkO2eVzIqEA/ids"
api_key = "RGAPI-0cc1e64a-f211-48f3-9c21-a9ac50cf8ec4"

response = requests.get(url, params={"api_key": api_key})

print("Status code:", response.status_code)
print("Response:", response.json())
