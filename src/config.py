# Multiple API keys for faster collection (rotate between them)
# Add your second API key to the list below
API_KEYS = [
    "RGAPI-b7596442-3c60-47eb-a545-0aa6199e3f0a",  # Clé 1
    # "RGAPI-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",  # Clé 2 - AJOUTEZ VOTRE 2ÈME CLÉ ICI
    "RGAPI-c1a4563d-3efe-44f9-98c4-824dd37bfa05",
]

# Single key for backward compatibility
API_KEY = API_KEYS[0]

REGION = "euw1"            # Région du shard LoL (euw1, na1, kr, etc.)
QUEUE = "RANKED_SOLO_5x5"
TIER = "DIAMOND"
DIVISION = "I"