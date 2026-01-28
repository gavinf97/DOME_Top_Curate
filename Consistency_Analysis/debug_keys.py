import json

FLATTENED_SOURCE = '../DOME_Registry_JSON_Files/flattened_DOME_Registry_Contents_2026-01-09.json'

with open(FLATTENED_SOURCE, 'r') as f:
    data = json.load(f)
    print("Keys in first entry:", list(data[0].keys()))
    
    # Try to find an entry with model/duration or similar
    found = False
    for entry in data:
        for k in entry.keys():
            if 'duration' in k:
                print(f"Found key with duration: {k}")
                found = True
                break
        if found: break
