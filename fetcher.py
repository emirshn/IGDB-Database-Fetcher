import requests
import csv
import time
import json
import os
import pandas as pd
from datetime import datetime
from requests.exceptions import RequestException, ConnectionError, HTTPError

CLIENT_ID = ""
CLIENT_SECRET = ""
api_url = "https://api.igdb.com/v4"
output_csv = "game_dataset.csv"

def requests_post_with_retry(url, headers=None, data=None, max_retries=5, backoff_factor=0.5):
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, data=data)
            response.raise_for_status()
            return response
        except (ConnectionError, HTTPError, RequestException) as e:
            wait = backoff_factor * (2 ** attempt)
            print(f"Warning: Request failed ({e}), retrying in {wait:.1f}s... (attempt {attempt + 1} of {max_retries})")
            time.sleep(wait)
    print(f"Error: Failed to get a successful response from {url} after {max_retries} attempts.")
    return None

# Authenticate once
auth = requests_post_with_retry("https://id.twitch.tv/oauth2/token", data={
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "grant_type": "client_credentials"
})

if auth is None:
    raise Exception("Failed to authenticate with Twitch API")

auth = auth.json()

headers = {
    "Client-ID": CLIENT_ID,
    "Authorization": f"Bearer {auth['access_token']}"
}

lookup_caches = {
    "genres": {}, "platforms": {}, "themes": {}, "game_modes": {},
    "player_perspectives": {}, "franchises": {}, "collections": {},
    "game_engines": {}, "keywords": {}, "companies": {}
}

checkpoint_path = "checkpoint.txt"
cache_path = "lookup_cache.json"

# Load previous checkpoint
if os.path.exists(checkpoint_path):
    with open(checkpoint_path, "r") as f:
        offset = int(f.read().strip())
else:
    offset = 0

# Load previous cache
if os.path.exists(cache_path):
    with open(cache_path, "r", encoding="utf-8") as f:
        lookup_caches.update(json.load(f))

def bulk_fetch_names(endpoint, ids):
    if not ids:
        return {}
    result = {}
    ids = list(set(ids))
    for i in range(0, len(ids), 500):
        chunk = ids[i:i+500]
        body = f"fields id,name; where id = ({','.join(map(str, chunk))}); limit 500;"
        r = requests_post_with_retry(f"{api_url}/{endpoint}", headers=headers, data=body)
        if r:
            for entry in r.json():
                result[entry["id"]] = entry["name"]
        else:
            print(f"Error fetching {endpoint} for ids {chunk}")
        time.sleep(0.25)
    return result

def fetch_games(offset=0, limit=500):
    print(f"Fetching games {offset}–{offset+limit}")
    body = (
        'fields id,name,genres,platforms,summary,rating,aggregated_rating,first_release_date,'
        'themes,player_perspectives,game_modes,franchise,franchises,collections,cover,screenshots,'
        'involved_companies,artworks,game_engines,keywords,similar_games,category; '
        'where category = (0,4,8,9,10) & version_parent = null & summary != null '
        '& (rating > 50 | aggregated_rating > 50 | rating = null | aggregated_rating = null); '
        'sort id asc; '
        f'limit {limit}; offset {offset};'
    )
    r = requests_post_with_retry(f"{api_url}/games", headers=headers, data=body)
    if r:
        return r.json()
    else:
        return []

def fetch_image_urls(endpoint, ids, size='cover_big'):
    if not ids:
        return {}
    result = {}
    ids = list(set(ids))
    for i in range(0, len(ids), 500):
        chunk = ids[i:i+500]
        body = f'fields id,image_id; where id = ({",".join(map(str, chunk))}); limit 500;'
        r = requests_post_with_retry(f"{api_url}/{endpoint}", headers=headers, data=body)
        if r:
            for item in r.json():
                if "image_id" in item:
                    result[item["id"]] = f"https://images.igdb.com/igdb/image/upload/t_{size}/{item['image_id']}.jpg"
        else:
            print(f"Error fetching images for {endpoint} ids {chunk}")
        time.sleep(0.25)
    return result

def fetch_involved_companies(game_ids):
    involved_data = {}
    offset = 0
    while True:
        if not game_ids:
            break
        query = (
            f"fields game,company,developer,publisher,supporting,porting;"
            f" where game = ({','.join(map(str, game_ids))});"
            f" limit 500; offset {offset};"
        )
        response = requests_post_with_retry(f"{api_url}/involved_companies", headers=headers, data=query)
        if not response:
            print(f"Failed to fetch involved companies for offset {offset}")
            break
        results = response.json()
        if not results:
            break
        for entry in results:
            game_id = entry["game"]
            if game_id not in involved_data:
                involved_data[game_id] = []
            involved_data[game_id].append({
                "company_id": entry["company"],
                "developer": entry.get("developer", False),
                "publisher": entry.get("publisher", False),
                "supporting": entry.get("supporting", False),
                "porting": entry.get("porting", False),
            })
        offset += 500
    return involved_data

def safe_timestamp_to_date(ts):
    try:
        if not ts or ts <= 0:
            return ""
        if ts > 1e12: 
            ts = ts / 1000
        return datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
    except Exception:
        return ""
    
category_map = {
    0: "main_game",
    1: "dlc_addon",
    2: "expansion",
    3: "bundle",
    4: "standalone_expansion",
    5: "mod",
    6: "episode",
    7: "season",
    8: "remake",
    9: "remaster",
    10: "expanded_game",
    11: "port",
    12: "fork",
    13: "pack",
    14: "update",
}

def flatten_game(g, cover_urls, ss_urls, involved_data, artwork_urls):
    main_devs, supporting_devs, pubs = [], [], []
    for ic in involved_data.get(g["id"], []):
        cname = lookup_caches["companies"].get(ic["company_id"], str(ic["company_id"]))
        if ic.get("developer", False) and not ic.get("porting", False):
            main_devs.append(cname)
        if ic.get("supporting", False):
            supporting_devs.append(cname)
        if ic.get("publisher", False):
            pubs.append(cname)
    franchise_ids = [g.get("franchise")] if g.get("franchise") else []
    if g.get("franchises"):
        franchise_ids.extend(g["franchises"])
    collection_ids = g.get("collections", []) or []
    similar_games_ids = g.get("similar_games", [])
    category_str = category_map.get(g.get("category"), "unknown")
    return {
        "id": g.get("id"),
        "name": g.get("name"),
        "category": category_str, 
        "release_date": safe_timestamp_to_date(g.get("first_release_date")),
        "rating": g.get("rating"),
        "aggregated_rating": g.get("aggregated_rating"),
        "genres": ", ".join([lookup_caches["genres"].get(i, str(i)) for i in g.get("genres", [])]),
        "themes": ", ".join([lookup_caches["themes"].get(i, str(i)) for i in g.get("themes", [])]),
        "franchise": ", ".join([lookup_caches["franchises"].get(i, str(i)) for i in set(franchise_ids)]),
        "series": ", ".join([lookup_caches["collections"].get(i, str(i)) for i in collection_ids]),
        "main_developers": json.dumps(list(set(main_devs)), ensure_ascii=False),
        "supporting_developers": json.dumps(list(set(supporting_devs)), ensure_ascii=False),
        "publishers": json.dumps(list(set(pubs)), ensure_ascii=False),
        "platforms": ", ".join([lookup_caches["platforms"].get(i, str(i)) for i in g.get("platforms", [])]),
        "player_perspectives": ", ".join([lookup_caches["player_perspectives"].get(i, str(i)) for i in g.get("player_perspectives", [])]),
        "game_modes": ", ".join([lookup_caches["game_modes"].get(i, str(i)) for i in g.get("game_modes", [])]),
        "game_engines": ", ".join([lookup_caches["game_engines"].get(i, str(i)) for i in g.get("game_engines", [])]),
        "similar_games": json.dumps(similar_games_ids),
        "keywords": ", ".join([lookup_caches["keywords"].get(i, str(i)) for i in g.get("keywords", [])]),
        "cover_url": cover_urls.get(g.get("cover"), ""),
        "summary": g.get("summary", "").replace("\n", " "),
        "screenshot_urls": json.dumps([ss_urls.get(sid, "") for sid in g.get("screenshots", [])], ensure_ascii=False),
        "artwork_urls": json.dumps([artwork_urls.get(aid, "") for aid in g.get("artworks", [])], ensure_ascii=False),
    }

# Batch processing loop
while True:
    games = fetch_games(offset=offset)
    if not games:
        break
    game_ids = [g["id"] for g in games]

    ids = {
        "genres": set(), "platforms": set(), "themes": set(), "player_perspectives": set(),
        "game_modes": set(), "franchises": set(), "collections": set(),
        "game_engines": set(), "keywords": set(), "cover_ids": set(),
        "screenshot_ids": set(), "artwork_ids": set()
    }

    for g in games:
        ids["genres"].update(g.get("genres", []))
        ids["platforms"].update(g.get("platforms", []))
        ids["themes"].update(g.get("themes", []))
        ids["player_perspectives"].update(g.get("player_perspectives", []))
        ids["game_modes"].update(g.get("game_modes", []))
        if g.get("franchise"): ids["franchises"].add(g["franchise"])
        ids["franchises"].update(g.get("franchises", []))
        ids["collections"].update(g.get("collections", []))
        ids["game_engines"].update(g.get("game_engines", []))
        ids["keywords"].update(g.get("keywords", []))
        if g.get("cover"): ids["cover_ids"].add(g["cover"])
        ids["screenshot_ids"].update(g.get("screenshots", []))
        ids["artwork_ids"].update(g.get("artworks", []))

    for k in ["genres", "platforms", "themes", "player_perspectives", "game_modes",
              "franchises", "collections", "game_engines", "keywords"]:
        lookup_caches[k].update(bulk_fetch_names(k, list(ids[k])))

    involved_data = fetch_involved_companies(game_ids)
    all_company_ids = {e["company_id"] for v in involved_data.values() for e in v}
    lookup_caches["companies"].update(bulk_fetch_names("companies", list(all_company_ids)))
    cover_urls = fetch_image_urls("covers", list(ids["cover_ids"]), size="cover_big")
    ss_urls = fetch_image_urls("screenshots", list(ids["screenshot_ids"]), size="screenshot_big")
    artwork_urls = fetch_image_urls("artworks", list(ids["artwork_ids"]), size="screenshot_big")

    batch_games = [flatten_game(g, cover_urls, ss_urls, involved_data, artwork_urls) for g in games]

    # Append to CSV
    write_mode = "a" if os.path.exists(output_csv) and offset > 0 else "w"
    with open(output_csv, write_mode, newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=batch_games[0].keys())
        if write_mode == "w":
            writer.writeheader()
        writer.writerows(batch_games)

    # Save updated offset
    with open(checkpoint_path, "w") as f:
        f.write(str(offset))

    # Save updated lookup cache
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(lookup_caches, f, ensure_ascii=False, indent=2)

    print(f"Saved batch of {len(batch_games)} games to CSV. Offset now {offset + 500}")
    offset += 500
    time.sleep(0.5)

print("Done.")
