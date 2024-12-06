import os
import json
import aiohttp
import asyncio

# Base URL components
BASE_URL = "https://kubra.io/cluster-data"
DEPLOYMENT_ID = "e0192d42-e3c7-4d12-9872-137fb6304921"
INSTANCE_ID = "4220cd7e-b7f9-4748-bbce-a467233b18da"
OUTPUT_DIR = "outage_data"
GLOBAL_OUTAGE_FILE = "ontariohydro.json"

# Possible ranges for X and Y (both 0-3)
TILE_RANGE = range(4)

# Global list for storing all outages
global_outages = []
fetched_tiles = set()  # Keep track of already fetched tiles to avoid re-fetching

def reverse_and_build_url(tile_name):
    """Reverse the digits of the tile name and construct the URL."""
    reversed_prefix = tile_name[-3:][::-1]  # Reverse the last three digits of the tile name
    return f"{BASE_URL}/{reversed_prefix}/{DEPLOYMENT_ID}/{INSTANCE_ID}/public/cluster-1/{tile_name}.json"

async def fetch_tile(session, tile_name, expected_count=None):
    """Fetch a single tile and recursively search for children tiles."""
    if tile_name in fetched_tiles:
        return 0  # Skip already fetched tiles
    fetched_tiles.add(tile_name)

    url = reverse_and_build_url(tile_name)
    output_path = os.path.join(OUTPUT_DIR, f"{tile_name}.json")
    sub_tile_count = 0  # Tracks how many valid sub-tiles are found

    try:
        print(f"Fetching {url}...")
        async with session.get(url) as response:
            if response.status == 200:
                # Save the content to a local file
                data = await response.json()
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)
                print(f"Saved: {tile_name}.json")

                # Process the JSON and look for children tiles
                if "file_data" in data:
                    print(f"\n--- Successfully Retrieved Tile: {tile_name} ---")
                    for entry in data["file_data"]:
                        n_out = entry["desc"].get("n_out", 0)
                        geom = entry.get("geom", {}).get("p", [])
                        entry["decoded_geom"] = decode_polyline_list(geom)

                        if n_out == 1:
                            global_outages.append(entry)

                        # Add children tiles to the fetch list
                        tasks = []
                        for x in TILE_RANGE:
                            for y in TILE_RANGE:
                                if expected_count and sub_tile_count >= expected_count:
                                    print(f"Found {sub_tile_count} sub-tiles, stopping further fetch.")
                                    break
                                child_tile_name = f"{tile_name}{x}{y}"
                                tasks.append(fetch_tile(session, child_tile_name, expected_count=n_out))
                        sub_tile_count += len(await asyncio.gather(*tasks))

            else:
                print(f"File not found: {tile_name}.json (status code {response.status})")
    except Exception as e:
        print(f"Error fetching {tile_name}.json: {e}")

    return sub_tile_count

def decode_polyline_list(encoded_list):
    """Decode a list of encoded polylines into latitude and longitude coordinates."""
    return [decode_polyline(encoded) for encoded in encoded_list]

def decode_polyline(encoded):
    """Decode Google Maps Polyline format into latitude and longitude coordinates."""
    coords = []
    index = 0
    lat = 0
    lng = 0

    while index < len(encoded):
        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat

        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlng = ~(result >> 1) if (result & 1) else (result >> 1)
        lng += dlng

        coords.append((lat / 1e5, lng / 1e5))
    return coords

async def fetch_all_tiles():
    """Fetch all top-level tiles and recursively check for children."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for x in TILE_RANGE:
            for y in TILE_RANGE:
                top_level_tile_name = f"0{x}{y}"
                tasks.append(fetch_tile(session, top_level_tile_name))
        await asyncio.gather(*tasks)

    # Save all collected outages to the global JSON file
    with open(GLOBAL_OUTAGE_FILE, "w", encoding="utf-8") as f:
        json.dump(global_outages, f, indent=4)
    print(f"\nGlobal outage data saved to {GLOBAL_OUTAGE_FILE}")

import json
import sqlite3
from datetime import datetime

# Database configuration
DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"
COMPANY_NAME = "Hydro One"
GLOBAL_OUTAGE_FILE = "ontariohydro.json"

def store_in_db():
    """Read the global JSON file and store each entry in the database."""
    if not os.path.exists(GLOBAL_OUTAGE_FILE):
        print(f"Global outage file {GLOBAL_OUTAGE_FILE} not found.")
        return

    # Load JSON data
    with open(GLOBAL_OUTAGE_FILE, "r", encoding="utf-8") as f:
        outages = json.load(f)

    # Connect to the database
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    api_call_timestamp = datetime.utcnow().isoformat()

    for outage in outages:
        try:
            desc = outage.get("desc", {})
            geom_list = outage.get("decoded_geom", [])
            polygon = geom_list[0] if geom_list else []
            latitude, longitude = polygon[0][1], polygon[0][0] if polygon else (0.0, 0.0)

            cursor.execute("""
                INSERT OR IGNORE INTO outages (
                    id, municipality, area, cause, numCustomersOut,
                    crewStatusDescription, latitude, longitude,
                    dateOff, crewEta, polygon, company, planned, apiCallTimestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                outage.get("id", "Unknown"),
                "Unknown",  # Municipality placeholder
                "Unknown",  # Area placeholder
                desc.get("cause", {}).get("EN-US", "Unknown"),
                desc.get("cust_a", {}).get("val", 0),
                desc.get("crew_status", {}).get("EN-US", "Unknown"),
                latitude,  # Latitude
                longitude,  # Longitude
                "Unknown",
                "Unknown",  # Crew ETA
                json.dumps(polygon),  # Save flattened polygon as JSON
                COMPANY_NAME,
                False,  # Planned outage flag, defaulting to False
                api_call_timestamp
            ))
        except Exception as e:
            print(f"Error storing outage {outage.get('id', 'Unknown')}: {e}")

    # Commit changes and close the database connection
    conn.commit()
    conn.close()
    print("All outages stored in the database.")

if __name__ == "__main__":
    asyncio.run(fetch_all_tiles())
    store_in_db()

