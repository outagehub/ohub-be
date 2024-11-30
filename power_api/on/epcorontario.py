import requests
import json
import sqlite3
from datetime import datetime, timezone

# Database configuration
DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"

# EPCOR Ontario configuration
COMPANY_NAME = "EPCOR Ontario"
API_URL = "https://utilityoutagemap.com/api/1107/true?_=1732086583302"

# Headers for the API request
HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Priority": "u=1, i",
    "Referer": "https://utilityoutagemap.com/epcor",
    "Sec-CH-UA": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": "macOS",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}

# Function to fetch outages from the API
def fetch_outages():
    response = requests.get(API_URL, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        outages = []
        for event in data.get("outageEvents", []):
            # Extract boundary locations or fallback to average latitude/longitude
            boundary_locations = event.get("boundaryLocations", {}).get("result", [])
            if boundary_locations:
                first_boundary = boundary_locations[0]
                latitude = first_boundary["x"]
                longitude = first_boundary["y"]
            else:
                latitude = event.get("averageLatitude", None)
                longitude = event.get("averageLongitude", None)
            
            # Structure outage data
            outages.append({
                "id": event.get("outageId"),
                "cause": event.get("cause", "Unknown"),
                "numImpactedMeters": event.get("numImpactedMeters", 0),
                "planned": event.get("planned", False),
                "crewsDispatched": event.get("crewsDispatched", False),
                "estimatedStart": event.get("estimatedStart"),
                "estimatedRestoration": event.get("estimatedRestoration"),
                "latitude": latitude,
                "longitude": longitude
            })
        return outages
    else:
        print(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
        return []

# Function to store outages in the database
def store_outages(outages, company_name):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    api_call_timestamp = datetime.now(timezone.utc).isoformat()

    for outage in outages:
        cursor.execute("""
            INSERT INTO outages (
                id, municipality, area, cause, numCustomersOut,
                crewStatusDescription, latitude, longitude,
                dateOff, crewEta, polygon, company, planned, apiCallTimestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            outage["id"],                   # id
            "N/A",                          # municipality
            "N/A",                          # area
            outage["cause"],                # cause
            outage["numImpactedMeters"],    # numCustomersOut
            "Crews Dispatched" if outage["crewsDispatched"] else "Pending", # crewStatusDescription
            outage["latitude"],             # latitude
            outage["longitude"],            # longitude
            outage["estimatedStart"],       # dateOff
            outage["estimatedRestoration"], # crewEta
            json.dumps([]),                 # polygon
            company_name,                   # company
            int(outage["planned"]),         # planned
            api_call_timestamp              # apiCallTimestamp
        ))

    conn.commit()
    conn.close()
    print(f"Inserted {len(outages)} outage records for {company_name}.")

if __name__ == "__main__":
    # Fetch and store outages
    outages = fetch_outages()
    if outages:
        store_outages(outages, COMPANY_NAME)

