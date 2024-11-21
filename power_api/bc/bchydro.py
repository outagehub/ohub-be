import requests
import sqlite3
import json

# Path to your SQLite database
DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"

# URL for BC Hydro outages
URL = "https://www.bchydro.com/power-outages/app/outages-map-data.json"

# Headers for the request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://www.bchydro.com",
    "Referer": "https://www.bchydro.com/power-outages/app/outage-map.html"
}

def fetch_outages():
    """Fetch outage data from BC Hydro API."""
    try:
        response = requests.get(URL, headers=HEADERS)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

def store_outages(outages):
    """Store fetched outage data in the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    for outage in outages:
        cursor.execute("""
            INSERT OR REPLACE INTO outages (
                id, municipality, area, cause, numCustomersOut,
                crewStatusDescription, latitude, longitude,
                dateOff, crewEta, polygon
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            outage.get("id"),
            outage.get("municipality", "N/A"),
            outage.get("area", "N/A"),
            outage.get("cause", "Unknown"),
            outage.get("numCustomersOut", 0),
            outage.get("crewStatusDescription", "N/A"),
            outage.get("latitude", 0.0),
            outage.get("longitude", 0.0),
            outage.get("dateOff", "Unknown"),
            outage.get("crewEta", "Unknown"),
            json.dumps(outage.get("polygon", []))
        ))

    conn.commit()
    conn.close()
    print(f"Inserted {len(outages)} outage records.")

if __name__ == "__main__":
    data = fetch_outages()
    if data:
        store_outages(data)


