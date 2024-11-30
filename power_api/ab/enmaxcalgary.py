import requests
import sqlite3
from datetime import datetime, timezone
import json

DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"
COMPANY_NAME = "ENMAX Calgary"

def fetch_calgary_outages():
    """Fetch outage data from ENMAX Calgary API."""
    url = "https://powerservices.enmax.com/api/outage?type=Current"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "referer": "https://powerservices.enmax.com/",
        "sec-ch-ua": '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "macOS",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        try:
            outage_data = response.json()
            return outage_data
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {e}")
            return []
    else:
        print(f"Failed to retrieve data. HTTP Status code: {response.status_code}")
        return []

def store_outages(outages, company_name):
    """Store outage data in the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    api_call_timestamp = datetime.now(timezone.utc).isoformat()

    for outage in outages:
        print("-- Debug: Full outage data --")
        print(json.dumps(outage, indent=4))

        # Extract relevant outage data
        incident_id = outage.get("incidentID", "N/A")
        municipality = outage.get("areasAffected", "N/A")
        cause = outage.get("outageCause", "Unknown")
        num_customers = outage.get("customersAffected", 0)
        latitude = outage.get("latitude", 0.0)
        longitude = outage.get("longitude", 0.0)
        date_off = outage.get("outageStart", "Unknown")
        crew_eta = outage.get("estimatedRestoration", "Unknown")
        is_planned = outage.get("isPlanned", False)

        # Insert the outage into the database
        cursor.execute(
            """
            INSERT OR REPLACE INTO outages (
                id, municipality, area, cause, numCustomersOut,
                crewStatusDescription, latitude, longitude,
                dateOff, crewEta, polygon, company, planned, apiCallTimestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                incident_id,
                municipality,
                "N/A",  # Area is not provided explicitly
                cause,
                num_customers,
                outage.get("status", "N/A"),  # Use the status field for crew status
                latitude,
                longitude,
                date_off,
                crew_eta,
                json.dumps([]),  # No polygon data available, so store an empty list
                company_name,
                int(is_planned),  # Convert boolean to int for database storage
                api_call_timestamp,
            ),
        )

    conn.commit()
    conn.close()
    print(f"Inserted {len(outages)} outage records for {company_name}.")

if __name__ == "__main__":
    outages = fetch_calgary_outages()
    if outages:
        store_outages(outages, COMPANY_NAME)
    else:
        print("No outages to store.")

