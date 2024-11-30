import requests
import sqlite3
import json
from datetime import datetime, timezone

# Database configuration
DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"

# Define the URL for the detailed outage information
url_outages = "https://ems2.equs.ca:7576/data/outages.json"

# Define headers
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

def fetch_outages():
    """Fetch outage data from the Equs Alberta API."""
    response = requests.get(url_outages, headers=headers)
    if response.status_code != 200:
        print(json.dumps({
            "success": False,
            "error": f"Failed to retrieve data. Status code: {response.status_code}"
        }, indent=4))
        return []

    try:
        outage_data = response.json()
        if not isinstance(outage_data, list):
            print(json.dumps({
                "success": False,
                "error": "Expected a list but got a different type",
                "rawResponse": outage_data
            }, indent=4))
            return []

        outages = []
        for outage in outage_data:
            latitude = outage.get('outagePoint', {}).get('lat')
            longitude = outage.get('outagePoint', {}).get('lng')
            if latitude is None or longitude is None:
                continue

            outages.append({
                "id": outage.get("outageRecID", "N/A"),
                "latitude": float(latitude),
                "longitude": float(longitude),
                "startTime": outage.get("outageStartTime", "N/A"),
                "estimatedRestoration": outage.get("estimatedTimeOfRestoral", "Not available"),
                "customersOutInitially": outage.get("customersOutInitially", 0),
                "customersOutNow": outage.get("customersOutNow", 0),
                "cause": outage.get("cause", "Unknown"),
                "status": outage.get("outageWorkStatus", "Not available"),
            })
        return outages
    except ValueError as e:
        print(json.dumps({
            "success": False,
            "error": "Failed to parse JSON response",
            "exception": str(e),
            "rawResponse": response.text
        }, indent=4))
        return []

def store_outages(outages, company_name="Equs Alberta"):
    """Store processed outage data in the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Get the current timestamp
    api_call_timestamp = datetime.now(timezone.utc).isoformat()

    for outage in outages:
        cursor.execute("""
            INSERT INTO outages (
                id, municipality, area, cause, numCustomersOut,
                crewStatusDescription, latitude, longitude,
                dateOff, crewEta, polygon, company, planned, apiCallTimestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            outage["id"],
            "N/A",  # Municipality not provided
            "N/A",  # Area not provided
            outage["cause"],
            outage["customersOutNow"],
            outage["status"],
            outage["latitude"],
            outage["longitude"],
            outage["startTime"],
            outage["estimatedRestoration"],
            json.dumps([]),  # No polygon data
            company_name,
            0,  # Not a planned outage by default
            api_call_timestamp
        ))

    conn.commit()
    conn.close()
    print(f"Inserted {len(outages)} outage records for {company_name}.")

if __name__ == "__main__":
    # Fetch and store outages
    outages = fetch_outages()
    if outages:
        store_outages(outages)

