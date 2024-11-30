import requests
import json
import sqlite3
from datetime import datetime, timezone

# Database configuration
DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"

# Algoma Power configuration
COMPANY_NAME = "Algoma Power"
API_URL = "https://outagemap.algomapower.com/data/outages.json"

# Headers for the API request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://outagemap.algomapower.com/",
    "Sec-CH-UA": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": "macOS",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin"
}

# Function to fetch outages from the API
def fetch_outages():
    response = requests.get(API_URL, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        outages = []

        for outage in data:
            outages.append({
                "id": outage.get("outageRecID", "Unknown"),
                "name": outage.get("outageName", "Unknown"),
                "latitude": outage["outagePoint"]["lat"],
                "longitude": outage["outagePoint"]["lng"],
                "startTime": outage.get("outageStartTime", "Unknown"),
                "endTime": outage.get("outageEndTime", None),
                "cause": outage.get("cause", "Unknown"),
                "customersOutInitially": outage.get("customersOutInitially", 0),
                "customersOutNow": outage.get("customersOutNow", 0),
                "customersRestored": outage.get("customersRestored", 0),
                "crewAssigned": outage.get("crewAssigned", False),
                "verified": outage.get("verified", False),
                "workStatus": outage.get("outageWorkStatus", "Unknown"),
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
            outage["name"],                 # area
            outage["cause"],                # cause
            outage["customersOutNow"],      # numCustomersOut
            "Crew Assigned" if outage["crewAssigned"] else "Pending", # crewStatusDescription
            outage["latitude"],             # latitude
            outage["longitude"],            # longitude
            outage["startTime"],            # dateOff
            outage["endTime"],              # crewEta
            json.dumps([]),                 # polygon
            company_name,                   # company
            int(outage["verified"]),        # planned (using verified field as planned equivalent)
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

