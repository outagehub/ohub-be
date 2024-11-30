import requests
import sqlite3
import json
from datetime import datetime
import pytz
from datetime import datetime, timezone

# Path to your SQLite database
DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"

# URL for Manitoba Hydro outages
URL = "https://account.hydro.mb.ca/portal/OuterOutage.aspx/loadLatLongOuterOutage"

# Headers for the request
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://account.hydro.mb.ca",
    "Referer": "https://account.hydro.mb.ca/portal/outeroutage.aspx"
}

# Cookies for the request
COOKIES = {
    "Language_code": "EN",
}

COMPANY_NAME = "Manitoba Hydro"  # Static company name

# Define Central Time Zone for Manitoba
CENTRAL = pytz.timezone("America/Winnipeg")
UTC = pytz.utc

def clear_company_data(company_name):
    """Clear all entries in the database for the specified company."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM outages WHERE company = ?", (company_name,))
    conn.commit()
    conn.close()
    print(f"Cleared all records for {company_name}.")

def parse_outage_date(date_str):
    """Parse and localize date strings from Manitoba Hydro API."""
    if not date_str:
        return None
    try:
        # Remove ' AM' or ' PM' from the date string
        date_str = date_str.replace(" AM", "").replace(" PM", "")
        # Parse the date string
        date = datetime.strptime(date_str, "%m/%d/%Y %H:%M")
        return CENTRAL.localize(date).astimezone(UTC).isoformat()
    except Exception as e:
        print(f"Error parsing date '{date_str}': {e}")
        return None

def fetch_outages(is_planned):
    """Fetch outage data from Manitoba Hydro API."""
    payload = {
        "Zipcode": "",
        "IsPlannedOutage": is_planned,
        "timeOffsetMinutes": 240
    }

    try:
        response = requests.post(URL, headers=HEADERS, cookies=COOKIES, json=payload)
        if response.status_code == 200:
            response_data = response.json()
            outage_data = json.loads(response_data["d"])
            return outage_data.get("Table1", [])
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

def process_outages(data):
    """Process the fetched data into the required format."""
    outages = []

    for outage in data:
        try:
            # Parse the `CustomerAffectedText` field conditionally
            customer_affected_text = outage.get("CustomerAffectedText", "0")
            if "Less than" in customer_affected_text:
                num_customers = 1  # Approximate "Less than 5" to 1 customer
            else:
                num_customers = int(customer_affected_text)  # Parse numeric text
            
            outages.append({
                "id": outage.get("UtilityOutageId", "Unknown"),
                "municipality": outage.get("CityName", None),
                "area": outage.get("Area", None),
                "cause": outage.get("OutageReportInfo", "Unknown"),
                "numCustomersOut": num_customers,
                "crewStatusDescription": outage.get("STATUS", "N/A"),
                "latitude": float(outage.get("OutageLatitude", 0.0)),
                "longitude": float(outage.get("OutageLongitude", 0.0)),
                "dateOff": parse_outage_date(outage.get("Outagedate")),
                "crewEta": parse_outage_date(outage.get("RestorationTime")),
                "polygon": json.dumps([]),  # Manitoba Hydro API does not provide polygon data
                "company": COMPANY_NAME,
                "planned": 1 if outage.get("Title") == "Planned" else 0,
            })
        except Exception as e:
            print(f"Error processing outage: {e}")
            print(json.dumps(outage, indent=4))  # Debugging output for the problematic outage
            continue

    return outages

def store_outages(outages, company_name):
    """Store fetched outage data in the SQLite database."""
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
            json.dumps(outage.get("polygon", [])),
            company_name,
            outage.get("planned", 0),  # Default planned to 0 if not provided
            api_call_timestamp
        ))

    conn.commit()
    conn.close()
    print(f"Inserted {len(outages)} outage records for {company_name}.")

if __name__ == "__main__":
    # Clear the database for Manitoba Hydro
    # clear_company_data(COMPANY_NAME)

    # Fetch and process planned outages
    raw_planned_outages = fetch_outages("B")
    planned_outages = process_outages(raw_planned_outages)

    # Fetch and process unplanned outages
    raw_unplanned_outages = fetch_outages("C")
    unplanned_outages = process_outages(raw_unplanned_outages)

    # Combine all outages and store them in the database
    all_outages = planned_outages + unplanned_outages
    store_outages(all_outages, "Manitoba Hydro")

