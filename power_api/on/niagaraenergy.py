import requests
import xml.etree.ElementTree as ET
import json
import sqlite3
from datetime import datetime, timezone

# Constants
DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"
COMPANY_NAME = "Niagara Energy"
KML_URL = "https://www.npei.ca/sites/npei/files/kml/outage_polygons_public.kml?1732085755610"

# Headers for the request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.npei.ca/outages/outage-map",
    "Sec-CH-UA": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": "macOS",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin"
}

def fetch_outages():
    """Fetch outages from the Niagara Energy KML data."""
    response = requests.get(KML_URL, headers=HEADERS)

    if response.status_code != 200:
        print(f"Failed to fetch KML data. HTTP Status Code: {response.status_code}")
        return []

    try:
        root = ET.fromstring(response.content)
        namespace = {'kml': 'http://www.opengis.net/kml/2.2'}
        outages = []

        for placemark in root.findall(".//kml:Placemark", namespace):
            # Extract relevant fields
            name = placemark.find("kml:name", namespace).text if placemark.find("kml:name", namespace) is not None else "Unknown"
            extended_data = placemark.find("kml:ExtendedData/kml:SchemaData", namespace)
            coordinates_element = placemark.find(".//kml:coordinates", namespace)

            # Extract ExtendedData fields
            customers_out = None
            cause = None
            if extended_data is not None:
                for field in extended_data.findall("kml:SimpleData", namespace):
                    if field.attrib['name'] == "CUSTOMERS_OUT":
                        customers_out = int(field.text)
                    if field.attrib['name'] == "CAUSE_CODE":
                        cause = field.text

            # Extract coordinates and split them into lat/lng
            if coordinates_element is not None:
                coordinates = coordinates_element.text.strip().split()[0]  # Take the first coordinate pair
                longitude, latitude, _ = map(float, coordinates.split(','))  # Extract lat/lng

                # Append to outages list
                outages.append({
                    "id": name,
                    "municipality": "Niagara Region",  # No municipality in data; use a generic label
                    "area": name,  # Use name as a placeholder for area
                    "cause": cause if cause else "Unknown",
                    "numCustomersOut": customers_out if customers_out is not None else 0,
                    "latitude": latitude,
                    "longitude": longitude,
                    "polygon": None,  # No polygon data available
                })

        return outages

    except ET.ParseError as e:
        print(f"Error parsing KML data: {e}")
        return []

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
            outage["id"],
            outage["municipality"],
            outage["area"],
            outage["cause"],
            outage["numCustomersOut"],
            "Unknown",  # crewStatusDescription not available
            outage["latitude"],
            outage["longitude"],
            "Unknown",  # dateOff not available
            "Unknown",  # crewEta not available
            json.dumps(outage["polygon"]) if outage["polygon"] else None,
            company_name,
            0,  # Planned flag not available
            api_call_timestamp
        ))

    conn.commit()
    conn.close()
    print(f"Inserted {len(outages)} outage records for {company_name}.")

if __name__ == "__main__":
    # Fetch and store outages
    data = fetch_outages()
    if data:
        store_outages(data, COMPANY_NAME)

