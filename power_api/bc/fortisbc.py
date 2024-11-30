import json
import sqlite3
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"

def fetch_outage_data():
    """Fetch and parse FortisBC outage data."""
    url = "https://outages.fortisbc.com/outages/Home/UpdatePushpin"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            outage_list = []
            for oms_case in root.findall('OMSCASES'):
                outage_details = {
                    'serial': oms_case.find('SERIAL').text,
                    'description': oms_case.find('DESC').text,
                    'notes': oms_case.find('NOTES').text,
                    'planned': oms_case.find('PLANNED').text == "1",
                    'casestatus': oms_case.find('CASESTAT').text,
                    'workstatus': oms_case.find('WORKSTAT').text,
                    'latitude': float(oms_case.find('AVGLAT').text),
                    'longitude': float(oms_case.find('AVGLONG').text),
                    'outage_time': oms_case.find('OUTTIME').text,
                    'initial_customers': int(oms_case.find('INITCUST').text),
                    'current_customers': int(oms_case.find('CURCUST').text),
                    'restore_time': oms_case.find('RESTORETIM').text,
                    'restore_range': oms_case.find('RESTRANGE').text,
                    'cause': oms_case.find('DESC_CAUSE').text or "Unknown",
                    'coordinates_list': oms_case.find('COORDLIST').text
                }
                outage_list.append(outage_details)

            return outage_list
        else:
            print(f"Failed to fetch data: HTTP {response.status_code}")
            return []
    except (ET.ParseError, Exception) as e:
        print(f"Error fetching data: {e}")
        return []

def parse_coordinates(coord_list):
    """Parse the coordinates list into a list of [latitude, longitude]."""
    coordinates = []
    if coord_list:
        try:
            # Split the list into pairs of latitude and longitude
            coord_pairs = coord_list.split(',')
            for i in range(0, len(coord_pairs), 2):  # Step by 2 to get pairs
                lat = float(coord_pairs[i])
                lng = float(coord_pairs[i + 1])
                coordinates.append([lat, lng])
        except (ValueError, IndexError):
            print(f"Skipping malformed coordinates in list: {coord_list}")
    return coordinates

def store_outages(outages, company_name="FortisBC"):
    """Store fetched outage data in the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Get the current timestamp
    api_call_timestamp = datetime.now(timezone.utc).isoformat()

    for outage in outages:
        coordinates = parse_coordinates(outage['coordinates_list'])

        cursor.execute("""
            INSERT INTO outages (
                id, municipality, area, cause, numCustomersOut,
                crewStatusDescription, latitude, longitude,
                dateOff, crewEta, polygon, company, planned, apiCallTimestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            outage['serial'],
            outage['description'] or "N/A",
            outage['notes'] or "N/A",
            outage['cause'],
            outage['current_customers'],
            outage['workstatus'] or "Unknown",
            outage['latitude'],
            outage['longitude'],
            outage['outage_time'] or "Unknown",
            outage['restore_time'] or "Unknown",
            json.dumps(coordinates),
            company_name,
            outage['planned'],
            api_call_timestamp
        ))

    conn.commit()
    conn.close()
    print(f"Inserted {len(outages)} outage records for {company_name}.")

if __name__ == "__main__":
    outages = fetch_outage_data()
    if outages:
        store_outages(outages)

