import requests
import sqlite3
import json
from datetime import datetime, timezone
import pytz
from math import radians, cos, sin, sqrt, atan2

# Path to your SQLite database
DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"

# URLs for Quebec Hydro outages and polygons
OUTAGE_URL = "https://services5.arcgis.com/0akaykIdiPuMhFIy/arcgis/rest/services/bs_infoPannes_prd_vue/FeatureServer/0/query"
POLYGON_URL = "https://services5.arcgis.com/0akaykIdiPuMhFIy/arcgis/rest/services/bs_infoPannes_prd_vue/FeatureServer/1/query"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

# Parameters for the ArcGIS API request
PARAMS = {
    'where': '1=1',
    'outFields': '*',
    'geometryType': 'esriGeometryEnvelope',
    'spatialRel': 'esriSpatialRelIntersects',
    'resultType': 'tile',
    'f': 'geojson'
}

COMPANY_NAME = "Quebec Hydro"  # Define the company name

# Define time zones
utc = pytz.utc
eastern = pytz.timezone('US/Eastern')


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate the Haversine distance between two geographic points."""
    R = 6371  # Radius of Earth in kilometers
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


def clear_company_data(company_name):
    """Clear all entries in the database for the specified company."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM outages WHERE company = ?", (company_name,))
    conn.commit()
    conn.close()
    print(f"Cleared all records for {company_name}.")


def fetch_data(url):
    """Fetch data from the given URL."""
    try:
        response = requests.get(url, headers=HEADERS, params=PARAMS)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return {}
    except Exception as e:
        print(f"Error fetching data: {e}")
        return {}


def process_outages_and_polygons(outage_data, polygon_data):
    """Process outages and assign the closest polygon to each outage if needed."""
    polygons = []
    # Extract all polygons into a list
    for feature in polygon_data.get('features', []):
        geometry = feature.get('geometry', {})
        raw_coordinates = geometry.get('coordinates', [])

        # Flatten polygon coordinates if necessary
        flattened_polygon = []
        if raw_coordinates:
            for ring in raw_coordinates:  # Iterate through rings
                for lon, lat in ring:  # Extract longitude and latitude
                    flattened_polygon.append([lat, lon])

        polygons.append(flattened_polygon)

    outages = []
    # Process outage data and assign the closest polygon if necessary
    for feature in outage_data.get('features', []):
        properties = feature.get('properties', {})
        geometry = feature.get('geometry', {})

        # Determine outage status
        major_outage = "Major Outage" if properties.get('panneMajeure') == 1 else "Minor Outage"

        # Extract coordinates
        coordinates = geometry.get('coordinates', [None, None])
        longitude, latitude = coordinates

        # Convert Unix timestamp in milliseconds to a human-readable date in Eastern time
        timestamp_ms = properties.get('dateCreation')
        if timestamp_ms:
            utc_time = datetime.utcfromtimestamp(timestamp_ms / 1000).replace(tzinfo=utc)
            eastern_time = utc_time.astimezone(eastern).strftime('%Y-%m-%d %H:%M:%S %Z')
        else:
            eastern_time = "Unknown"

        # Determine if the outage already has a polygon
        polygon = []
        if latitude and longitude:
            min_distance = float("inf")
            closest_polygon = None

            # Find the closest polygon
            for poly in polygons:
                if poly:  # Ensure polygon is not empty
                    poly_lat, poly_lon = poly[0]  # Use the first coordinate of the polygon
                    distance = haversine_distance(latitude, longitude, poly_lat, poly_lon)
                    if distance < min_distance:
                        min_distance = distance
                        closest_polygon = poly

            polygon = closest_polygon if closest_polygon else []

        # Append each outage to the list
        outages.append({
            "id": properties.get('idInterruption', "Unknown"),
            "municipality": properties.get('nomMunicipalite', "N/A"),
            "area": properties.get('secteur', "N/A"),
            "cause": properties.get('cause', "Unknown"),
            "numCustomersOut": properties.get('nbClients', 0),
            "crewStatusDescription": properties.get('statutEquipe', "N/A"),
            "latitude": latitude or 0.0,
            "longitude": longitude or 0.0,
            "dateOff": eastern_time,
            "crewEta": "Unknown",  # Replace with a real field if available
            "polygon": polygon,
            "company": COMPANY_NAME
        })

    return outages


def store_outages(outages):
    """Store processed outage data in the SQLite database."""
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
            outage.get("id", "Unknown"),
            outage.get("municipality", "N/A"),
            outage.get("area", "N/A"),
            outage.get("cause", "Unknown"),
            outage.get("numCustomersOut", 0),
            outage.get("crewStatusDescription", "N/A"),
            outage.get("latitude", 0.0),
            outage.get("longitude", 0.0),
            outage.get("dateOff", "Unknown"),
            outage.get("crewEta", "Unknown"),
            json.dumps(outage.get("polygon", [])),  # Store polygon as JSON
            outage.get("company", COMPANY_NAME),
            0,  # Assuming planned outages are marked as 0
            api_call_timestamp
        ))

    conn.commit()
    conn.close()
    print(f"Inserted {len(outages)} outage records for {COMPANY_NAME}.")


if __name__ == "__main__":
    # Fetch outage and polygon data
    outage_data = fetch_data(OUTAGE_URL)
    polygon_data = fetch_data(POLYGON_URL)

    if outage_data and polygon_data:
        processed_outages = process_outages_and_polygons(outage_data, polygon_data)
        store_outages(processed_outages)

