import requests
import sqlite3
import json
from pyproj import Transformer
from shapely.geometry import Polygon
from datetime import datetime, timezone

# Path to your SQLite database
DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"

# URL for NB Power outages
URL = "https://services1.arcgis.com/nXhKU3TMjpIZsCx0/arcgis/rest/services/PublicOutageFC_Prod/FeatureServer/6/query"

# Parameters for the ArcGIS API request
PARAMS = {
    'f': 'json',
    'where': '1=1',
    'returnGeometry': 'true',
    'spatialRel': 'esriSpatialRelIntersects',
    'outFields': '*',
    'maxRecordCountFactor': '4',
    'outSR': '102100',  # Web Mercator spatial reference system
    'resultOffset': '0',
    'resultRecordCount': '4000',
    'cacheHint': 'true',
}

COMPANY_NAME = "NB Power"  # Define the company name

# Transformer for Web Mercator (EPSG:102100) to WGS84 (EPSG:4326)
transformer = Transformer.from_crs("EPSG:102100", "EPSG:4326", always_xy=True)

def clear_company_data(company_name):
    """Clear all entries in the database for the specified company."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM outages WHERE company = ?", (company_name,))
    conn.commit()
    conn.close()
    print(f"Cleared all records for {company_name}.")

def convert_polygon(polygon_coords):
    """Convert all polygon coordinates from EPSG:102100 to EPSG:4326 and flatten them."""
    flattened_polygon = []
    for ring in polygon_coords:  # Handle multiple rings (outer and inner boundaries)
        for point in ring:
            lon, lat = transformer.transform(point[0], point[1])  # Transform to WGS84
            flattened_polygon.extend([lon, lat])  # Append as [lon, lat]
    return flattened_polygon

def fetch_outages():
    """Fetch outage data from NB Power API."""
    try:
        response = requests.get(URL, params=PARAMS)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return {}
    except Exception as e:
        print(f"Error fetching data: {e}")
        return {}

def process_outages(data):
    """Process the fetched data into the required format."""
    outages = []
    for feature in data.get('features', []):
        attributes = feature.get('attributes', {})
        geometry = feature.get('geometry', {})

        outage_id = attributes.get('GlobalID', "Unknown")
        customers_affected = attributes.get('CustEff', 0)
        number_of_outages = attributes.get('NoOfOutages', "N/A")
        raw_polygon = geometry.get('rings', [])  # Get raw polygon data

        # Convert polygon coordinates
        polygon = convert_polygon(raw_polygon) if raw_polygon else []

        # Extract coordinates for the centroid if available
        if polygon:
            # Use the first pair as a representative point for marker placement
            lon, lat = polygon[:2]
        else:
            lat, lon = 0.0, 0.0  # Default if no geometry is available

        outages.append({
            "id": outage_id,
            "municipality": "N/A",  # Not provided by NB Power
            "area": "N/A",  # Not provided by NB Power
            "cause": "Unknown",  # Cause is not available in the API
            "num_customers": customers_affected,
            "crew_status": "N/A",  # Crew status not provided
            "latitude": lat,
            "longitude": lon,
            "date_off": "Unknown",  # Not provided by NB Power
            "crew_eta": "Unknown",  # Not provided by NB Power
            "polygon": polygon,  # Flattened polygon
            "power_company": COMPANY_NAME,
            "planned": 0  # Assume all outages are unplanned
        })

    return outages

def store_outages(outages, company_name):
    """Store processed outage data in the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    api_call_timestamp = datetime.now(timezone.utc).isoformat()

    for outage in outages:
        cursor.execute("""
            INSERT OR REPLACE INTO outages (
                id, municipality, area, cause, numCustomersOut,
                crewStatusDescription, latitude, longitude,
                dateOff, crewEta, polygon, company, planned, apiCallTimestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            outage["id"],
            outage["municipality"],
            outage["area"],
            outage["cause"],
            outage["num_customers"],
            outage["crew_status"],
            outage["latitude"],
            outage["longitude"],
            outage["date_off"],
            outage["crew_eta"],
            json.dumps(outage["polygon"]),
            outage["power_company"],
            outage["planned"],
            api_call_timestamp
        ))

    conn.commit()
    conn.close()
    print(f"Inserted {len(outages)} outage records for {company_name}.")

if __name__ == "__main__":
    # Clear the database for the specific company
    # clear_company_data(COMPANY_NAME)

    # Fetch and process new data
    data = fetch_outages()
    if data:
        processed_outages = process_outages(data)
        store_outages(processed_outages, COMPANY_NAME)

