import asyncio
from playwright.async_api import async_playwright
import requests
import re
import json
import polyline
import sqlite3
from datetime import datetime, timezone

DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"
COMPANY_NAME = "Hydro Ottawa"

combined_data = {"file_title": "Combined Hydro Ottawa Outages", "file_data": []}
captured_requests = {}

async def capture_all_requests():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # Headless mode
        page = await browser.new_page()

        page.on("request", capture_request)  # Capture requests
        await page.goto("https://outages.hydroottawa.com/")
        await page.wait_for_timeout(3000)  # Wait for initial load

        # Simulate scrolling to load resources
        for _ in range(10):
            await page.mouse.wheel(0, 1000)
            await asyncio.sleep(1)

        # Fetch JSON data from captured URLs
        for url, filename in captured_requests.items():
            if filename.endswith(".json") and re.match(r"^\d+\.json$", filename):
                fetch_and_combine_json_content(url)

        await browser.close()

    return combined_data

def capture_request(request):
    if request.resource_type == "fetch" or request.resource_type == "xhr":
        url = request.url
        captured_requests[url] = url.split("/")[-1]

def fetch_and_combine_json_content(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        json_data = response.json()

        for entry in json_data.get("file_data", []):
            if "geom" in entry and "p" in entry["geom"]:
                decoded_coords = [
                    polyline.decode(encoded_polyline)
                    for encoded_polyline in entry["geom"]["p"]
                ]
                entry["geom"]["coordinates"] = decoded_coords
                del entry["geom"]["p"]

            combined_data["file_data"].append(entry)
    except Exception as e:
        print(f"Failed to fetch or parse URL {url}: {e}")

def store_outages(outages, company_name):
    """Store fetched outage data in the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    api_call_timestamp = datetime.now(timezone.utc).isoformat()

    for outage in outages:
        # Process the polygon to flatten it
        coordinates = outage.get("geom", {}).get("coordinates", [])
        polygon = []
        if coordinates and isinstance(coordinates, list):
            try:
                # Flatten nested coordinates into a single list [lng1, lat1, lng2, lat2, ...]
                for segment in coordinates:
                    for point in segment:
                        polygon.extend([point[1], point[0]])  # Append [longitude, latitude]
            except (IndexError, TypeError):
                print(f"Invalid polygon data for outage ID {outage.get('id', 'Unknown')}")
                polygon = []

        # Insert data into the database
        cursor.execute("""
            INSERT INTO outages (
                id, municipality, area, cause, numCustomersOut,
                crewStatusDescription, latitude, longitude,
                dateOff, crewEta, polygon, company, planned, apiCallTimestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            outage.get("id", "Unknown"),
            "Unknown",  # Municipality placeholder
            "Unknown",  # Area placeholder
            outage.get("desc", {}).get("cause", {}).get("EN-US", "Unknown"),
            outage.get("desc", {}).get("cust_a", {}).get("val", 0),
            outage.get("desc", {}).get("crew_status", {}).get("EN-US", "Unknown"),
            polygon[1] if polygon else 0.0,  # Latitude
            polygon[0] if polygon else 0.0,  # Longitude
            outage.get("desc", {}).get("start_time", "Unknown"),
            "Unknown",  # Crew ETA placeholder
            json.dumps(polygon),  # Save flattened polygon as JSON
            company_name,
            False,  # Planned outage flag, defaulting to False
            api_call_timestamp
        ))

    conn.commit()
    conn.close()
    print(f"Inserted {len(outages)} outage records for {company_name}.")

async def main():
    data = await capture_all_requests()
    if data["file_data"]:
        store_outages(data["file_data"], COMPANY_NAME)
    else:
        print(f"No data found for {COMPANY_NAME}.")

# Run the script
asyncio.run(main())

