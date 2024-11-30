import asyncio
from playwright.async_api import async_playwright
import requests
import re
import json
import sqlite3
from datetime import datetime, timezone
import polyline

# Database configuration
DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"
COMPANY_NAME = "Hydro One"

# Combined data structure
combined_data = {"file_title": "Combined Hydro One Outages", "file_data": []}


async def capture_all_requests():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # Set to headless mode
        page = await browser.new_page()

        captured_requests = {}
        page.on("request", lambda request: capture_request(request, captured_requests))

        await page.goto("https://d8bkcndcv6jca.cloudfront.net/")
        await page.wait_for_timeout(3000)  # Wait for the page to load

        # Simulate scrolling to load resources
        for _ in range(10):
            await page.mouse.wheel(0, 1000)
            await asyncio.sleep(1)

        # Fetch JSON data from captured URLs
        for url, filename in captured_requests.items():
            if filename.endswith(".json") and re.match(r"^\d+\.json$", filename):
                fetch_and_combine_json_content(url)

        await browser.close()

    # Save to database
    store_outages(combined_data["file_data"], COMPANY_NAME)


def capture_request(request, captured_requests):
    """Capture JSON requests from the page."""
    if request.resource_type == "fetch" or request.resource_type == "xhr":
        url = request.url
        captured_requests[url] = url.split("/")[-1]


def fetch_and_combine_json_content(url):
    """Fetch JSON content from a URL and combine it."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        json_data = response.json()

        for entry in json_data.get("file_data", []):
            # Fix coordinates to the proper format
            if "geom" in entry and "coordinates" in entry["geom"]:
                coordinates = entry["geom"]["coordinates"]
                entry["polygon"] = [
                    coord for pair in coordinates for coord in pair[::-1]
                ]  # Flatten and reverse lat/lng
                del entry["geom"]

            combined_data["file_data"].append(entry)

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch or parse URL {url}: {e}")


def store_outages(outages, company_name):
    """Store outage data in the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    api_call_timestamp = datetime.now(timezone.utc).isoformat()

    for outage in outages:
        print("-- Debug: Full outage data --")
        print(json.dumps(outage, indent=4))  # Print the full outage record for debugging

        # Initialize default values for latitude, longitude, and polygon
        latitude = 0.0
        longitude = 0.0
        polygon = []

        # Check and decode polyline
        if "geom" in outage and "p" in outage["geom"]:
            encoded_polylines = outage["geom"]["p"]
            print("-- Debug: Encoded polylines --")
            print(encoded_polylines)

            # Decode the polyline
            for encoded_polyline in encoded_polylines:
                try:
                    decoded_coords = polyline.decode(encoded_polyline)
                    polygon.extend(decoded_coords)
                except Exception as e:
                    print(f"Failed to decode polyline: {encoded_polyline}, Error: {e}")

            # Extract the first pair as latitude and longitude if available
            if polygon and len(polygon) > 0:
                latitude, longitude = polygon[0]
                print(f"-- Extracted lat/lon from polyline: {latitude}, {longitude} --")

            # Flatten the polygon if it contains only one set of coordinates
            if len(polygon) == 1 and isinstance(polygon[0], list):
                polygon = polygon[0]  # Remove the nested list

        cursor.execute(
            """
            INSERT OR REPLACE INTO outages (
                id, municipality, area, cause, numCustomersOut,
                crewStatusDescription, latitude, longitude,
                dateOff, crewEta, polygon, company, planned, apiCallTimestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                outage.get("id"),
                outage.get("title", "N/A"),
                "N/A",  # Area is not provided in this dataset
                outage["desc"].get("cause", "Unknown") if "desc" in outage else "Unknown",
                outage["desc"].get("cust_a", {}).get("val", 0) if "desc" in outage else 0,
                outage["desc"].get("crew_status", "N/A") if "desc" in outage else "N/A",
                latitude,
                longitude,
                outage["desc"].get("start_time", "Unknown") if "desc" in outage else "Unknown",
                outage["desc"].get("etr", "Unknown") if "desc" in outage else "Unknown",
                json.dumps(polygon),  # Save decoded polygon as JSON
                company_name,
                False,  # Planned is not provided in this dataset
                api_call_timestamp,
            ),
        )
    conn.commit()
    conn.close()
    print(f"Inserted {len(outages)} outage records for {company_name}.")

# Run the script
if __name__ == "__main__":
    asyncio.run(capture_all_requests())

