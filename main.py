from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
import sqlite3
import json
import asyncio
import os

app = FastAPI()

# Hardcoded paths
INDEX_HTML = "/root/ohub/ohub-fe/index.html"
CSS_FILE = "/root/ohub/ohub-fe/styles.css"
JS_FILE = "/root/ohub/ohub-fe/script.js"
DB_PATH = "/root/ohub/ohub-db/ohub-db/outages_db"
CACHE_FILE_PATH = "/root/ohub/ohub-be/outages_cache.json"

# Global cache for preloaded outages
outages_cache = {"data": [], "last_updated": None}

def save_cache_to_file(cache_data):
    """
    Save the cache data to a file in JSON format.
    """
    try:
        os.makedirs(os.path.dirname(CACHE_FILE_PATH), exist_ok=True)  # Ensure the directory exists
        with open(CACHE_FILE_PATH, "w") as cache_file:
            json.dump(cache_data, cache_file, indent=4)
        print(f"Cache saved to file: {CACHE_FILE_PATH}")
    except Exception as e:
        print(f"Error saving cache to file: {e}")

def fetch_outages_from_db():
    """
    Fetch the latest outages for each company from the database.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        query = """
            SELECT id, municipality, area, cause, numCustomersOut, 
                   crewStatusDescription, latitude, longitude, 
                   dateOff, crewEta, polygon, company, planned,
                   apiCallTimestamp
            FROM outages
            WHERE apiCallTimestamp IN (
                SELECT MAX(apiCallTimestamp)
                FROM outages
                GROUP BY company
            )
        """
        rows = cursor.execute(query).fetchall()
        outages = [
            {
                "id": row[0],
                "municipality": row[1],
                "area": row[2],
                "cause": row[3],
                "num_customers": row[4],
                "crew_status": row[5],
                "latitude": row[6],
                "longitude": row[7],
                "date_off": row[8],
                "crew_eta": row[9],
                "polygon": json.loads(row[10]) if row[10] else [],
                "power_company": row[11],
                "planned": row[12],
                "time_stamp": row[13],
            }
            for row in rows
        ]
        return outages
    except Exception as e:
        print(f"Error fetching outages from database: {e}")
        return []
    finally:
        conn.close()


async def update_outages_cache():
    """
    Update the global outages cache periodically and save it to a file.
    """
    while True:
        try:
            outages_cache["data"] = fetch_outages_from_db()
            outages_cache["last_updated"] = asyncio.get_event_loop().time()
            print("Outages cache updated")
            
            # Save the cache to a file
            save_cache_to_file(outages_cache)
        except Exception as e:
            print(f"Error updating outages cache: {e}")
        await asyncio.sleep(300)  # Refresh every 5 minutes


@app.on_event("startup")
async def startup_event():
    """
    Initialize the outages cache on startup.
    """
    asyncio.create_task(update_outages_cache())


@app.get("/")
async def serve_index():
    """Serve the main HTML file."""
    return FileResponse(INDEX_HTML)


@app.get("/styles.css")
async def serve_css():
    """Serve the CSS file."""
    return FileResponse(CSS_FILE)


@app.get("/script.js")
async def serve_js():
    """Serve the JavaScript file."""
    return FileResponse(JS_FILE)

@app.get("/feedback")
async def serve_feedback():
    """Serve the feedback HTML file."""
    feedback_file = "/root/ohub/ohub-fe/feedback.html"  # Adjust path if needed
    return FileResponse(feedback_file)

@app.get("/preloaded-outages")
async def get_preloaded_outages():
    """
    Serve preloaded outages data from the cache.
    """
    if not outages_cache["data"]:
        return JSONResponse({"error": "Outages cache is empty"}, status_code=500)
    return JSONResponse(outages_cache["data"])

@app.get("/outages")
async def get_outages(timestamp: str = None):
    """
    Fetch outage data filtered by a specific timestamp or the latest outages.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        if timestamp:
            # Fetch the latest outage data for each power company up to the given timestamp
            query = """
                SELECT id, municipality, area, cause, numCustomersOut, 
                       crewStatusDescription, latitude, longitude, 
                       dateOff, crewEta, polygon, company, planned,
                       apiCallTimestamp
                FROM outages
                WHERE apiCallTimestamp IN (
                    SELECT MAX(apiCallTimestamp)
                    FROM outages
                    WHERE apiCallTimestamp <= ?
                    GROUP BY company
                )
            """
            rows = cursor.execute(query, (timestamp,)).fetchall()
        else:
            # Fetch the latest outage data for each power company
            query = """
                SELECT id, municipality, area, cause, numCustomersOut, 
                       crewStatusDescription, latitude, longitude, 
                       dateOff, crewEta, polygon, company, planned,
                       apiCallTimestamp
                FROM outages
                WHERE apiCallTimestamp IN (
                    SELECT MAX(apiCallTimestamp)
                    FROM outages
                    GROUP BY company
                )
            """
            rows = cursor.execute(query).fetchall()

        # Process the rows into a JSON-compatible structure
        outages = [
            {
                "id": row[0],
                "municipality": row[1],
                "area": row[2],
                "cause": row[3],
                "num_customers": row[4],
                "crew_status": row[5],
                "latitude": row[6],
                "longitude": row[7],
                "date_off": row[8],
                "crew_eta": row[9],
                "polygon": json.loads(row[10]) if row[10] else [],
                "power_company": row[11],
                "planned": row[12],
                "time_stamp": row[13],
            }
            for row in rows
        ]
        return JSONResponse(outages)

    except Exception as e:
        print(f"Error fetching outages: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

    finally:
        conn.close()

@app.get("/weather-alerts")
async def get_weather_alerts():
    """
    Serve the matched weather alerts with polygons from the JSON file.
    """
    alerts_file_path = "/root/ohub/ohub-be/weather_api/matched_weather_alerts_with_polygons.json"
    if os.path.exists(alerts_file_path):
        return FileResponse(alerts_file_path)
    return JSONResponse({"error": "File not found"}, status_code=404)
