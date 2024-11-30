from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
import sqlite3
import json

app = FastAPI()

# Hardcoded paths
INDEX_HTML = "/root/ohub/ohub-fe/index.html"
CSS_FILE = "/root/ohub/ohub-fe/styles.css"
JS_FILE = "/root/ohub/ohub-fe/script.js"
DB_PATH = "/root/ohub/ohub-db/ohub-db/outages_db"

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

@app.get("/outages")
async def get_outages(timestamp: str = None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        if timestamp:
            query = """
                SELECT id, municipality, area, cause, numCustomersOut, 
                       crewStatusDescription, latitude, longitude, 
                       dateOff, crewEta, polygon, company, planned,
                       apiCallTimestamp
                FROM outages
                WHERE apiCallTimestamp <= ?
                AND (
                    apiCallTimestamp IN (
                        SELECT MAX(apiCallTimestamp)
                        FROM outages
                        WHERE apiCallTimestamp <= ?
                        GROUP BY company
                    )
                    OR planned = 1
                )
            """
            rows = cursor.execute(query, (timestamp, timestamp)).fetchall()
        else:
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

        return JSONResponse(outages)

    except Exception as e:
        print(f"Error fetching outages: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

    finally:
        conn.close()

@app.get("/outages/latest")
async def get_latest_outages():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

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
    conn.close()

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

