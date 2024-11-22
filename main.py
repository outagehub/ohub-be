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
async def get_outages():
    """Retrieve outage data from the SQLite database."""
    # Connect to the database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Query the outage data
    cursor.execute("""
        SELECT id, municipality, area, cause, numCustomersOut, 
               crewStatusDescription, latitude, longitude, 
               dateOff, crewEta, polygon, company, planned
        FROM outages
    """)
    rows = cursor.fetchall()
    conn.close()

    # Prepare JSON response
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
            "planned": row[12]  # Ensure this field is included
        }
        for row in rows
    ]

    return JSONResponse(outages)

