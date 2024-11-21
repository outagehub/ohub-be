from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
import sqlite3

app = FastAPI()

# Hardcoded paths
INDEX_HTML = "/root/ohub/ohub-fe/index.html"
CSS_FILE = "/root/ohub/ohub-fe/styles.css"
JS_FILE = "/root/ohub/ohub-fe/script.js"
DB_PATH = "/root/ohub/ohub-db/ohub-db/outages_db"

@app.get("/")
async def serve_index():
    return FileResponse(INDEX_HTML)

@app.get("/styles.css")
async def serve_css():
    return FileResponse(CSS_FILE)

@app.get("/script.js")
async def serve_js():
    return FileResponse(JS_FILE)

@app.get("/outages")
async def get_outages():
    # Connect to the database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Query the outage data
    cursor.execute("""
        SELECT id, municipality, area, cause, numCustomersOut, crewStatusDescription, latitude, longitude 
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
            "longitude": row[7]
        }
        for row in rows
    ]

    return JSONResponse(outages)

