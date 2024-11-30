import os
import secrets
import sqlite3
import json
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security.api_key import APIKeyHeader
import logging

# FastAPI instance
app = FastAPI(title="Quebec Hydro Outages API", version="1.0")

# Path to your SQLite database
DB_FILE = "/root/ohub/ohub-db/ohub-db/outages_db"

# API Key Configuration
API_KEY_NAME = "X-API-KEY"
API_KEY_FILE = "api_key.txt"  # File to store the generated API key
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

with open(API_KEY_FILE, "r") as f:
    print(f"API Key: {f.read().strip()}")

# Secure API Key Storage and Generation
def get_or_generate_api_key():
    """Generate and store a secure API key if it doesn't exist."""
    if not os.path.exists(API_KEY_FILE):
        # Generate a secure random API key
        api_key = secrets.token_urlsafe(32)
        with open(API_KEY_FILE, "w") as f:
            f.write(api_key)
        print(f"Generated new API key: {api_key} (stored in {API_KEY_FILE})")
    else:
        # Read the existing API key from the file
        with open(API_KEY_FILE, "r") as f:
            api_key = f.read().strip()
    return api_key

# Load or generate the API key
VALID_API_KEY = get_or_generate_api_key()

# Verify API key
def verify_api_key(api_key: str):
    if api_key != VALID_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

# Database Query Helper Function
def query_latest_outages():
    """Query the outages database and retrieve the latest records for Quebec Hydro."""
    if not os.path.exists(DB_FILE):
        raise HTTPException(status_code=500, detail="Database file not found.")

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # Allows fetching rows as dictionaries
    cursor = conn.cursor()

    try:
        # Fetch the latest timestamp for Quebec Hydro entries
        cursor.execute("""
            SELECT MAX(apiCallTimestamp) as latest_timestamp  FROM outages 
            WHERE company = 'Quebec Hydro'
        """)
        latest_timestamp = cursor.fetchone()["latest_timestamp"]

        if not latest_timestamp:
            return []  # No records found for Quebec Hydro

        # Query the database for entries with the latest timestamp for Quebec Hydro
        cursor.execute("""
            SELECT * FROM outages 
            WHERE company = 'Quebec Hydro' 
            AND apiCallTimestamp = ?
        """, (latest_timestamp,))
        rows = cursor.fetchall()

        # Convert rows to a list of dictionaries
        outages = [dict(row) for row in rows]

        # Deserialize the polygon field (stored as JSON in the database)
        for outage in outages:
            if 'polygon' in outage and outage['polygon']:
                outage['polygon'] = json.loads(outage['polygon'])

        return outages
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    finally:
        conn.close()

# GET endpoint to retrieve latest outage data
@app.get("/hydro-outages", summary="Retrieve Latest Quebec Hydro Outage Data")
async def get_hydro_outages(api_key: str = Depends(api_key_header)):
    # Verify API key
    verify_api_key(api_key)
    print("API key verified successfully.")

    try:
        # Fetch the latest outage data
        outages = query_latest_outages()
        print(f"Fetched {len(outages)} records for Quebec Hydro.")
        return {"outages": outages, "total_outages": len(outages)}
    except Exception as e:
        print("Error occurred:", str(e))
        raise HTTPException(status_code=500, detail="Internal server error.")

# Root endpoint for testing
@app.get("/")
async def root():
    return {"message": "Welcome to the Quebec Hydro Outages API. Use /hydro-outages with your API key."} 
