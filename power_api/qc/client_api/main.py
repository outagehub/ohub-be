import os
import secrets
import json
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security.api_key import APIKeyHeader
from fastapi import Query, HTTPException
from math import radians, sin, cos, sqrt, atan2

# FastAPI instance
app = FastAPI(title="Quebec Hydro Outages API", version="1.0")

# Path to your JSON file
CACHE_FILE = "/root/ohub/ohub-be/outages_cache.json"

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

# Read and filter outages from JSON cache
def query_latest_outages_from_cache():
    """Fetch Quebec Hydro outages from the JSON cache."""
    if not os.path.exists(CACHE_FILE):
        raise HTTPException(status_code=500, detail="Cache file not found.")

    try:
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)

        # Filter outages for Quebec Hydro
        quebec_hydro_outages = [outage for outage in data if outage.get("power_company") == "Quebec Hydro"]

        return quebec_hydro_outages
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Failed to decode cache file: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading cache file: {str(e)}")

@app.get("/hydro-outages", summary="Retrieve Latest Quebec Hydro Outage Data")
async def get_hydro_outages(api_key: str = Depends(api_key_header)):
    # Verify API key
    verify_api_key(api_key)
    print("API key verified successfully.")

    cache_file_path = "/root/ohub/ohub-be/outages_cache.json"

    try:
        # Open and parse the cache file
        with open(cache_file_path, "r") as f:
            cache_data = json.load(f)  # Properly parse JSON
        
        # Access the "data" key
        all_outages = cache_data.get("data", [])
        print(f"Loaded {len(all_outages)} records from cache.")

        # Filter for Quebec Hydro outages
        hydro_outages = [
            outage for outage in all_outages
            if outage["power_company"] == "Quebec Hydro"
        ]

        print(f"Returning {len(hydro_outages)} Quebec Hydro outages.")
        return {"outages": hydro_outages, "total_outages": len(hydro_outages)}
    
    except Exception as e:
        print(f"Error occurred while processing /hydro-outages endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance in kilometers between two points on Earth.
    """
    R = 6371  # Radius of Earth in kilometers
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

@app.get("/outages-nearby", summary="Check if there are outages near a location")
async def check_outages_nearby(
    lat: float = Query(..., alias="lat"),
    lon: float = Query(None, alias="lon"),
    long: float = Query(None, alias="long"),
    distance_km: float = Query(..., alias="distance_km"),
    api_key: str = Depends(api_key_header)
):
    """
    Check if there are outages within a specified distance from the given latitude and longitude.
    Returns True if there is at least one outage within the distance, otherwise False.
    """
    # Verify API key
    verify_api_key(api_key)
    print("API key verified successfully.")

    # Handle both 'lon' and 'long'
    if lon is None and long is not None:
        lon = long
    elif lon is None and long is None:
        raise HTTPException(status_code=400, detail="Either 'lon' or 'long' must be provided for longitude.")

    cache_file_path = "/root/ohub/ohub-be/outages_cache.json"

    try:
        # Open and parse the cache file
        with open(cache_file_path, "r") as f:
            cache_data = json.load(f)  # Parse the JSON cache

        # Access the "data" key
        all_outages = cache_data.get("data", [])
        print(f"Loaded {len(all_outages)} records from cache.")

        # Check if any outage is within the specified distance
        is_nearby = any(
            haversine(lat, lon, outage["latitude"], outage["longitude"]) <= distance_km
            for outage in all_outages
        )

        print(f"Outages nearby: {is_nearby}")
        return {"nearby_outage": is_nearby}

    except Exception as e:
        print(f"Error occurred while processing /outages-nearby endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")

# Root endpoint for testing
@app.get("/")
async def root():
    return {"message": "Welcome to the Quebec Hydro Outages API. Use /hydro-outages with your API key."}

