import requests

# API configuration
API_KEY = "5b8GiRB8xYymn9f0hzQ9BdrzX1ikPzQ-c6z5NfLKxkM"
API_URL = "http://149.28.41.69:9000/hydro-outages"  # Replace with your server IP if needed

# Send a request to the API
headers = {
    "X-API-KEY": API_KEY
}

try:
    response = requests.get(API_URL, headers=headers)
    response.raise_for_status()  # Raise an exception for HTTP errors
    data = response.json()  # Parse the JSON response

    # Pretty-print the response
    print("Total Outages:", data.get("total_outages", 0))
    print("Outage Details:")
    for outage in data.get("outages", []):
        print(f"- ID: {outage['id']}")
        print(f"  Municipality: {outage.get('municipality', 'N/A')}")
        print(f"  Area: {outage.get('area', 'N/A')}")
        print(f"  Cause: {outage.get('cause', 'Unknown')}")
        print(f"  Customers Affected: {outage.get('num_customers', 0)}")
        print(f"  Crew Status: {outage.get('crew_status', 'Unknown')}")
        print(f"  Latitude: {outage.get('latitude')}, Longitude: {outage.get('longitude')}")
        print(f"  Date Off: {outage.get('date_off', 'Unknown')}")
        print(f"  Crew ETA: {outage.get('crew_eta', 'Unknown')}")
        print(f"  Polygon: {outage.get('polygon', [])}")
        print()

except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")

