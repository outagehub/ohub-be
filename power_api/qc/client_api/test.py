import requests
import time  # Import the time module for timing

# API configuration
API_KEY = "5b8GiRB8xYymn9f0hzQ9BdrzX1ikPzQ-c6z5NfLKxkM"
API_HYDRO_OUTAGES_URL = "http://canadianpoweroutages.ca:9000/hydro-outages"  # Replace with your server IP if needed
API_OUTAGES_NEARBY_URL = "http://canadianpoweroutages.ca:9000/outages-nearby"

# Headers for the API request
headers = {
    "X-API-KEY": API_KEY
}

def test_api():
    """
    Test the /hydro-outages API endpoint.
    Measures the API response time and validates the response format.
    """
    try:
        start_time = time.time()  # Record the start time
        
        # Make the API request
        response = requests.get(API_HYDRO_OUTAGES_URL, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        end_time = time.time()  # Record the end time
        
        elapsed_time = end_time - start_time  # Calculate elapsed time
        
        # Parse the JSON response
        data = response.json()

        # Validate and print the response
        total_outages = data.get("total_outages", 0)
        print("Total Outages:", total_outages)
        
        if not isinstance(data.get("outages", []), list):
            raise ValueError("The 'outages' field is not a list.")

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
        
        print(f"API call took {elapsed_time:.2f} seconds")  # Print elapsed time

    except requests.exceptions.RequestException as e:
        print(f"An HTTP error occurred: {e}")
    except ValueError as ve:
        print(f"Validation error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def test_outages_nearby(lat, lon, distance_km):
    """
    Test the /outages-nearby API endpoint.
    Validates the functionality with the given latitude, longitude, and distance.
    """
    try:
        start_time = time.time()  # Record the start time
        
        # Make the API request
        response = requests.get(
            API_OUTAGES_NEARBY_URL,
            headers=headers,
            params={"lat": lat, "lon": lon, "distance_km": distance_km}
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        end_time = time.time()  # Record the end time
        
        elapsed_time = end_time - start_time  # Calculate elapsed time
        
        # Parse the JSON response
        data = response.json()
        print(f"Nearby Outage Found: {data['nearby_outage']}")
        print(f"API call took {elapsed_time:.2f} seconds")  # Print elapsed time
        return data['nearby_outage']

    except requests.exceptions.RequestException as e:
        print(f"An HTTP error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Test /hydro-outages API
    print("Testing /hydro-outages API...")
    test_api()

    # Test /outages-nearby API with True case
    print("\nTesting /outages-nearby API for True case...")
    result_true = test_outages_nearby(45.4215, -75.6972, 50)  # Adjust the lat, lon, and distance as needed
    assert result_true == True, "Expected True, but got False"

    # Test /outages-nearby API with False case
    print("\nTesting /outages-nearby API for False case...")
    result_false = test_outages_nearby(60.0000, -100.0000, 50)  # Adjust the lat, lon, and distance as needed
    assert result_false == False, "Expected False, but got True"

