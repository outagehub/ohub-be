# Import necessary libraries
import requests
import time

# API configuration
API_KEY = "5b8GiRB8xYymn9f0hzQ9BdrzX1ikPzQ-c6z5NfLKxkM"
API_HYDRO_OUTAGES_URL = "http://canadianpoweroutages.ca:9000/hydro-outages"
API_OUTAGES_NEARBY_URL = "http://canadianpoweroutages.ca:9000/outages-nearby"

# Headers for the API request
headers = {"X-API-KEY": API_KEY}

def test_api():
    """
    Test the /hydro-outages API endpoint.
    Measures the API response time and validates the response format.
    """
    try:
        start_time = time.time()  # Record the start time
        response = requests.get(API_HYDRO_OUTAGES_URL, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        elapsed_time = time.time() - start_time  # Calculate elapsed time
        data = response.json()
        total_outages = data.get("total_outages", 0)
        print(f"Total Outages: {total_outages}")
        print(f"API call took {elapsed_time:.2f} seconds")
    except Exception as e:
        print(f"An error occurred: {e}")

def test_outages_nearby(lat, lon, distance_km):
    """
    Test the /outages-nearby API endpoint.
    Validates the functionality with the given latitude, longitude, and distance.
    """
    try:
        start_time = time.time()
        response = requests.get(
            API_OUTAGES_NEARBY_URL,
            headers=headers,
            params={"lat": lat, "lon": lon, "distance_km": distance_km}
        )
        response.raise_for_status()
        elapsed_time = time.time() - start_time
        data = response.json()
        print(f"Nearby Outage Found: {data['nearby_outage']}")
        print(f"API call took {elapsed_time:.2f} seconds")
        return data['nearby_outage']
    except Exception as e:
        print(f"An error occurred: {e}")

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
