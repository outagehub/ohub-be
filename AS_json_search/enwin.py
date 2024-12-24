import requests
import time
import json
import os

# URL of the JSON file endpoint
URL = "https://services1.arcgis.com/0IrnZVYhnEoPl7Kr/arcgis/rest/services/Interruptions/FeatureServer/0?f=json"  # Replace with the actual URL to the JSON file
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Connection": "keep-alive"
}

# Function to fetch the JSON data
def fetch_json(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()  # Parse the JSON data
    else:
        response.raise_for_status()

# Function to save JSON data to a text file
def save_as_txt(data, filename):
    with open(filename, 'w') as f:
        # Convert the JSON data to a pretty-printed string and save it
        f.write(json.dumps(data, indent=4))

# Main function to monitor changes in the JSON file
def monitor_json_changes(url, headers, interval=300):  # 5 minutes = 300 seconds
    previous_data = None

    while True:
        try:
            # Fetch the current JSON data
            current_data = fetch_json(url, headers)

            # Save the original data if it's the first iteration
            if previous_data is None:
                save_as_txt(current_data, "enwin_original_data.txt")
                print("Original JSON data saved as original_data.txt.")
            elif current_data != previous_data:
                # Save the updated data if it's different from the previous one
                save_as_txt(current_data, "enwin_updated_data.txt")
                print("Updated JSON data saved as updated_data.txt. Stopping the script.")
                break

            # Update previous_data for the next iteration
            previous_data = current_data

            # Wait for the specified interval before fetching again
            time.sleep(interval)

        except Exception as e:
            print(f"Error: {e}")
            break

if __name__ == "__main__":
    monitor_json_changes(URL, HEADERS)
