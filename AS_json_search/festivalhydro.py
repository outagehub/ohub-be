import requests
import time

# URL to scan
url = "https://utilityoutagemap.com/api/1138/false?_=1735025657513" #pretty sure its this one ?? This was the only one that upadted in like 5min

# File to save the original response
original_file = "FestivalHydro_original_updatepushpin.txt"
new_file = "FestivalHydro_new_updatepushpin.txt"

# Headers for the request
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "referer": "https://outages.bluewaterpower.com/"
}

def fetch_data():
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.text
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def save_to_file(filename, data):
    with open(filename, "w") as file:
        file.write(data)

def compare_data(data1, data2):
    return data1 != data2

# Main function to monitor changes
def monitor_changes():
    print("Fetching initial data...")
    original_data = fetch_data()

    if original_data is None:
        print("Could not fetch initial data. Exiting.")
        return

    save_to_file(original_file, original_data)
    print(f"Original data saved to {original_file}")

    while True:
        print("Waiting 5 minutes before checking for updates...")
        time.sleep(300)  # Wait for 5 minutes

        print("Fetching new data...")
        new_data = fetch_data()

        if new_data is None:
            print("Failed to fetch new data. Retrying in 5 minutes.")
            continue

        if compare_data(original_data, new_data):
            print("Change detected! Saving new data...")
            save_to_file(new_file, new_data)
            print(f"New data saved to {new_file}. Exiting.")
            break
        else:
            print("No changes detected. Continuing monitoring...")

if __name__ == "__main__":
    monitor_changes()
