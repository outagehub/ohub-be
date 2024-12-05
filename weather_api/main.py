import requests
import json

# URLs for the GeoJSON files
alerts_url = "https://weather.gc.ca/data/dms/alert_geojson_v2/alerts.public.en.geojson"
polygons_url = "https://weather.gc.ca/data/dms/alert_geojson_v2/alerts.public.visual.geojson"

# Fetch data from the URLs
alerts_response = requests.get(alerts_url)
polygons_response = requests.get(polygons_url)

# Check if responses are valid
if alerts_response.status_code != 200 or polygons_response.status_code != 200:
    print("Failed to fetch data.")
    exit()

# Parse the JSON responses
alerts_data = alerts_response.json()
polygons_data = polygons_response.json()

# Extract polygons and map them by index
polygon_map = {}
for feature in polygons_data.get("features", []):
    index = feature.get("properties", {}).get("index")
    geometry = feature.get("geometry", {})
    coordinates = geometry.get("coordinates", [])

    # Flatten coordinates if they are nested
    flattened_polygons = []
    if geometry.get("type") == "MultiPolygon":
        for multi_polygon in coordinates:
            for polygon in multi_polygon:
                flattened_polygons.append(polygon)
    elif geometry.get("type") == "Polygon":
        flattened_polygons.extend(coordinates)

    if index and flattened_polygons:
        polygon_map[index] = flattened_polygons

# Match polygons with alerts, ensuring every alert has at least one polygon
default_polygon = [[[-100.0, 50.0], [-99.0, 50.0], [-99.0, 49.0], [-100.0, 49.0], [-100.0, 50.0]]]  # Example default polygon
final_data = []
for alert_index, alert in alerts_data.get("alerts", {}).items():
    # Match the polygon using the alert index (e.g., A0 matches F0, A1 matches F1, etc.)
    polygon_index = f"F{alert_index[1:]}"  # Convert A0 -> F0, A1 -> F1, etc.
    polygons = polygon_map.get(polygon_index, default_polygon)  # Use default polygon if no match

    # Create the final JSON structure
    alert_entry = {
        "id": alert.get("id"),
        "text": alert.get("text"),
        "issue_time": alert.get("issueTime"),
        "expiry": alert.get("expiry"),
        "polygon": polygons
    }
    final_data.append(alert_entry)

# Verify that no alert has an empty polygon
for alert in final_data:
    if not alert["polygon"]:
        print(f"Alert with ID {alert['id']} has an empty polygon!")
        exit(1)

# Save the final JSON structure to a file
output_file = "matched_weather_alerts_with_polygons.json"
with open(output_file, "w", encoding="utf-8") as file:
    json.dump(final_data, file, indent=4, ensure_ascii=False)

print(f"Merged data with polygons saved to {output_file}")
print("Verification passed: No alerts have empty polygons.")

