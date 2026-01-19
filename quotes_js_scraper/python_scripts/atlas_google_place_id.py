import json
import requests

# Replace with your actual Google API Key
API_KEY = "AIzaSyBe-suda095R60l0G1NrdYeIVKjUCxwK_8"

# Input JSON file containing the places data
INPUT_FILE = "atlasobscura_mumbai_india_places.json"
# Output file to store the enriched data with Google Place IDs
OUTPUT_FILE = "atlasobscura_mumbai_india_places_with_google_place_id.json"

def get_google_place_id(place_name, latitude, longitude):
    """
    Use the Google Places API 'Find Place from Text' endpoint
    to search for the place and return its google place_id.
    """
    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    
    # Construct the request parameters
    params = {
        "input": place_name,
        "inputtype": "textquery",
        "fields": "place_id",
        # Use location biasing to help Google narrow down the results:
        "locationbias": f"point:{latitude},{longitude}",
        "key": API_KEY
    }
    
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        print(f"Error fetching data for {place_name}: HTTP {response.status_code}")
        return None

    data = response.json()
    candidates = data.get("candidates", [])
    if candidates:
        # Return the first candidate's place_id
        return candidates[0].get("place_id")
    else:
        print(f"No candidate found for {place_name}")
        return None

def main():
    # Load the places data from JSON file
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        places = json.load(f)

    # Iterate over each place, query Google Place API, and update the data
    for place in places:
        name = place.get("place_name")
        latitude = place.get("latitude")
        longitude = place.get("longitude")
        
        if name and latitude and longitude:
            print(f"Fetching Google Place ID for: {name}")
            google_place_id = get_google_place_id(name, latitude, longitude)
            place["placeId"] = google_place_id
        else:
            print(f"Missing data for place: {place}")
            place["placeId"] = None

    # Write the updated data to a new JSON file
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(places, f, ensure_ascii=False, indent=4)
    
    print(f"Saved updated data with Google Place IDs to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
