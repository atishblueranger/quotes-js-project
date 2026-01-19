import json
import requests
import os

# Replace with your actual Google API Key
API_KEY = "AIzaSyBe-suda095R60l0G1NrdYeIVKjUCxwK_8"

# Input file that contains your places with Google Place IDs
INPUT_FILE = "atlasobscura_mumbai_india_places_with_google_place_id.json"
# Output file that will store the enriched data
OUTPUT_FILE = "atlasobscura_mumbai_india_places_with_detail.json"

def get_place_details(place_id):
    """
    Calls the Google Place Details API using the given place_id
    and returns the details result if the request is successful.
    """
    base_url = "https://maps.googleapis.com/maps/api/place/details/json"
    # Specify the fields you need (adjust as necessary)
    fields = (
        "formatted_address,name,international_phone_number,geometry,"
        "opening_hours,website,price_level,rating,user_ratings_total,photos,"
        "types,permanently_closed,utc_offset"
    )
    params = {
        "place_id": place_id,
        "fields": fields,
        "key": API_KEY
    }
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        print(f"HTTP error {response.status_code} for place_id: {place_id}")
        return None

    data = response.json()
    if data.get("status") != "OK":
        print(f"Error for place_id {place_id}: {data.get('status')}")
        return None

    return data.get("result")

def build_image_urls(photos):
    """
    From the photos array in the details response, build a list of
    Google Place Photo URLs (using maxwidth=400) and return both the URLs
    and the underlying photo references (imageKeys).
    """
    image_urls = []
    image_keys = []
    if not photos:
        return image_urls, image_keys

    for photo in photos:
        photo_reference = photo.get("photo_reference")
        if photo_reference:
            image_keys.append(photo_reference)
            # Build URL for the photo using the Place Photo API endpoint
            url = (
                f"https://maps.googleapis.com/maps/api/place/photo?"
                f"maxwidth=400&photo_reference={photo_reference}&key={API_KEY}"
            )
            image_urls.append(url)
    return image_urls, image_keys

def enrich_place_data(place):
    """
    For a given place record (with a google place id), fetch additional details
    from the Google Place Details API and update the record with the fields:
      1.  address               -> formatted_address
      2.  categories            -> types
      3.  description           -> (Not available from Google; set to None)
      4.  g_image_urls          -> list built from photos
      5.  id                    -> google place id (same as place_id)
      6.  imageKeys             -> list of photo references
      7.  internationalPhoneNumber -> international_phone_number
      8.  latitude              -> geometry.location.lat
      9.  longitude             -> geometry.location.lng
      10. name                  -> name (override if needed)
      11. numRatings            -> user_ratings_total
      12. openingPeriods        -> opening_hours.periods (if available)
      13. permanentlyClosed     -> permanently_closed
      14. placeId               -> original google place id
      15. priceLevel            -> price_level
      16. rating                -> rating
      17. ratingDistribution   -> (Not provided by Google; set to None)
      18. utcOffset             -> utc_offset
      19. website               -> website
    """
    google_place_id = place.get("placeId")
    if not google_place_id:
        print(f"Missing google place id for {place.get('place_name')}")
        return place  # Return unchanged if no google place id

    details = get_place_details(google_place_id)
    if not details:
        return place

    # Map fields from details to our record
    place["address"] = details.get("formatted_address")
    place["name"] = details.get("name")  # May override the scraped name
    place["internationalPhoneNumber"] = details.get("international_phone_number")
    place["categories"] = details.get("types")
    # Google does not provide a description; set it to None (or update as needed)
    place["description"] = None

    # Update latitude and longitude using details (if available)
    if details.get("geometry") and details["geometry"].get("location"):
        location = details["geometry"]["location"]
        place["latitude"] = location.get("lat", place.get("latitude"))
        place["longitude"] = location.get("lng", place.get("longitude"))

    # Ratings and pricing
    place["rating"] = details.get("rating")
    place["numRatings"] = details.get("user_ratings_total")
    place["priceLevel"] = details.get("price_level")

    # Opening periods from opening_hours (if available)
    if details.get("opening_hours"):
        place["openingPeriods"] = details["opening_hours"].get("periods")
    else:
        place["openingPeriods"] = None

    # Permanently closed flag
    place["permanentlyClosed"] = details.get("permanently_closed")
    # utcOffset (if available)
    place["utcOffset"] = details.get("utc_offset")
    # Website
    place["website"] = details.get("website")

    # Build image URLs and collect photo references
    photos = details.get("photos")
    image_urls, image_keys = build_image_urls(photos)
    place["g_image_urls"] = image_urls
    place["imageKeys"] = image_keys

    # Use the Google Place ID as the ID in our record
    place["id"] = details.get("place_id")
    place["placeId"] = details.get("place_id")
    # ratingDistribution is not provided by Google; set to None
    place["ratingDistribution"] = None

    return place

def main():
    # Load the JSON data
    with open(INPUT_FILE, "r", encoding="utf-8") as infile:
        places = json.load(infile)

    enriched_places = []
    for place in places:
        print(f"Processing: {place.get('place_name')}")
        enriched_place = enrich_place_data(place)
        enriched_places.append(enriched_place)

    # Save the enriched data to a new JSON file
    with open(OUTPUT_FILE, "w", encoding="utf-8") as outfile:
        json.dump(enriched_places, outfile, ensure_ascii=False, indent=4)

    print(f"Saved enriched place details to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
