import json

# Load cleanedTripPlannercities.json
with open('cleanedTripPlannercities.json', 'r', encoding='utf-8') as f:
    cleaned_trip_planner_cities = json.load(f)

# Load places_list.json
with open('places_list.json', 'r', encoding='utf-8') as f:
    places_list = json.load(f)

# Create a set of placeIds from places_list.json for quick lookup, converting IDs to strings
processed_place_ids = set(str(item['placeId']) for item in places_list)

# Filter cleaned_trip_planner_cities to remove entries with place_id in processed_place_ids, converting IDs to strings
pending_trip_planner_cities = [
    item for item in cleaned_trip_planner_cities if str(item['place_id']) not in processed_place_ids
]

# Write the filtered list to a new JSON file
with open('pendingTripPlannercities.json', 'w', encoding='utf-8') as f:
    json.dump(pending_trip_planner_cities, f, ensure_ascii=False, indent=4)

filtered_count = len(cleaned_trip_planner_cities) - len(pending_trip_planner_cities)
print(f"Filtered out {filtered_count} processed places.")
print(f"New file 'pendingTripPlannercities.json' created with {len(pending_trip_planner_cities)} pending places.")
