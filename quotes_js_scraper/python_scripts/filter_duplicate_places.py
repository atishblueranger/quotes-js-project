import json

# Load tripPlannercities.json
with open('tripPlannercities.json', 'r', encoding='utf-8') as f:
    trip_planner_cities = json.load(f)

# Load citiesData.json
with open('citiesData.json', 'r', encoding='utf-8') as f:
    cities_data = json.load(f)

# Create a set of place_ids from citiesData.json for quick lookup
cities_data_place_ids = set(item['place_id'] for item in cities_data)

# Filter trip_planner_cities to remove entries with place_id in cities_data_place_ids
filtered_trip_planner_cities = [
    item for item in trip_planner_cities if item['place_id'] not in cities_data_place_ids
]

# Write the filtered list to a new JSON file
with open('filteredTripPlannercities.json', 'w', encoding='utf-8') as f:
    json.dump(filtered_trip_planner_cities, f, ensure_ascii=False, indent=4)

print(f"Filtered out {len(trip_planner_cities) - len(filtered_trip_planner_cities)} duplicate places.")
print(f"New file 'filteredTripPlannercities.json' created with {len(filtered_trip_planner_cities)} entries.")
