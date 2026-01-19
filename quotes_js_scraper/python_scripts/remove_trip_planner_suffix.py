import json

# Load filteredTripPlannercities.json
with open('filteredTripPlannercities.json', 'r', encoding='utf-8') as f:
    trip_planner_cities = json.load(f)

# Process each entry to remove '-trip-planner' from 'city_name_page'
for item in trip_planner_cities:
    if 'city_name_page' in item:
        city_name_page = item['city_name_page']
        # Remove the suffix if it exists
        if city_name_page.endswith('-trip-planner'):
            item['city_name_page'] = city_name_page[:-len('-trip-planner')]

# Write the updated data to a new JSON file
with open('cleanedTripPlannercities.json', 'w', encoding='utf-8') as f:
    json.dump(trip_planner_cities, f, ensure_ascii=False, indent=4)

print(f"Processed {len(trip_planner_cities)} entries.")
print("New file 'cleanedTripPlannercities.json' created with updated 'city_name_page' values.")
