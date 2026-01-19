import json

# Load explore_place_ids.json which contains a list of place_id strings
with open('explore_place_ids.json', 'r', encoding='utf-8') as f:
    explore_place_ids = json.load(f)

# Convert the list of IDs to a set for efficient lookup
explore_ids_set = set(explore_place_ids)

# Load tripPlannercities.json which contains the full city entries
with open('tripPlannercities.json', 'r', encoding='utf-8') as f:
    trip_planner_cities = json.load(f)

# Filter the entries to include only those whose place_id is in explore_place_ids.json
filtered_cities = [
    city for city in trip_planner_cities 
    if city.get('place_id') in explore_ids_set
]

# Process each filtered entry to remove '-trip-planner' from 'city_name_page'
for city in filtered_cities:
    if 'city_name_page' in city:
        page_name = city['city_name_page']
        suffix = '-trip-planner'
        if page_name.endswith(suffix):
            city['city_name_page'] = page_name[:-len(suffix)]

# Write the updated data to a new JSON file
with open('cleanedExploreTripPlannercities.json', 'w', encoding='utf-8') as f:
    json.dump(filtered_cities, f, ensure_ascii=False, indent=4)

print(f"Processed {len(filtered_cities)} entries and saved them to 'cleanedExploreTripPlannercities.json'")





# import json

# # Read the files
# with open('tripPlannercities.json', 'r') as f:
#     trip_planner_cities = json.load(f)

# with open('explore_places_data.json', 'r') as f:
#     explore_places = json.load(f)

# with open('cleanedTripPlannercities.json', 'r') as f:
#     cleanedTripPlannercities = json.load(f)


# # Print the lengths of both files
# print(f"Number of cities in tripPlannercities.json: {len(trip_planner_cities)}")
# print(f"Number of cities in explore_places_data.json: {len(explore_places)}")
# print(f"Number of cities in cleanedTripPlannercities.json: {len(cleanedTripPlannercities)}")

# # Create sets of place_ids for easy comparison (convert to string for consistency)
# trip_planner_ids = set(str(city['place_id']) for city in trip_planner_cities)
# explore_places_ids = set(str(city['place_id']) for city in explore_places)

# # Find cities that are in both files
# common_ids = trip_planner_ids.intersection(explore_places_ids)

# # Create new list with the common cities (keeping tripPlannercities format)
# common_cities = [
#     city for city in trip_planner_cities 
#     if str(city['place_id']) in common_ids
# ]

# # Write the result to a new JSON file
# with open('common_cities.json', 'w') as f:
#     json.dump(common_cities, f, indent=2)

# print(f"Found {len(common_cities)} cities that exist in both tripPlannercities.json and explore_places_data.json")







# Script to find length of entiries in json file
# import json

# # Read the files with specified encoding
# with open('tripPlannercities.json', 'r', encoding='utf-8') as f:
#     trip_planner_cities = json.load(f)

# with open('explore_places_data.json', 'r', encoding='utf-8') as f:
#     explore_places = json.load(f)

# with open('cleanedTripPlannercities.json', 'r', encoding='utf-8') as f:
#     cleanedTripPlannercities = json.load(f)
# with open('pendingTripPlannercities.json', 'r', encoding='utf-8') as f:
#     pendingTripPlannercities = json.load(f)    

# # Print the lengths of the files
# print(f"Number of cities in tripPlannercities.json: {len(trip_planner_cities)}")
# print(f"Number of cities in explore_places_data.json: {len(explore_places)}")
# print(f"Number of cities in cleanedTripPlannercities.json: {len(cleanedTripPlannercities)}")
# print(f"Number of cities in pendingTripPlannercities.json: {len(pendingTripPlannercities)}")
