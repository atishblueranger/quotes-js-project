import json

# Load the matched national parks data from the JSON file
with open('matched_national_parks.json', 'r', encoding='utf-8') as f:
    parks_data = json.load(f)

# Dictionary to store unique entries based on place_id
unique_parks = {}

for park in parks_data:
    place_id = park["place_id"]

    # Clean the 'city_name_page' by removing the 'trip-planner' part
    city_name_page = park["city_name_page"].replace("-trip-planner", "")

    # Update the city_name_page with the cleaned version
    park["city_name_page"] = city_name_page

    # Store only unique entries based on place_id
    if place_id not in unique_parks:
        unique_parks[place_id] = park

# Convert the unique_parks dictionary back to a list
cleaned_parks_data = list(unique_parks.values())

# Save the cleaned data to a new JSON file
output_file = 'cleaned_matched_national_parks.json'
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(cleaned_parks_data, f, ensure_ascii=False, indent=4)

print(f"Saved {len(cleaned_parks_data)} unique parks to {output_file}")



# import json

# # Load the data from merged_national_park_name_url.json and tripPlannercities.json
# with open('merged_national_park_names_urls.json', 'r', encoding='utf-8') as f:
#     national_parks_data = json.load(f)

# with open('tripPlannercities.json', 'r', encoding='utf-8') as f:
#     trip_planner_data = json.load(f)

# # Initialize a list to store matched data
# matched_parks = []

# # Loop through each national park and find the matching place_id and city_name_page in trip_planner_data
# for park in national_parks_data:
#     park_name = park.get("trail_name")  # Use .get() to avoid KeyError

#     if park_name:  # Proceed only if park_name is not None or empty
#         park_name = park_name.lower()  # Convert to lowercase for case-insensitive comparison

#         # Search for the park in trip planner cities
#         for trip_place in trip_planner_data:
#             city_name = trip_place.get("city_name", "").lower()  # Convert to lowercase for case-insensitive comparison
#             if park_name in city_name:
#                 matched_parks.append({
#                     "place_id": trip_place["place_id"],
#                     "national_park": park["trail_name"],
#                     "city_name_page": trip_place["city_name_page"]
#                 })
#                 break  # Stop after the first match to avoid duplicates

# # Save the matched results to a new file
# output_file = 'matched_national_parks.json'
# with open(output_file, 'w', encoding='utf-8') as f:
#     json.dump(matched_parks, f, ensure_ascii=False, indent=4)

# print(f"Saved {len(matched_parks)} matched parks to {output_file}")




# import json

# # Load the data from merged_national_park_name_url.json and tripPlannercities.json
# with open('merged_national_park_names_urls.json', 'r', encoding='utf-8') as f:
#     national_parks_data = json.load(f)

# with open('tripPlannercities.json', 'r', encoding='utf-8') as f:
#     trip_planner_data = json.load(f)

# # Initialize a list to store matched data
# matched_parks = []

# # Loop through each national park and find the matching place_id and city_name_page in trip_planner_data
# for park in national_parks_data:
#     park_name = park["trail_name"].lower()  # Convert to lowercase for case-insensitive comparison

#     # Search for the park in trip planner cities
#     for trip_place in trip_planner_data:
#         city_name = trip_place["city_name"].lower()  # Convert to lowercase for case-insensitive comparison
#         if park_name in city_name:
#             matched_parks.append({
#                 "place_id": trip_place["place_id"],
#                 "national_park": park["trail_name"],
#                 "city_name_page": trip_place["city_name_page"]
#             })
#             break  # Stop after the first match to avoid duplicates

# # Save the matched results to a new file
# output_file = 'matched_national_parks.json'
# with open(output_file, 'w', encoding='utf-8') as f:
#     json.dump(matched_parks, f, ensure_ascii=False, indent=4)

# print(f"Saved {len(matched_parks)} matched parks to {output_file}")
