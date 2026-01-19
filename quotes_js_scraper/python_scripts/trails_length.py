import json

def count_trails(filename):
    try:
        # Load the JSON file
        with open(filename, 'r', encoding='utf-8') as f:
            trail_data = json.load(f)
        
        # Count the number of trails in the file
        trail_count = len(trail_data)
        print(f"Total number of trails fetched: {trail_count}")
        
    except FileNotFoundError:
        print(f"File '{filename}' not found.")
    except json.JSONDecodeError:
        print(f"Error decoding JSON from file '{filename}'.")

# Call the function with your JSON file
count_trails('trail_names_urls_C.json')
