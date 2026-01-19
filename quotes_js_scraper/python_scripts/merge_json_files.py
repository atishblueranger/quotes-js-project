import json

def check_length_of_merged_data(file_path):
    try:
        # Open the merged JSON file and load the data
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Check the length of the data
        data_length = len(data)
        print(f"The total number of national parks in the file is: {data_length}")

    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except json.JSONDecodeError as e:
        print(f"Error reading JSON file: {e}")

# Path to the merged JSON file
merged_file_path = 'national_park_names_urls_M.json'

# Call the function
check_length_of_merged_data(merged_file_path)





# import os
# import json

# def merge_json_files(output_file):
#     # Define the directory containing the JSON files
#     directory = os.getcwd()  # You can modify this if your files are in a different directory

#     # List of files to merge, from A to Z
#     file_names = [f'national_park_names_urls_{chr(letter)}.json' for letter in range(ord('A'), ord('Z') + 1)]

#     # Initialize an empty list to hold all the merged data
#     all_data = []

#     # Loop through each file and load the data
#     for file_name in file_names:
#         file_path = os.path.join(directory, file_name)
#         if os.path.exists(file_path):
#             with open(file_path, 'r', encoding='utf-8') as file:
#                 try:
#                     data = json.load(file)
#                     all_data.extend(data)  # Append the data from each file to the list
#                 except json.JSONDecodeError as e:
#                     print(f"Error reading {file_name}: {e}")
#         else:
#             print(f"File {file_name} does not exist.")
    
#     # Write all the merged data into a new output file
#     with open(output_file, 'w', encoding='utf-8') as output_file:
#         json.dump(all_data, output_file, ensure_ascii=False, indent=4)
    
#     print(f"Data from all files has been merged into {output_file.name}")

# # Call the function with the desired output file name
# merge_json_files('merged_national_park_names_urls.json')
