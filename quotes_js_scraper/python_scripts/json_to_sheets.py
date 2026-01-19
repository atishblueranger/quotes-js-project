"""
Script to convert JSON data to Google Sheets. USed in my Excel sheet for cities.
"""

import json
import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

# ─── CONFIGURATION ────────────────────────────────────────────────────────────
# 1. The ID of your Google Sheet (found in the URL after /d/)
SPREADSHEET_ID = "14X-BC5ag1mSE-ffPeaT_Z0_M0SBOgdS3RGMOYCoDp6A"

# 2. Path to your Service Account JSON key file
# (Ensure this service account has 'Editor' access to the Sheet)
SERVICE_ACCOUNT_FILE = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\gscontentms-391716692f08.json"

# 3. Path to the JSON data file you want to upload
JSON_FILE_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\traveltriangle_blog.json"

# 4. The name of the tab/worksheet where data should go
TARGET_TAB_NAME = "Global"

# 5. Column Mapping: (JSON Key -> Sheet Header Name)
COLUMN_MAPPING = [
    ("title", "Title"),
    ("category", "Category"),
    ("source_url", "Source URL"),
    ("city", "City"),
    ("country", "Country")
]

# ─── SCRIPT ───────────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def upload_json_to_sheet():
    # --- Step 1: Authentication ---
    print("Authenticating with Google Sheets...")
    try:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        print(f"❌ Error during authentication: {e}")
        return

    # --- Step 2: Load JSON Data ---
    print(f"Reading data from {JSON_FILE_PATH}...")
    if not os.path.exists(JSON_FILE_PATH):
        print(f"❌ File not found: {JSON_FILE_PATH}")
        return

    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    if not raw_data:
        print("⚠️ The JSON file is empty.")
        return

    # --- Step 3: Process Data into DataFrame ---
    # We loop through the raw data and pick only the columns we defined in COLUMN_MAPPING
    records = []
    for item in raw_data:
        row = {}
        for json_key, sheet_header in COLUMN_MAPPING:
            # .get(key, "") ensures we don't crash if a key is missing
            row[sheet_header] = item.get(json_key, "")
        records.append(row)

    df = pd.DataFrame(records)
    
    # Reorder DataFrame columns to match our mapping order explicitly
    ordered_headers = [header for _, header in COLUMN_MAPPING]
    df = df[ordered_headers]

    # --- Step 4: access or Create Worksheet ---
    try:
        worksheet = sheet.worksheet(TARGET_TAB_NAME)
        print(f"Found existing tab: '{TARGET_TAB_NAME}'")
    except WorksheetNotFound:
        print(f"Tab '{TARGET_TAB_NAME}' not found. Creating it...")
        # Create tab with enough rows/cols
        worksheet = sheet.add_worksheet(
            title=TARGET_TAB_NAME, 
            rows=len(df) + 50, 
            cols=len(COLUMN_MAPPING)
        )

    # --- Step 5: Upload Data ---
    print(f"Clearing old data and uploading {len(df)} new rows...")
    
    # Clear existing data to avoid conflicts
    worksheet.clear()
    
    # Convert DataFrame to a list of lists (Header + Data)
    # fillna("") is crucial because Google Sheets API rejects NaN values
    sheet_data = [df.columns.values.tolist()] + df.fillna("").values.tolist()
    
    # Update the sheet starting from cell A1
    worksheet.update(range_name="A1", values=sheet_data)
    
    print("✅ Upload complete!")

if __name__ == "__main__":
    upload_json_to_sheet()



# # Script for adding country names to cities sheet
# #!/usr/bin/env python3
# from pathlib import Path
# import json
# import gspread
# from google.oauth2.service_account import Credentials

# # ─── CONFIG ──────────────────────────────────────────────────────────────────
# SPREADSHEET_ID   = "14X-BC5ag1mSE-ffPeaT_Z0_M0SBOgdS3RGMOYCoDp6A"
# WORKSHEET_NAME   = "islands"
# CREDENTIALS_FILE = Path(r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\gscontentms-391716692f08.json")
# JSON_FILE        = Path(r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\islands_output2.json")  # your JSON

# # ─── AUTH ─────────────────────────────────────────────────────────────────────
# creds  = Credentials.from_service_account_file(
#     CREDENTIALS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets"]
# )
# client = gspread.authorize(creds)
# ws     = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)

# # ─── LOAD JSON & BUILD MAP ─────────────────────────────────────────────────────
# with JSON_FILE.open(encoding="utf-8") as f:
#     data = json.load(f)
# country_map = {item["id"]: item["country"] for item in data}

# # ─── READ SHEET IDS & PREPARE UPDATES ──────────────────────────────────────────
# sheet_ids = ws.col_values(1)[1:]  # A2:A…
# requests  = []

# for row_idx, cid in enumerate(sheet_ids, start=2):
#     country = country_map.get(cid)
#     if country:
#         # column C is “Country”
#         requests.append({
#           "range": f"D{row_idx}",
#           "values": [[country]]
#         })

# # ─── BATCH-UPDATE IN ONE CALL ─────────────────────────────────────────────────
# if requests:
#     ws.batch_update(requests, value_input_option="RAW")
#     print(f"Filled Country for {len(requests)} rows ✔")
# else:
#     print("No matches found; check your JSON IDs vs sheet IDs.")





# from pathlib import Path
# from google.oauth2.service_account import Credentials
# import gspread

# # ─── CONFIG ──────────────────────────────────────────────────────────────────
# SPREADSHEET_ID   = "14X-BC5ag1mSE-ffPeaT_Z0_M0SBOgdS3RGMOYCoDp6A"
# WORKSHEET_NAME   = "cities"
# CREDENTIALS_FILE = Path(
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\gscontentms-391716692f08.json"
# )

# # your list of IDs whose Names (col B) should turn blue
# HIGHLIGHT_IDS = {
#      "13",
#     "25",
#     "24",
#     "60",
#     "35",
#     "78",
#     "91",
#     "69",

# }

# # ─── AUTH ─────────────────────────────────────────────────────────────────────
# creds  = Credentials.from_service_account_file(
#     CREDENTIALS_FILE,
#     scopes=["https://www.googleapis.com/auth/spreadsheets"]
# )
# client = gspread.authorize(creds)
# ws     = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)

# # ─── READ & BUILD REQUESTS ────────────────────────────────────────────────────
# ids = ws.col_values(1)[1:]   # A2:A…
# requests = []

# for row_idx, cid in enumerate(ids, start=2):
#     if cid in HIGHLIGHT_IDS:
#         requests.append({
#             "range": f"B{row_idx}",       # column B
#             "format": {
#                 "textFormat": {
#                     "foregroundColor": {
#                         "red":   0.0,
#                         "green": 0.0,
#                         "blue":  1.0
#                     }
#                 }
#             }
#         })

# # ─── SEND ONE BATCH CALL ─────────────────────────────────────────────────────
# if requests:
#     ws.batch_format(requests)
#     print(f"Colored {len(requests)} names blue ✔")
# else:
#     print("No matching IDs found – nothing to highlight.")




# from pathlib import Path
# from google.oauth2.service_account import Credentials
# import gspread

# SPREADSHEET_ID   = "14X-BC5ag1mSE-ffPeaT_Z0_M0SBOgdS3RGMOYCoDp6A"
# WORKSHEET_NAME   = "cities"
# CREDENTIALS_FILE = Path(
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\gscontentms-391716692f08.json"
# )

# TARGET_IDS = {
#     # "58144","9613","9614","9616","9617","58152","9625","9621","131072", 
#     # … etc (all your IDs) …

# }

# # ─── AUTH ─────────────────────────────────────────────────────────────────────
# # Authenticate
# creds  = Credentials.from_service_account_file(
#     CREDENTIALS_FILE,
#     scopes=["https://www.googleapis.com/auth/spreadsheets"]
# )
# client = gspread.authorize(creds)
# ws     = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)

# # Read your city IDs from A2:A
# ids = ws.col_values(1)[1:]

# # Build a single batch-format request
# requests = []
# for row_idx, cid in enumerate(ids, start=2):
#     if cid in TARGET_IDS:
#         requests.append({
#             "range": f"A{row_idx}",
#             "format": {
#                 "backgroundColor": {
#                     "red":   1.0,
#                     "green": 1.0,
#                     "blue":  0.0
#                 }
#             }
#         })

# # Fire off exactly one API call
# if requests:
#     ws.batch_format(requests)
#     print(f"Colored {len(requests)} cells yellow ✔")
# else:
#     print("No matching IDs found – nothing to highlight.")



# from pathlib import Path
# from google.oauth2.service_account import Credentials
# import gspread

# SPREADSHEET_ID   = "14X-BC5ag1mSE-ffPeaT_Z0_M0SBOgdS3RGMOYCoDp6A"
# WORKSHEET_NAME   = "municipalities"
# CREDENTIALS_FILE = Path(
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\gscontentms-391716692f08.json"
# )

# TARGET_IDS = {
#    "10008", "10046", "10061", "10064", "10074", "10211", "10219", "10235", "11", "11769", "118", "131078", "131079", "131086", "131083", "131090", "131093", "131094", "131100", "131115", "131117", "131118", "131132", "131134", "131150", "131318", "131360", "131478", "15", "176", "18", "184", "1868", "19", "20", "2", "21", "22", "2253", "23", "24", "28", "29", "297", "30", "3", "304", "31", "313", "32", "329", "34", "37", "38", "405", "41", "412", "42", "428", "461", "51", "53", "541", "58049", "58150", "58170", "58175", "58182", "58183", "58187", "58191", "58198", "59", "68", "701", "753", "783", "787", "78744", "78752", "79", "79306", "79309", "79303", "80", "81180", "81194", "81226", "814", "81904", "81905", "81909", "82574", "82576", "82578", "82581", "82585", "82593", "82594", "842", "85939", "85940", "956", "9630", "9635", "9636", "9637", "9643", "9649", "9656", "9654", "9663", "9666", "9668", "9670", "9676", "9677", "9679", "9683", "9689", "9687", "9690", "9691", "9694", "9696", "9707", "9712", "9709", "9713", "9711", "9723", "9725", "9724", "9727", "9730", "9734", "9745", "9735", "9763", "9751", "9756", "9765", "9767", "9792", "9799", "9798", "9800", "9801", "98", "9802", "9803", "9826", "9836", "9838", "9840", "9850", "9861", "9889", "9891", "9903", "9917", "9919", "9935", "9937", "9945", "9998",
# # "1", "10", "1000", "10015", "1002", "10030", "10031", "10033", "10106", "10123", "1013", "10143", "1015", "10171", "10176", "10230", "10241", "10260", "105", "1055", "10595", "10792", "1089", "110", "1135", "1138", "11489", "1159", "1177", "1212", "122", "1222", "1241", "12491", "12525", "128", "1291", "13", "1302", "131071", "131072", "131073", "131074", "131075", "131076", "131077", "131080", "131081", "131084", "131085", "131087", "131088", "131089", "131091", "131092", "131095", "131098", "131099", "131102", "131103", "131104", "131105", "131106", "131107", "131108", "131109", "131110", "131113", "131114", "131119", "131122", "131125", "131127", "131131", "131138", "131139", "131140", "131146", "131147", "131151", "131157", "131162", "131164", "131168", "131177", "131178", "131180", "131315", "131327", "131337", "131362", "131389", "131395", "131438", "131447", "131457", "1316", "131626", "1325", "133", "135226", "135383", "135385", "135391", "135393", "135408", "135438", "135468", "135498", "137", "1406", "1407", "143", "1435", "1442", "1443", "1444", "145", "146238", "146242", "146244", "146271", "146282", "1463", "146468", "1473", "147352", "1480", "1540", "16", "161", "1668", "17", "174", "1743", "175", "1763", "1765", "1784", "1808", "183", "187", "1874", "1886", "1887", "194", "1942", "1966", "200", "2027", "207", "209", "2177", "223", "235", "2371", "248", "2540", "256", "257", "287", "3129", "33", "330", "334", "348", "349", "35", "350", "371", "375", "384", "39", "398", "4", "43", "44", "444", "449", "46", "465", "468", "472", "478", "479", "480", "485", "49", "50", "503", "518", "52", "524", "527", "536", "558", "563", "57", "57258", "58045", "58046", "58047", "58048", "58050", "58051", "58052", "58058", "58059", "58068", "58079", "58144", "58145", "58146", "58147", "58148", "58152", "58153", "58154", "58156", "58157", "58158", "58159", "58160", "58161", "58162", "58164", "58166", "58168", "58169", "58171", "58172", "58173", "58174", "58176", "58177", "58178", "58179", "58180", "58181", "58184", "58185", "58188", "58189", "58192", "58193", "58195", "58196", "58199", "58201", "58202", "58203", "58205", "58206", "58210", "58211", "58212", "58213", "58216", "58218", "58219", "58222", "58224", "58226", "58228", "58231", "58232", "58241", "58242", "58244", "58248", "58269", "58275", "58276", "58284", "58286", "58289", "58300", "58342", "58345", "58455", "58476", "58563", "58748", "589", "59165", "6", "60", "607", "615", "62", "64", "662", "67", "673", "687", "69", "696", "7", "714", "722", "728", "73", "730", "738", "74", "75", "76078", "78", "785", "78746", "79299", "79300", "79302", "79304", "79305", "79321", "798", "8", "805", "81182", "81189", "81196", "81197", "81213", "81219", "81250", "81903", "81906", "81911", "81912", "81913", "81918", "81924", "81941", "81981", "82", "82575", "82577", "82579", "82582", "82584", "82586", "82588", "849", "85", "850", "85941", "85943", "85946", "86647", "86651", "86652", "86654", "86655", "86656", "86659", "86661", "86667", "86668", "86674", "86675", "86676", "86686", "86689", "86691", "86693", "86706", "86714", "86716", "86722", "86729", "86731", "86743", "86772", "86776", "86779", "86797", "86805", "86819", "86831", "86851", "86887", "86904", "86937", "86938", "86988", "87077", "87236", "87411", "877", "88352", "88358", "88359", "88360", "88362", "88366", "88367", "88368", "88373", "88375", "88380", "88384", "88386", "88402", "88403", "88406", "88407", "88408", "88419", "88420", "88423", "88432", "88434", "88437", "88438", "88445", "88449", "88455", "88464", "88477", "88478", "88486", "88503", "88527", "88609", "88654", "88723", "88731", "894", "895", "9", "90374", "90405", "90407", "90409", "90412", "90413", "90419", "90420", "90647", "90648", "90651", "90652", "90656", "90660", "90759", "90763", "90764", "91", "91342", "91387", "91388", "91389", "91394", "91397", "91399", "914", "91403", "91406", "91524", "91525", "91526", "91530", "91534", "91535", "91536", "91537", "91540", "91548", "92", "926", "93", "948", "954", "9613", "9614", "9615", "9616", "9617", "9620", "9621", "9622", "9623", "9624", "9625", "9626", "9629", "9631", "9634", "9638", "9640", "9641", "9642", "9645", "9646", "9647", "9650", "9651", "9653", "9655", "9657", "9658", "9659", "9660", "9662", "9665", "9667", "9669", "9672", "9673", "9674", "9675", "9680", "9681", "9682", "9688", "9692", "9693", "9695", "9702", "9703", "9704", "9705", "9708", "9714", "9715", "9717", "9719", "9721", "9722", "9737", "9738", "9739", "9744", "975", "9750", "9753", "9757", "976", "9761", "9769", "9771", "9772", "978", "9785", "979", "9790", "9804", "981", "9812", "9817", "9819", "982", "9830", "9841", "9843", "9849", "9852", "9857", "9875", "9886", "9887", "9896", "9899", "99", "9915", "9928", "994", "9942", "9950", "9952"
# #  "135383",
# #     "135385",
# #     "135391",
# #     "135393",
# #     "135408",
# #     "135438",
# #     "135468",
# #     "135498",
# #     "79304",
# #     "86647",
# #     "86651",
# #     "86652",
# #     "86654",
# #     "86655",
# #     "86656",
# #     "86659",
# #     "86661",
# #     "86667",
# #     "86668",
# #     "86693",
# #     "86729",
# #     "86731",
# #     "86772",
# #     "86776",
# #     "86779",
# #     "86797",
# #     "86819",
# #     "86831",
# #     "86851",
# #     "86938",
# #     "87077",
# #     "88352",
# #     "88358",
# #     "88359",
# #     "88360",
# #     "88362",
# #     "88366",
# #     "88367",
# #     "88368",
# #     "88373",
# #     "88375",
# #     "88380",
# #     "88384",
# #     "88386",
# #     "88402",
# #     "88403",
# #     "88407",
# #     "88408",
# #     "88419",
# #     "88420",
# #     "88423",
# #     "88432",
# #     "88434",
# #     "88437",
# #     "88438",
# #     "88445",
# #     "88449",
# #     "88455",
# #     "88464",
# #     "88477",
# #     "88478",
# #     "88486",
# #     "88503",
# #     "88527",
# #     "88609",
# #     "88654",
# #     "88723",
# #     "88731",
# #     "90374",
# #     "90407",
# #     "90647",
# #     "90648",
# #     "90651",
# #     "90652",
# #     "90656",
# #     "90660",
# #     "90759",
# #     "90763",
# #     "90764",
# #     "91342",
# #     "91387",
# #     "91394",
# #     "91403",
# #     "91524",
# #     "91525",
# #     "91526",
# #     "91530",
# #     "91534",
# #     "91535",
# #     "91536",
# #     "91537",
# #     "91540",
# #     "91548",
# }

# # auth
# creds  = Credentials.from_service_account_file(
#     CREDENTIALS_FILE,
#     scopes=["https://www.googleapis.com/auth/spreadsheets"]
# )
# client = gspread.authorize(creds)
# ws     = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)

# # read all IDs from A2 down
# ids = ws.col_values(1)[1:]
# updates = []

# for idx, country_id in enumerate(ids, start=2):
#     if country_id in TARGET_IDS:
#         # qualify with sheet name if you like, but ws.batch_update assumes current sheet
#         updates.append({
#             "range": f"J{idx}",    
#             "values": [["Done"]]
#         })

# if updates:
#     ws.batch_update(updates, value_input_option="RAW")
#     print(f"Updated {len(updates)} rows ✔")
# else:
#     print("No matching IDs found – nothing to update.")




# #!/usr/bin/env python3
# """
# upload_any_json.py – Push multiple JSON datasets into separate tabs
#                          of the same Google Sheet.

# Usage:
#   export GOOGLE_APPLICATION_CREDENTIALS="/path/service_account.json"
#   python upload_any_json.py
# """

# import json, os
# from pathlib import Path

# import gspread
# import pandas as pd
# from google.oauth2.service_account import Credentials
# from gspread.exceptions import WorksheetNotFound

# # ─── GLOBAL CONFIG ────────────────────────────────────────────────────────────
# SPREADSHEET_ID = "14X-BC5ag1mSE-ffPeaT_Z0_M0SBOgdS3RGMOYCoDp6A"           # master spreadsheet
# SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
# CREDENTIALS_FILE = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\gscontentms-391716692f08.json"

# DATA_DIR = Path(r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts")
# # Each entry = one JSON source -> one Sheet tab + column map
# DATASETS = [
#         # {
#         # "json_file":  DATA_DIR / "countries.json",
#         # "tab": "countries",
#         #  "columns": [("id", "Id"), ("name", "Name")],
#         # },

#     # {
#     #     "json_file": DATA_DIR /"states_output.json",
#     #     "tab": "states",
#     #     "columns": [("id", "Id"), ("name", "Name"),
#     #                  ("popularity", "Popularity")],
#     # },
#     # {
#     #     "json_file": DATA_DIR /"regions_with_popularity.json",
#     #     "tab": "regions",
#     #     "columns": [("id", "Id"), ("name", "Name"),
#     #                 ("popularity", "Popularity")],
#     # },
#     # {
#     #     "json_file": DATA_DIR /"islands_output.json",
#     #     "tab": "islands",
#     #     "columns": [("id", "Id"), ("name", "Name"),
#     #                 ("popularity", "Popularity")],
#     # },
#     # {
#     #     "json_file": DATA_DIR /"districts_output.json",
#     #     "tab": "districts",
#     #     "columns": [("id", "Id"), ("name", "Name"),
#     #                 ("popularity", "Popularity")],
#     # },
#     # {
#     #     "json_file": DATA_DIR /"cities_output.json",
#     #     "tab": "cities",
#     #     "columns": [("id", "Id"), ("name", "Name"),
#     #                 ("popularity", "Popularity")],
#     # },
#       {
#         "json_file": DATA_DIR /"municipality_output.json",
#         "tab": "municipalities",
#         "columns": [("id", "Id"), ("name", "Name"),
#                     ("popularity", "Popularity"), ("country", "Country")],
#     },
# ]

# # ─── AUTH ─────────────────────────────────────────────────────────────────────
# creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
# client = gspread.authorize(creds)
# sheet = client.open_by_key(SPREADSHEET_ID)

# # ─── PUSH EACH DATASET ────────────────────────────────────────────────────────
# for ds in DATASETS:
#     path = Path(ds["json_file"])
#     if not path.exists():
#         print(f"⚠️  {path} not found – skipping")
#         continue

#     with path.open(encoding="utf-8") as f:
#         raw = json.load(f)

#     # Build list-of-dicts with desired column names
#     records = []
#     for item in raw:
#         row = {}
#         for src_key, dest_col in ds["columns"]:
#             row[dest_col] = item.get(src_key, "")
#         records.append(row)

#     df = pd.DataFrame(records)
#     # ws = sheet.worksheet(ds["tab"])  # tab must already exist
#     try:
#         ws = sheet.worksheet(ds["tab"])
#     except WorksheetNotFound:
#     # create an empty worksheet sized to your header
#         ws = sheet.add_worksheet(
#         title=ds["tab"],
#         rows=1,                    # start with one row; gspread will expand automatically
#         cols=len(ds["columns"])    # number of columns you’re writing
#     )

#     ws.clear()
#     ws.update(
#         [df.columns.values.tolist()] + df.values.tolist(),
#         value_input_option="RAW",
#     )
#     print(f"✓ {len(df)} rows → '{ds['tab']}' tab")

# print("All uploads complete ✅")






