#!/usr/bin/env python3
"""
patch_top_wonders_source.py
────────────────────────────
Reads lonely_planet_countries_attractions.json and, for each country name
in column B of the 'countries' sheet, writes its attractions_url into column I.
"""

from pathlib import Path
import json
import gspread
from google.oauth2.service_account import Credentials

# ─── CONFIG ──────────────────────────────────────────────────────────────────
SPREADSHEET_ID    = "14X-BC5ag1mSE-ffPeaT_Z0_M0SBOgdS3RGMOYCoDp6A"
WORKSHEET_NAME    = "cities"
CREDENTIALS_FILE  = Path(r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\gscontentms-391716692f08.json")
JSON_FILE         = Path(r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lonely_planet_cities_attractions.json")

# ─── AUTH ─────────────────────────────────────────────────────────────────────
creds  = Credentials.from_service_account_file(
    CREDENTIALS_FILE,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(creds)
ws     = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)

# ─── LOAD JSON & BUILD TITLE→URL MAP ───────────────────────────────────────────
with JSON_FILE.open(encoding="utf-8") as f:
    data = json.load(f)

url_map = { item["title"]: item["attractions_url"] for item in data }

# ─── READ EXISTING COUNTRY NAMES & PREPARE UPDATES ────────────────────────────
country_names = ws.col_values(2)[1:]   # B2:B…
requests = []

for row_idx, name in enumerate(country_names, start=2):
    url = url_map.get(name)
    if url:
        requests.append({
            "range": f"K{row_idx}",       # column I
            "values": [[url]]
        })

# ─── COMMIT ALL UPDATES IN ONE BATCH ─────────────────────────────────────────
if requests:
    ws.batch_update(requests, value_input_option="RAW")
    print(f"Written {len(requests)} URLs into Top Wonders (Source) ✔")
else:
    print("No matching country names found in the JSON. Nothing updated.")
