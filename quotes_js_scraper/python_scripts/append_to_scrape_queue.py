# #!/usr/bin/env python3
# """
# export_scrape_queue.py

# Reads the 'scrape_queue' sheet and writes out a JSON array of:
#   { "title": ..., "url": ..., "category": ... }
# for each row that has non-empty values.
# """

# import json
# from pathlib import Path

# import gspread
# from google.oauth2.service_account import Credentials

# # ─── CONFIG ──────────────────────────────────────────────────────────────────
# SPREADSHEET_ID   = "14X-BC5ag1mSE-ffPeaT_Z0_M0SBOgdS3RGMOYCoDp6A"
# WORKSHEET_NAME   = "scrape_queue"
# CREDENTIALS_FILE = Path(r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\gscontentms-391716692f08.json")
# OUTPUT_FILE      = Path("scrape_queue_export.json")

# # ─── AUTHENTICATE ────────────────────────────────────────────────────────────
# creds  = Credentials.from_service_account_file(
#     CREDENTIALS_FILE,
#     scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
# )
# client = gspread.authorize(creds)
# ws     = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)

# # ─── READ ALL ROWS ────────────────────────────────────────────────────────────
# records = ws.get_all_records()

# # ─── TRANSFORM TO JSON STRUCTURE ──────────────────────────────────────────────
# entries = []
# for row in records:
#     place_name = row.get("Place Name", "").strip()
#     title      = row.get("Target Playlist Title", "").strip()
#     url        = row.get("Source URL", "").strip()
#     category   = row.get("Content Type", "").strip()

#     if  title and url and category and place_name:
#         entries.append({
#             "placeName": place_name,
#             "title":     title,
#             "url":       url,
#             "category":  category
#         })

# # ─── WRITE OUT JSON ───────────────────────────────────────────────────────────
# with OUTPUT_FILE.open("w", encoding="utf-8") as f:
#     json.dump(entries, f, ensure_ascii=False, indent=2)

# print(f"Wrote {len(entries)} entries to {OUTPUT_FILE.resolve()}")


# #!/usr/bin/env python3
# """
# append_to_scrape_queue.py

# Reads a JSON list of playlist jobs and appends them as new rows
# in the 'scrape_queue' sheet, filling columns B–E & G.
# """

import json
from pathlib import Path
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

# ─── CONFIG ──────────────────────────────────────────────────────────────────
SPREADSHEET_ID   = "14X-BC5ag1mSE-ffPeaT_Z0_M0SBOgdS3RGMOYCoDp6A"  # your Getaway CMS sheet
WORKSHEET_NAME   = "scrape_queue"
CREDENTIALS_FILE = Path(r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\gscontentms-391716692f08.json")
JSON_FILE        = Path(r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lbb_mumbai_filtered.json")

# ─── AUTH ─────────────────────────────────────────────────────────────────────
creds  = Credentials.from_service_account_file(
    CREDENTIALS_FILE,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(creds)
ws     = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)

# ─── LOAD JSON & PREPARE ROWS ──────────────────────────────────────────────────
with JSON_FILE.open(encoding="utf-8") as f:
    jobs = json.load(f)

today = datetime.now().strftime("%Y-%m-%d")
rows = []
for job in jobs:
    rows.append([
        "",                       # A: Place ID (blank)
        job.get("placeName", ""), # B: Place Name
        job.get("category", ""),  # C: Content Type
        job.get("title", ""),     # D: Target Playlist Title
        job.get("url", ""),       # E: Source URL
        "",                       # F: Priority (blank)
        today,                    # G: Queued On
        ""                        # H: Remarks (blank)
    ])

# ─── APPEND ALL ROWS AT ONCE ──────────────────────────────────────────────────
if rows:
    # This will append each sub-list as a new row in the sheet.
    ws.append_rows(rows, value_input_option="RAW")
    print(f"Appended {len(rows)} rows to '{WORKSHEET_NAME}' successfully.")
else:
    print("No jobs found in JSON; nothing appended.")
