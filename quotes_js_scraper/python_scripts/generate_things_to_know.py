# #!/usr/bin/env python3
# """
# generate_things_to_know_to_json.py
# ────────────────────────────────────
# For each (doc_id, country_name) in COUNTRIES:
#  1) Generate a “things_to_know” JSON via OpenAI with exactly 8 keys.
#  2) Save as a local JSON file named {country}_things_to_know.json.
# """

# import os
# import json
# from json.decoder import JSONDecodeError

# import openai

# # ─── CONFIG ────────────────────────────────────────────────────────────
# # List of (doc_id, country_name) just for naming; doc_id is unused here.
# COUNTRIES = [
#     ("86661", "India"),
#     ("90374", "Canada"),
#     # ("<AZERBAIJAN_DOC_ID>", "Azerbaijan"),
#     # …add more pairs as needed…
# ]

# OPENAI_MODEL = "gpt-4o-mini"

# # ─── INITIALIZE OPENAI ─────────────────────────────────────────────────────
# openai.api_key = "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"
# if not openai.api_key:
#     raise RuntimeError("Missing OPENAI_API_KEY environment variable")

# # ─── SECTION GENERATOR ─────────────────────────────────────────────────────
# def generate_things_to_know(country: str) -> dict:
#     """
#     Generates a JSON object with exactly 8 keys.
#     Each key is a concise heading for 'Things to Know Before Visiting {country}',
#     and each value is a 2–3 sentence descriptive tip.
#     """
#     system_msg = "You are a JSON-only generator. Output MUST BE valid JSON and nothing else."
#     user_prompt = f"""
# Generate a JSON object with exactly 8 keys.
# Each key should be a short, title-style heading for the 'Things to Know Before Visiting {country}' section (for example, 'Cultural Dress Codes').
# Each value should be a 2–3 sentence paragraph expanding on that tip.
# Do NOT output markdown, code fences, or any extra keys.
# """
#     resp = openai.ChatCompletion.create(
#         model=OPENAI_MODEL,
#         messages=[
#             {"role": "system",  "content": system_msg},
#             {"role": "user",    "content": user_prompt},
#         ],
#         temperature=0.0,
#     )
#     content = resp.choices[0].message.content.strip()
#     try:
#         return json.loads(content)
#     except JSONDecodeError:
#         print(f"⚠️ Failed to parse JSON for {country}. Raw response:\n{content}")
#         raise

# # ─── MAIN WORKFLOW ─────────────────────────────────────────────────────────
# def main():
#     for doc_id, country in COUNTRIES:
#         print(f"🛠 Generating 'Things to Know' for {country}…")
#         things = generate_things_to_know(country)

#         # Wrap under a top‐level key if you like
#         output = {"things_to_know": things}
        
#         fname = f"{country.lower().replace(' ', '_')}_things_to_know.json"
#         with open(fname, "w", encoding="utf-8") as f:
#             json.dump(output, f, ensure_ascii=False, indent=2)
#         print(f"💾 Wrote local file: {fname}")

#     print("\n✅ All done.")

# if __name__ == "__main__":
#     main()


#!/usr/bin/env python3
# """
# generate_and_store_things_to_know.py
# ───────────────────────────────────────

# For each (doc_id, country_name) in COUNTRIES:
#  1) Generate a “things_to_know” JSON via OpenAI with exactly 9 keys:
#       • "Introduction": a 2–3 sentence lead-in paragraph
#       • 8 concise heading→tip entries
#  2) Store under one Firestore document named "things_to_know" in the
#     travelInfo subcollection of the allplaces collection.
# """

# import os
# import json
# from json.decoder import JSONDecodeError

# import openai
# import firebase_admin
# from firebase_admin import credentials, firestore

# # ─── CONFIG ───────────────────────────────────────────────────────────────────
# # Fill this list with your Firestore document IDs and country names.
# # e.g.: [("86661", "India"), ("<AZERBAIJAN_DOC_ID>", "Azerbaijan"), …]
# COUNTRIES = [
#     # ("86661", "India"),
#     ("86647", "Japan"),
#     ("86651", "Thailand"),
#     # ("86661", "India"),
#     ("86663", "Indonesia"),
#     ("86667", "Nepal"),
#     ("86683", "Malaysia"),
#     ("86693", "Sri Lanka"),
#     ("86656", "South Korea"),
#     ("86654", "Singapore"),
#     ("86653", "China"),
#     ("86819", "Bhutan"),
#     ("91525", "Israel"),
#     ("91534", "Jordan"),
#     ("91536", "Oman"),
#     ("86831", "Pakistan"),
#     ("91535", "Qatar"),
#     ("88367", "Turkiye"),
#     ("91524", "United Arab Emirates"),
#     ("88362", "Spain"),
#     ("88373", "The Netherlands"),
#     ("88352", "Italy"),
#     ("88386", "Ireland"),
#     ("88419", "Iceland"),
#     ("88380", "Hungary"),
#     ("88375", "Greece"),
#     ("88368", "Germany"),
#     ("88358", "France"),
#     ("88359", "United Kingdom"),
#     ("91387", "Australia"),
#     ("91394", "New Zealand"),
#     ("91403", "Fiji"),
#     ("90764", "South Africa"),
#     ("90763", "Egypt"),
#     ("90759", "Morocco"),
#     ("79304", "Mauritius"),
#     ("90407", "United States"),
#     ("91342", "Mexico"),
#     ("90374", "Canada"),
#     # …add more (doc_id, country_name) pairs as needed…
# ]

# # Path to your Firebase service account JSON
# SERVICE_ACCOUNT_JSON = (
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
#     r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# )

# # OpenAI model to use
# OPENAI_MODEL = "gpt-4o-mini"

# # ─── INITIALIZE OPENAI & FIRESTORE ────────────────────────────────────────────
# openai.api_key = "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"
# if not openai.api_key:
#     raise RuntimeError("Missing OPENAI_API_KEY environment variable")

# if not firebase_admin._apps:
#     cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
#     firebase_admin.initialize_app(cred)
# db = firestore.client()


# ─── SECTION GENERATOR ────────────────────────────────────────────────────────
# def generate_things_to_know(country: str) -> dict:
#     """
#     Generates a JSON object with exactly 9 keys:
#       • "Introduction": a 2–3 sentence introductory paragraph
#       • 8 keys, each a short heading paired with a 2–3 sentence tip
#     """
#     system_msg = "You are a JSON-only generator. Output MUST BE valid JSON and nothing else."
#     user_prompt = f"""
# Generate a JSON object with exactly 9 keys.
# 1) The first key must be "Introduction", whose value is a 2–3 sentence introductory paragraph for the 'Things to Know Before Visiting {country}' section.
# 2) The next 8 keys should each be a concise, title-style heading (e.g., "Cultural Dress Codes") with a 2–3 sentence paragraph as the value.
# Do NOT output markdown, code fences, or any extra keys."""
#     resp = openai.ChatCompletion.create(
#         model=OPENAI_MODEL,
#         messages=[
#             {"role": "system",  "content": system_msg},
#             {"role": "user",    "content": user_prompt},
#         ],
#         temperature=0.0,
#     )

#     content = resp.choices[0].message.content.strip()
#     try:
#         return json.loads(content)
#     except JSONDecodeError:
#         print(f"⚠️ Failed to parse JSON for {country}. Raw response:\n{content}")
#         raise


# # ─── MAIN WORKFLOW ─────────────────────────────────────────────────────────────
# def main():
#     for doc_id, country in COUNTRIES:
#         print(f"\n🛠 Generating 'Things to Know' for {country} (doc {doc_id})…")

#         # 1. Generate the section
#         things_to_know = generate_things_to_know(country)

#         # 2. Write local JSON for inspection
#         # output = {"things_to_know": things_to_know}
#         # fname = f"{country.lower().replace(' ', '_')}_things_to_know.json"
#         # with open(fname, "w", encoding="utf-8") as f:
#         #     json.dump(output, f, ensure_ascii=False, indent=2)
#         # print(f"💾 Wrote local file: {fname}")

#         # 3. Store in Firestore under travelInfo/things_to_know
#         ti_coll = (
#             db.collection("allplaces")
#               .document(doc_id)
#               .collection("travelInfo")
#         )
#         ti_coll.document("things_to_know").set(things_to_know)
#         print(f"☁️ Wrote Firestore: allplaces/{doc_id}/travelInfo/things_to_know")

#     print("\n✅ All done.")


# if __name__ == "__main__":
#     main()





# #!/usr/bin/env python3
# """
# generate_and_store_things_to_know.py
# ───────────────────────────────────────

# For each (doc_id, country_name) in COUNTRIES:
#  1) Generate a “things_to_know” JSON via OpenAI with exactly 10 keys:
#       • "Introduction": 2–3 sentence lead-in
#       • 8 entries: each starts with a relevant emoji + title, then a 2–3 sentence tip
#       • "Conclusion": 1–2 sentence wrap-up
#  2) Write that JSON into Firestore under travelInfo/things_to_know.
# """

# import os
# import json
# from json.decoder import JSONDecodeError

# import openai
# import firebase_admin
# from firebase_admin import credentials, firestore

# # ─── CONFIG ───────────────────────────────────────────────────────────────────
# COUNTRIES = [
#     # ("58144", "New York City"),
#     # ("9613", "London"),
#     # ("9614", "Paris"),
#     # ("9616", "Rome"),
  

#     # …add more (doc_id, country_name) tuples…
# ]

# SERVICE_ACCOUNT_JSON = (
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
#     r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# )
# OPENAI_MODEL = "gpt-4o-mini"

# # ─── INITIALIZE ────────────────────────────────────────────────────────────────
# openai.api_key = "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"
# if not openai.api_key:
#     raise RuntimeError("Missing OPENAI_API_KEY")

# if not firebase_admin._apps:
#     cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
#     firebase_admin.initialize_app(cred)
# db = firestore.client()


# # ─── GENERATOR ────────────────────────────────────────────────────────────────
# def generate_things_to_know(country: str) -> dict:
#     """
#     Returns a JSON object with exactly 10 keys:
#       1) "Introduction": 2–3 sentences
#       2–9) Eight keys: each begins with an emoji + title, with a 2–3 sentence tip
#       10) "Conclusion": 1–2 sentences
#     """
#     system_msg = "You are a JSON-only generator. Output valid JSON, nothing else."
#     user_prompt = f"""
# Generate a JSON object with exactly 10 keys.
# 1) The first key must be "Introduction", value = a 2–3 sentence introductory paragraph for 'Things to Know Before Visiting {country}'.
# 2) The next 8 keys: each must start with one suitable emoji (like "👗", "😋", "🚕"), then a space, then a concise title (e.g. "👗 Cultural Dress Codes"). Value = a 2–3 sentence tip.
# 3) The final key must be "Conclusion", value = a 1–2 sentence wrap-up encouraging the traveler.
# Do NOT include any other keys, markdown, or code fences."""
#     resp = openai.ChatCompletion.create(
#         model=OPENAI_MODEL,
#         messages=[
#             {"role": "system",  "content": system_msg},
#             {"role": "user",    "content": user_prompt},
#         ],
#         temperature=0.0,
#     )

#     content = resp.choices[0].message.content.strip()
#     try:
#         return json.loads(content)
#     except JSONDecodeError:
#         print(f"⚠️ Failed to parse JSON for {country}. Raw response:\n{content}")
#         raise


# # ─── MAIN WORKFLOW ─────────────────────────────────────────────────────────────
# def main():
#     for doc_id, country in COUNTRIES:
#         print(f"\n🛠 Generating 'Things to Know' for {country} (doc {doc_id})…")

#         tips = generate_things_to_know(country)

#         # Write into Firestore
#         (
#             db.collection("allplaces")
#               .document(doc_id)
#               .collection("travelInfo")
#               .document("things_to_know")
#               .set(tips)
#         )
#         print(f"☁️ Wrote allplaces/{doc_id}/travelInfo/things_to_know")

#     print("\n✅ All done.")


# if __name__ == "__main__":
#     main()



# # above Script with PLaces + parent country
# #!/usr/bin/env python3
# """
# generate_and_store_things_to_know.py

# For each (doc_id, place_name, country_name) in COUNTRIES:
#  1) Generate a “things_to_know” JSON via OpenAI with exactly 10 keys:
#       • "Introduction": 2–3 sentence lead-in
#       • 8 entries: each starts with a relevant emoji + title, then a 2–3 sentence tip
#       • "Conclusion": 1–2 sentence wrap-up
#  2) Write that JSON into Firestore under travelInfo/things_to_know.
# """

# import json
# from json.decoder import JSONDecodeError

# import openai
# import firebase_admin
# from firebase_admin import credentials, firestore

# # ─── CONFIG ───────────────────────────────────────────────────────────────────
# # Each entry is now (doc_id, place_name, country_name)
# COUNTRIES = [
#     # ("58144", "New York City", "United States"),
#     # ("9613",  "London",         "United Kingdom"),
#     # ("9614",  "Paris",          "France"),
#     # ("85939", "Dubai",          "United Arab Emirates"),
#      ("85939", "Dubai", "United Arab Emirates"),
#      ("9630", "Florence", "Italy"),
#      ("9636", "Edinburgh", "United Kingdom"),
#      ("82574", "Sydney", "Australia"),
#      ("15", "Hong Kong", "China"),
#      ("9618", "St. Petersburg", "Russia"),
#      ("2", "Kyoto", "Japan"),
#      ("9637", "Athens", "Greece"),
#      ("58150", "San Diego", "United States"),
#      ("9643", "Copenhagen", "Denmark"),
#      ("11", "Siem Reap", "Cambodia"),
#      ("9635", "Naples", "Italy"),
#      ("9649", "Brussels", "Belgium"),
#      ("81905", "Playa del Carmen", "Mexico"),
#      ("9707", "York", "United Kingdom"),
#      ("131100", "Natal", "Brazil"),
#      ("3", "Osaka", "Japan"),
#      ("131086", "Florianopolis", "Brazil"),
#      ("18", "Taipei", "Taiwan"),
#      ("131118", "Porto Seguro", "Brazil"),
#     #  ("7865", "Mumbai", "India"),
#      ("82576", "Auckland", "New Zealand"),
#      ("9745", "Bruges", "Belgium"),
#      ("58186", "St. Augustine", "United States"),
#      ("9798", "Blackpool", "United Kingdom"),
#      ("58183", "Branson", "United States"),
#      ("22", "Ubud", "Indonesia"),
#      ("78744", "Panama City", "Panama"),
#      ("24", "Jaipur", "India"),
#      ("9656", "Reykjavik", "Iceland"),
#      ("82578", "Gold Coast", "Australia"),
#      ("58170", "Charleston", "United States"),
#      ("53", "Kathu", "Thailand"),
#      ("9711", "Verona", "Italy"),
#      ("10413", "Marne-la-Vallee", "France"),
#      ("58175", "Saint Louis", "United States"),
#      ("9683", "Genoa", "Italy"),
#      ("9666", "Birmingham", "United Kingdom"),
#      ("9670", "Lyon", "France"),
#      ("9735", "Funchal", "Portugal"),
#      ("80", "Pattaya", "Thailand"),
#      ("9730", "Bath", "United Kingdom"),
#      ("81904", "Cancun", "Mexico"),
#      ("81180", "Punta Cana", "Caribbean"),
#      ("9654", "Palermo", "Italy"),
#      ("9679", "Tallinn", "Estonia"),
#      ("131150", "Ipojuca", "Brazil"),
#      ("131090", "San Carlos de Bariloche", "Argentina"),
#      ("81909", "Tulum", "Mexico"),
#      ("21", "Hoi An", "Vietnam"),
#      ("19", "Chiang Mai", "Thailand"),
#      ("131083", "Mendoza", "Argentina"),
#      ("9767", "Pisa", "Italy"),
#      ("131078", "Quito", "Ecuador"),
#      ("131132", "Petropolis", "Brazil"),
#      ("37", "Kuta", "Indonesia"),
#      ("131115", "Maceio", "Brazil"),
#      ("79", "Krabi Town", "Thailand"),
#      ("131134", "Jijoca de Jericoacoara", "Brazil"),
#      ("28", "Taito", "Japan"),
#      ("131094", "Paraty", "Brazil"),
#      ("9712", "Cardiff", "United Kingdom"),
#      ("9792", "Maspalomas", "Spain"),
#      ("82593", "Rotorua", "New Zealand"),
#      ("9826", "Adeje", "Spain"),
#      ("9800", "Syracuse", "Italy"),
#      ("9751", "Strasbourg", "France"),
#      ("9663", "Bordeaux", "France"),
#      ("9763", "Paphos", "Cyprus"),
#      ("82585", "Canberra", "Australia"),
#      ("9802", "Siena", "Italy"),
#      ("9696", "Bratislava", "Slovakia"),
#      ("23", "Minato", "Japan"),
#      ("10219", "Windsor", "United Kingdom"),
#      ("29", "Sapporo", "Japan"),
#      ("9803", "Portsmouth", "United Kingdom"),
#      ("9668", "Zurich", "Switzerland"),
#      ("85940", "Tel Aviv", "Israel"),
#      ("10061", "Versailles", "France"),
#      ("38", "Shibuya", "Japan"),
#      ("58187", "Sarasota", "United States"),
#      ("9801", "Bergen", "Norway"),
#      ("131079", "Medellin", "Colombia"),
#      ("184", "Luang Prabang", "Laos"),
#      ("79309", "Sharm El Sheikh", "Egypt"),
#      ("9850", "Chester", "United Kingdom"),
#      ("131318", "Machu Picchu", "Peru"),
#      ("20", "Yokohama", "Japan"),
#      ("58049", "Ottawa", "Canada"),
#      ("81226", "Varadero", "Caribbean"),
#      ("9724", "Geneva", "Switzerland"),
#      ("58182", "Greater Palm Springs", "United States"),
#      ("10008", "Lucerne", "Switzerland"),
#      ("82580", "Perth", "Australia"),
#      ("9690", "Padua", "Italy"),
#      ("9889", "Benalmadena", "Spain"),
#      ("82581", "Adelaide", "Australia"),
#      ("9723", "Oxford", "United Kingdom"),
#      ("9836", "Killarney", "Ireland"),
#      ("9677", "Sochi", "Russia"),
#      ("58191", "Albuquerque", "United States"),
#      ("51", "Phuket Town", "Thailand"),
#      ("32", "Shinjuku", "Japan"),
#      ("9765", "Albufeira", "Portugal"),
#      ("42", "Chiyoda", "Japan"),
#      ("59", "Hiroshima", "Japan"),
#      ("131093", "Angra Dos Reis", "Brazil"),
#      ("9840", "Lucca", "Italy"),
#      ("9689", "Leeds", "United Kingdom"),
#      ("81194", "Nassau", "Caribbean"),
#      ("10046", "Ravenna", "Italy"),
#      ("9687", "Belgrade", "Serbia"),
#      ("131117", "Ouro Preto", "Brazil"),
#      ("9725", "The Hague", "The Netherlands"),
#      ("9691", "Trieste", "Italy"),
#      ("9930", "Stratford-upon-Avon", "United Kingdom"),
#      ("9694", "Split", "Croatia"),
#      ("9891", "Matera", "Italy"),
#      ("9935", "Taormina", "Italy"),
#      ("9998", "Llandudno", "United Kingdom"),
#      ("79306", "Johannesburg", "South Africa"),
#      ("11769", "Lindos", "Greece"),
#      ("58198", "Anchorage", "United States"),
#      ("9709", "Toulouse", "France"),
#      ("9676", "Sofia", "Bulgaria"),
#      ("9838", "Santiago de Compostela", "Spain"),
#      ("9945", "Assisi", "Italy"),
#      ("9756", "Rimini", "Italy"),
#      ("30", "Nagoya", "Japan"),
#      ("9799", "Nantes", "France"),
#      ("41", "New Taipei", "Taiwan"),
#      ("10074", "Agrigento", "Italy"),
#      ("176", "Karon", "Thailand"),
#      ("58062", "Niagara-on-the-Lake", "Canada"),
#      ("9903", "Carcassonne", "France"),
#      ("68", "Kochi (Cochin)", "India"),
#      ("131478", "Mata de Sao Joao", "Brazil"),
#      ("78752", "La Fortuna de San Carlos", "Costa Rica"),
#      ("9713", "Newcastle upon Tyne", "United Kingdom"),
#      ("34", "Kobe", "Japan"),
#      ("79303", "Nairobi", "Kenya"),
#      ("9919", "Avignon", "France"),
#      ("131360", "Maragogi", "Brazil"),
#      ("82594", "Dunedin", "New Zealand"),
#      ("31", "Fukuoka", "Japan"),
#      ("10064", "Weymouth", "United Kingdom"),
#      ("10235", "Bled", "Slovenia"),
#      ("10211", "Vila Nova de Gaia", "Portugal"),
#      ("118", "Kandy", "Sri Lanka"),
#      ("9937", "Scarborough", "United Kingdom"),
#      ("9861", "Innsbruck", "Austria"),
#      ("9917", "Lincoln", "United Kingdom"),
#      ("9727", "Thessaloniki", "Greece"),
#      ("9734", "Galway", "Ireland"),
#      ("98", "Bophut", "Thailand"),
#      ("7865", "Mumbai", "India"),
#      ("24", "Jaipur", "India"),
#      ("68", "Kochi", "India"),
#      ("412", "Munnar", "India"),
#      ("297", "Leh", "India"),
#      ("313", "Manali Tehsil", "India"),
#      ("428", "Shimla", "India"),
#      ("842", "Kodaikanal", "India"),
#      ("405", "Dharamsala", "India"),
#      ("304", "Chandigarh", "India"),
#      ("753", "Thekkady", "India"),
#      ("701", "Lonavala", "India"),
#      ("329", "Bardez", "India"),
#      ("783", "Haridwar", "India"),
#      ("1868", "Baga", "India"),
#      ("787", "Chikmagalur", "India"),
#      ("814", "Canacona", "India"),
#      ("2253", "Pahalgam", "India"),
#      ("956", "Ajmer", "India"),
#      ("461", "Kannur", "India"),
#      ("541", "Thane", "India"),
#      ("2761", "Ganpatipule", "India"),
#      ("2397", "Kasauli", "India"),
#      ("3684", "Kandaghat Tehsil", "India"),
#      ("1673", "Palampur", "India")   

#     # …add more (doc_id, place, country) tuples…
# ]

# SERVICE_ACCOUNT_JSON = (
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
#     r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# )
# OPENAI_MODEL = "gpt-4o-mini"

# # ─── INITIALIZE OPENAI & FIRESTORE ────────────────────────────────────────────
# openai.api_key = "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"
# if not openai.api_key:
#     raise RuntimeError("Missing OPENAI_API_KEY")

# if not firebase_admin._apps:
#     cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
#     firebase_admin.initialize_app(cred)
# db = firestore.client()


# # ─── GENERATOR ────────────────────────────────────────────────────────────────
# def generate_things_to_know(location: str) -> dict:
#     """
#     Returns a JSON object with exactly 10 keys:
#       1) "Introduction": 2–3 sentences
#       2–9) Eight keys: each begins with an emoji + title, with a 2–3 sentence tip
#       10) "Conclusion": 1–2 sentences
#     """
#     system_msg = "You are a JSON-only generator. Output valid JSON, nothing else."
#     user_prompt = f"""
# Generate a JSON object with exactly 10 keys.
# 1) The first key must be "Introduction", value = a 2–3 sentence introductory paragraph for 'Things to Know Before Visiting {location}'.
# 2) The next 8 keys: each must start with one suitable emoji (like "👗", "😋", "🚕"), then a space, then a concise title (e.g. "👗 Cultural Dress Codes"). Value = a 2–3 sentence tip.
# 3) The final key must be "Conclusion", value = a 1–2 sentence wrap-up encouraging the traveler.
# Do NOT include any other keys, markdown, or code fences."""
#     resp = openai.ChatCompletion.create(
#         model=OPENAI_MODEL,
#         messages=[
#             {"role": "system",  "content": system_msg},
#             {"role": "user",    "content": user_prompt},
#         ],
#         temperature=0.0,
#     )

#     content = resp.choices[0].message.content.strip()
#     try:
#         return json.loads(content)
#     except JSONDecodeError:
#         print(f"⚠️ Failed to parse JSON for {location}. Raw response:\n{content}")
#         raise


# # ─── MAIN WORKFLOW ────────────────────────────────────────────────────────────
# def main():
#     for doc_id, place, country in COUNTRIES:
#         full_location = f"{place}, {country}"
#         print(f"\n🛠 Generating 'Things to Know' for {full_location} (doc {doc_id})…")

#         tips = generate_things_to_know(full_location)

#         # Write into Firestore
#         (
#             db.collection("allplaces")
#               .document(doc_id)
#               .collection("travelInfo")
#               .document("things_to_know")
#               .set(tips)
#         )
#         print(f"☁️ Wrote allplaces/{doc_id}/travelInfo/things_to_know")

#     print("\n✅ All done.")


# if __name__ == "__main__":
#     main()



#!/usr/bin/env python3
"""
generate_and_store_things_to_know.py

For each (doc_id, place_name, country_name) in COUNTRIES:
 1) Generate a “things_to_know” JSON via OpenAI with exactly 10 keys:
      • "Introduction": 2–3 sentence lead-in
      • 8 entries: each starts with a relevant emoji + title, then a 2–3 sentence tip
      • "Conclusion": 1–2 sentence wrap-up
 2) Write that JSON into Firestore under travelInfo/things_to_know.
"""

import os
import json
from json.decoder import JSONDecodeError

from openai import OpenAI
import firebase_admin
from firebase_admin import credentials, firestore

# ─── CONFIG ───────────────────────────────────────────────────────────────────
# Each entry is (doc_id, place_name, country_name)
COUNTRIES = [
    # ("58144", "New York City", "United States"),
    # ("9613",  "London",        "United Kingdom"),
    # ("3684", "Kandaghat Tehsil", "India"),
    # ("349", "Darjeeling", "India"),
    # ("1006", "Gulmarg", "India"),
    # ("472", "Navi Mumbai", "India"),
    # ("1089", "Bikaner", "India"),
    # ("1743", "Diu", "India"),
    # ("1874", "Vrindavan", "India"),
    # ("981", "Jammu City", "India"),
    # ("805", "Tiruchirappalli", "India"),
    # ("1015", "Kolhapur", "India"),
    # ("2371", "Gokarna", "India"),
    # ("1241", "Alwar", "India"),
    # ("1750", "Somnath", "India"),
    # ("536", "Thrissur", "India"),
    # ("398", "Nagpur", "India"),
    # ("2824", "Arpora", "India"),
    # ("1000", "Udupi", "India"),
    # ("1808", "Daman", "India"),
    # ("1215", "Pachmarhi", "India"),
    # ("2027", "Orchha", "India"),
    # ("527", "Kozhikode", "India"),
    # ("1406", "Jabalpur", "India"),
    # ("1667", "Matheran", "India"),
    # ("785", "Allahabad", "India"),
    # ("1942", "Shimoga", "India"),
    # ("146301", "Khandala", "India"),
    # ("994", "Junagadh", "India"),
    # ("1480", "Alibaug", "India"),
    # ("1002", "Mathura", "India"),
    # ("1302", "Kumbakonam", "India"),
    # ("1407", "Kalimpong", "India"),
    # ("1573", "Tawang", "India"),
    # ("976", "Kanchipuram", "India"),
    # ("3369", "Konark", "India"),
    # ("1212", "Vijayawada", "India"),
    # ("894", "Rajkot", "India"),
    # ("2711", "Auroville", "India"),
    # ("1159", "Bundi", "India"),
    # ("146275", "Anjuna", "India"),
    # ("1177", "Ranchi", "India"),
    # ("798", "Raipur", "India"),
    # ("714", "Kollam", "India"),
    # ("1540", "Ponda", "India"),
    # ("1463", "Vellore", "India"),
    # ("1444", "Malvan", "India"),
    # ("979", "Palakkad", "India"),
    # ("1059", "Idukki", "India"),
    # ("2401", "Silvassa", "India"),
    # ("982", "Ratnagiri", "India"),
    # ("730", "Jamnagar", "India"),
    # ("2540", "Kumarakom", "India"),
    # ("1473", "Kutch", "India"),
    # ("2177", "Gandhinagar", "India"),
    # ("1765", "Almora", "India"),
    # ("1763", "Jamshedpur", "India"),
    # ("1135", "Kanpur", "India"),
    # ("3129", "Digha", "India"),
    # ("1852", "Kurnool", "India"),
    # ("849", "Kottayam", "India"),
    # ("948", "Satara", "India"),
    # ("1013", "Imphal", "India"),
    # ("1668", "Hassan", "India"),
    # ("1442", "Warangal", "India"),
    # ("1251", "Salem", "India"),
    # ("2185", "Kargil", "India"),
    # ("2919", "Badrinath", "India"),
    # ("3260", "Bhimtal", "India"),
    # ("909", "Agartala", "India"),
    # ("1679", "Panchkula", "India"),
    # ("923", "Ghaziabad", "India"),
    # ("1163", "Jalandhar", "India"),
    # ("1218", "Kota", "India"),
    # ("1331", "Porbandar", "India"),
    # ("1114", "Siliguri", "India"),
    # ("1581", "Hubli-Dharwad", "India"),
    # ("2141", "Kohima", "India"),
    # ("3466", "Patnitop", "India"),
    # ("1812", "Patiala", "India"),
    # ("2040", "Kurukshetra", "India"),
    # ("1418", "Mandi", "India"),
    # ("1306", "Faridabad", "India"),
    # ("2477", "Auli", "India"),
    # ("86944", "Andaman and Nicobar Islands", "India"),
    # ("87277", "Daman and Diu", "India"),
    # ("87478", "Dadra and Nagar Haveli", "India"),
    # ("88144", "Lakshadweep", "India"),
    #     ("2643", "Ayodhya", "India"),
    # ("4655", "Velankanni", "India"),
    # ("3396", "Ambaji", "India"),
    # ("2976", "Katra", "India"),
    # ("4318", "Dharmasthala", "India"),
    # ("4300", "Pavagadh", "India"),
    # ("2936", "Thiruvannamalai", "India"),
    # ("1729", "Gaya", "India"),
    # ("3900", "Pandharpur", "India"),
    # ("3710", "Srisailam", "India"),
    # ("4276", "Kedarnath", "India"),
    # ("581", "Calangute", "India"),
    # ("1664", "Fatehpur Sikri", "India"),
    # ("4947", "Gangotri", "India"),
    # ("8658", "Ajanta", "India"),
    # ("2953", "Nalanda", "India"),
    # ("2707", "Sanchi", "India"),
    # ("2562", "Jhansi", "India"),
    # ("4497", "Araku Valley", "India"),
    # ("2839", "Sonamarg", "India"),
    # ("8980", "Dawki", "India"),
    # ("7184", "Sasan Gir", "India"),
    # ("3376", "Manipal", "India"),
    # ("2083", "Howrah", "India"),
    # ("2231", "Guruvayur", "India"),
    # ("3635", "Nathdwara", "India"),
    # ("1060","Ludhiana","India"),
    # ('581','Calangute','India'),
    # ('7268','Sadri','India'),
    # ('1664','Fatehpur Sikri','India')
    # ('2150','Agonda','India'),
    # ('1424','Bhavnagar', 'India'),
    # ('2083','Howrah','India'),
    # # ('2264','Rourkela','India'),
    # # ('2346','Bathinda','India')
    # ("2231", "Guruvayur", "India"),
    # ("1854", "Saputara", "India"),
    # ("1402", "Chamba", "India"),
    # ("1083", "Mandu", "India"),
    # ("1312", "Kasaragod", "India"),
    # ("827", "Tirunelveli", "India"),
    # ("1436", "Murshidabad", "India"),
    # ("1479", "Karwar", "India"),
    # ("2654", "Valparai", "India"),
    # ("1250", "Mandya", "India"),
    # ("1699", "Margao", "India"),
    # ("1659", "Vasco da Gama", "India"),
    # ("1949", "Greater Noida", "India"),
    # ("1531", "Ahmednagar", "India"),
    # ("1729", "Gaya", "India"),
    # ("1754", "Bharuch", "India"),
    # ("3446", "Palani", "India"),
    # ("1776", "Rajgir", "India"),
    # ("1997", "Tiruvannamalai", "India"),
    # ("2131", "Solapur", "India"),
    # ("2018", "Cuttack", "India"),
    # ("3396", "Ambaji", "India"),
    # ("2562", "Jhansi", "India"),
    # ("1970", "Bishnupur", "India"),
    # ("1759", "Valsad", "India"),
    # ("1690", "Chittoor", "India"),
    # ("1604", "Sirsi", "India"),
    # ("2038", "Kolar", "India"),
    # ("2366", "Balasore", "India"),
    # ("2160", "Amarkantak", "India"),
    # ("1733", "Belgaum", "India"),
    # ("1678", "Sambalpur", "India"),
    # ("1354", "Dindigul", "India"),
    # ("2263", "Theni", "India"),
    # ("1936", "Pathanamthitta", "India"),
    # ("1781", "Hooghly", "India"),
    # ("1660", "Malappuram", "India"),
    # ("1523", "Secunderabad", "India"),
    # ("2976", "Katra", "India"),
    # ("2123", "Jalpaiguri", "India"),
    # ("1706", "Tumkur", "India"),
    # ("1996", "Tezpur", "India"),
    # ("1139", "Nagapattinam", "India"),
    # ("4947", "Gangotri", "India"),
    ("60831", "Hammond", "United States"),
    ("59140", "Poipu", "United States"),
    ("147376", "Saint Augustine Beach", "United States"),
    ("116", "Nikko", "Japan"),
    ("12573", "Pamukkale", "Turkiye"),
    ("58370", "Rapid City", "United States"),
    ("58368", "Carlsbad", "United States"),
    ("85943", "Tehran", "Iran"),
    ("78760", "Manuel Antonio", "Costa Rica"),
    ("188", "Cebu City", "Philippines"),
    ("9701", "Nizhny Novgorod", "Russia"),
    ("292", "Vientiane", "Laos"),
    ("10979", "Merida", "Spain"),
    ("10101", "Puerto Del Carmen", "Spain"),
    ("131102", "Guayaquil", "Ecuador"),
    ("58314", "Hot Springs", "United States"),
    ("10698", "Oia", "Greece"),
    ("121", "Kamakura", "Japan"),
    ("10058", "Potsdam", "Germany"),
    ("85945", "Beirut", "Lebanon"),
    ("58949", "Hershey", "United States"),
    ("10696", "Bonifacio", "France"),
    ("584", "Kanchanaburi", "Thailand"),
    ("58053", "Winnipeg", "Canada"),
    ("82615", "Taupo", "New Zealand"),
    ("131332", "Vila Velha", "Brazil"),
    ("10229", "Nimes", "France"),
    ("12927", "Oswiecim", "Poland"),
    ("13458", "Akrotiri", "Greece"),
    ("9956", "Mykonos Town", "Greece"),
    ("58299", "Fort Myers Beach", "United States"),
    ("85", "Suzhou", "China"),
    ("58214", "Sacramento", "United States"),
    ("131457", "Casablanca", "Chile"),
    ("248", "Lhasa", "China"),
    ("58455", "Athens", "United States"),
    # …add more (doc_id, place, country) tuples…
]

SERVICE_ACCOUNT_JSON = (
    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
    r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
)

OPENAI_MODEL = "gpt-4.1-mini"

# ─── INITIALIZE OPENAI & FIRESTORE ────────────────────────────────────────────
api_key = "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"
if not api_key:
    raise RuntimeError("Missing OPENAI_API_KEY")

client = OpenAI(api_key=api_key)

if not firebase_admin._apps:
    cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
    firebase_admin.initialize_app(cred)
db = firestore.client()


# ─── GENERATOR ────────────────────────────────────────────────────────────────
def generate_things_to_know(location: str) -> dict:
    """
    Returns a JSON object with exactly 10 keys:
      1) "Introduction": 2–3 sentences
      2–9) Eight keys: each begins with an emoji + title, with a 2–3 sentence tip
      10) "Conclusion": 1–2 sentences
    """
    system_msg = "You are a JSON-only generator. Output valid JSON, nothing else."
    user_prompt = f"""
Generate a JSON object with exactly 10 keys.
1) The first key must be "Introduction", value = a 2–3 sentence introductory paragraph for 'Things to Know Before Visiting {location}'.
2) The next 8 keys: each must start with one suitable emoji (like "👗", "😋", "🚕"), then a space, then a concise title (e.g. "👗 Cultural Dress Codes"). Value = a 2–3 sentence tip.
3) The final key must be "Conclusion", value = a 1–2 sentence wrap-up encouraging the traveler.
Do NOT include any other keys, markdown, or code fences."""
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.0,
        response_format={"type": "json_object"},  # enforce strict JSON
    )

    content = resp.choices[0].message.content.strip()
    try:
        return json.loads(content)
    except JSONDecodeError:
        print(f"⚠️ Failed to parse JSON for {location}. Raw response:\n{content}")
        raise


# ─── MAIN WORKFLOW ────────────────────────────────────────────────────────────
def main():
    for doc_id, place, country in COUNTRIES:
        full_location = f"{place}, {country}"
        print(f"\n🛠 Generating 'Things to Know' for {full_location} (doc {doc_id})…")

        tips = generate_things_to_know(full_location)

        # Write into Firestore
        (
            db.collection("allplaces")
              .document(doc_id)
              .collection("travelInfo")
              .document("things_to_know")
              .set(tips)
        )
        print(f"☁️ Wrote allplaces/{doc_id}/travelInfo/things_to_know")

    print("\n✅ All done.")


if __name__ == "__main__":
    main()
