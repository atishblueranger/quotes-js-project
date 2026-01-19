#!/usr/bin/env python3
"""
generate_and_store_visa_requirements.py
─────────────────────────────────────────

For each (doc_id, country_name) in COUNTRIES:
 1) Generate a “visa_requirements” JSON via OpenAI with exactly:
      • "Introduction": a 2–3 sentence lead-in
      • 10 entries: each starts with an emoji + space + concise title, value = a 2–3 sentence tip
      • "Conclusion": a 1–2 sentence wrap-up
 2) Store that JSON under travelInfo/visa_requirements in Firestore.
"""

import os
import json
from json.decoder import JSONDecodeError

import openai
import firebase_admin
from firebase_admin import credentials, firestore

# ─── CONFIG ───────────────────────────────────────────────────────────────────
# List your Firestore document IDs and country names here:
COUNTRIES = [
    # ("86661", "India"),
    # ("86647", "Japan"),
    # ("86651", "Thailand"),
    # ("86663", "Indonesia"),
    # ("86667", "Nepal"),
    # ("86683", "Malaysia"),
    # ("86693", "Sri Lanka"),
    # ("86656", "South Korea"),
    # ("86654", "Singapore"),
    # ("86653", "China"),
    # ("86819", "Bhutan"),
    # ("91525", "Israel"),
    # ("91534", "Jordan"),
    # ("91536", "Oman"),
    # ("86831", "Pakistan"),
    # ("91535", "Qatar"),
    # ("88367", "Turkiye"),
    # ("91524", "United Arab Emirates"),
    # ("88362", "Spain"),
    # ("88373", "The Netherlands"),
    # ("88352", "Italy"),
    # ("88386", "Ireland"),
    # ("88419", "Iceland"),
    # ("88380", "Hungary"),
    # ("88375", "Greece"),
    # ("88368", "Germany"),
    # ("88358", "France"),
    # ("88359", "United Kingdom"),
    # ("91387", "Australia"),
    # ("91394", "New Zealand"),
    # ("91403", "Fiji"),
    # ("90764", "South Africa"),
    # ("90763", "Egypt"),
    # ("90759", "Morocco"),
    # ("79304", "Mauritius"),
    # ("90407", "United States"),
    # ("91342", "Mexico"),
    # ("90374", "Canada"),
     ("10165", "Gibraltar"),
    ("10705", "Vatican City"),
    ("135383", "Brazil"),
    ("135385", "Argentina"),
    ("135391", "Chile"),
    ("135393", "Colombia"),
    ("135396", "Ecuador"),
    ("135408", "Uruguay"),
    ("135438", "Bolivia"),
    ("135468", "Venezuela"),
    ("135498", "Paraguay"),
    ("135642", "Suriname"),
    ("135733", "Guyana"),
      ("86655", "Vietnam"),
    ("86668", "Taiwan"),
    ("86683", "Malaysia"),
    ("86851", "Bangladesh"),
    ("86983", "Maldives"),
    ("87389", "Afghanistan"),
    ("88360", "Russia"),
    ("88366", "Czech Republic"),
    ("88384", "Austria"),
    ("88394", "Ukraine"),
    ("88402", "Denmark"),
    ("88403", "Poland"),
    ("88405", "Romania"),
    ("88407", "Malta"),
    ("88408", "Belgium"),
    ("88432", "Norway"),
    ("88434", "Finland"),
    ("88437", "Switzerland"),
    ("88438", "Croatia"),
    ("88445", "Sweden"),
    ("88449", "Bulgaria"),
    ("88455", "Estonia"),
    ("88464", "Serbia"),
    ("88477", "Lithuania"),
    ("88478", "Slovakia"),
    ("88486", "Slovenia"),
    ("88503", "Belarus"),
    ("88527", "Cyprus"),
    ("88597", "Bosnia and Herzegovina"),
    ("88609", "Albania"),
    ("88654", "Montenegro"),
    ("88723", "Luxembourg"),
    ("88727", "Republic of North Macedonia"),
    ("91537", "Kuwait"),
    ("91540", "Bahrain"),
    ("91548", "Saudi Arabia"),
    ("91564", "Iraq"),
    ("91579", "Yemen"),
    ("91526", "Lebanon"),
    ("91530", "Iran"),
    ("86719", "Armenia"),
    ("86729", "Azerbaijan"),
    ("86731", "Kazakhstan"),
    ("86772", "Mongolia"),
    ("86776", "Kyrgyzstan"),
    ("86779", "Brunei Darussalam"),
    ("86797", "Laos"),
    ("86652","Philippines"),
    # …add more (doc_id, country_name) tuples as needed…
]

# Path to your Firebase service account JSON
SERVICE_ACCOUNT_JSON = (
    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
    r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
)

# OpenAI model to use
OPENAI_MODEL = "gpt-4o-mini"


# ─── INITIALIZE OPENAI & FIRESTORE ────────────────────────────────────────────
openai.api_key = "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"
if not openai.api_key:
    raise RuntimeError("Missing OPENAI_API_KEY environment variable")

if not firebase_admin._apps:
    cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
    firebase_admin.initialize_app(cred)
db = firestore.client()


# ─── SECTION GENERATOR ─────────────────────────────────────────────────────────
def generate_visa_requirements(country: str) -> dict:
    """
    Generates a JSON object with exactly 12 keys:
      1) "Introduction": 2–3 sentence opening
      2–11) Ten keys: each begins with emoji + space + title, value = 2–3 sentence detail
      12) "Conclusion": 1–2 sentence wrap-up
    """
    system_msg = "You are a JSON-only generator. Output valid JSON and nothing else."
    user_prompt = f"""
Generate a JSON object with exactly 12 keys.
1) The first key must be "Introduction", whose value is a 2–3 sentence introductory paragraph for 'Everything You Need to Know About Visa Requirements for {country}'.
2) The next ten keys should each:
   - Start with one suitable emoji (e.g., "🛂", "🌐", "💻"),
   - Followed by a space and a concise title (e.g., "🛂 Visa Categories & Eligibility"),
   - Contain a 2–3 sentence descriptive paragraph as the value.
3) The final key must be "Conclusion", whose value is a 1–2 sentence summary wrap-up.
Do NOT output markdown, code fences, or any extra keys."""
    resp = openai.ChatCompletion.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system",  "content": system_msg},
            {"role": "user",    "content": user_prompt},
        ],
        temperature=0.0,
    )
    content = resp.choices[0].message.content.strip()
    try:
        return json.loads(content)
    except JSONDecodeError:
        print(f"⚠️ Failed to parse JSON for {country}. Raw response:\n{content}")
        raise


# ─── MAIN WORKFLOW ─────────────────────────────────────────────────────────────
def main():
    for doc_id, country in COUNTRIES:
        print(f"\n🛠 Generating 'Visa Requirements' for {country} (doc {doc_id})…")
        visa_info = generate_visa_requirements(country)

        # Store into Firestore under travelInfo/visa_requirements
        (
            db.collection("allplaces")
              .document(doc_id)
              .collection("travelInfo")
              .document("visa_requirements")
              .set(visa_info)
        )
        print(f"☁️ Written allplaces/{doc_id}/travelInfo/visa_requirements")

    print("\n✅ All done.")


if __name__ == "__main__":
    main()
