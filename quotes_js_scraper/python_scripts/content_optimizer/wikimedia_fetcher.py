import os
import csv
import json
import time
import unicodedata
from typing import List, Dict, Optional, Tuple
from difflib import SequenceMatcher

from urllib.parse import quote
import requests
from requests.adapters import HTTPAdapter, Retry

import firebase_admin
from firebase_admin import credentials, firestore, storage

# ───── CONFIG ─────────────────────────────────────────────────────────────────

SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
STORAGE_BUCKET = "mycasavsc.appspot.com"

# Location mapping (ids must match your Firestore structure under allplaces/{id})
LOCATION_NAMES = {
    #    "1": "Tokyo",
    # "10": "Shanghai",
    # "1000": "Udupi",
    # "10008": "Lucerne",
    # "10015": "Ronda",
    # "1002": "Mathura",
    # "10021": "Corfu Town",
    # "10024": "Sitges",
    # "10025": "Tarragona",
    # "10030": "Bursa",
    # "10031": "Lloret de Mar",
    # "10033": "Valletta",
    # "10046": "Ravenna",
    # "1006": "Gulmarg",
    # "10061": "Versailles",
    # "10062": "Bonn",
    # "10064": "Weymouth",
    # "10074": "Agrigento",
    # "10086": "Augsburg",
    # "10095": "Koblenz",
    # "10106": "Torremolinos",
    # "10114": "Mannheim",
    # "10123": "Puerto de la Cruz",
    # "1013": "Imphal",
    # "1014": "Trincomalee",
    # "10143": "Torquay",
    # "1015": "Kolhapur",
    # "10171": "Nerja",
    # "10176": "Salou",
    # "10184": "Aachen",
    # "10186": "Groningen",
    # "10193": "Delft",
    # "10211": "Vila Nova de Gaia",
    # "10219": "Windsor",
    # "10230": "Segovia",
    # "10235": "Bled",
    # "10241": "Playa Blanca",
    # "1025": "Ujjain",
    # "10260": "Selcuk",
    # "10262": "Trogir",
    # "10279": "Nijmegen",
    # "10291": "Cuenca",
    # "10366": "Akureyri",
    # "10382": "Cremona",
    # "10421": "Bamberg",
    # "10453": "Konstanz",
    # "105": "Macau",
    # "10533": "Ulm",
    # "1055": "Mahabalipuram",
    # "10590": "Savona",
    # "10595": "Peterhof",
    # "1060": "Ludhiana",
    # "10792": "Monte-Carlo",
    # "10804": "Ceuta",
    # "10876": "Uppsala",
    # "10884": "Tropea",
    # "1089": "Bikaner",
    # "11": "Siem Reap",
    # "110": "Hakodate",
    # "11061": "Lund",
    # "1131": "Sukhothai",
    # "1135": "Kanpur",
    # "1138": "Sawai Madhopur",
    # "11423": "Grindelwald",
    # "11489": "Mont-Saint-Michel",
    # "1159": "Bundi",
    # "1160": "Sandakan",
    # "1163": "Jalandhar",
    # "11655": "Berchtesgaden",
    # "1168": "Hat Yai",
    # "11706": "Trujillo",
    # "11769": "Lindos",
    # "1177": "Ranchi",
    # "118": "Kandy",
    # "1192": "Chonburi",
    # "12": "Phuket",
    # "121": "Kamakura",
    # "1212": "Vijayawada",
    # "1218": "Kota",
    # "122": "Varanasi",
    # "1222": "Kalpetta",
    # "1241": "Alwar",
    # "12491": "Mdina",
    # "12525": "Stretford",
    # "128": "Hua Hin",
    # "129": "Incheon",
    # "1291": "Gwalior",
    # "13": "New Delhi",
    # "1302": "Kumbakonam",
    # "131071": "Buenos Aires",
    # "131072": "Rio de Janeiro",
    #  "131073": "Sao Paulo",
    # "131074": "Cusco",
    # "131075": "Santiago",
    # "131076": "Lima",
    # "131077": "Bogota",
    # "131078": "Quito",
    # "131079": "Medellin",
    # "131080": "Cartagena",
    # "131081": "Porto Alegre",
    # "131083": "Mendoza",
    # "131084": "Montevideo",
    # "131085": "Salvador",
    # "131086": "Florianopolis",
    # "131087": "Brasilia",
    # "131088": "Belo Horizonte",
    # "131089": "Recife",
    # "131090": "San Carlos de Bariloche",
    # "131091": "Fortaleza",
    # "131092": "Manaus",
    # "131093": "Angra Dos Reis",
    # "131094": "Paraty",
    # "131095": "La Paz",
    # "131098": "Valparaiso",
    # "131099": "Arequipa",
    # "131100": "Natal",
    # "131102": "Guayaquil",
    # "131103": "Gramado",
    # "131104": "San Pedro de Atacama",
    # "131105": "Salta",
    # "131106": "Campinas",
    # "131107": "Santa Marta",
    # "131108": "Ubatuba",
    # "131109": "Joao Pessoa",
    # "131110": "Mar del Plata",
    # "131113": "Rosario",
    # "131114": "Belem",
    # "131115": "Maceio",
    # "131117": "Ouro Preto",
    # "131118": "Porto Seguro",
    # "131119": "Ushuaia",
    # "131122": "Santos",
    # "131125": "Niteroi",
    # "131127": "Vitoria",
    # "131131": "Sao Luis",
    # "131132": "Petropolis",
    # "131134": "Jijoca de Jericoacoara",
    # "131138": "Puerto Varas",
    # "131139": "El Calafate",
    # "131140": "Puno",
    # "131146": "Cabo Frio",
    # "131147": "Aracaju",
    # "131150": "Ipojuca",
    # "131151": "Campos Do Jordao",
    # "131157": "Canela",
    # "131162": "Punta del Este",
    # "131164": "Puerto Iguazu",
    # "131168": "San Juan",
    # "131172": "Fernando de Noronha",
    # "131174": "San Andres Island",
    # "131177": "Tiradentes",
    # "131178": "Pocos de Caldas",
    # "131180": "Puerto Ayora",
    # "1312": "Kasaragod",
    # "131315": "Bonito",
    # "131318": "Machu Picchu",
    # "131327": "Bombinhas",
    # "131337": "Guaruja",
    # "131360": "Maragogi",
    # "131362": "Cafayate",
    # "131389": "San Andres",
    # "131395": "Praia da Pipa",
    # "131438": "Morro de Sao Paulo",
    # "131447": "Caldas Novas",
    # "131457": "Casablanca",
    # "131478": "Mata de Sao Joao",
    # "1316": "Shirdi",
    # "131626": "Penha",
    # "161": "Ahmedabad",
    # "1765": "Almora",
    # "2601": "Amphawa",
    # "1868": "Baga",
    # "228": "Beppu",
    # "19": "Chiang Mai",
    # "257": "Chiang Rai",
    # "1887": "Coonoor",
    # "174": "Da Lat",
    # "1784": "Dalhousie",
    # "1808": "Daman",
    # "2121": "Deoghar",
    # "283": "Dhaka City",
    # "1743": "Diu",
    # "1983": "Dwarka",
    # "2177": "Gandhinagar",
    # "1729": "Gaya",
    # "2371": "Gokarna",
    # "207": "Gurugram (Gurgaon)",
    # "1668": "Hassan",
    # "16": "Ho Chi Minh City",
    # "21": "Hoi An",
    # "2083": "Howrah",
    # "247": "Ise",
    # "24": "Jaipur",
    # "183": "Jaisalmer",
    # "1763": "Jamshedpur",
    # "288": "Karachi",
    # "176": "Karon",
    # "17": "Kathmandu",
    # "2141": "Kohima",
    # "1966": "Kovalam",
    # "223": "Kuching",
    # "2540": "Kumarakom",
    # "2": "Kyoto",
    # "235": "Lahore",
    # "297": "Leh",
    # "248": "Lhasa",
    # "184": "Luang Prabang",
    # "1886": "Madikeri",
    # "175": "Manila",
    # "1699": "Margao",
    # "187": "Melaka",
    # "23": "Minato",
    # "25": "Mumbai",
    # "2027": "Orchha",
    # "2253": "Pahalgam",
    # "287": "Panjim",
    # "194": "Rishikesh",
    # "221": "Sapa",
    # "29": "Sapporo",
    # "285": "Seogwipo",
    # "1942": "Shimoga",
    # "256": "Srinagar",
    # "18": "Taipei",
    # "28": "Taito",
    # "209": "Thimphu",
    # "200": "Thiruvananthapuram (Trivandrum)",
    # "1997": "Tiruvannamalai",
    # "22": "Ubud",
    # "1659": "Vasco da Gama",
    # "1874": "Vrindavan",
    # "20": "Yokohama",
    # "60": "Agra",
    # "58191": "Albuquerque",
    # "58242": "Anaheim",
    # "58198": "Anchorage",
    # "58342": "Arlington",
    # "58185": "Asheville",
    # "58455": "Athens",
    # "58169": "Atlanta",
    # "607": "Aurangabad",
    # "58163": "Austin",
    # "58179": "Baltimore",
    # "58068": "Banff",
    # "570": "Batu",
    # "6": "Beijing",
    # "58162": "Boston",
    # "58183": "Branson",
    # "58164": "Brooklyn",
    # "61": "Busan",
    # "581": "Calangute",
    # "58048": "Calgary",
    # "58170": "Charleston",
    # "58193": "Charlotte",
    # "58226": "Chattanooga",
    # "67": "Chengdu",
    # "58146": "Chicago",
    # "58201": "Cincinnati",
    # "58231": "Clearwater",
    # "58210": "Cleveland",
    # "58563": "Columbus",
    # "58173": "Dallas",
    # "58248": "Daytona Beach",
    # "58166": "Denver",
    # "58218": "Detroit",
    # "57258": "Dublin",
    # "58052": "Edmonton",
    # "660": "Ella",
    # "58300": "Flagstaff",
    # "58177": "Fort Lauderdale",
    # "58211": "Fort Myers",
    # "58212": "Fort Worth",
    # "58284": "Fredericksburg",
    # "58244": "Galveston",
    # "503": "Gangtok",
    # "58286": "Gettysburg",
    # "58182": "Greater Palm Springs",
    # "524": "Guwahati",
    # "58059": "Halifax",
    # "696": "Hampi",
    # "64": "Hangzhou",
    # "59": "Hiroshima",
    # "58153": "Honolulu",
    # "58161": "Houston",
    # "58205": "Indianapolis",
    # "558": "Indore",
    # "58155": "Island of Hawaii",
    # "62": "Kanazawa",
    # "584": "Kanchanaburi",
    # "58219": "Kansas City",
    # "53": "Kathu",
    # "58167": "Kauai",
    # "58165": "Key West",
    # "59165": "Keystone",
    # "58067": "Kingston",
    # "68": "Kochi (Cochin)",
    # "69": "Kolkata (Calcutta)",
    # "527": "Kozhikode",
    # "58203": "Lahaina",
    # "58148": "Las Vegas",
    # "58145": "Los Angeles",
    # "58196": "Louisville",
    # "615": "Madurai",
    # "58151": "Maui",
    # "58224": "Memphis",
    # "58157": "Miami",
    # "58180": "Miami Beach",
    # "58195": "Milwaukee",
    # "58184": "Minneapolis",
    # "58269": "Moab",
    # "58276": "Monterey",
    # "58046": "Montreal",
    # "687": "Mussoorie",
    # "58202": "Myrtle Beach",
    # "50": "Naha",
    # "58189": "Naples",
    # "58171": "Nashville",
    # "58156": "New Orleans",
    # "58144": "New York City",
    # "58058": "Niagara Falls",
    # "563": "Noida",
    # "58079": "North Vancouver",
    # "58232": "Oklahoma City",
    # "58213": "Omaha",
    # "58152": "Orlando",
    # "58049": "Ottawa",
    # "58476": "Page",
    # "673": "Patna",
    # "58160": "Philadelphia",
    # "58181": "Phoenix",
    # "51": "Phuket Town",
    # "58241": "Pigeon Forge",
    # "58199": "Pittsburgh",
    # "589": "Port Blair",
    # "58158": "Portland",
    # "57": "Pune",
    # "58051": "Quebec City",
    # "58222": "Richmond",
    # "58175": "Saint Louis",
    # "58289": "Salem",
    # "58216": "Salt Lake City",
    # "58168": "San Antonio",
    # "58150": "San Diego",
    # "58147": "San Francisco",
    # "58206": "Santa Barbara",
    # "58178": "Santa Fe",
    # "58228": "Santa Monica",
    # "58187": "Sarasota",
    # "58174": "Savannah",
    # "58192": "Scottsdale",
    # "58154": "Seattle",
    # "58188": "Sedona",
    # "650": "Sentosa Island",
    # "528": "Singaraja",
    # "583": "Solo",
    # "58176": "Tampa",
    # "541": "Thane",
    # "536": "Thrissur",
    # "58748": "Titusville",
    # "58077": "Tofino",
    # "58045": "Toronto",
    # "58172": "Tucson",
    # "58047": "Vancouver",
    # "58044": "Vancouver Island",
    # "662": "Varkala Town",
    # "58050": "Victoria",
    # "518": "Visakhapatnam",
    # "58159": "Washington DC",
    # "58275": "Williamsburg",
    # "58345": "Wisconsin Dells",
    # "52": "Yangon (Rangoon)",
    # "350": "Alappuzha",
    # "384": "Amritsar",
    # "468": "Ayutthaya",
    # "4": "Bangkok",
    # "329": "Bardez",
    # "35": "Bengaluru",
    # "485": "Bhopal",
    # "444": "Bhubaneswar",
    # # "304": "Chandigarh",
    # "42": "Chiyoda",
    # "371": "Coimbatore",
    # "43": "Colombo",
    # "49": "Da Nang",
    # "349": "Darjeeling",
    # "405": "Dharamsala",
    # "3129": "Digha",
    # "31": "Fukuoka",
    # "39": "Guangzhou",
    # "465": "Gyeongju",
    # "419": "Hikkaduwa",
    # "376": "Ipoh",
    # "46": "Jakarta",
    # "461": "Kannur",
    # "34": "Kobe",
    # "33": "Kuala Lumpur",
    # "37": "Kuta",
    # "375": "Lucknow",
    # "313": "Manali Tehsil",
    # "479": "Mangalore",
    # "382": "Medan",
    # "412": "Munnar",
    # "30": "Nagoya",
    # "398": "Nagpur",
    # "449": "Nashik",
    # "472": "Navi Mumbai",
    # "41": "New Taipei",
    # "480": "Ooty (Udhagamandalam)",
    # "44": "Phnom Penh",
    # "334": "Pondicherry",
     "381": "Semarang",
    "38": "Shibuya",
    "428": "Shimla",
    "32": "Shinjuku",
    "478": "Surat",
    "330": "Tashkent",
    "348": "Vadodara",
    "79337": "Alexandria",
    "785": "Allahabad",
    "79302": "Cairo",
    "79300": "Cape Town Central",
    "787": "Chikmagalur",
    "79333": "Dahab",
    "79305": "Fes",
    "79321": "Giza",
    "783": "Haridwar",
    "78": "Hyderabad",
    "730": "Jamnagar",
    "79306": "Johannesburg",
    "714": "Kollam",
    "79": "Krabi Town",
    "78752": "La Fortuna de San Carlos",
    "76078": "Lake Louise",
    "701": "Lonavala",
    "717": "Male",
    "79299": "Marrakech",
    "79304": "Mauritius",
    "75": "Nagasaki",
    "79303": "Nairobi",
    "724": "Ninh Binh",
    "78744": "Panama City",
    "722": "Pushkar",
    "798": "Raipur",
    "78746": "San Jose",
    "79309": "Sharm El Sheikh",
    "728": "Shillong",
    "738": "Thanjavur",
    "753": "Thekkady",
    "74": "Xi'an",
    "73": "Yerevan",
    "85942": "Abu Dhabi",
    "82581": "Adelaide",
    "81941": "Akumal",
    "85962": "Al Ain",
    "88609": "Albania",
    "87236": "Arunachal Pradesh",
    "86937": "Assam",
    "82576": "Auckland",
    "88384": "Austria",
    "86729": "Azerbaijan",
    "82": "Baku",
    "86851": "Bangladesh",
    "88406": "Bavaria",
    "88503": "Belarus",
    "88408": "Belgium",
    "86819": "Bhutan",
    "82577": "Brisbane",
    "82614": "Broome",
    "86779": "Brunei Darussalam",
    "88449": "Bulgaria",
    "82584": "Cairns",
    "86659": "Cambodia",
    "814": "Canacona",
    "82585": "Canberra",
    "81904": "Cancun",
    "82579": "Christchurch",
    "81908": "Cozumel",
    "88438": "Croatia",
    "88527": "Cyprus",
    "88366": "Czech Republic",
    "82588": "Darwin",
    "88402": "Denmark",
    "85946": "Doha",
    "85939": "Dubai",
    "82594": "Dunedin",
    "88455": "Estonia",
    "88434": "Finland",
    "88358": "France",
    "86743": "Fujian",
    "88420": "Georgia",
    "88368": "Germany",
    "82578": "Gold Coast",
    "81187": "Grand Cayman",
    "88375": "Greece",
    "81912": "Guadalajara",
    "81924": "Guanajuato",
    "86689": "Guangdong",
    "81182": "Havana",
    "82586": "Hobart",
    "86676": "Hokkaido",
    "88380": "Hungary",
    "88419": "Iceland",
    "86661": "India",
    "88386": "Ireland",
    "88352": "Italy",
    "86647": "Japan",
    "85959": "Jeddah",
    "85941": "Jerusalem",
    "86722": "Jiangsu",
    "86686": "Karnataka",
    "86731": "Kazakhstan",
    "86714": "Kerala",
    "850": "Khajuraho",
    "842": "Kodaikanal",
    "849": "Kottayam",
    "86776": "Kyrgyzstan",
    "86797": "Laos",
    "88423": "Latvia",
    "88477": "Lithuania",
    "88723": "Luxembourg",
    "86675": "Maharashtra",
    "88407": "Malta",
    "85954": "Manama",
    "85998": "Mecca",
    "86988": "Meghalaya",
    "82575": "Melbourne",
    "81913": "Merida",
    "81903": "Mexico City",
    "88731": "Moldova",
    "86772": "Mongolia",
    "88654": "Montenegro",
    "877": "Mount Abu",
    "87411": "Nagaland",
    "895": "Nainital",
    "81194": "Nassau",
    "86667": "Nepal",
    "81190": "New Providence Island",
    "82608": "Newcastle",
    "87077": "North Korea",
    "88432": "Norway",
    "81911": "Oaxaca",
    "81213": "Ocho Rios",
    "86904": "Odisha",
    "86831": "Pakistan",
    "81981": "Palenque",
    "80": "Pattaya",
    "86652": "Philippines",
    "81905": "Playa del Carmen",
    "88403": "Poland",
    "81918": "Puebla",
    "81219": "Puerto Plata",
    "81906": "Puerto Vallarta",
    "86887": "Punjab",
    "81180": "Punta Cana",
    "86674": "Rajasthan",
    "894": "Rajkot",
    "82593": "Rotorua",
    "88360": "Russia",
    "81189": "San Juan",
    "81250": "Santiago de Cuba",
    "81197": "Santo Domingo",
    "88464": "Serbia",
    "86654": "Singapore",
    "88478": "Slovakia",
    "88486": "Slovenia",
    "86656": "South Korea",
    "88362": "Spain",
    "86693": "Sri Lanka",
    "85": "Suzhou",
    "88445": "Sweden",
    "88437": "Switzerland",
    "82574": "Sydney",
    "86668": "Taiwan",
    "86938": "Tajikistan",
    "86691": "Tamil Nadu",
    "82615": "Taupo",
    "85943": "Tehran",
    "85940": "Tel Aviv",
    "86651": "Thailand",
    "88373": "The Netherlands",
    "805": "Tiruchirappalli",
    "827": "Tirunelveli",
    "81909": "Tulum",
    "88367": "Turkiye",
    "88359": "United Kingdom",
    "86706": "Uttar Pradesh",
    "86805": "Uttarakhand",
    "81226": "Varadero",
    "86655": "Vietnam",
    "82582": "Wellington",
    "86716": "West Bengal",
    "81196": "Willemstad",
 "3": "Osaka",
 "5": "Luzon",
  "9": "Seoul",
  "8": "Hanoi",
  "7":"Singapore",
  "91":"Udaipur",  
  "92":"Hue",
  "98":"Bophut",
  "99":"Nara"
    
    # "304": "Chandigarh",
}

SUBCOLLECTION_NAME = "top_attractions"

# Image settings
MAX_IMAGES_PER_PLACE = 1        # Number of images to fetch per place (recommended: set to fill gap to 5)
MIN_IMAGE_WIDTH = 800
MIN_IMAGE_HEIGHT = 600

# ═══ IMPORTANT CONSTRAINT ═══
# Script ONLY processes places with LESS THAN 5 images in g_image_urls
# Examples:
#   - 0 images → Will fetch up to MAX_IMAGES_PER_PLACE
#   - 2 images → Will fetch up to MAX_IMAGES_PER_PLACE (total could be 5)
#   - 4 images → Will fetch up to MAX_IMAGES_PER_PLACE (total could be 7, or cap at 5 by setting MAX_IMAGES_PER_PLACE=1)
#   - 5+ images → Will SKIP (no fetching)

# ═══ NEW: Image Quality & Relevance Settings ═══
MIN_TITLE_SIMILARITY = 0.4      # 0-1: How similar the image title must be to attraction name
PREFER_EXACT_MATCH = True        # Prefer images with exact name match
EXCLUDE_KEYWORDS = [             # Exclude images with these keywords (maps, logos, etc.)
    "map", "logo", "diagram", "chart", "graph", "icon", 
    "flag", "coat of arms", "emblem", "symbol", "sign",
    "interior", "floor plan", "blueprint", "sketch"
]
MIN_ASPECT_RATIO = 0.5          # Minimum width/height ratio (exclude very tall/narrow images)
MAX_ASPECT_RATIO = 2.5          # Maximum width/height ratio
REQUIRE_LOCATION_MATCH = True    # Image must mention the location name
DEBUG_RELEVANCE = True           # Show relevance scoring details

# Processing mode
TRIAL_MODE = False   # True = download locally for review; False = upload to Firebase + update Firestore
LOCAL_DOWNLOAD_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\wikimedia_images3"

# Processing
API_DELAY = 0.5
DOWNLOAD_TIMEOUT = 30
DRY_RUN = False      # Only applies when TRIAL_MODE=False
ONLY_LOCATIONS = []  # If empty -> process all keys from LOCATION_NAMES

# Output
LOG_PATH = "wikimedia_fetch_log.csv"
CHECKPOINT_PATH = "wikimedia_checkpoint.json"

# ───── INIT ───────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": "PlanUp-WikimediaFetcher/1.0 (contact: youremail@example.com)"
}

# Shared session with retries/backoff
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
retries = Retry(
    total=5,
    backoff_factor=0.6,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=["GET"]
)
SESSION.mount("https://", HTTPAdapter(max_retries=retries))
SESSION.mount("http://", HTTPAdapter(max_retries=retries))

# Firebase
cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
try:
    firebase_admin.get_app()
except ValueError:
    if TRIAL_MODE:
        firebase_admin.initialize_app(cred)
    else:
        firebase_admin.initialize_app(cred, {"storageBucket": STORAGE_BUCKET})

db = firestore.client()
bucket = None
if not TRIAL_MODE:
    bucket = storage.bucket()

# Local folder
if TRIAL_MODE:
    os.makedirs(LOCAL_DOWNLOAD_PATH, exist_ok=True)
    print(f"📁 Local download folder: {LOCAL_DOWNLOAD_PATH}\n")

# ───── HELPERS ────────────────────────────────────────────────────────────────

ALLOWED_LICENSE_KEYS = {"cc-by", "cc-by-sa", "publicdomain", "pd", "cc0"}

def ascii_fallback(text: str) -> str:
    """Normalize string by stripping diacritics → ASCII."""
    if not text:
        return text
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")

def normalize_for_comparison(text: str) -> str:
    """Normalize text for fuzzy matching: lowercase, no special chars."""
    if not text:
        return ""
    # Convert to lowercase, remove special characters
    text = text.lower()
    text = ascii_fallback(text)
    # Keep only alphanumeric and spaces
    return ''.join(c if c.isalnum() or c.isspace() else ' ' for c in text).strip()

def calculate_similarity(str1: str, str2: str) -> float:
    """Calculate similarity ratio between two strings (0-1)."""
    norm1 = normalize_for_comparison(str1)
    norm2 = normalize_for_comparison(str2)
    return SequenceMatcher(None, norm1, norm2).ratio()

def check_relevance(image_data: Dict, attraction_name: str, location_name: str) -> Tuple[bool, float, str]:
    """
    Check if an image is relevant to the attraction.
    Returns: (is_relevant, relevance_score, reason)
    
    Scoring factors:
    - Title similarity to attraction name (0-40 points)
    - Location mentioned in title/description (0-30 points)
    - Aspect ratio quality (0-10 points)
    - No excluded keywords (0-20 points)
    """
    title = image_data.get("title", "")
    description = image_data.get("description", "")
    
    # Remove "File:" prefix from title
    clean_title = title.replace("File:", "").replace("_", " ").strip()
    
    score = 0
    reasons = []
    
    # ─── 1. Title Similarity (0-40 points) ───
    title_sim = calculate_similarity(clean_title, attraction_name)
    title_score = title_sim * 40
    score += title_score
    
    if title_sim >= 0.8:
        reasons.append(f"✅ Exact match (sim={title_sim:.2f})")
    elif title_sim >= MIN_TITLE_SIMILARITY:
        reasons.append(f"✓ Good match (sim={title_sim:.2f})")
    else:
        reasons.append(f"⚠️ Weak match (sim={title_sim:.2f})")
    
    # ─── 2. Location Match (0-30 points) ───
    location_norm = normalize_for_comparison(location_name)
    title_norm = normalize_for_comparison(clean_title)
    desc_norm = normalize_for_comparison(description)
    
    location_in_title = location_norm in title_norm
    location_in_desc = location_norm in desc_norm
    
    if location_in_title:
        score += 30
        reasons.append(f"✅ Location '{location_name}' in title")
    elif location_in_desc:
        score += 15
        reasons.append(f"✓ Location '{location_name}' in description")
    elif REQUIRE_LOCATION_MATCH:
        reasons.append(f"❌ Location '{location_name}' NOT found")
        return (False, score, "; ".join(reasons))
    else:
        reasons.append(f"⚠️ Location '{location_name}' not mentioned")
    
    # ─── 3. Aspect Ratio (0-10 points) ───
    width = image_data.get("width", 0)
    height = image_data.get("height", 0)
    
    if height > 0:
        aspect_ratio = width / height
        
        if MIN_ASPECT_RATIO <= aspect_ratio <= MAX_ASPECT_RATIO:
            score += 10
            reasons.append(f"✓ Good aspect ratio ({aspect_ratio:.2f})")
        else:
            reasons.append(f"⚠️ Unusual aspect ratio ({aspect_ratio:.2f})")
    
    # ─── 4. Excluded Keywords (0-20 points, or disqualification) ───
    title_lower = clean_title.lower()
    desc_lower = description.lower()
    
    found_excluded = []
    for keyword in EXCLUDE_KEYWORDS:
        if keyword in title_lower or keyword in desc_lower:
            found_excluded.append(keyword)
    
    if found_excluded:
        reasons.append(f"❌ Contains excluded keywords: {', '.join(found_excluded)}")
        return (False, score, "; ".join(reasons))
    else:
        score += 20
        reasons.append("✓ No excluded keywords")
    
    # ─── Final Decision ───
    # Need minimum score to pass
    min_score = 50  # Out of 100
    is_relevant = score >= min_score
    
    if not is_relevant:
        reasons.append(f"❌ Score {score:.1f}/100 < {min_score}")
    
    return (is_relevant, score, "; ".join(reasons))

def license_ok(extmeta: Dict) -> Tuple[bool, str, str]:
    """Check license from Commons extmetadata."""
    lic_key = (extmeta.get("License", {}) or {}).get("value", "").lower()
    short = (extmeta.get("LicenseShortName", {}) or {}).get("value", "")
    url = (extmeta.get("LicenseUrl", {}) or {}).get("value", "")
    ok = any(k in lic_key for k in ALLOWED_LICENSE_KEYS)
    return ok, short or lic_key or "Unknown", url or ""

def write_log(row: Dict):
    exists = os.path.exists(LOG_PATH)
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "ts","location_id","location_name","place_id","place_name",
            "status","images_found","images_added","message"
        ])
        if not exists:
            w.writeheader()
        w.writerow(row)

def load_checkpoint() -> set:
    if os.path.exists(CHECKPOINT_PATH):
        try:
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()

def save_checkpoint(done: set):
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        json.dump(sorted(list(done)), f, ensure_ascii=False, indent=2)

# ───── WIKIDATA ENTITY SEARCH (for better accuracy) ───────────────────────────

def search_wikidata_entity(attraction_name: str, location_name: str) -> Optional[str]:
    """
    Search Wikidata for the exact entity ID of an attraction.
    This helps us find the canonical image for that specific place.
    
    Returns: Wikidata entity ID (e.g., "Q15088") or None
    """
    api = "https://www.wikidata.org/w/api.php"
    
    params = {
        "action": "wbsearchentities",
        "format": "json",
        "language": "en",
        "search": f"{attraction_name} {location_name}",
        "limit": 5,
        "type": "item"
    }
    
    try:
        r = SESSION.get(api, params=params, timeout=10)
        if r.status_code != 200:
            return None
        
        data = r.json()
        results = data.get("search", [])
        
        if not results:
            return None
        
        # Score each result by relevance
        best_match = None
        best_score = 0
        
        for result in results:
            label = result.get("label", "")
            description = result.get("description", "")
            
            # Calculate relevance score
            label_sim = calculate_similarity(label, attraction_name)
            location_in_desc = location_name.lower() in description.lower()
            
            score = label_sim * 70 + (30 if location_in_desc else 0)
            
            if DEBUG_RELEVANCE:
                print(f"      Wikidata: {label} (ID: {result['id']}) - Score: {score:.1f}")
                print(f"         Description: {description}")
            
            if score > best_score:
                best_score = score
                best_match = result["id"]
        
        if best_score >= 50:  # Minimum confidence threshold
            if DEBUG_RELEVANCE:
                print(f"      ✓ Selected Wikidata entity: {best_match} (score: {best_score:.1f})")
            return best_match
        
        return None
        
    except Exception as e:
        print(f"      ⚠️ Wikidata search error: {e}")
        return None

def get_wikidata_images(entity_id: str, attraction_name: str, location_name: str) -> List[Dict]:
    """
    Fetch images directly from Wikidata entity.
    This gives us the CANONICAL image for that specific place.
    """
    api = "https://www.wikidata.org/w/api.php"
    
    params = {
        "action": "wbgetclaims",
        "format": "json",
        "entity": entity_id,
        "property": "P18"  # Image property
    }
    
    try:
        r = SESSION.get(api, params=params, timeout=10)
        if r.status_code != 200:
            return []
        
        data = r.json()
        claims = data.get("claims", {}).get("P18", [])
        
        if not claims:
            return []
        
        images = []
        for claim in claims:
            try:
                filename = claim["mainsnak"]["datavalue"]["value"]
                
                # Get full image info from Commons
                commons_api = "https://commons.wikimedia.org/w/api.php"
                info_params = {
                    "action": "query",
                    "format": "json",
                    "titles": f"File:{filename}",
                    "prop": "imageinfo",
                    "iiprop": "url|size|mime|sha1|extmetadata",
                    "iiurlwidth": 1600,
                }
                
                info_r = SESSION.get(commons_api, params=info_params, timeout=10)
                info_data = info_r.json()
                
                pages = info_data.get("query", {}).get("pages", {})
                for page_data in pages.values():
                    if "imageinfo" not in page_data:
                        continue
                    
                    ii = page_data["imageinfo"][0]
                    w, h = ii.get("width", 0), ii.get("height", 0)
                    
                    if w < MIN_IMAGE_WIDTH or h < MIN_IMAGE_HEIGHT:
                        continue
                    
                    extmeta = ii.get("extmetadata", {}) or {}
                    ok, lic_short, lic_url = license_ok(extmeta)
                    
                    if not ok:
                        continue
                    
                    title = page_data.get("title", "")
                    description = (extmeta.get("ImageDescription", {}) or {}).get("value", "")
                    
                    images.append({
                        "url": ii.get("url"),
                        "thumb_url": ii.get("thumburl"),
                        "width": w,
                        "height": h,
                        "mime": ii.get("mime", ""),
                        "title": title,
                        "description": description,
                        "license": lic_short,
                        "license_url": lic_url,
                        "artist": (extmeta.get("Artist", {}) or {}).get("value", ""),
                        "file_page": f"https://commons.wikimedia.org/wiki/{quote(title)}",
                        "sha1": ii.get("sha1", ""),
                        "source": "wikidata",
                        "relevance_score": 100.0  # Wikidata images are canonical
                    })
                
                time.sleep(0.2)
                
            except Exception as e:
                if DEBUG_RELEVANCE:
                    print(f"      ⚠️ Error processing Wikidata image: {e}")
                continue
        
        return images
        
    except Exception as e:
        print(f"      ⚠️ Wikidata claims error: {e}")
        return []

# ───── COMMONS SEARCH ─────────────────────────────────────────────────────────

def _pack_pages_to_results(pages: Dict, attraction_name: str, location_name: str) -> List[Dict]:
    """Convert Commons 'pages' dict to filtered list of image records WITH relevance checking."""
    out: List[Dict] = []
    for _, page in (pages or {}).items():
        infos = page.get("imageinfo") or []
        if not infos:
            continue
        ii = infos[0]
        w, h = ii.get("width", 0), ii.get("height", 0)
        if w < MIN_IMAGE_WIDTH or h < MIN_IMAGE_HEIGHT:
            continue
        
        extmeta = ii.get("extmetadata", {}) or {}
        ok, lic_short, lic_url = license_ok(extmeta)
        if not ok:
            continue

        title = page.get("title", "")
        description = (extmeta.get("ImageDescription", {}) or {}).get("value", "")
        file_page = f"https://commons.wikimedia.org/wiki/{quote(title)}" if title else ""

        # Create image data dict
        img_data = {
            "url": ii.get("url"),
            "thumb_url": ii.get("thumburl"),
            "width": w,
            "height": h,
            "mime": ii.get("mime", ""),
            "title": title,
            "description": description,
            "license": lic_short,
            "license_url": lic_url,
            "artist": (extmeta.get("Artist", {}) or {}).get("value", ""),
            "file_page": file_page,
            "sha1": ii.get("sha1", ""),
            "source": "commons_search"
        }
        
        # ═══ NEW: Check relevance ═══
        is_relevant, score, reason = check_relevance(img_data, attraction_name, location_name)
        
        if DEBUG_RELEVANCE:
            status = "✅" if is_relevant else "❌"
            print(f"         {status} {title[:60]}... Score: {score:.1f}/100")
            print(f"            {reason}")
        
        if is_relevant:
            img_data["relevance_score"] = score
            out.append(img_data)
    
    return out

def search_wikimedia_images(query: str, location: str = "") -> List[Dict]:
    """
    Multi-strategy search with relevance filtering:
    1. Try Wikidata entity search first (most accurate)
    2. Fall back to Commons search with relevance scoring
    """
    all_results = []
    
    # ═══ STRATEGY 1: Wikidata Entity Search (BEST ACCURACY) ═══
    if DEBUG_RELEVANCE:
        print(f"   🔍 Step 1: Searching Wikidata for exact entity...")
    
    entity_id = search_wikidata_entity(query, location)
    
    if entity_id:
        wikidata_images = get_wikidata_images(entity_id, query, location)
        if wikidata_images:
            if DEBUG_RELEVANCE:
                print(f"      ✅ Found {len(wikidata_images)} canonical images from Wikidata")
            all_results.extend(wikidata_images)
            # If we found Wikidata images, we're done - these are the most accurate
            return all_results[:MAX_IMAGES_PER_PLACE]
    
    if DEBUG_RELEVANCE:
        print(f"   🔍 Step 2: Searching Wikimedia Commons...")
    
    # ═══ STRATEGY 2: Commons Search with Relevance Filtering ═══
    api = "https://commons.wikimedia.org/w/api.php"

    # Build search queries with location context
    base = f"{query}".strip()
    loc  = f"{query} {location}".strip() if location else None
    base_ascii = ascii_fallback(base)
    loc_ascii  = ascii_fallback(loc) if loc else None
    candidates = [c for c in [loc, loc_ascii, base, base_ascii] if c]  # Prioritize location-specific

    seen_sha1, seen_url = set(), set()

    for idx, s in enumerate(candidates, 1):
        if DEBUG_RELEVANCE:
            print(f"      Trying query {idx}/{len(candidates)}: '{s}'")
        
        # Pass A: generator=search
        params_a = {
            "action": "query",
            "format": "json",
            "generator": "search",
            "gsrsearch": f"filetype:bitmap {s}",
            "gsrlimit": 50,
            "gsrnamespace": 6,
            "prop": "imageinfo",
            "iiprop": "url|size|mime|sha1|extmetadata",
            "iiurlwidth": 1600,
            "origin": "*",
        }
        
        try:
            r = SESSION.get(api, params=params_a, timeout=15)
            if r.status_code == 200:
                data = r.json()
                pages = (data.get("query", {}) or {}).get("pages", {})
                results = _pack_pages_to_results(pages, query, location)
                
                # Dedupe and collect
                for item in sorted(results, key=lambda x: x.get("relevance_score", 0), reverse=True):
                    s1, u = item.get("sha1"), item.get("url")
                    if s1 and s1 in seen_sha1:
                        continue
                    if (not s1) and u and u in seen_url:
                        continue
                    if s1: seen_sha1.add(s1)
                    if u:  seen_url.add(u)
                    all_results.append(item)
                
                if len(all_results) >= MAX_IMAGES_PER_PLACE:
                    break
        except Exception as e:
            print(f"         ⚠️ Commons search error: {e}")
        
        time.sleep(API_DELAY)
    
    # Sort by relevance score (highest first)
    all_results.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
    
    return all_results[:MAX_IMAGES_PER_PLACE]

# ───── PROCESS ATTRACTION ─────────────────────────────────────────────────────

def download_image(url: str, save_path: str) -> bool:
    """Download an image from URL to local path."""
    try:
        response = SESSION.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        return False
    except Exception as e:
        print(f"      ⚠️ Download error: {e}")
        return False

def process_attraction(doc, location_name: str) -> Tuple[bool, int, str]:
    """Process one attraction: search, validate, and store images."""
    data = doc.to_dict() or {}
    name = data.get("name", "Unknown")
    place_id = doc.id  # This is the document ID (e.g., ChIJBVDeRkaMGGARXsIjrhYCIhs)
    
    # ═══ CONSTRAINT: Only fetch if less than 5 existing images ═══
    current_images = data.get("g_image_urls", [])
    if len(current_images) >= 5:
        return (False, 0, f"Already has {len(current_images)} images (≥5, skipping)")
    
    # Calculate how many images needed to reach 5
    images_needed = 5 - len(current_images)
    images_to_fetch = min(MAX_IMAGES_PER_PLACE, images_needed)
    
    print(f"   🔍 Searching for images of '{name}' in '{location_name}'...")
    print(f"      Current: {len(current_images)} images, Target: 5, Will fetch: {images_to_fetch}")
    
    results = search_wikimedia_images(name, location_name)
    
    if not results:
        return (False, 0, "No relevant images found after filtering")
    
    # Limit results to exactly what we need to reach 5 total
    results = results[:images_to_fetch]
    print(f"   ✅ Found {len(results)} relevant image(s) (limited to reach target of 5)")
    
    # Get current image count to continue numbering
    start_index = len(current_images) + 1
    
    downloaded = []
    
    for idx, img_data in enumerate(results, start_index):
        try:
            relevance = img_data.get("relevance_score", 0)
            source = img_data.get("source", "unknown")
            
            print(f"      [{idx-start_index+1}/{len(results)}] Processing image #{idx} (score: {relevance:.1f}, source: {source})")
            
            if TRIAL_MODE:
                place_folder = os.path.join(LOCAL_DOWNLOAD_PATH, location_name, name.replace('/', '-'))
                os.makedirs(place_folder, exist_ok=True)
                
                ext = img_data['mime'].split('/')[-1] if '/' in img_data['mime'] else 'jpg'
                if ext not in ['jpg', 'jpeg', 'png', 'gif']:
                    ext = 'jpg'
                
                # Show what the Firebase filename would be
                firebase_filename = f"{place_id}_{idx}.jpg"
                filename = f"image_{idx}_score{int(relevance)}_{firebase_filename}"
                filepath = os.path.join(place_folder, filename)
                
                if download_image(img_data['url'], filepath):
                    # Save detailed metadata
                    metadata_path = filepath.rsplit('.', 1)[0] + '_info.txt'
                    with open(metadata_path, 'w', encoding='utf-8') as f:
                        f.write(f"Attraction: {name}\n")
                        f.write(f"Location: {location_name}\n")
                        f.write(f"Place ID: {place_id}\n")
                        f.write(f"Image Index: {idx}\n")
                        f.write(f"Firebase Filename: {firebase_filename}\n")
                        f.write(f"Firebase Path: lp_attractions/{firebase_filename}\n")
                        f.write(f"Relevance Score: {relevance:.1f}/100\n")
                        f.write(f"Source: {source}\n")
                        f.write(f"Title: {img_data['title']}\n")
                        f.write(f"Description: {img_data.get('description', 'N/A')}\n")
                        f.write(f"License: {img_data['license']}\n")
                        f.write(f"License URL: {img_data.get('license_url', 'N/A')}\n")
                        f.write(f"Artist: {img_data.get('artist', 'Unknown')}\n")
                        f.write(f"Size: {img_data['width']}x{img_data['height']}\n")
                        f.write(f"File Page: {img_data.get('file_page', 'N/A')}\n")
                        f.write(f"URL: {img_data['url']}\n")
                    
                    downloaded.append({
                        'url': filepath,
                        'firebase_filename': firebase_filename,
                        'source': source,
                        'relevance_score': relevance,
                        'license': img_data['license']
                    })
                    print(f"         ✓ Downloaded successfully (will be: lp_attractions/{firebase_filename})")
            else:
                # Firebase upload mode - MATCHES YOUR EXISTING STRUCTURE
                # Format: lp_attractions/{place_id}_{number}.jpg
                filename = f"{place_id}_{idx}.jpg"
                blob = bucket.blob(f"lp_attractions/{filename}")
                temp_path = f"/tmp/{filename}"
                
                if download_image(img_data['url'], temp_path):
                    blob.upload_from_filename(temp_path)
                    blob.make_public()
                    os.remove(temp_path)
                    
                    # This will create URLs like:
                    # https://storage.googleapis.com/mycasavsc.appspot.com/lp_attractions/ChIJBVDeRkaMGGARXsIjrhYCIhs_1.jpg
                    downloaded.append({
                        'url': blob.public_url,
                        'source': source,
                        'relevance_score': relevance,
                        'license': img_data['license'],
                        'metadata': {
                            'title': img_data['title'],
                            'artist': img_data.get('artist', ''),
                            'license_url': img_data.get('license_url', '')
                        }
                    })
                    print(f"         ✓ Uploaded to Firebase: lp_attractions/{filename}")
        
        except Exception as e:
            print(f"      ⚠️ Error processing image {idx}: {e}")
            continue
    
    if not downloaded:
        return (False, 0, "Failed to download any images")
    
    # Update Firestore in production mode
    if not TRIAL_MODE and not DRY_RUN:
        current_images = data.get("g_image_urls", [])
        new_urls = [d["url"] for d in downloaded]
        updated_images = current_images + new_urls
        
        # Update fields
        update_data = {
            "g_image_urls": updated_images,
            "images_updated_at": firestore.SERVER_TIMESTAMP
        }
        
        # Set image_url to first image if not already set
        if not data.get("image_url") and updated_images:
            update_data["image_url"] = updated_images[0]
        
        # Store wikimedia metadata (optional, can be removed if not needed)
        # update_data["wikimedia_assets"] = downloaded
        
        doc.reference.update(update_data)
    
    avg_score = sum(d.get('relevance_score', 0) for d in downloaded) / len(downloaded)
    return (True, len(downloaded), f"Added {len(downloaded)} images (avg relevance: {avg_score:.1f}/100)")

def process_location(location_id: str) -> Tuple[int, int, int]:
    """Process all attractions under a location."""
    location_name = LOCATION_NAMES.get(location_id, f"Location_{location_id}")

    print(f"\n{'='*70}")
    print(f"📍 Processing: {location_name} (ID: {location_id})")
    print(f"{'='*70}\n")

    coll = db.collection("allplaces").document(location_id).collection(SUBCOLLECTION_NAME)
    docs = list(coll.stream())

    if not docs:
        print("   ⚠️  No attractions found\n")
        return (0, 0, 0)

    total = len(docs)
    succeeded = 0
    failed = 0

    checkpoint_done = load_checkpoint()

    from datetime import datetime
    for idx, doc in enumerate(docs, 1):
        if doc.id in checkpoint_done:
            continue

        data = doc.to_dict() or {}
        name = data.get("name", "Unknown")
        print(f"\n[{idx}/{total}] {name}")

        success, count, message = process_attraction(doc, location_name)
        status = "ok" if success else "skip"
        if success:
            succeeded += 1
        else:
            failed += 1

        write_log({
            "ts": datetime.utcnow().isoformat(),
            "location_id": location_id,
            "location_name": location_name,
            "place_id": doc.id,
            "place_name": name,
            "status": status,
            "images_found": count,
            "images_added": count if (success and not TRIAL_MODE and not DRY_RUN) else 0,
            "message": message
        })

        checkpoint_done.add(doc.id)
        if idx % 5 == 0:  # Save checkpoint more frequently
            save_checkpoint(checkpoint_done)

    save_checkpoint(checkpoint_done)

    print(f"\n{'─'*70}")
    print(f"Summary for {location_name}:")
    print(f"  Total attractions: {total}")
    print(f"  Successfully processed: {succeeded}")
    print(f"  Skipped/Failed: {failed}")
    print(f"{'─'*70}")

    return (total, succeeded, failed)

# ───── REVIEW HTML ────────────────────────────────────────────────────────────

def generate_review_html(location_name: str):
    """Generate an HTML file to review downloaded images with relevance scores."""
    html_path = os.path.join(LOCAL_DOWNLOAD_PATH, f"{location_name}_review.html")

    html_content = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Wikimedia Images Review - {location_name}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
h1 {{ color: #333; }}
.place {{
  background: white; margin: 20px 0; padding: 20px;
  border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}}
.place h2 {{ color: #2196F3; margin-top: 0; }}
.images {{
  display: grid; grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
  gap: 15px; margin-top: 15px;
}}
.image-container {{
  border: 1px solid #ddd; padding: 10px; background: #fafafa; border-radius: 4px;
  position: relative;
}}
.image-container img {{
  width: 100%; height: 250px; object-fit: cover; border-radius: 4px;
}}
.relevance-badge {{
  position: absolute; top: 15px; right: 15px;
  background: rgba(0,0,0,0.7); color: white;
  padding: 5px 10px; border-radius: 4px; font-weight: bold;
}}
.high-score {{ background: #4CAF50 !important; }}
.medium-score {{ background: #FF9800 !important; }}
.low-score {{ background: #f44336 !important; }}
.image-info {{
  font-size: 11px; color: #666; margin-top: 5px;
}}
.stats {{
  background: #e3f2fd; padding: 15px; border-radius: 4px; margin-bottom: 20px;
}}
.warning {{ background: #fff3cd; padding: 10px; border-left: 4px solid #ffc107; margin: 10px 0; }}
</style>
</head>
<body>
  <h1>📸 Wikimedia Images Review - {location_name}</h1>
  <div class="stats">
    <strong>Instructions:</strong> Review the images below. Images are scored for relevance (0-100).
    <ul>
      <li>🟢 High score (70-100): Very likely correct</li>
      <li>🟡 Medium score (50-69): Probably correct, verify</li>
      <li>🔴 Low score (&lt;50): May be wrong place, review carefully</li>
    </ul>
    Delete any incorrect images from the folders before uploading to Firebase.
  </div>
  <div class="warning">
    ⚠️ <strong>Relevance Filters Applied:</strong> Images were filtered by title similarity, location match, aspect ratio, and keyword exclusion. Only relevant images are shown.
  </div>
"""

    loc_folder = os.path.join(LOCAL_DOWNLOAD_PATH, location_name)
    if os.path.exists(loc_folder):
        for place_name in sorted(os.listdir(loc_folder)):
            place_path = os.path.join(loc_folder, place_name)
            if not os.path.isdir(place_path):
                continue

            images = [f for f in os.listdir(place_path) if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif'))]
            if not images:
                continue

            html_content += f'<div class="place"><h2>{place_name}</h2><div class="images">'
            for img_file in sorted(images):
                img_path = os.path.join(place_path, img_file)
                rel_path = os.path.relpath(img_path, LOCAL_DOWNLOAD_PATH)

                # Extract score from filename (e.g., image_1_score85.jpg)
                score_text = "N/A"
                badge_class = ""
                if "_score" in img_file:
                    try:
                        score = int(img_file.split("_score")[1].split(".")[0])
                        score_text = f"{score}"
                        if score >= 70:
                            badge_class = "high-score"
                        elif score >= 50:
                            badge_class = "medium-score"
                        else:
                            badge_class = "low-score"
                    except:
                        pass

                info_file = img_path.rsplit('.', 1)[0] + '_info.txt'
                metadata = ""
                if os.path.exists(info_file):
                    with open(info_file, 'r', encoding='utf-8') as f:
                        metadata = f.read()

                html_content += f'''
                <div class="image-container">
                  <div class="relevance-badge {badge_class}">Score: {score_text}</div>
                  <img src="{rel_path}" alt="{img_file}">
                  <div class="image-info">
                    <strong>{img_file}</strong><br>
                    <pre style="font-size: 10px; margin: 5px 0; max-height: 150px; overflow-y: auto;">{metadata}</pre>
                  </div>
                </div>
                '''
            html_content += '</div></div>'

    html_content += """
</body>
</html>
"""
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    return html_path

# ───── MAIN ───────────────────────────────────────────────────────────────────

def main():
    mode_desc = "TRIAL MODE (downloading locally)" if TRIAL_MODE else "PRODUCTION MODE"
    print("📸 Wikimedia Image Fetcher for Top Attractions (with Relevance Filtering)")
    print(f"Mode: {mode_desc}")

    if TRIAL_MODE:
        print(f"Download location: {LOCAL_DOWNLOAD_PATH}")
    else:
        print(f"Firebase mode: {'DRY RUN (preview)' if DRY_RUN else 'LIVE (will update)'}")

    print("\nQuality Settings:")
    print(f"  Max images per place: {MAX_IMAGES_PER_PLACE}")
    print(f"  Min image size: {MIN_IMAGE_WIDTH}x{MIN_IMAGE_HEIGHT}")
    print(f"  Min title similarity: {MIN_TITLE_SIMILARITY}")
    print(f"  Require location match: {REQUIRE_LOCATION_MATCH}")
    print(f"  Debug relevance: {DEBUG_RELEVANCE}")
    print()

    if not TRIAL_MODE and not DRY_RUN:
        confirm = input("⚠️  This will UPDATE Firestore and upload images. Continue? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("Cancelled.")
            return

    location_ids = ONLY_LOCATIONS or list(LOCATION_NAMES.keys())
    total_all = succeeded_all = failed_all = 0

    for loc_id in location_ids:
        if loc_id not in LOCATION_NAMES:
            print(f"⚠️  Unknown location id {loc_id}")
            continue

        total, ok, bad = process_location(loc_id)
        total_all += total
        succeeded_all += ok
        failed_all += bad

        if TRIAL_MODE:
            loc_name = LOCATION_NAMES[loc_id]
            html_path = generate_review_html(loc_name)
            print(f"\n📄 Review HTML generated: {html_path}")
            print("   Open this file in your browser to review all images with relevance scores!")

    print(f"\n{'='*70}")
    print("🎉 Wikimedia Image Fetch Complete!")
    print(f"{'='*70}")
    print(f"Total attractions processed: {total_all}")
    print(f"Successfully processed: {succeeded_all}")
    print(f"Skipped/Failed: {failed_all}")

    if TRIAL_MODE:
        print(f"\n📁 All images saved to: {LOCAL_DOWNLOAD_PATH}")
        print("   Review the images and their relevance scores in the HTML file.")
        print("   Delete any incorrect images, then set TRIAL_MODE = False to upload.")
    print(f"{'='*70}")

if __name__ == "__main__":
    main()

# import os
# import csv
# import json
# import time
# import unicodedata
# from typing import List, Dict, Optional, Tuple
# from difflib import SequenceMatcher

# from urllib.parse import quote
# import requests
# from requests.adapters import HTTPAdapter, Retry

# import firebase_admin
# from firebase_admin import credentials, firestore, storage

# # ───── CONFIG ─────────────────────────────────────────────────────────────────

# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# STORAGE_BUCKET = "mycasavsc.appspot.com"

# # Location mapping (ids must match your Firestore structure under allplaces/{id})
# LOCATION_NAMES = {
#     "13": "New Delhi",
#     # "25": "Mumbai",
# }

# SUBCOLLECTION_NAME = "top_attractions"

# # Image settings
# MAX_IMAGES_PER_PLACE = 3
# MIN_IMAGE_WIDTH = 800
# MIN_IMAGE_HEIGHT = 600
# SKIP_IF_HAS_IMAGES = 3  # Only used in production mode

# # ═══ NEW: Image Quality & Relevance Settings ═══
# MIN_TITLE_SIMILARITY = 0.4      # 0-1: How similar the image title must be to attraction name
# PREFER_EXACT_MATCH = True        # Prefer images with exact name match
# EXCLUDE_KEYWORDS = [             # Exclude images with these keywords (maps, logos, etc.)
#     "map", "logo", "diagram", "chart", "graph", "icon", 
#     "flag", "coat of arms", "emblem", "symbol", "sign",
#     "interior", "floor plan", "blueprint", "sketch"
# ]
# MIN_ASPECT_RATIO = 0.5          # Minimum width/height ratio (exclude very tall/narrow images)
# MAX_ASPECT_RATIO = 2.5          # Maximum width/height ratio
# REQUIRE_LOCATION_MATCH = True    # Image must mention the location name
# DEBUG_RELEVANCE = True           # Show relevance scoring details

# # Processing mode
# TRIAL_MODE = True   # True = download locally for review; False = upload to Firebase + update Firestore
# LOCAL_DOWNLOAD_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\wikimedia_images2"

# # Processing
# API_DELAY = 0.5
# DOWNLOAD_TIMEOUT = 30
# DRY_RUN = True      # Only applies when TRIAL_MODE=False
# ONLY_LOCATIONS = ["13"]  # If empty -> process all keys from LOCATION_NAMES

# # Output
# LOG_PATH = "wikimedia_fetch_log.csv"
# CHECKPOINT_PATH = "wikimedia_checkpoint.json"

# # ───── INIT ───────────────────────────────────────────────────────────────────

# HEADERS = {
#     "User-Agent": "PlanUp-WikimediaFetcher/1.0 (contact: youremail@example.com)"
# }

# # Shared session with retries/backoff
# SESSION = requests.Session()
# SESSION.headers.update(HEADERS)
# retries = Retry(
#     total=5,
#     backoff_factor=0.6,
#     status_forcelist=(429, 500, 502, 503, 504),
#     allowed_methods=["GET"]
# )
# SESSION.mount("https://", HTTPAdapter(max_retries=retries))
# SESSION.mount("http://", HTTPAdapter(max_retries=retries))

# # Firebase
# cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
# try:
#     firebase_admin.get_app()
# except ValueError:
#     if TRIAL_MODE:
#         firebase_admin.initialize_app(cred)
#     else:
#         firebase_admin.initialize_app(cred, {"storageBucket": STORAGE_BUCKET})

# db = firestore.client()
# bucket = None
# if not TRIAL_MODE:
#     bucket = storage.bucket()

# # Local folder
# if TRIAL_MODE:
#     os.makedirs(LOCAL_DOWNLOAD_PATH, exist_ok=True)
#     print(f"📁 Local download folder: {LOCAL_DOWNLOAD_PATH}\n")

# # ───── HELPERS ────────────────────────────────────────────────────────────────

# ALLOWED_LICENSE_KEYS = {"cc-by", "cc-by-sa", "publicdomain", "pd", "cc0"}

# def ascii_fallback(text: str) -> str:
#     """Normalize string by stripping diacritics → ASCII."""
#     if not text:
#         return text
#     return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")

# def normalize_for_comparison(text: str) -> str:
#     """Normalize text for fuzzy matching: lowercase, no special chars."""
#     if not text:
#         return ""
#     # Convert to lowercase, remove special characters
#     text = text.lower()
#     text = ascii_fallback(text)
#     # Keep only alphanumeric and spaces
#     return ''.join(c if c.isalnum() or c.isspace() else ' ' for c in text).strip()

# def calculate_similarity(str1: str, str2: str) -> float:
#     """Calculate similarity ratio between two strings (0-1)."""
#     norm1 = normalize_for_comparison(str1)
#     norm2 = normalize_for_comparison(str2)
#     return SequenceMatcher(None, norm1, norm2).ratio()

# def check_relevance(image_data: Dict, attraction_name: str, location_name: str) -> Tuple[bool, float, str]:
#     """
#     Check if an image is relevant to the attraction.
#     Returns: (is_relevant, relevance_score, reason)
    
#     Scoring factors:
#     - Title similarity to attraction name (0-40 points)
#     - Location mentioned in title/description (0-30 points)
#     - Aspect ratio quality (0-10 points)
#     - No excluded keywords (0-20 points)
#     """
#     title = image_data.get("title", "")
#     description = image_data.get("description", "")
    
#     # Remove "File:" prefix from title
#     clean_title = title.replace("File:", "").replace("_", " ").strip()
    
#     score = 0
#     reasons = []
    
#     # ─── 1. Title Similarity (0-40 points) ───
#     title_sim = calculate_similarity(clean_title, attraction_name)
#     title_score = title_sim * 40
#     score += title_score
    
#     if title_sim >= 0.8:
#         reasons.append(f"✅ Exact match (sim={title_sim:.2f})")
#     elif title_sim >= MIN_TITLE_SIMILARITY:
#         reasons.append(f"✓ Good match (sim={title_sim:.2f})")
#     else:
#         reasons.append(f"⚠️ Weak match (sim={title_sim:.2f})")
    
#     # ─── 2. Location Match (0-30 points) ───
#     location_norm = normalize_for_comparison(location_name)
#     title_norm = normalize_for_comparison(clean_title)
#     desc_norm = normalize_for_comparison(description)
    
#     location_in_title = location_norm in title_norm
#     location_in_desc = location_norm in desc_norm
    
#     if location_in_title:
#         score += 30
#         reasons.append(f"✅ Location '{location_name}' in title")
#     elif location_in_desc:
#         score += 15
#         reasons.append(f"✓ Location '{location_name}' in description")
#     elif REQUIRE_LOCATION_MATCH:
#         reasons.append(f"❌ Location '{location_name}' NOT found")
#         return (False, score, "; ".join(reasons))
#     else:
#         reasons.append(f"⚠️ Location '{location_name}' not mentioned")
    
#     # ─── 3. Aspect Ratio (0-10 points) ───
#     width = image_data.get("width", 0)
#     height = image_data.get("height", 0)
    
#     if height > 0:
#         aspect_ratio = width / height
        
#         if MIN_ASPECT_RATIO <= aspect_ratio <= MAX_ASPECT_RATIO:
#             score += 10
#             reasons.append(f"✓ Good aspect ratio ({aspect_ratio:.2f})")
#         else:
#             reasons.append(f"⚠️ Unusual aspect ratio ({aspect_ratio:.2f})")
    
#     # ─── 4. Excluded Keywords (0-20 points, or disqualification) ───
#     title_lower = clean_title.lower()
#     desc_lower = description.lower()
    
#     found_excluded = []
#     for keyword in EXCLUDE_KEYWORDS:
#         if keyword in title_lower or keyword in desc_lower:
#             found_excluded.append(keyword)
    
#     if found_excluded:
#         reasons.append(f"❌ Contains excluded keywords: {', '.join(found_excluded)}")
#         return (False, score, "; ".join(reasons))
#     else:
#         score += 20
#         reasons.append("✓ No excluded keywords")
    
#     # ─── Final Decision ───
#     # Need minimum score to pass
#     min_score = 50  # Out of 100
#     is_relevant = score >= min_score
    
#     if not is_relevant:
#         reasons.append(f"❌ Score {score:.1f}/100 < {min_score}")
    
#     return (is_relevant, score, "; ".join(reasons))

# def license_ok(extmeta: Dict) -> Tuple[bool, str, str]:
#     """Check license from Commons extmetadata."""
#     lic_key = (extmeta.get("License", {}) or {}).get("value", "").lower()
#     short = (extmeta.get("LicenseShortName", {}) or {}).get("value", "")
#     url = (extmeta.get("LicenseUrl", {}) or {}).get("value", "")
#     ok = any(k in lic_key for k in ALLOWED_LICENSE_KEYS)
#     return ok, short or lic_key or "Unknown", url or ""

# def write_log(row: Dict):
#     exists = os.path.exists(LOG_PATH)
#     with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
#         w = csv.DictWriter(f, fieldnames=[
#             "ts","location_id","location_name","place_id","place_name",
#             "status","images_found","images_added","message"
#         ])
#         if not exists:
#             w.writeheader()
#         w.writerow(row)

# def load_checkpoint() -> set:
#     if os.path.exists(CHECKPOINT_PATH):
#         try:
#             with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
#                 return set(json.load(f))
#         except Exception:
#             return set()
#     return set()

# def save_checkpoint(done: set):
#     with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
#         json.dump(sorted(list(done)), f, ensure_ascii=False, indent=2)

# # ───── WIKIDATA ENTITY SEARCH (for better accuracy) ───────────────────────────

# def search_wikidata_entity(attraction_name: str, location_name: str) -> Optional[str]:
#     """
#     Search Wikidata for the exact entity ID of an attraction.
#     This helps us find the canonical image for that specific place.
    
#     Returns: Wikidata entity ID (e.g., "Q15088") or None
#     """
#     api = "https://www.wikidata.org/w/api.php"
    
#     params = {
#         "action": "wbsearchentities",
#         "format": "json",
#         "language": "en",
#         "search": f"{attraction_name} {location_name}",
#         "limit": 5,
#         "type": "item"
#     }
    
#     try:
#         r = SESSION.get(api, params=params, timeout=10)
#         if r.status_code != 200:
#             return None
        
#         data = r.json()
#         results = data.get("search", [])
        
#         if not results:
#             return None
        
#         # Score each result by relevance
#         best_match = None
#         best_score = 0
        
#         for result in results:
#             label = result.get("label", "")
#             description = result.get("description", "")
            
#             # Calculate relevance score
#             label_sim = calculate_similarity(label, attraction_name)
#             location_in_desc = location_name.lower() in description.lower()
            
#             score = label_sim * 70 + (30 if location_in_desc else 0)
            
#             if DEBUG_RELEVANCE:
#                 print(f"      Wikidata: {label} (ID: {result['id']}) - Score: {score:.1f}")
#                 print(f"         Description: {description}")
            
#             if score > best_score:
#                 best_score = score
#                 best_match = result["id"]
        
#         if best_score >= 50:  # Minimum confidence threshold
#             if DEBUG_RELEVANCE:
#                 print(f"      ✓ Selected Wikidata entity: {best_match} (score: {best_score:.1f})")
#             return best_match
        
#         return None
        
#     except Exception as e:
#         print(f"      ⚠️ Wikidata search error: {e}")
#         return None

# def get_wikidata_images(entity_id: str, attraction_name: str, location_name: str) -> List[Dict]:
#     """
#     Fetch images directly from Wikidata entity.
#     This gives us the CANONICAL image for that specific place.
#     """
#     api = "https://www.wikidata.org/w/api.php"
    
#     params = {
#         "action": "wbgetclaims",
#         "format": "json",
#         "entity": entity_id,
#         "property": "P18"  # Image property
#     }
    
#     try:
#         r = SESSION.get(api, params=params, timeout=10)
#         if r.status_code != 200:
#             return []
        
#         data = r.json()
#         claims = data.get("claims", {}).get("P18", [])
        
#         if not claims:
#             return []
        
#         images = []
#         for claim in claims:
#             try:
#                 filename = claim["mainsnak"]["datavalue"]["value"]
                
#                 # Get full image info from Commons
#                 commons_api = "https://commons.wikimedia.org/w/api.php"
#                 info_params = {
#                     "action": "query",
#                     "format": "json",
#                     "titles": f"File:{filename}",
#                     "prop": "imageinfo",
#                     "iiprop": "url|size|mime|sha1|extmetadata",
#                     "iiurlwidth": 1600,
#                 }
                
#                 info_r = SESSION.get(commons_api, params=info_params, timeout=10)
#                 info_data = info_r.json()
                
#                 pages = info_data.get("query", {}).get("pages", {})
#                 for page_data in pages.values():
#                     if "imageinfo" not in page_data:
#                         continue
                    
#                     ii = page_data["imageinfo"][0]
#                     w, h = ii.get("width", 0), ii.get("height", 0)
                    
#                     if w < MIN_IMAGE_WIDTH or h < MIN_IMAGE_HEIGHT:
#                         continue
                    
#                     extmeta = ii.get("extmetadata", {}) or {}
#                     ok, lic_short, lic_url = license_ok(extmeta)
                    
#                     if not ok:
#                         continue
                    
#                     title = page_data.get("title", "")
#                     description = (extmeta.get("ImageDescription", {}) or {}).get("value", "")
                    
#                     images.append({
#                         "url": ii.get("url"),
#                         "thumb_url": ii.get("thumburl"),
#                         "width": w,
#                         "height": h,
#                         "mime": ii.get("mime", ""),
#                         "title": title,
#                         "description": description,
#                         "license": lic_short,
#                         "license_url": lic_url,
#                         "artist": (extmeta.get("Artist", {}) or {}).get("value", ""),
#                         "file_page": f"https://commons.wikimedia.org/wiki/{quote(title)}",
#                         "sha1": ii.get("sha1", ""),
#                         "source": "wikidata",
#                         "relevance_score": 100.0  # Wikidata images are canonical
#                     })
                
#                 time.sleep(0.2)
                
#             except Exception as e:
#                 if DEBUG_RELEVANCE:
#                     print(f"      ⚠️ Error processing Wikidata image: {e}")
#                 continue
        
#         return images
        
#     except Exception as e:
#         print(f"      ⚠️ Wikidata claims error: {e}")
#         return []

# # ───── COMMONS SEARCH ─────────────────────────────────────────────────────────

# def _pack_pages_to_results(pages: Dict, attraction_name: str, location_name: str) -> List[Dict]:
#     """Convert Commons 'pages' dict to filtered list of image records WITH relevance checking."""
#     out: List[Dict] = []
#     for _, page in (pages or {}).items():
#         infos = page.get("imageinfo") or []
#         if not infos:
#             continue
#         ii = infos[0]
#         w, h = ii.get("width", 0), ii.get("height", 0)
#         if w < MIN_IMAGE_WIDTH or h < MIN_IMAGE_HEIGHT:
#             continue
        
#         extmeta = ii.get("extmetadata", {}) or {}
#         ok, lic_short, lic_url = license_ok(extmeta)
#         if not ok:
#             continue

#         title = page.get("title", "")
#         description = (extmeta.get("ImageDescription", {}) or {}).get("value", "")
#         file_page = f"https://commons.wikimedia.org/wiki/{quote(title)}" if title else ""

#         # Create image data dict
#         img_data = {
#             "url": ii.get("url"),
#             "thumb_url": ii.get("thumburl"),
#             "width": w,
#             "height": h,
#             "mime": ii.get("mime", ""),
#             "title": title,
#             "description": description,
#             "license": lic_short,
#             "license_url": lic_url,
#             "artist": (extmeta.get("Artist", {}) or {}).get("value", ""),
#             "file_page": file_page,
#             "sha1": ii.get("sha1", ""),
#             "source": "commons_search"
#         }
        
#         # ═══ NEW: Check relevance ═══
#         is_relevant, score, reason = check_relevance(img_data, attraction_name, location_name)
        
#         if DEBUG_RELEVANCE:
#             status = "✅" if is_relevant else "❌"
#             print(f"         {status} {title[:60]}... Score: {score:.1f}/100")
#             print(f"            {reason}")
        
#         if is_relevant:
#             img_data["relevance_score"] = score
#             out.append(img_data)
    
#     return out

# def search_wikimedia_images(query: str, location: str = "") -> List[Dict]:
#     """
#     Multi-strategy search with relevance filtering:
#     1. Try Wikidata entity search first (most accurate)
#     2. Fall back to Commons search with relevance scoring
#     """
#     all_results = []
    
#     # ═══ STRATEGY 1: Wikidata Entity Search (BEST ACCURACY) ═══
#     if DEBUG_RELEVANCE:
#         print(f"   🔍 Step 1: Searching Wikidata for exact entity...")
    
#     entity_id = search_wikidata_entity(query, location)
    
#     if entity_id:
#         wikidata_images = get_wikidata_images(entity_id, query, location)
#         if wikidata_images:
#             if DEBUG_RELEVANCE:
#                 print(f"      ✅ Found {len(wikidata_images)} canonical images from Wikidata")
#             all_results.extend(wikidata_images)
#             # If we found Wikidata images, we're done - these are the most accurate
#             return all_results[:MAX_IMAGES_PER_PLACE]
    
#     if DEBUG_RELEVANCE:
#         print(f"   🔍 Step 2: Searching Wikimedia Commons...")
    
#     # ═══ STRATEGY 2: Commons Search with Relevance Filtering ═══
#     api = "https://commons.wikimedia.org/w/api.php"

#     # Build search queries with location context
#     base = f"{query}".strip()
#     loc  = f"{query} {location}".strip() if location else None
#     base_ascii = ascii_fallback(base)
#     loc_ascii  = ascii_fallback(loc) if loc else None
#     candidates = [c for c in [loc, loc_ascii, base, base_ascii] if c]  # Prioritize location-specific

#     seen_sha1, seen_url = set(), set()

#     for idx, s in enumerate(candidates, 1):
#         if DEBUG_RELEVANCE:
#             print(f"      Trying query {idx}/{len(candidates)}: '{s}'")
        
#         # Pass A: generator=search
#         params_a = {
#             "action": "query",
#             "format": "json",
#             "generator": "search",
#             "gsrsearch": f"filetype:bitmap {s}",
#             "gsrlimit": 50,
#             "gsrnamespace": 6,
#             "prop": "imageinfo",
#             "iiprop": "url|size|mime|sha1|extmetadata",
#             "iiurlwidth": 1600,
#             "origin": "*",
#         }
        
#         try:
#             r = SESSION.get(api, params=params_a, timeout=15)
#             if r.status_code == 200:
#                 data = r.json()
#                 pages = (data.get("query", {}) or {}).get("pages", {})
#                 results = _pack_pages_to_results(pages, query, location)
                
#                 # Dedupe and collect
#                 for item in sorted(results, key=lambda x: x.get("relevance_score", 0), reverse=True):
#                     s1, u = item.get("sha1"), item.get("url")
#                     if s1 and s1 in seen_sha1:
#                         continue
#                     if (not s1) and u and u in seen_url:
#                         continue
#                     if s1: seen_sha1.add(s1)
#                     if u:  seen_url.add(u)
#                     all_results.append(item)
                
#                 if len(all_results) >= MAX_IMAGES_PER_PLACE:
#                     break
#         except Exception as e:
#             print(f"         ⚠️ Commons search error: {e}")
        
#         time.sleep(API_DELAY)
    
#     # Sort by relevance score (highest first)
#     all_results.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
    
#     return all_results[:MAX_IMAGES_PER_PLACE]

# # ───── PROCESS ATTRACTION ─────────────────────────────────────────────────────

# def download_image(url: str, save_path: str) -> bool:
#     """Download an image from URL to local path."""
#     try:
#         response = SESSION.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
#         if response.status_code == 200:
#             with open(save_path, 'wb') as f:
#                 for chunk in response.iter_content(chunk_size=8192):
#                     f.write(chunk)
#             return True
#         return False
#     except Exception as e:
#         print(f"      ⚠️ Download error: {e}")
#         return False

# def process_attraction(doc, location_name: str) -> Tuple[bool, int, str]:
#     """Process one attraction: search, validate, and store images."""
#     data = doc.to_dict() or {}
#     name = data.get("name", "Unknown")
    
#     if not TRIAL_MODE:
#         current_images = data.get("g_image_urls", [])
#         if len(current_images) >= SKIP_IF_HAS_IMAGES:
#             return (False, 0, f"Already has {len(current_images)} images")
    
#     print(f"   🔍 Searching for images of '{name}' in '{location_name}'...")
#     results = search_wikimedia_images(name, location_name)
    
#     if not results:
#         return (False, 0, "No relevant images found after filtering")
    
#     print(f"   ✅ Found {len(results)} relevant image(s)")
    
#     downloaded = []
    
#     for idx, img_data in enumerate(results, 1):
#         try:
#             relevance = img_data.get("relevance_score", 0)
#             source = img_data.get("source", "unknown")
            
#             print(f"      [{idx}/{len(results)}] Processing image (score: {relevance:.1f}, source: {source})")
            
#             if TRIAL_MODE:
#                 place_folder = os.path.join(LOCAL_DOWNLOAD_PATH, location_name, name.replace('/', '-'))
#                 os.makedirs(place_folder, exist_ok=True)
                
#                 ext = img_data['mime'].split('/')[-1] if '/' in img_data['mime'] else 'jpg'
#                 if ext not in ['jpg', 'jpeg', 'png', 'gif']:
#                     ext = 'jpg'
                
#                 filename = f"image_{idx}_score{int(relevance)}.{ext}"
#                 filepath = os.path.join(place_folder, filename)
                
#                 if download_image(img_data['url'], filepath):
#                     # Save detailed metadata
#                     metadata_path = filepath.rsplit('.', 1)[0] + '_info.txt'
#                     with open(metadata_path, 'w', encoding='utf-8') as f:
#                         f.write(f"Attraction: {name}\n")
#                         f.write(f"Location: {location_name}\n")
#                         f.write(f"Relevance Score: {relevance:.1f}/100\n")
#                         f.write(f"Source: {source}\n")
#                         f.write(f"Title: {img_data['title']}\n")
#                         f.write(f"Description: {img_data.get('description', 'N/A')}\n")
#                         f.write(f"License: {img_data['license']}\n")
#                         f.write(f"License URL: {img_data.get('license_url', 'N/A')}\n")
#                         f.write(f"Artist: {img_data.get('artist', 'Unknown')}\n")
#                         f.write(f"Size: {img_data['width']}x{img_data['height']}\n")
#                         f.write(f"File Page: {img_data.get('file_page', 'N/A')}\n")
#                         f.write(f"URL: {img_data['url']}\n")
                    
#                     downloaded.append({
#                         'url': filepath,
#                         'source': source,
#                         'relevance_score': relevance,
#                         'license': img_data['license']
#                     })
#                     print(f"         ✓ Downloaded successfully")
#             else:
#                 # Firebase upload mode
#                 filename = f"{doc.id}_wikimedia_{idx}.jpg"
#                 blob = bucket.blob(f"attractions/{location_name}/{filename}")
#                 temp_path = f"/tmp/{filename}"
                
#                 if download_image(img_data['url'], temp_path):
#                     blob.upload_from_filename(temp_path)
#                     blob.make_public()
#                     os.remove(temp_path)
                    
#                     downloaded.append({
#                         'url': blob.public_url,
#                         'source': source,
#                         'relevance_score': relevance,
#                         'license': img_data['license'],
#                         'metadata': {
#                             'title': img_data['title'],
#                             'artist': img_data.get('artist', ''),
#                             'license_url': img_data.get('license_url', '')
#                         }
#                     })
#                     print(f"         ✓ Uploaded to Firebase")
        
#         except Exception as e:
#             print(f"      ⚠️ Error processing image {idx}: {e}")
#             continue
    
#     if not downloaded:
#         return (False, 0, "Failed to download any images")
    
#     # Update Firestore in production mode
#     if not TRIAL_MODE and not DRY_RUN:
#         current_images = data.get("g_image_urls", [])
#         new_urls = [d["url"] for d in downloaded]
        
#         doc.reference.update({
#             "g_image_urls": current_images + new_urls,
#             "image_url": new_urls[0] if new_urls else data.get("image_url"),
#             "wikimedia_assets": downloaded,
#             "images_updated_at": firestore.SERVER_TIMESTAMP
#         })
    
#     avg_score = sum(d.get('relevance_score', 0) for d in downloaded) / len(downloaded)
#     return (True, len(downloaded), f"Added {len(downloaded)} images (avg relevance: {avg_score:.1f}/100)")

# def process_location(location_id: str) -> Tuple[int, int, int]:
#     """Process all attractions under a location."""
#     location_name = LOCATION_NAMES.get(location_id, f"Location_{location_id}")

#     print(f"\n{'='*70}")
#     print(f"📍 Processing: {location_name} (ID: {location_id})")
#     print(f"{'='*70}\n")

#     coll = db.collection("allplaces").document(location_id).collection(SUBCOLLECTION_NAME)
#     docs = list(coll.stream())

#     if not docs:
#         print("   ⚠️  No attractions found\n")
#         return (0, 0, 0)

#     total = len(docs)
#     succeeded = 0
#     failed = 0

#     checkpoint_done = load_checkpoint()

#     from datetime import datetime
#     for idx, doc in enumerate(docs, 1):
#         if doc.id in checkpoint_done:
#             continue

#         data = doc.to_dict() or {}
#         name = data.get("name", "Unknown")
#         print(f"\n[{idx}/{total}] {name}")

#         success, count, message = process_attraction(doc, location_name)
#         status = "ok" if success else "skip"
#         if success:
#             succeeded += 1
#         else:
#             failed += 1

#         write_log({
#             "ts": datetime.utcnow().isoformat(),
#             "location_id": location_id,
#             "location_name": location_name,
#             "place_id": doc.id,
#             "place_name": name,
#             "status": status,
#             "images_found": count,
#             "images_added": count if (success and not TRIAL_MODE and not DRY_RUN) else 0,
#             "message": message
#         })

#         checkpoint_done.add(doc.id)
#         if idx % 5 == 0:  # Save checkpoint more frequently
#             save_checkpoint(checkpoint_done)

#     save_checkpoint(checkpoint_done)

#     print(f"\n{'─'*70}")
#     print(f"Summary for {location_name}:")
#     print(f"  Total attractions: {total}")
#     print(f"  Successfully processed: {succeeded}")
#     print(f"  Skipped/Failed: {failed}")
#     print(f"{'─'*70}")

#     return (total, succeeded, failed)

# # ───── REVIEW HTML ────────────────────────────────────────────────────────────

# def generate_review_html(location_name: str):
#     """Generate an HTML file to review downloaded images with relevance scores."""
#     html_path = os.path.join(LOCAL_DOWNLOAD_PATH, f"{location_name}_review.html")

#     html_content = f"""<!DOCTYPE html>
# <html>
# <head>
# <meta charset="UTF-8">
# <title>Wikimedia Images Review - {location_name}</title>
# <style>
# body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
# h1 {{ color: #333; }}
# .place {{
#   background: white; margin: 20px 0; padding: 20px;
#   border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
# }}
# .place h2 {{ color: #2196F3; margin-top: 0; }}
# .images {{
#   display: grid; grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
#   gap: 15px; margin-top: 15px;
# }}
# .image-container {{
#   border: 1px solid #ddd; padding: 10px; background: #fafafa; border-radius: 4px;
#   position: relative;
# }}
# .image-container img {{
#   width: 100%; height: 250px; object-fit: cover; border-radius: 4px;
# }}
# .relevance-badge {{
#   position: absolute; top: 15px; right: 15px;
#   background: rgba(0,0,0,0.7); color: white;
#   padding: 5px 10px; border-radius: 4px; font-weight: bold;
# }}
# .high-score {{ background: #4CAF50 !important; }}
# .medium-score {{ background: #FF9800 !important; }}
# .low-score {{ background: #f44336 !important; }}
# .image-info {{
#   font-size: 11px; color: #666; margin-top: 5px;
# }}
# .stats {{
#   background: #e3f2fd; padding: 15px; border-radius: 4px; margin-bottom: 20px;
# }}
# .warning {{ background: #fff3cd; padding: 10px; border-left: 4px solid #ffc107; margin: 10px 0; }}
# </style>
# </head>
# <body>
#   <h1>📸 Wikimedia Images Review - {location_name}</h1>
#   <div class="stats">
#     <strong>Instructions:</strong> Review the images below. Images are scored for relevance (0-100).
#     <ul>
#       <li>🟢 High score (70-100): Very likely correct</li>
#       <li>🟡 Medium score (50-69): Probably correct, verify</li>
#       <li>🔴 Low score (&lt;50): May be wrong place, review carefully</li>
#     </ul>
#     Delete any incorrect images from the folders before uploading to Firebase.
#   </div>
#   <div class="warning">
#     ⚠️ <strong>Relevance Filters Applied:</strong> Images were filtered by title similarity, location match, aspect ratio, and keyword exclusion. Only relevant images are shown.
#   </div>
# """

#     loc_folder = os.path.join(LOCAL_DOWNLOAD_PATH, location_name)
#     if os.path.exists(loc_folder):
#         for place_name in sorted(os.listdir(loc_folder)):
#             place_path = os.path.join(loc_folder, place_name)
#             if not os.path.isdir(place_path):
#                 continue

#             images = [f for f in os.listdir(place_path) if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif'))]
#             if not images:
#                 continue

#             html_content += f'<div class="place"><h2>{place_name}</h2><div class="images">'
#             for img_file in sorted(images):
#                 img_path = os.path.join(place_path, img_file)
#                 rel_path = os.path.relpath(img_path, LOCAL_DOWNLOAD_PATH)

#                 # Extract score from filename (e.g., image_1_score85.jpg)
#                 score_text = "N/A"
#                 badge_class = ""
#                 if "_score" in img_file:
#                     try:
#                         score = int(img_file.split("_score")[1].split(".")[0])
#                         score_text = f"{score}"
#                         if score >= 70:
#                             badge_class = "high-score"
#                         elif score >= 50:
#                             badge_class = "medium-score"
#                         else:
#                             badge_class = "low-score"
#                     except:
#                         pass

#                 info_file = img_path.rsplit('.', 1)[0] + '_info.txt'
#                 metadata = ""
#                 if os.path.exists(info_file):
#                     with open(info_file, 'r', encoding='utf-8') as f:
#                         metadata = f.read()

#                 html_content += f'''
#                 <div class="image-container">
#                   <div class="relevance-badge {badge_class}">Score: {score_text}</div>
#                   <img src="{rel_path}" alt="{img_file}">
#                   <div class="image-info">
#                     <strong>{img_file}</strong><br>
#                     <pre style="font-size: 10px; margin: 5px 0; max-height: 150px; overflow-y: auto;">{metadata}</pre>
#                   </div>
#                 </div>
#                 '''
#             html_content += '</div></div>'

#     html_content += """
# </body>
# </html>
# """
#     with open(html_path, 'w', encoding='utf-8') as f:
#         f.write(html_content)
#     return html_path

# # ───── MAIN ───────────────────────────────────────────────────────────────────

# def main():
#     mode_desc = "TRIAL MODE (downloading locally)" if TRIAL_MODE else "PRODUCTION MODE"
#     print("📸 Wikimedia Image Fetcher for Top Attractions (with Relevance Filtering)")
#     print(f"Mode: {mode_desc}")

#     if TRIAL_MODE:
#         print(f"Download location: {LOCAL_DOWNLOAD_PATH}")
#     else:
#         print(f"Firebase mode: {'DRY RUN (preview)' if DRY_RUN else 'LIVE (will update)'}")

#     print("\nQuality Settings:")
#     print(f"  Max images per place: {MAX_IMAGES_PER_PLACE}")
#     print(f"  Min image size: {MIN_IMAGE_WIDTH}x{MIN_IMAGE_HEIGHT}")
#     print(f"  Min title similarity: {MIN_TITLE_SIMILARITY}")
#     print(f"  Require location match: {REQUIRE_LOCATION_MATCH}")
#     print(f"  Debug relevance: {DEBUG_RELEVANCE}")
#     print()

#     if not TRIAL_MODE and not DRY_RUN:
#         confirm = input("⚠️  This will UPDATE Firestore and upload images. Continue? (yes/no): ").strip().lower()
#         if confirm != "yes":
#             print("Cancelled.")
#             return

#     location_ids = ONLY_LOCATIONS or list(LOCATION_NAMES.keys())
#     total_all = succeeded_all = failed_all = 0

#     for loc_id in location_ids:
#         if loc_id not in LOCATION_NAMES:
#             print(f"⚠️  Unknown location id {loc_id}")
#             continue

#         total, ok, bad = process_location(loc_id)
#         total_all += total
#         succeeded_all += ok
#         failed_all += bad

#         if TRIAL_MODE:
#             loc_name = LOCATION_NAMES[loc_id]
#             html_path = generate_review_html(loc_name)
#             print(f"\n📄 Review HTML generated: {html_path}")
#             print("   Open this file in your browser to review all images with relevance scores!")

#     print(f"\n{'='*70}")
#     print("🎉 Wikimedia Image Fetch Complete!")
#     print(f"{'='*70}")
#     print(f"Total attractions processed: {total_all}")
#     print(f"Successfully processed: {succeeded_all}")
#     print(f"Skipped/Failed: {failed_all}")

#     if TRIAL_MODE:
#         print(f"\n📁 All images saved to: {LOCAL_DOWNLOAD_PATH}")
#         print("   Review the images and their relevance scores in the HTML file.")
#         print("   Delete any incorrect images, then set TRIAL_MODE = False to upload.")
#     print(f"{'='*70}")

# if __name__ == "__main__":
#     main()



# Working code of ChatGPT
# # -*- coding: utf-8 -*-
# """
# Wikimedia → (Trial) local download / (Production) Firebase upload
# - Searches Commons in File namespace with ASCII fallbacks
# - Filters by size & license (CC BY / CC BY-SA / PD/CC0)
# - Retries/backoff + proper User-Agent
# - Dedupes by SHA1
# - Saves full attribution metadata
# - Trial mode: saves locally + review HTML
# - Production mode: uploads to Firebase Storage and updates Firestore cleanly
# """

# import os
# import csv
# import json
# import time
# import unicodedata
# from typing import List, Dict, Optional, Tuple

# from urllib.parse import quote
# import requests
# from requests.adapters import HTTPAdapter, Retry

# import firebase_admin
# from firebase_admin import credentials, firestore, storage

# # ───── CONFIG ─────────────────────────────────────────────────────────────────

# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# STORAGE_BUCKET = "mycasavsc.appspot.com"

# # Location mapping (ids must match your Firestore structure under allplaces/{id})
# LOCATION_NAMES = {
#     "13": "New Delhi",
#     # "25": "Mumbai",
# }

# SUBCOLLECTION_NAME = "top_attractions"

# # Image settings
# MAX_IMAGES_PER_PLACE = 3
# MIN_IMAGE_WIDTH = 800
# MIN_IMAGE_HEIGHT = 600
# SKIP_IF_HAS_IMAGES = 3  # Only used in production mode

# # Processing mode
# TRIAL_MODE = True   # True = download locally for review; False = upload to Firebase + update Firestore
# LOCAL_DOWNLOAD_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\wikimedia_images"

# # Processing
# API_DELAY = 0.5
# DOWNLOAD_TIMEOUT = 30
# DRY_RUN = True      # Only applies when TRIAL_MODE=False
# ONLY_LOCATIONS = ["13"]  # If empty -> process all keys from LOCATION_NAMES

# # Output
# LOG_PATH = "wikimedia_fetch_log.csv"
# CHECKPOINT_PATH = "wikimedia_checkpoint.json"

# # ───── INIT ───────────────────────────────────────────────────────────────────

# HEADERS = {
#     "User-Agent": "PlanUp-WikimediaFetcher/1.0 (contact: youremail@example.com)"
# }

# # Shared session with retries/backoff
# SESSION = requests.Session()
# SESSION.headers.update(HEADERS)
# retries = Retry(
#     total=5,
#     backoff_factor=0.6,
#     status_forcelist=(429, 500, 502, 503, 504),
#     allowed_methods=["GET"]
# )
# SESSION.mount("https://", HTTPAdapter(max_retries=retries))
# SESSION.mount("http://", HTTPAdapter(max_retries=retries))

# # Firebase
# cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
# try:
#     firebase_admin.get_app()
# except ValueError:
#     if TRIAL_MODE:
#         firebase_admin.initialize_app(cred)
#     else:
#         firebase_admin.initialize_app(cred, {"storageBucket": STORAGE_BUCKET})

# db = firestore.client()
# bucket = None
# if not TRIAL_MODE:
#     bucket = storage.bucket()

# # Local folder
# if TRIAL_MODE:
#     os.makedirs(LOCAL_DOWNLOAD_PATH, exist_ok=True)
#     print(f"📁 Local download folder: {LOCAL_DOWNLOAD_PATH}\n")

# # ───── HELPERS ────────────────────────────────────────────────────────────────

# ALLOWED_LICENSE_KEYS = {"cc-by", "cc-by-sa", "publicdomain", "pd", "cc0"}

# def ascii_fallback(text: str) -> str:
#     """Normalize string by stripping diacritics → ASCII."""
#     if not text:
#         return text
#     return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")

# def license_ok(extmeta: Dict) -> Tuple[bool, str, str]:
#     """Check license from Commons extmetadata."""
#     lic_key = (extmeta.get("License", {}) or {}).get("value", "").lower()  # e.g., "cc-by-sa-4.0"
#     short = (extmeta.get("LicenseShortName", {}) or {}).get("value", "")
#     url = (extmeta.get("LicenseUrl", {}) or {}).get("value", "")
#     # Simple allowlist check
#     ok = any(k in lic_key for k in ALLOWED_LICENSE_KEYS)
#     return ok, short or lic_key or "Unknown", url or ""

# def write_log(row: Dict):
#     exists = os.path.exists(LOG_PATH)
#     with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
#         w = csv.DictWriter(f, fieldnames=[
#             "ts","location_id","location_name","place_id","place_name",
#             "status","images_found","images_added","message"
#         ])
#         if not exists:
#             w.writeheader()
#         w.writerow(row)

# def load_checkpoint() -> set:
#     if os.path.exists(CHECKPOINT_PATH):
#         try:
#             with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
#                 return set(json.load(f))
#         except Exception:
#             return set()
#     return set()

# def save_checkpoint(done: set):
#     with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
#         json.dump(sorted(list(done)), f, ensure_ascii=False, indent=2)

# # ───── COMMONS SEARCH ─────────────────────────────────────────────────────────

# def _pack_pages_to_results(pages: Dict) -> List[Dict]:
#     """Convert Commons 'pages' dict to filtered list of image records."""
#     out: List[Dict] = []
#     for _, page in (pages or {}).items():
#         infos = page.get("imageinfo") or []
#         if not infos:
#             continue
#         ii = infos[0]
#         w, h = ii.get("width", 0), ii.get("height", 0)
#         if w < MIN_IMAGE_WIDTH or h < MIN_IMAGE_HEIGHT:
#             continue
#         extmeta = ii.get("extmetadata", {}) or {}
#         ok, lic_short, lic_url = license_ok(extmeta)
#         if not ok:
#             continue

#         title = page.get("title", "")
#         file_page = f"https://commons.wikimedia.org/wiki/{quote(title)}" if title else ""

#         out.append({
#             "url": ii.get("url"),
#             "thumb_url": ii.get("thumburl"),
#             "width": w,
#             "height": h,
#             "mime": ii.get("mime", ""),
#             "title": title,
#             "license": lic_short,
#             "license_url": lic_url,
#             "artist": (extmeta.get("Artist", {}) or {}).get("value", ""),
#             "file_page": file_page,
#             "sha1": ii.get("sha1", "")
#         })
#     return out

# def search_wikimedia_images(query: str, location: str = "") -> List[Dict]:
#     """
#     Two-pass search limited to File namespace (6):
#       A) generator=search
#       B) list=search -> pageids -> imageinfo
#     Also tries ASCII-normalized variants to improve recall.
#     """
#     api = "https://commons.wikimedia.org/w/api.php"

#     # candidate search strings
#     base = f"{query}".strip()
#     loc  = f"{query} {location}".strip() if location else None
#     base_ascii = ascii_fallback(base)
#     loc_ascii  = ascii_fallback(loc) if loc else None
#     candidates = [c for c in [base, loc, base_ascii, loc_ascii] if c]

#     collected: List[Dict] = []
#     seen_sha1, seen_url = set(), set()

#     for s in candidates:
#         # ---- Pass A: generator=search (namespace=6)
#         params_a = {
#             "action": "query",
#             "format": "json",
#             "generator": "search",
#             "gsrsearch": f"filetype:bitmap {s}",
#             "gsrlimit": 50,
#             "gsrnamespace": 6,
#             "prop": "imageinfo",
#             "iiprop": "url|size|mime|sha1|extmetadata",
#             "iiurlwidth": 1600,
#             "origin": "*",
#         }
#         try:
#             r = SESSION.get(api, params=params_a, timeout=15)
#             if r.status_code == 200:
#                 data = r.json()
#                 pages = (data.get("query", {}) or {}).get("pages", {})
#                 results = _pack_pages_to_results(pages)
#                 # dedupe as we collect
#                 for item in sorted(results, key=lambda x: x["width"] * x["height"], reverse=True):
#                     s1, u = item.get("sha1"), item.get("url")
#                     if s1 and s1 in seen_sha1:
#                         continue
#                     if (not s1) and u and u in seen_url:
#                         continue
#                     if s1: seen_sha1.add(s1)
#                     if u:  seen_url.add(u)
#                     collected.append(item)
#                 if collected:
#                     break
#         except Exception as e:
#             print(f"   ⚠️  Commons search A error for '{s}': {e}")
#         time.sleep(API_DELAY)

#         # ---- Pass B: list=search then pageids→imageinfo
#         try:
#             params_b1 = {
#                 "action": "query",
#                 "format": "json",
#                 "list": "search",
#                 "srsearch": f"filetype:bitmap {s}",
#                 "srnamespace": 6,
#                 "srlimit": 50,
#                 "origin": "*",
#             }
#             rb1 = SESSION.get(api, params=params_b1, timeout=15)
#             if rb1.status_code != 200:
#                 time.sleep(API_DELAY)
#                 continue
#             data_b1 = rb1.json()
#             hits = data_b1.get("query", {}).get("search", []) or []
#             if not hits:
#                 time.sleep(API_DELAY)
#                 continue

#             pageids = "|".join(str(h["pageid"]) for h in hits[:50])
#             params_b2 = {
#                 "action": "query",
#                 "format": "json",
#                 "pageids": pageids,
#                 "prop": "imageinfo",
#                 "iiprop": "url|size|mime|sha1|extmetadata",
#                 "iiurlwidth": 1600,
#                 "origin": "*",
#             }
#             rb2 = SESSION.get(api, params=params_b2, timeout=15)
#             if rb2.status_code != 200:
#                 time.sleep(API_DELAY)
#                 continue
#             data_b2 = rb2.json()
#             pages = (data_b2.get("query", {}) or {}).get("pages", {})
#             results = _pack_pages_to_results(pages)
#             for item in sorted(results, key=lambda x: x["width"] * x["height"], reverse=True):
#                 s1, u = item.get("sha1"), item.get("url")
#                 if s1 and s1 in seen_sha1:
#                     continue
#                 if (not s1) and u and u in seen_url:
#                     continue
#                 if s1: seen_sha1.add(s1)
#                 if u:  seen_url.add(u)
#                 collected.append(item)
#             if collected:
#                 break
#         except Exception as e:
#             print(f"   ⚠️  Commons search B error for '{s}': {e}")
#         time.sleep(API_DELAY)

#     # Keep top N by area
#     collected.sort(key=lambda x: x["width"] * x["height"], reverse=True)
#     return collected[:MAX_IMAGES_PER_PLACE]

# def alternative_search_wikidata(name: str, location: str) -> List[Dict]:
#     """
#     Fallback via Wikidata P18 (Image). Converts filenames to Commons file paths.
#     Note: this does not fetch extmetadata; treat as last resort.
#     """
#     try:
#         # 1) Search entity
#         api = "https://www.wikidata.org/w/api.php"
#         params = {
#             "action": "wbsearchentities",
#             "format": "json",
#             "language": "en",
#             "search": f"{name} {location}",
#             "limit": 1
#         }
#         r = SESSION.get(api, params=params, timeout=15)
#         if r.status_code != 200:
#             return []
#         data = r.json()
#         if not data.get("search"):
#             return []

#         entity_id = data["search"][0]["id"]

#         # 2) Get P18 claims
#         params2 = {
#             "action": "wbgetclaims",
#             "format": "json",
#             "entity": entity_id,
#             "property": "P18"
#         }
#         r2 = SESSION.get(api, params=params2, timeout=15)
#         if r2.status_code != 200:
#             return []
#         data2 = r2.json()
#         if "claims" not in data2 or "P18" not in data2["claims"]:
#             return []

#         out = []
#         for claim in data2["claims"]["P18"]:
#             filename = claim["mainsnak"]["datavalue"]["value"]
#             commons_url = f"https://commons.wikimedia.org/wiki/Special:FilePath/{quote(filename)}"
#             out.append({
#                 "url": commons_url,
#                 "thumb_url": f"{commons_url}?width=1200",
#                 "width": 2000,  # unknown
#                 "height": 1500,
#                 "mime": "image/jpeg",
#                 "title": f"File:{filename}",
#                 "license": "Wikimedia Commons",
#                 "license_url": "",
#                 "artist": "",
#                 "file_page": f"https://commons.wikimedia.org/wiki/File:{quote(filename)}",
#                 "sha1": ""
#             })
#         time.sleep(API_DELAY)
#         return out[:MAX_IMAGES_PER_PLACE]
#     except Exception as e:
#         print(f"   ⚠️  Wikidata search error: {e}")
#         return []

# # ───── IMAGE IO ───────────────────────────────────────────────────────────────

# def download_image_locally(img: Dict, place_name: str, location_name: str, index: int) -> Optional[str]:
#     """Download to local folder with metadata sidecar."""
#     url = img.get("thumb_url") or img.get("url")
#     if not url: return None
#     try:
#         safe_loc = "".join(c for c in location_name if c.isalnum() or c in (" ", "-", "_")).strip()
#         safe_place = "".join(c for c in place_name if c.isalnum() or c in (" ", "-", "_")).strip()
#         place_folder = os.path.join(LOCAL_DOWNLOAD_PATH, safe_loc, safe_place)
#         os.makedirs(place_folder, exist_ok=True)

#         resp = SESSION.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
#         resp.raise_for_status()

#         mime = img.get("mime", "image/jpeg")
#         ext = "jpg" if ("jpeg" in mime or mime == "image/jpg") else mime.split("/")[-1]
#         filename = f"wikimedia_{index:02d}.{ext}"
#         filepath = os.path.join(place_folder, filename)

#         with open(filepath, "wb") as f:
#             for chunk in resp.iter_content(chunk_size=8192):
#                 f.write(chunk)

#         # metadata sidecar
#         meta_file = os.path.join(place_folder, f"wikimedia_{index:02d}_info.txt")
#         with open(meta_file, "w", encoding="utf-8") as f:
#             f.write(f"Title: {img.get('title','Unknown')}\n")
#             f.write(f"Size: {img.get('width')}x{img.get('height')}\n")
#             f.write(f"License: {img.get('license','Unknown')} ({img.get('license_url','')})\n")
#             f.write(f"Artist: {img.get('artist','')}\n")
#             f.write(f"File page: {img.get('file_page','')}\n")
#             f.write(f"Source URL: {img.get('url')}\n")
#             f.write(f"SHA1: {img.get('sha1','')}\n")

#         return filepath
#     except Exception as e:
#         print(f"   ⚠️  Download failed for {url}: {e}")
#         return None

# def download_and_upload_wikimedia_image(img: Dict, place_id: str, index: int) -> Optional[Dict]:
#     """Download then upload to Firebase Storage; return asset dict with attribution."""
#     url = img.get("thumb_url") or img.get("url")
#     if not url or not bucket:
#         return None
#     try:
#         resp = SESSION.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
#         resp.raise_for_status()
#         image_data = resp.content

#         mime = img.get("mime", "image/jpeg")
#         ext = "jpg" if ("jpeg" in mime or mime == "image/jpg") else mime.split("/")[-1]
#         filename = f"lp_attractions/{place_id}_wikimedia_{index}.{ext}"

#         blob = bucket.blob(filename)
#         blob.upload_from_string(image_data, content_type=mime or "image/jpeg")
#         blob.make_public()

#         return {
#             "url": blob.public_url,
#             "sha1": img.get("sha1",""),
#             "title": img.get("title",""),
#             "artist": img.get("artist",""),
#             "license": img.get("license",""),
#             "license_url": img.get("license_url",""),
#             "file_page": img.get("file_page",""),
#             "source_url": img.get("url","")
#         }
#     except Exception as e:
#         print(f"   ⚠️  Download/upload failed for {url}: {e}")
#         return None

# # ───── PIPELINE ───────────────────────────────────────────────────────────────

# def process_attraction(doc, location_name: str) -> Tuple[bool, int, str]:
#     """
#     Process a single attraction document.
#     Returns (success, images_added_or_downloaded, message)
#     """
#     data = doc.to_dict() or {}
#     name = data.get("name", "Unknown")
#     place_id = data.get("placeId") or doc.id

#     # In production, skip if already has enough images
#     if not TRIAL_MODE:
#         current_images = data.get("g_image_urls", []) or []
#         if len(current_images) >= SKIP_IF_HAS_IMAGES:
#             return (False, 0, f"Already has {len(current_images)} images")

#     print(f"   🔍 Searching Wikimedia for images...")
#     imgs = search_wikimedia_images(name, location_name)
#     if not imgs:
#         # Fallback via Wikidata (less strict)
#         imgs = alternative_search_wikidata(name, location_name)

#     if not imgs:
#         return (False, 0, "No suitable images found on Wikimedia")

#     print(f"   📸 Found {len(imgs)} potential images")

#     new_items = []
#     for idx, img in enumerate(imgs):
#         print(f"   ⬇️  Downloading image {idx+1}/{len(imgs)}...")
#         if TRIAL_MODE:
#             p = download_image_locally(img, name, location_name, idx)
#             if p:
#                 new_items.append(p)
#                 print(f"      ✅ Saved: {os.path.basename(p)}")
#         else:
#             asset = download_and_upload_wikimedia_image(img, place_id, idx)
#             if asset:
#                 new_items.append(asset)
#                 print(f"      ✅ Uploaded: {img.get('title','image')[:60]}")

#     if not new_items:
#         return (False, 0, "Failed to download any images")

#     if TRIAL_MODE:
#         return (True, len(new_items), f"Downloaded {len(new_items)} image(s) to local folder")

#     # ── Production: update Firestore cleanly with metadata and g_image_urls
#     current_images = data.get("g_image_urls", []) or []
#     existing_assets = data.get("wikimedia_assets", []) or []
#     existing_sha1 = {a.get("sha1") for a in existing_assets if a.get("sha1")}

#     # Filter out duplicates by sha1
#     filtered_assets = []
#     for a in new_items:
#         s1 = a.get("sha1")
#         if s1 and s1 in existing_sha1:
#             continue
#         filtered_assets.append(a)

#     if not filtered_assets:
#         return (False, 0, "All candidate images already exist (by SHA1)")

#     # Merge arrays and update (avoid ArrayUnion on objects; do client-side merge)
#     merged_assets = existing_assets + filtered_assets
#     new_urls = [a["url"] for a in filtered_assets if a.get("url")]
#     updated_g_images = current_images + new_urls

#     if not DRY_RUN:
#         doc.reference.update({
#             "g_image_urls": updated_g_images,
#             "image_url": updated_g_images[0] if updated_g_images else data.get("image_url"),
#             "wikimedia_assets": merged_assets,
#             "wikimedia_images_added": len(filtered_assets),
#             "images_updated_at": firestore.SERVER_TIMESTAMP
#         })

#     return (True, len(filtered_assets), f"Added {len(filtered_assets)} Wikimedia image(s)")

# def process_location(location_id: str) -> Tuple[int, int, int]:
#     """
#     Process all attractions under a location:
#     Returns (total, succeeded, failed/skipped)
#     """
#     location_name = LOCATION_NAMES.get(location_id, f"Location_{location_id}")

#     print(f"\n{'='*70}")
#     print(f"📍 Processing: {location_name} (ID: {location_id})")
#     print(f"{'='*70}\n")

#     coll = db.collection("allplaces").document(location_id).collection(SUBCOLLECTION_NAME)
#     docs = list(coll.stream())

#     if not docs:
#         print("   ⚠️  No attractions found\n")
#         return (0, 0, 0)

#     total = len(docs)
#     succeeded = 0
#     failed = 0

#     checkpoint_done = load_checkpoint()

#     from datetime import datetime
#     for idx, doc in enumerate(docs, 1):
#         if doc.id in checkpoint_done:
#             continue

#         data = doc.to_dict() or {}
#         name = data.get("name", "Unknown")
#         print(f"\n[{idx}/{total}] {name}")

#         success, count, message = process_attraction(doc, location_name)
#         status = "ok" if success else "skip"
#         if success:
#             succeeded += 1
#         else:
#             failed += 1

#         write_log({
#             "ts": datetime.utcnow().isoformat(),
#             "location_id": location_id,
#             "location_name": location_name,
#             "place_id": doc.id,
#             "place_name": name,
#             "status": status,
#             "images_found": count,
#             "images_added": count if (success and not TRIAL_MODE and not DRY_RUN) else 0,
#             "message": message
#         })

#         checkpoint_done.add(doc.id)
#         if idx % 20 == 0:
#             save_checkpoint(checkpoint_done)

#     save_checkpoint(checkpoint_done)

#     # Summary
#     print(f"\n{'─'*70}")
#     print(f"Summary for {location_name}:")
#     print(f"  Total attractions: {total}")
#     print(f"  Successfully processed: {succeeded}")
#     print(f"  Skipped/Failed: {failed}")
#     print(f"{'─'*70}")

#     return (total, succeeded, failed)

# # ───── REVIEW HTML (trial mode) ───────────────────────────────────────────────

# def generate_review_html(location_name: str):
#     """Generate an HTML file to review downloaded images."""
#     html_path = os.path.join(LOCAL_DOWNLOAD_PATH, f"{location_name}_review.html")

#     html_content = f"""<!DOCTYPE html>
# <html>
# <head>
# <meta charset="UTF-8">
# <title>Wikimedia Images Review - {location_name}</title>
# <style>
# body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
# h1 {{ color: #333; }}
# .place {{
#   background: white; margin: 20px 0; padding: 20px;
#   border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
# }}
# .place h2 {{ color: #2196F3; margin-top: 0; }}
# .images {{
#   display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
#   gap: 15px; margin-top: 15px;
# }}
# .image-container {{
#   border: 1px solid #ddd; padding: 10px; background: #fafafa; border-radius: 4px;
# }}
# .image-container img {{
#   width: 100%; height: 200px; object-fit: cover; border-radius: 4px;
# }}
# .image-info {{
#   font-size: 11px; color: #666; margin-top: 5px; word-break: break-all;
# }}
# .stats {{
#   background: #e3f2fd; padding: 10px; border-radius: 4px; margin-bottom: 20px;
# }}
# </style>
# </head>
# <body>
#   <h1>📸 Wikimedia Images Review - {location_name}</h1>
#   <div class="stats">
#     <strong>Instructions:</strong> Review the images below. Delete poor quality images from the folders before uploading to Firebase.
#   </div>
# """

#     # Walk downloaded folders
#     loc_folder = os.path.join(LOCAL_DOWNLOAD_PATH, location_name)
#     if os.path.exists(loc_folder):
#         for place_name in sorted(os.listdir(loc_folder)):
#             place_path = os.path.join(loc_folder, place_name)
#             if not os.path.isdir(place_path):
#                 continue

#             images = [f for f in os.listdir(place_path) if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif'))]
#             if not images:
#                 continue

#             html_content += f'<div class="place"><h2>{place_name}</h2><div class="images">'
#             for img_file in sorted(images):
#                 img_path = os.path.join(place_path, img_file)
#                 rel_path = os.path.relpath(img_path, LOCAL_DOWNLOAD_PATH)

#                 info_file = img_path.rsplit('.', 1)[0] + '_info.txt'
#                 metadata = ""
#                 if os.path.exists(info_file):
#                     with open(info_file, 'r', encoding='utf-8') as f:
#                         metadata = f.read()

#                 html_content += f'''
#                 <div class="image-container">
#                   <img src="{rel_path}" alt="{img_file}">
#                   <div class="image-info">
#                     <strong>{img_file}</strong><br>
#                     <pre style="font-size: 10px; margin: 5px 0;">{metadata}</pre>
#                   </div>
#                 </div>
#                 '''
#             html_content += '</div></div>'

#     html_content += """
# </body>
# </html>
# """
#     with open(html_path, 'w', encoding='utf-8') as f:
#         f.write(html_content)
#     return html_path

# # ───── MAIN ───────────────────────────────────────────────────────────────────

# def main():
#     mode_desc = "TRIAL MODE (downloading locally)" if TRIAL_MODE else "PRODUCTION MODE"
#     print("📸 Wikimedia Image Fetcher for Top Attractions")
#     print(f"Mode: {mode_desc}")

#     if TRIAL_MODE:
#         print(f"Download location: {LOCAL_DOWNLOAD_PATH}")
#     else:
#         print(f"Firebase mode: {'DRY RUN (preview)' if DRY_RUN else 'LIVE (will update)'}")

#     print("\nSettings:")
#     print(f"  Max images per place: {MAX_IMAGES_PER_PLACE}")
#     print(f"  Min image size: {MIN_IMAGE_WIDTH}x{MIN_IMAGE_HEIGHT}")
#     if not TRIAL_MODE:
#         print(f"  Skip if has >= {SKIP_IF_HAS_IMAGES} images")
#     print()

#     if not TRIAL_MODE and not DRY_RUN:
#         confirm = input("⚠️  This will UPDATE Firestore and upload images. Continue? (yes/no): ").strip().lower()
#         if confirm != "yes":
#             print("Cancelled.")
#             return

#     location_ids = ONLY_LOCATIONS or list(LOCATION_NAMES.keys())
#     total_all = succeeded_all = failed_all = 0

#     for loc_id in location_ids:
#         if loc_id not in LOCATION_NAMES:
#             print(f"⚠️  Unknown location id {loc_id}")
#             continue

#         total, ok, bad = process_location(loc_id)
#         total_all += total
#         succeeded_all += ok
#         failed_all += bad

#         if TRIAL_MODE:
#             loc_name = LOCATION_NAMES[loc_id]
#             html_path = generate_review_html(loc_name)
#             print(f"\n📄 Review HTML generated: {html_path}")
#             print("   Open this file in your browser to review all images!")

#     print(f"\n{'='*70}")
#     print("🎉 Wikimedia Image Fetch Complete!")
#     print(f"{'='*70}")
#     print(f"Total attractions processed: {total_all}")
#     print(f"Successfully processed: {succeeded_all}")
#     print(f"Skipped/Failed: {failed_all}")

#     if TRIAL_MODE:
#         print(f"\n📁 All images saved to: {LOCAL_DOWNLOAD_PATH}")
#         print("   Review the images, delete any poor quality ones,")
#         print("   then set TRIAL_MODE = False to upload to Firebase")
#     print(f"{'='*70}")

# if __name__ == "__main__":
#     main()
