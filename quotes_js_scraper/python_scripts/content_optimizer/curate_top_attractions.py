
# """
# Enhanced Top Attractions Curation Script
# Combines best features from both versions:
# - Multi-provider LLM support (OpenAI + Anthropic)
# - Intelligent scoring fusion (heuristics + LLM)
# - Non-destructive curation mode + optional deletion
# - JSON response parsing for reliability
# - Incremental processing with caching
# - Comprehensive logging and reporting
# """

# import os
# import re
# import csv
# import json
# import hashlib
# import time
# import html
# import unicodedata
# from typing import Dict, Optional, List, Tuple
# from datetime import datetime, timezone
# from enum import Enum

# import firebase_admin
# from firebase_admin import credentials, firestore

# # ═══════════════════════════════════════════════════════════════════════════════
# # CONFIGURATION
# # ═══════════════════════════════════════════════════════════════════════════════

# SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# # AI Provider Selection
# class AIProvider(Enum):
#     OPENAI = "openai"
#     ANTHROPIC = "anthropic"

# AI_PROVIDER = AIProvider.OPENAI  # Change to AIProvider.ANTHROPIC if needed

# # API Configuration
# OPENAI_API_KEY = "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"
# ANTHROPIC_API_KEY = "your-anthropic-key"
# OPENAI_MODEL = "gpt-4.1-mini"
# ANTHROPIC_MODEL = "claude-sonnet-4-20250514"

# # Country/City mapping
# COUNTRY_NAMES = {  
#     # "131175": "Itacare",
#     # "58309": "Hilo",
#     # "9827": "Cannes",
#     # "79308": "Casablanca",
#     # "10134": "Burgos",
#     # "131145": "Blumenau",
#     # "60831": "Hammond",
#     # "59140": "Poipu",
#     # "147376": "Saint Augustine Beach",
#     # "116": "Nikko",
#     # "12573": "Pamukkale",
#     # "58370": "Rapid City",
#     # "58368": "Carlsbad",
#     # "78760": "Manuel Antonio",
#     # "188": "Cebu City",
#     # "9701": "Nizhny Novgorod",
#     # "292": "Vientiane",
#     # "10979": "Merida",
#     # "10101": "Puerto Del Carmen",
#     # "58314": "Hot Springs",
#     # "10698": "Oia",
#     # "121": "Kamakura",
#     # "10058": "Potsdam",
#     # "85945": "Beirut",
#     # "58949": "Hershey",
#     # "10696": "Bonifacio",
#     # "584": "Kanchanaburi",
#     # "58053": "Winnipeg",
#     # "82615": "Taupo",
#     # "131332": "Vila Velha",
#     # "10229": "Nimes",
#     # "12927": "Oswiecim",
#     # "13458": "Akrotiri",
#     # "9956": "Mykonos Town",
#     # "58299": "Fort Myers Beach",
#     # "58214": "Sacramento"
#     # "1163": "Jalandhar", 

#     # "40": "Chennai (Madras)",
#     # "1983": "Dwarka",
#     # "993": "Bhuj",
#     # "1025": "Ujjain",
#     # "1365": "Bodh Gaya",
#     # "2791": "Lansdowne",
#     # "3543": "Bharatpur",
#     # "1529": "Chittaurgarh",
#     # "2178": "Panchgani",
#     # "2149": "Arambol",
#     # "830": "Dehradun",
#     # "1863": "Kullu",
#     # "2761": "Ganpatipule",
#     # "2397": "Kasauli",
#     # "3684": "Kandaghat Tehsil",
#     # "1673": "Palampur",
#     #    "1": "Tokyo",
#     # "10": "Shanghai",
#     # "1000": "Udupi",
#     # "10008": "Lucerne",
#     # "10015": "Ronda",
#     # "1002": "Mathura",
#     # "10021": "Corfu Town",
#     # "10024": "Sitges",
#     # "10025": "Tarragona",
#     # "10030": "Bursa",
#     # "10031": "Lloret de Mar",
#     # "10033": "Valletta",
#     # "10046": "Ravenna",
#     # "1006": "Gulmarg",
#     # "10061": "Versailles",
#     # "10062": "Bonn",
#     # "10064": "Weymouth",
#     # "10074": "Agrigento",
#     # "10086": "Augsburg",
#     # "10095": "Koblenz",
#     # "10106": "Torremolinos",
#     # "10114": "Mannheim",
#     # "10123": "Puerto de la Cruz",
#     # "1013": "Imphal",
#     # "1014": "Trincomalee",
#     # "10143": "Torquay",
#     # "1015": "Kolhapur",
#     # "10171": "Nerja",
#     # "10176": "Salou",
#     # "10184": "Aachen",
#     # "10186": "Groningen",
#     # "10193": "Delft",
#     # "10211": "Vila Nova de Gaia",
#     # "10219": "Windsor",
#     # "10230": "Segovia",
#     # "10235": "Bled",
#     # "10241": "Playa Blanca",
#     # "1025": "Ujjain",
#     # "10260": "Selcuk",
#     # "10262": "Trogir",
#     # "10279": "Nijmegen",
#     # "10291": "Cuenca",
#     # "10366": "Akureyri",
#     # "10382": "Cremona",
#     # "10421": "Bamberg",
#     # "10453": "Konstanz",
#     # "105": "Macau",
#     # "10533": "Ulm",
#     # "1055": "Mahabalipuram",
#     # "10590": "Savona",
#     # "10595": "Peterhof",
#     # "1060": "Ludhiana",
#     # "10792": "Monte-Carlo",
#     # "10804": "Ceuta",
#     # "10876": "Uppsala",
#     # "10884": "Tropea",
#     # "1089": "Bikaner",
#     # "11": "Siem Reap",
#     # "110": "Hakodate",
#     # "11061": "Lund",
#     # "1131": "Sukhothai",
#     # "1135": "Kanpur",
#     # "1138": "Sawai Madhopur",
#     # "11423": "Grindelwald",
#     # "11489": "Mont-Saint-Michel",
#     # "1159": "Bundi",
#     # "1160": "Sandakan",
#     # "1163": "Jalandhar",
#     # "11655": "Berchtesgaden",
#     # "1168": "Hat Yai",
#     # "11706": "Trujillo",
#     # "11769": "Lindos",
#     # "1177": "Ranchi",
#     # "118": "Kandy",
#     # "1192": "Chonburi",
#     # "12": "Phuket",
#     # "121": "Kamakura",
#     # "1212": "Vijayawada",
#     # "1218": "Kota",
#     # "122": "Varanasi",
#     # "1222": "Kalpetta",
#     # "1241": "Alwar",
#     # "12491": "Mdina",
#     # "12525": "Stretford",
#     # "128": "Hua Hin",
#     # "129": "Incheon",
#     # "1291": "Gwalior",
#     # "13": "New Delhi",
#     # "1302": "Kumbakonam",
#     # "131071": "Buenos Aires",
#     # "131072": "Rio de Janeiro",
#     # "131073": "Sao Paulo",
#     # "131074": "Cusco",
#     # "131075": "Santiago",
#     # "131076": "Lima",
#     # "131077": "Bogota",
#     # "131078": "Quito",
#     # "131079": "Medellin",
#     # "131080": "Cartagena",
#     # "131081": "Porto Alegre",
#     # "131083": "Mendoza",
#     # "131084": "Montevideo",
#     # "131085": "Salvador",
#     # "131086": "Florianopolis",
#     # "131087": "Brasilia",
#     # "131088": "Belo Horizonte",
#     # "131089": "Recife",
#     # "131090": "San Carlos de Bariloche",
#     # "131091": "Fortaleza",
#     # "131092": "Manaus",
#     # "131093": "Angra Dos Reis",
#     # "131094": "Paraty",
#     # "131095": "La Paz",
#     # "131098": "Valparaiso",
#     # "131099": "Arequipa",
#     # "131100": "Natal",
#     # "131102": "Guayaquil",
#     # "131103": "Gramado",
#     # "131104": "San Pedro de Atacama",
#     # "131105": "Salta",
#     # "131106": "Campinas",
#     # "131107": "Santa Marta",
#     # "131108": "Ubatuba",
#     # "131109": "Joao Pessoa",
#     # "131110": "Mar del Plata",
#     # "131113": "Rosario",
#     # "131114": "Belem",
#     # "131115": "Maceio",
#     # "131117": "Ouro Preto",
#     # "131118": "Porto Seguro",
#     # "131119": "Ushuaia",
#     # "131122": "Santos",
#     # "131125": "Niteroi",
#     # "131127": "Vitoria",
#     # "131131": "Sao Luis",
#     # "131132": "Petropolis",
#     # "131134": "Jijoca de Jericoacoara",
#     # "131138": "Puerto Varas",
#     # "131139": "El Calafate",
#     # "131140": "Puno",
#     # "131146": "Cabo Frio",
#     # "131147": "Aracaju",
#     # "131150": "Ipojuca",
#     # "131151": "Campos Do Jordao",
#     # "131157": "Canela",
#     # "131162": "Punta del Este",
#     # "131164": "Puerto Iguazu",
#     # "131168": "San Juan",
#     # "131172": "Fernando de Noronha",
#     # "131174": "San Andres Island",
#     # "131177": "Tiradentes",
#     # "131178": "Pocos de Caldas",
#     # "131180": "Puerto Ayora",
#     # "1312": "Kasaragod",
#     # "131315": "Bonito",
#     # "131318": "Machu Picchu",
#     # "131327": "Bombinhas",
#     # "131337": "Guaruja",
#     # "131360": "Maragogi",
#     # "131362": "Cafayate",
#     # "131389": "San Andres",
#     # "131395": "Praia da Pipa",
#     # "131438": "Morro de Sao Paulo",
#     # "131447": "Caldas Novas",
#     # "131457": "Casablanca",
#     # "131478": "Mata de Sao Joao",
#     # "1316": "Shirdi",
#     # "131626": "Penha",
#     # "161": "Ahmedabad",
#     # "1765": "Almora",
#     # "2601": "Amphawa",
#     # "1868": "Baga",
#     # "228": "Beppu",
#     # "19": "Chiang Mai",
#     # "257": "Chiang Rai",
#     # "1887": "Coonoor",
#     # "174": "Da Lat",
#     # "1784": "Dalhousie",
#     # "1808": "Daman",
#     # "2121": "Deoghar",
#     # "283": "Dhaka City",
#     # "1743": "Diu",
#     # "1983": "Dwarka",
#     # "2177": "Gandhinagar",
#     # "1729": "Gaya",
#     # "2371": "Gokarna",
#     # "207": "Gurugram (Gurgaon)",
#     # "1668": "Hassan",
#     # "16": "Ho Chi Minh City",
#     # "21": "Hoi An",
#     # "2083": "Howrah",
#     # "247": "Ise",
#     # "24": "Jaipur",
#     # "183": "Jaisalmer",
#     # "1763": "Jamshedpur",
#     # "288": "Karachi",
#     # "176": "Karon",
#     # "17": "Kathmandu",
#     # "2141": "Kohima",
#     # "1966": "Kovalam",
#     # "223": "Kuching",
#     # "2540": "Kumarakom",
#     # "2": "Kyoto",
#     # "235": "Lahore",
#     # "297": "Leh",
#     # "248": "Lhasa",
#     # "184": "Luang Prabang",
#     # "1886": "Madikeri",
#     # "175": "Manila",
#     # "1699": "Margao",
#     # "187": "Melaka",
#     # "23": "Minato",
#     # "25": "Mumbai",
#     # "2027": "Orchha",
#     # "2253": "Pahalgam",
#     # "287": "Panjim",
#     # "194": "Rishikesh",
#     # "221": "Sapa",
#     # "29": "Sapporo",
#     # "285": "Seogwipo",
#     # "1942": "Shimoga",
#     # "256": "Srinagar",
#     # "18": "Taipei",
#     # "28": "Taito",
#     # "209": "Thimphu",
#     # "200": "Thiruvananthapuram (Trivandrum)",
#     # "1997": "Tiruvannamalai",
#     # "22": "Ubud",
#     # "1659": "Vasco da Gama",
#     # "1874": "Vrindavan",
#     # "20": "Yokohama",
#     # "60": "Agra",
#     # "58191": "Albuquerque",
#     # "58242": "Anaheim",
#     # "58198": "Anchorage",
#     # "58342": "Arlington",
#     # "58185": "Asheville",
#     # "58455": "Athens",
#     # "58169": "Atlanta",
#     # "607": "Aurangabad",
#     # "58163": "Austin",
#     # "58179": "Baltimore",
#     # "58068": "Banff",
#     # "570": "Batu",
#     # "6": "Beijing",
#     # "58162": "Boston",
#     # "58183": "Branson",
#     # "58164": "Brooklyn",
#     # "61": "Busan",
#     # "581": "Calangute",
#     # "58048": "Calgary",
#     # "58170": "Charleston",
#     # "58193": "Charlotte",
#     # "58226": "Chattanooga",
#     # "67": "Chengdu",
#     # "58146": "Chicago",
#     # "58201": "Cincinnati",
#     # "58231": "Clearwater",
#     # "58210": "Cleveland",
#     # "58563": "Columbus",
#     # "58173": "Dallas",
#     # "58248": "Daytona Beach",
#     # "58166": "Denver",
#     # "58218": "Detroit",
#     # "57258": "Dublin",
#     # "58052": "Edmonton",
#     # "660": "Ella",
#     # "58300": "Flagstaff",
#     # "58177": "Fort Lauderdale",
#     # "58211": "Fort Myers",
#     # "58212": "Fort Worth",
#     # "58284": "Fredericksburg",
#     # "58244": "Galveston",
#     # "503": "Gangtok",
#     # "58286": "Gettysburg",
#     # "58182": "Greater Palm Springs",
#     # "524": "Guwahati",
#     # "58059": "Halifax",
#     # "696": "Hampi",
#     # "64": "Hangzhou",
#     # "59": "Hiroshima",
#     # "58153": "Honolulu",
#     # "58161": "Houston",
#     # "58205": "Indianapolis",
#     # "558": "Indore",
#     # "58155": "Island of Hawaii",
#     # "62": "Kanazawa",
#     # "584": "Kanchanaburi",
#     # "58219": "Kansas City",
#     # "53": "Kathu",
#     # "58167": "Kauai",
#     # "58165": "Key West",
#     # "59165": "Keystone",
#     # "58067": "Kingston",
#     # "68": "Kochi (Cochin)",
#     # "69": "Kolkata (Calcutta)",
#     # "527": "Kozhikode",
#     # "58203": "Lahaina",
#     # "58148": "Las Vegas",
#     # "58145": "Los Angeles",
#     # "58196": "Louisville",
#     # "615": "Madurai",
#     # "58151": "Maui",
#     # "58224": "Memphis",
#     # "58157": "Miami",
#     # "58180": "Miami Beach",
#     # "58195": "Milwaukee",
#     # "58184": "Minneapolis",
#     # "58269": "Moab",
#     # "58276": "Monterey",
#     # "58046": "Montreal",
#     # "687": "Mussoorie",
#     # "58202": "Myrtle Beach",
#     # "50": "Naha",
#     # "58189": "Naples",
#     # "58171": "Nashville",
#     # "58156": "New Orleans",
#     # "58144": "New York City",
#     # "58058": "Niagara Falls",
#     # "563": "Noida",
#     # "58079": "North Vancouver",
#     # "58232": "Oklahoma City",
#     # "58213": "Omaha",
#     # "58152": "Orlando",
#     # "58049": "Ottawa",
#     # "58476": "Page",
#     # "673": "Patna",
#     # "58160": "Philadelphia",
#     # "58181": "Phoenix",
#     # "51": "Phuket Town",
#     # "58241": "Pigeon Forge",
#     # "58199": "Pittsburgh",
#     # "589": "Port Blair",
#     # "58158": "Portland",
#     # "57": "Pune",
#     # "58051": "Quebec City",
#     # "58222": "Richmond",
#     # "58175": "Saint Louis",
#     # "58289": "Salem",
#     # "58216": "Salt Lake City",
#     # "58168": "San Antonio",
#     # "58150": "San Diego",
#     # "58147": "San Francisco",
#     # "58206": "Santa Barbara",
#     # "58178": "Santa Fe",
#     # "58228": "Santa Monica",
#     # "58187": "Sarasota",
#     # "58174": "Savannah",
#     # "58192": "Scottsdale",
#     # "58154": "Seattle",
#     # "58188": "Sedona",
#     # "650": "Sentosa Island",
#     # "528": "Singaraja",
#     # "583": "Solo",
#     # "58176": "Tampa",
#     # "541": "Thane",
#     # "536": "Thrissur",
#     # "58748": "Titusville",
#     # "58077": "Tofino",
#     # "58045": "Toronto",
#     # "58172": "Tucson",
#     # "58047": "Vancouver",
#     # "58044": "Vancouver Island",
#     # "662": "Varkala Town",
#     # "58050": "Victoria",
#     # "518": "Visakhapatnam",
#     # "58159": "Washington DC",
#     # "58275": "Williamsburg",
#     # "58345": "Wisconsin Dells",
#     # "52": "Yangon (Rangoon)",
#     # "350": "Alappuzha",
#     # "384": "Amritsar",
#     # "468": "Ayutthaya",
#     # "4": "Bangkok",
#     # "329": "Bardez",
#     # "35": "Bengaluru",
#     # "485": "Bhopal",
#     # "444": "Bhubaneswar",
#     # "304": "Chandigarh",
#     # "42": "Chiyoda",
#     # "371": "Coimbatore",
#     # "43": "Colombo",
#     # "49": "Da Nang",
#     # "349": "Darjeeling",
#     # "405": "Dharamsala",
#     # "3129": "Digha",
#     # "31": "Fukuoka",
#     # "39": "Guangzhou",
#     # "465": "Gyeongju",
#     # "419": "Hikkaduwa",
#     # "376": "Ipoh",
#     # "46": "Jakarta",
#     # "461": "Kannur",
#     # "34": "Kobe",
#     # "33": "Kuala Lumpur",
#     # "37": "Kuta",
#     # "375": "Lucknow",
#     # "313": "Manali Tehsil",
#     # "479": "Mangalore",
#     # "382": "Medan",
#     # "412": "Munnar",
#     # "30": "Nagoya",
#     # "398": "Nagpur",
#     # "449": "Nashik",
#     # "472": "Navi Mumbai",
#     # "41": "New Taipei",
#     # "480": "Ooty (Udhagamandalam)",
#     # "44": "Phnom Penh",
#     # "334": "Pondicherry",
# #     "381": "Semarang",
# #     "38": "Shibuya",
# #     "428": "Shimla",
# #     "32": "Shinjuku",
# #     "478": "Surat",
# #     "330": "Tashkent",
# #     "348": "Vadodara",
# #     "79337": "Alexandria",
# #     "785": "Allahabad",
# #     "79302": "Cairo",
# #     "79300": "Cape Town Central",
# #     "787": "Chikmagalur",
# #     "79333": "Dahab",
# #     "79305": "Fes",
# #     "79321": "Giza",
# #     "783": "Haridwar",
# #     "78": "Hyderabad",
# #     "730": "Jamnagar",
# #     "79306": "Johannesburg",
# #     "714": "Kollam",
# #     "79": "Krabi Town",
# #     "78752": "La Fortuna de San Carlos",
# #     "76078": "Lake Louise",
# #     "701": "Lonavala",
# #     "717": "Male",
# #     "79299": "Marrakech",
# #     "79304": "Mauritius",
# #     "75": "Nagasaki",
# #     "79303": "Nairobi",
# #     "724": "Ninh Binh",
# #     "78744": "Panama City",
# #     "722": "Pushkar",
# #     "798": "Raipur",
# #     "78746": "San Jose",
# #     "79309": "Sharm El Sheikh",
# #     "728": "Shillong",
# #     "738": "Thanjavur",
# #     "753": "Thekkady",
# #     "74": "Xi'an",
# #     "73": "Yerevan",
# #     "85942": "Abu Dhabi",
# #     "82581": "Adelaide",
# #     "81941": "Akumal",
# #     "85962": "Al Ain",
# #     "88609": "Albania",
# #     "87236": "Arunachal Pradesh",
# #     "86937": "Assam",
# #     "82576": "Auckland",
# #     "88384": "Austria",
# #     "86729": "Azerbaijan",
# #     "82": "Baku",
# #     "86851": "Bangladesh",
# #     "88406": "Bavaria",
# #     "88503": "Belarus",
# #     "88408": "Belgium",
# #     "86819": "Bhutan",
# #     "82577": "Brisbane",
# #     "82614": "Broome",
# #     "86779": "Brunei Darussalam",
# #     "88449": "Bulgaria",
# #     "82584": "Cairns",
# #     "86659": "Cambodia",
# #     "814": "Canacona",
# #     "82585": "Canberra",
# #     "81904": "Cancun",
# #     "82579": "Christchurch",
# #     "81908": "Cozumel",
# #     "88438": "Croatia",
# #     "88527": "Cyprus",
# #     "88366": "Czech Republic",
# #     "82588": "Darwin",
# #     "88402": "Denmark",
# #     "85946": "Doha",
# #     "85939": "Dubai",
# #     "82594": "Dunedin",
# #     "88455": "Estonia",
# #     "88434": "Finland",
# #     "88358": "France",
# #     "86743": "Fujian",
# #     "88420": "Georgia",
# #     "88368": "Germany",
# #     "82578": "Gold Coast",
# #     "81187": "Grand Cayman",
# #     "88375": "Greece",
# #     "81912": "Guadalajara",
# #     "81924": "Guanajuato",
# #     "86689": "Guangdong",
# #     "81182": "Havana",
# #     "82586": "Hobart",
# #     "86676": "Hokkaido",
# #     "88380": "Hungary",
# #     "88419": "Iceland",
# #     "86661": "India",
# #     "88386": "Ireland",
# #     "88352": "Italy",
# #     "86647": "Japan",
# #     "85959": "Jeddah",
# #     "85941": "Jerusalem",
# #     "86722": "Jiangsu",
# #     "86686": "Karnataka",
# #     "86731": "Kazakhstan",
# #     "86714": "Kerala",
# #     "850": "Khajuraho",
# #     "842": "Kodaikanal",
# #     "849": "Kottayam",
# #     "86776": "Kyrgyzstan",
# #     "86797": "Laos",
# #     "88423": "Latvia",
# #     "88477": "Lithuania",
# #     "88723": "Luxembourg",
# #     "86675": "Maharashtra",
# #     "88407": "Malta",
# #     "85954": "Manama",
# #     "85998": "Mecca",
# #     "86988": "Meghalaya",
# #     "82575": "Melbourne",
# #     "81913": "Merida",
# #     "81903": "Mexico City",
# #     "88731": "Moldova",
# #     "86772": "Mongolia",
# #     "88654": "Montenegro",
# #     "877": "Mount Abu",
# #     "87411": "Nagaland",
# #     "895": "Nainital",
# #     "81194": "Nassau",
# #     "86667": "Nepal",
# #     "81190": "New Providence Island",
# #     "82608": "Newcastle",
# #     "87077": "North Korea",
# #     "88432": "Norway",
# #     "81911": "Oaxaca",
# #     "81213": "Ocho Rios",
# #     "86904": "Odisha",
# #     "86831": "Pakistan",
# #     "81981": "Palenque",
# #     "80": "Pattaya",
# #     "86652": "Philippines",
# #     "81905": "Playa del Carmen",
# #     "88403": "Poland",
# #     "81918": "Puebla",
# #     "81219": "Puerto Plata",
# #     "81906": "Puerto Vallarta",
# #     "86887": "Punjab",
# #     "81180": "Punta Cana",
# #     "86674": "Rajasthan",
# #     "894": "Rajkot",
# #     "82593": "Rotorua",
# #     "88360": "Russia",
# #     "81189": "San Juan",
# #     "81250": "Santiago de Cuba",
# #     "81197": "Santo Domingo",
# #     "88464": "Serbia",
# #     "86654": "Singapore",
# #     "88478": "Slovakia",
# #     "88486": "Slovenia",
# #     "86656": "South Korea",
# #     "88362": "Spain",
# #     "86693": "Sri Lanka",
# #     "85": "Suzhou",
# #     "88445": "Sweden",
# #     "88437": "Switzerland",
# #     "82574": "Sydney",
# #     "86668": "Taiwan",
# #     "86938": "Tajikistan",
# #     "86691": "Tamil Nadu",
# #     "82615": "Taupo",
# #     "85943": "Tehran",
# #     "85940": "Tel Aviv",
# #     "86651": "Thailand",
# #     "88373": "The Netherlands",
# #     "805": "Tiruchirappalli",
# #     "827": "Tirunelveli",
# #     "81909": "Tulum",
# #     "88367": "Turkiye",
# #     "88359": "United Kingdom",
# #     "86706": "Uttar Pradesh",
# #     "86805": "Uttarakhand",
# #     "81226": "Varadero",
# #     "86655": "Vietnam",
# #     "82582": "Wellington",
# #     "86716": "West Bengal",
# #     "81196": "Willemstad",
# #  "3": "Osaka",
# #  "5": "Luzon",
# #   "9": "Seoul",
# #   "8": "Hanoi",
# #   "7":"Singapore",
# #   "91":"Udaipur",  
# #   "92":"Hue",
# #   "98":"Bophut",
# #   "99":"Nara"

#     # "909": "Agartala",
#     # "146275":"Anjuna", 
#     # "2824":"Arpora",
#     # "1365": "Bodh Gaya",
#     # "1306":"Faridabad", 
#     # "923":"Ghaziabad",
#     # "1581":"Hubli-Dharwad",
#     #  "1059":"Idukki",
#     # "1635": "Jim Corbett National Park",


#     # "3684":"Kandaghat Tehsil",
#     # "146301":"Khandala",
#     # "1852":"Kurnool",
#     #  "1418": "Mandi",
#     # "1667": "Matheran",
#     # #  "Mysuru (Mysore)",
#     # "1673":"Palampur",
#     #  "1679":"Panchkula",
#     #  "1331":"Porbandar",
#     #  "1114":"Siliguri",
#     #  "2401":"Silvassa",
#     # "1750": "Somnath",
#     # "58204":"St. Petersburg", 
#     # "58391":"Niagara Falls", 
#     # "215":"Hakone-machi",
#     # "9733":"Lille",
#     # "9897":"Snowdonia National Park", # Done
#     # "14":"Denpasar",
#     #  "55":"Kaohsiung",
#     # "113":"Sumida",
#     # "14467":"Wieliczka", # Done
#     # "106":"Xiamen",
#     # "9773":"Southampton",
#     #  "320":"El Nido", # Done
#     # "9823":"Utrecht",
#     # "82598":"Port Douglas",
#     # "81228":"Soufriere",



#     # "107":"Dalian",
#     # "10598": "Ohrid",
#     # "244":"Makati",
#     # "13843":"Hallstatt",
#     # "82935":"Strahan",
#     # "11883":"Sagres",
#     # "10065":"Pistoia", #Done
#     #  "11327":"Tomar",
#     # "58549": "Falmouth",
#     #  "10629":"Volterra",
#     #  "10522":"Le Mans",
#     # "9912":"Varese",
#     #  "10289":"Olbia",
#     #  "12552":"Braies",#Done
#     # "58057":"Mississauga",
#     #  "59263":"Capitol Reef National Park",
#     #  "58069":"Windsor",
#     #  "58393":"Crystal River",#Done
#     # "13760": "Uchisar",
#     #  "11482": "San Michele Al Tagliamento",
#     #  "79336":"Accra",
#     #  "1487":"Vigan",
#     #  "10403":"Piraeus",#Done
#     # "82697":"Kaikoura",
#     #  "645":"Fujiyoshida",
#     #  "58420":"Vancouver",
#     #  "456":"Vung Tau",
#     #  "12701":"Almagro",#Done
#     # "82687":"Victor Harbor",
#     #  "965":"Tomigusuku",
#     #  "16423":"Soltau",
#     #  "10549":"Reus",
#     #  "11584":"Dinard",
#     #  "13139":"Hondarribia",#Done
#     # "10880":"Bangor",
#     #  "634":"Kumejima-cho",
#     #  "12085":"Monreale",
#     #  "10018":"Positano",
#     # "11133":"Stresa",
#     #  "700":"Toyako-cho",#Done
#     # "10068":"Zakopane",
#     #  "10825":"Troyes",
#     # "11204":"Sainte-Maxime",
#     #  "58429":"Lihue",
#     #  "10464":"Cefalu",
#     # "11654":"Cadaques",#Done
#     # "10104":"Ragusa",
#     #  "11363":"Kalambaka",
#     #  "14777":"Himare",
#     #  "11152":"Plitvice Lakes National Park",
# #  "Arles",
#     # "131165":"Colonia del Sacramento",
#     #  "12489":"Vik",
#     #  "12352":"Kalmar",
# #  "San Sebastian - Donostia",
#     # "13534":"Arcos de la Frontera",
#     #  "12387":"Berat",
#     #  "10109":"Rovinj",
#     #  "10568":"Alesund",
#     #  "1047":"Shirakawa-mura",
#     # "10388":"Naxos Town",
#     #  "9731":"Cork",
# #  "Durnstein",
# #  "Sibenik",
# #  "Chioggia",
# #  "Visby",
# # "Valladolid",
# #  "Hirosaki",
# #  "Monteverde Cloud Forest Reserve", "Sibiu",
# # "Ayvalik",
# # "Sayulita"
#     # "2791":"Lansdowne",
#     # "1529":"Chittaurgarh",
#     # "2178":"Panchgani",
#     # "2149":"Arambol"
# #     "2711":"Auroville",
# #     "2424":"Kullu",
# #     "1251":"Salem" ,# Salem (1251)
# #    "58375":"Salem",
# #    -
# #    - Salem (58289)
# # "2185":"Kargil",
# # "3260":"Bhimtal",
# # "2919":"Badrinath",
# # "1163":"Jalandhar",
# # "1218":"Kota",
# # "2141":"Kohima",  
# # # "Howrah"
# # "3466":"Patnitop",
# # "3369":"Konark",
# # "1573":"Tawang",
# # "1812":"Patiala",
# # "2040":"Kurukshetra",
# # "2599":"Thiruvarur",
# # "2596":"Rupnagar",
# # "2583":"Ratlam",
# # "2578":"Udhampur",
# # "2562":"Jhansi",
# # "2557":"Kathua",
# # "2540": "Kumarakom",
# # "2513":"Pollachi",
# # "2477":"Auli",
# # "2476":"Barmer"

# # "58144":"New York City",
# # "9613":"London",
# # "9614":"Paris",
# # "9616":"Rome",
# # "9617":"Barcelona",
# # "58152":"Orlando",
# # "9625":"Amsterdam",
# # "9621":"Madrid",
# # "131072":"Rio de Janeiro",
# # "58148":"Las Vegas",
# # "7":"Singapore",
# # "9623":"Berlin",
# # "4":"Bangkok",
# # "9620":"Prague",
# # "9626":"Lisbon",
# # "131071":"Buenos Aires",
# # "58147":"San Francisco",
# # "9622":"Istanbul",
# # "131073":"Sao Paulo",
# # "9629":"Budapest",
# # "57258":"Dublin",
# # "9633":"Dublin",
# # "58159":"Washington DC",
# # "9631":"Vienna",
# # "9634":"Venice",
# # "9624":"Milan",
# # "58145":"Los Angeles",
# # "58146":"Chicago",
# # "131075":"Santiago",
# # "131103":"Gramado",
# # "58156":"New Orleans",
# # "9641":"Seville",
# # "9615":"Moscow",
# # "58153":"Honolulu",
# # "79299":"Marrakech",
# # "58045":"Toronto",
# # "6":"Beijing",
# # "58162":"Boston",
# # "81903":"Mexico City",
# # "58154":"Seattle",
# # "16":"Ho Chi Minh City",
# # "9657":"Valencia",
# # "33":"Kuala Lumpur",
# # "9645":"Munich",
# # "131112":"Foz do Iguacu",
# # "131082":"Curitiba",
# # "9673":"Stockholm",
# # "58047":"Vancouver",
# # "82575":"Melbourne",
# # "9":"Seoul",
# # "8":"Hanoi",
# # "9640":"Krakow",
# # "131076":"Lima",
# # "135226":"Lima",
# # "9650":"Turin",
# # "9653":"Porto",
# # "81182":"Havana",
# # "58046":"Montreal",
# # "58171":"Nashville",
# # "9693":"Granada",
# # "131087":"Brasilia",
# # "131085":"Salvador",
# # "58058":"Niagara Falls",
# # "131084":"Montevideo",
# # "10":"Shanghai",
# # "58168":"San Antonio",
# # "79300":"Cape Town Central",
# # "58160":"Philadelphia",
# # "58169":"Atlanta",
# # "131088":"Belo Horizonte",
# # "9704":"Dubrovnik",
# # "131091":"Fortaleza",
# # "85942":"Abu Dhabi",
# # "58180":"Miami Beach",
# # "85941":"Jerusalem",
# # "9647":"Hamburg",
# # "9642":"Warsaw",
# # "58157":"Miami",
# # "131074":"Cusco",
# # "9660":"Liverpool",
# # "9665":"Oslo",
# # "58174":"Savannah",
# # "9651":"Glasgow",
# # "131077":"Bogota",
# # "58241":"Pigeon Forge",
# # "131080":"Cartagena",
# # "9812":"Cordoba",
# # "9674":"Nice",
# # "131151":"Campos Do Jordao",
# # "58224":"Memphis",
# # "131089":"Recife",
# # "9681":"Malaga",
# # "9667":"Helsinki",
# # "58179":"Baltimore",
# # "9688":"Belfast",   
# # "131157":"Canela",
# # "82577":"Brisbane",
# # "131081":"Porto Alegre",
# # "44":"Phnom Penh",
# # "58051":"Quebec City",
# # "9750":"Salzburg",
# # "9675":"Marseille",
# # "58242":"Anaheim",
# # "9672":"Bologna",
# # "58176":"Tampa",
# # "9682":"Cologne",
# # "58173":"Dallas",
# # "58166":"Denver",
# # "58188":"Sedona",
# # "146244":"Patong",
# # "9662":"Manchester",
# # "58161":"Houston",
# # "10033":"Valletta",
# # "9915":"Sintra",
# # "9761":"Benidorm",
# # "10123":"Puerto de la Cruz",
# # "131119":"Ushuaia",
# # "58202":"Myrtle Beach",
# # "9680":"Palma de Mallorca",
# # "81189":"San Juan",
# # "82582":"Wellington",
# # "58210":"Cleveland",
# # "9702":"Brighton",
# # "105":"Macau",
# # "58275":"Williamsburg",
# # "131092":"Manaus",
# # "74":"Xi'an",
# # "58199":"Pittsburgh",
# # "58163":"Austin",
# # "131164":"Puerto Iguazu",
# # "58172":"Tucson",
# # "9659":"Riga",
# # "131104":"San Pedro de Atacama",
# # "58050":"Victoria",
# # "58196":"Louisville",
# # "58158":"Portland",
# # "9655":"Frankfurt",
# # "9692":"Bristol",
# # "17":"Kathmandu",
# # "9757":"Bilbao",
# # "9852":"Toledo",
# # "81906":"Puerto Vallarta",
# # "58181":"Phoenix",
# # "131162":"Punta del Este",
# # "9714":"Antwerp",
# # "58178":"Santa Fe",
# # "131114":"Belem",
# # "46":"Jakarta",
# # "9646":"Bucharest",
# # "131315":"Bonito",
# # "81213":"Ocho Rios",
# # "58228":"Santa Monica",
# # "82579":"Christchurch",
# # "131105":"Salta",
# # "58276":"Monterey",
# # "131147":"Aracaju",
# # "131109":"Joao Pessoa",
# # "10176":"Salou",
# # "81196":"Willemstad",
# # "79302":"Cairo",
# # "9719":"Rotterdam",
# # "58226":"Chattanooga",
# # "137":"George Town",
# # "131139":"El Calafate",
# # "9771":"Zaragoza",
# # "58219":"Kansas City",
# # "9695":"Vilnius",
# # "52":"Yangon (Rangoon)",
# # "58476":"Page",
# # "9717":"Dresden",
# # "58048":"Calgary",
# # "85946":"Doha",
# # "79316":"Luxor",
# # "58164":"Brooklyn",
# # "58203":"Lahaina",
# # "131108":"Ubatuba",
# # "49":"Da Nang",
# # "9950":"Pompeii",
# # "9721":"Antalya",
# # "58177":"Fort Lauderdale",
# # "58068":"Banff",
# # "9722":"Gdansk",
# # "131395":"Praia da Pipa",
# # "58231":"Clearwater",
# # "9638":"Kyiv",
# # "82586":"Hobart",
# # "93":"Nha Trang",
# # "9703":"Ljubljana",
# # "9658":"Tbilisi",
# # "58201":"Cincinnati",
# # "9875":"Salamanca",
# # "9669":"Zagreb",
# # "187":"Melaka",
# # "58286":"Gettysburg",
# # "9772":"Alicante",
# # "82584":"Cairns",
# # "92":"Hue",
# # "147352":"La Jolla",
# # "9753":"Wroclaw",
# # "9737":"Stuttgart",
# # "131110":"Mar del Plata",
# # "131122":"Santos",
# # "58195":"Milwaukee",
# # "9739":"Nuremberg",
# # "58269":"Moab",
# # "82588":"Darwin",
# # "79321":"Giza",
# # "10230":"Segovia",
# # "9744":"Ghent",
# # "128":"Hua Hin",
# # "58059":"Halifax",
# # "61":"Busan",
# # "62":"Kanazawa",
# # "9841":"Bournemouth",
# # "58205":"Indianapolis",
# # "9849":"Bergamo",
# # "58185":"Asheville",
# # "81197":"Santo Domingo",
# # "58216":"Salt Lake City",
# # "58079":"North Vancouver",
# # "9819":"Rhodes Town",
# # "131099":"Arequipa",
# # "131125":"Niteroi",
# # "58189":"Naples",
# # "58342":"Arlington",
# # "10260":"Selcuk",
# # "58218":"Detroit",
# # "58184":"Minneapolis",
# # "131127":"Vitoria",
# # "146468":"Carcassonne Center",
# # "131438":"Morro de Sao Paulo",
# # "58748":"Titusville",
# # "10143":"Torquay",
# # "58192":"Scottsdale",
# # "99":"Nara",
# # "50":"Naha",
# # "39":"Guangzhou",
# # "131146":"Cabo Frio",
# # "81941":"Akumal",
# # "58244":"Galveston",
# # "9942":"Heidelberg",
# # "58232":"Oklahoma City",
# # "10015":"Ronda",
# # "257":"Chiang Rai",
# # "58190":"Kailua-Kona",
# # "175":"Manila",
# # "9857":"Marmaris",
# # "9785":"Evora",
# # "76078":"Lake Louise",
# # "131389":"San Andres",
# # "146242":"Chalong",
# # "58212":"Fort Worth",
# # "67":"Chengdu",
# # "58193":"Charlotte",
# # "75":"Nagasaki",
# # "81911":"Oaxaca",
# # "9899":"Coimbra",
# # "9738":"Dusseldorf",
# # "58206":"Santa Barbara",
# # "131178":"Pocos de Caldas",
# # "131098":"Valparaiso",
# # "82":"Baku",
# # "9708":"Sheffield",
# # "11489":"Mont-Saint-Michel",
# # "64":"Hangzhou",
# # "10171":"Nerja",
# # "10792":"Monte-Carlo",
# # "58289":"Salem",
# # "58222":"Richmond",
# # "79305":"Fes",
# # "10595":"Peterhof",
# # "58052":"Edmonton",
# # "131626":"Penha",
# # "131095":"La Paz",
# # "131113":"Rosario",
# # "81913":"Merida",
# # "12525":"Stretford",
# # "81912":"Guadalajara",
# # "9896":"Goreme",
# # "9830":"Heraklion",
# # "73":"Yerevan",
# # "131175":"Itacare",
# # "131327":"Bombinhas",
# # "58309":"Hilo",
# # "9827":"Cannes",
# # "131140":"Puno",
# # "131107":"Santa Marta",
# # "110":"Hakodate",
# # "58211":"Fort Myers",
# # "131131":"Sao Luis",
# # "43":"Colombo",
# # "81918":"Puebla",
# # "79308":"Casablanca",
# # "9887":"Cadiz",
# # "131447":"Caldas Novas",
# # "58345":"Wisconsin Dells",
# # "10134":"Burgos",
# # "81219":"Puerto Plata",
# # "9769":"Kaliningrad",
# # "131145":"Blumenau",
# # "10241":"Playa Blanca",
# # "12491":"Mdina",
# # "58213":"Omaha",
# # "81924":"Guanajuato",
# # "954":"Pecatu",
# # "10106":"Torremolinos",
# # "9843":"Gothenburg",
# # "145":"Kota Kinabalu",
# # "133":"Bandung",
# # "9705":"Ankara",
# # "131177":"Tiradentes",
# # "9984":"Bern",
# # "468":"Ayutthaya",
# # "10031":"Lloret de Mar",
# # "9817":"Basel",
# # "58284":"Fredericksburg",
# # "174":"Da Lat",
# # "78746":"San Jose",
# # "9715":"Nottingham",
# # "131138":"Puerto Varas",
# # "131337":"Guaruja",
# # "9886":"Santander",
# # "58300":"Flagstaff",
# # "9804":"Sarajevo",
# # "59165":"Keystone",
# # "58248":"Daytona Beach",
# # "9928":"Malmo",
# # "131106":"Campinas",
# # "82950":"Waitomo Caves",
# # "60831":"Hammond",
# # "59140":"Poipu",
# # "147376":"Saint Augustine Beach",
# # "116":"Nikko",
# # "12573":"Pamukkale",
# # "58370":"Rapid City",
# # "58368":"Carlsbad",
# # "85943":"Tehran",
# # "78760":"Manuel Antonio",
# # "188":"Cebu City",
# # "9701":"Nizhny Novgorod",
# # "292":"Vientiane",
# # "10979":"Merida",
# # "10101":"Puerto Del Carmen",
# # "131102":"Guayaquil",
# # "58314":"Hot Springs",
# # "10698":"Oia",
# # "121":"Kamakura",
# # "10058":"Potsdam",
# # "85945":"Beirut",
# # "58949":"Hershey",
# # "10696":"Bonifacio",
# # "584":"Kanchanaburi",
# # "58053":"Winnipeg",
# # "82615":"Taupo",
# # "131332":"Vila Velha",
# # "10229":"Nimes",
# # "12927":"Oswiecim",
# # "13458":"Akrotiri",
# # "9956":"Mykonos Town",
# # "58299":"Fort Myers Beach",
# # "85":"Suzhou",
# # "58214":"Sacramento",
# # "223":"Kuching",
# # "9790":"Lviv",
# # "81981":"Palenque",
# # "131180":"Puerto Ayora",
# # "209":"Thimphu",
# # "131362":"Cafayate",
# # "10030":"Bursa",
# # "95":"Kumamoto",
# # "450":"Paro",
# # "465":"Gyeongju",
# # "330":"Tashkent",
# # "81250":"Santiago de Cuba",
# # "9952":"Plovdiv",
# # "79332":"Tunis",
# # "58563":"Columbus",
# # "131457":"Casablanca",
# # "248":"Lhasa",
# # "131168":"San Juan",
# # "1443":"Bhaktapur",
# # "235":"Lahore",
# # "58455":"Athens"
# # "85939":"Dubai",
# # "9630":"Florence",
# # "9636":"Edinburgh",
# # "82574":"Sydney",
# # "15":"Hong Kong",
# # "9618":"St. Petersburg",
# # "2":"Kyoto",
# # "9637":"Athens",
# # "58150":"San Diego",
# # "9643":"Copenhagen",
# # "11":"Siem Reap",
# # "9635":"Naples",
# # "9649":"Brussels",
# # "81905":"Playa del Carmen",
# # "9707":"York",
# # "131100":"Natal",
# # "3":"Osaka",
# # "131086":"Florianopolis",
# # "18":"Taipei",
# # "131118":"Porto Seguro",
# # "82576":"Auckland",
# # "9745":"Bruges",
# # "9798":"Blackpool",
# # "58183":"Branson",
# # "22":"Ubud",
# # "78744":"Panama City",
# # "9656":"Reykjavik",
# # "82578":"Gold Coast",
# # "58170":"Charleston",
# # "53":"Kathu",
# # "9711":"Verona",
# # "58175":"Saint Louis",
# # "9683":"Genoa",
# # "9666":"Birmingham",
# # "9670":"Lyon",
# # "9735":"Funchal",
# # "80":"Pattaya",
# # "9730":"Bath",
# # "81904":"Cancun",
# # "81180":"Punta Cana",
# # "9654":"Palermo",
# # "9679":"Tallinn",
# # "131150":"Ipojuca",
# # "131090":"San Carlos de Bariloche",
# # "81909":"Tulum",
# # "21":"Hoi An",
# # "19":"Chiang Mai",
# # "131083":"Mendoza",
# # "9767":"Pisa",
# # "131078":"Quito",
# # "131132":"Petropolis",
# # "37":"Kuta",
# # "131115":"Maceio",
# # "79":"Krabi Town",
# # "131134":"Jijoca de Jericoacoara",
# # "28":"Taito",
# # "131094":"Paraty",
# # "9712":"Cardiff",
# # "9792":"Maspalomas",
# # "82593":"Rotorua",
# # "9826":"Adeje",
# # "9800":"Syracuse",
# # "9751":"Strasbourg",
# # "9663":"Bordeaux",
# # "9763":"Paphos",
# # "82585":"Canberra",
# # "9802":"Siena",
# # "9696":"Bratislava",
# # "23":"Minato",
# # "10219":"Windsor",
# # "29":"Sapporo",
# # "9803":"Portsmouth",
# # "9668":"Zurich",
# # "85940":"Tel Aviv",
# # "10061":"Versailles",
# # "38":"Shibuya",
# # "58187":"Sarasota",
# # "9801":"Bergen",
# # "131079":"Medellin",
# # "184":"Luang Prabang",
# # "79309":"Sharm El Sheikh",
# # "9850":"Chester",   
# # "131318":"Machu Picchu",
# # "20":"Yokohama",
# # "58049":"Ottawa",
# # "81226":"Varadero",
# # "9724":"Geneva",
# # "58182":"Greater Palm Springs",
# # "10008":"Lucerne",
# # "9690":"Padua",
# # "9889":"Benalmadena",
# # "82581":"Adelaide",
# # "9723":"Oxford",
# # "9836":"Killarney" , 
# # "9677":"Sochi",
# # "58191":"Albuquerque",
# # "51":"Phuket Town",
# # "32":"Shinjuku",
# # "9765":"Albufeira",

# # "42":"Chiyoda",
# # "59":"Hiroshima",
# # "131093":"Angra Dos Reis",
# # "9840":"Lucca",
# # "9689":"Leeds",
# # "81194":"Nassau",
# # "10046":"Ravenna",
# # "9687":"Belgrade",
# # "131117":"Ouro Preto",
# # "9725":"The Hague",
# # "9691":"Trieste",
# # "9694":"Split",
# # "9891":"Matera",
# # "9935":"Taormina",
# # "9998":"Llandudno",
# # "79306":"Johannesburg",
# # "11769":"Lindos",
# # "58198":"Anchorage",
# # "9709":"Toulouse",
# # "9676":"Sofia",
# # "9838":"Santiago de Compostela",
# # "9945":"Assisi",
# # "9756":"Rimini",
# # "30":"Nagoya",
# # "9799":"Nantes",
# # "41":"New Taipei",
# # "10074":"Agrigento",
# # "176":"Karon",
# # "9903":"Carcassonne",
# # "131478":"Mata de Sao Joao",
# # "78752":"La Fortuna de San Carlos",
# # "9713":"Newcastle upon Tyne",
# # "34":"Kobe",
# # "79303":"Nairobi",
# # "9919":"Avignon",
# # "131360":"Maragogi",
# # "82594":"Dunedin",
# # "31":"Fukuoka",
# # "10064":"Weymouth",
# # "10235":"Bled",
# # "10211":"Vila Nova de Gaia",
# # "118":"Kandy",
# # "9937":"Scarborough",
# # "9861":"Innsbruck",
# # "9917":"Lincoln",
# # "9727":"Thessaloniki",
# # "9734":"Galway",
# # "98":"Bophut"
# # "9612":"Sicily",
# # "58149":"Oahu",
# # "9632":"Tenerife",
# # "9628":"Crete",
# # "9619":"Sardinia",
# # "9627":"Majorca",
# # "12":"Phuket",
# # "9648":"Island of Malta",
# # "58165":"Key West",
# # "9671":"Lanzarote",
# # "81177":"Puerto Rico",
# # "58044":"Vancouver Island",
# # "58151":"Maui",
# # "9661":"Rhodes",
# # "9639":"Gran Canaria",
# # "58155":"Island of Hawaii",
# # "5":"Luzon",
# # "9678":"Madeira",
# # "9697":"Santorini",
# # "58167":"Kauai",
# # "9644":"Corsica",
# # "9749":"Isle of Wight",
# # "9686":"Corfu",
# # "650":"Sentosa Island",
# # "9718":"Menorca",
# # "9699":"Islands of Sicily",
# # "9685":"Fuerteventura",
# # "146":"Langkawi",
# # "81908":"Cozumel",
# # "131172":"Fernando de Noronha",
# # "81190":"New Providence Island",
# # "81187":"Grand Cayman",
# # "9732":"Zakynthos",
# # "131174":"San Andres Island",
# # "9664":"Ibiza",

# # "135383":"Brazil",
# # "135385":"Argentina",
# # "135391":"Chile",
# # "135393":"Colombia",
# # "135408":"Uruguay",
# # "135438":"Bolivia",
# # "135468":"Venezuela",
# # "135498":"Paraguay",
# # "79304":"Mauritius",
# # "86647":"Japan",
# # "86651":"Thailand",
# # "86652":"Philippines",
# # "86654":"Singapore",
# # "86655":"Vietnam",
# # "86656":"South Korea",
# # "86659":"Cambodia",
# # "86661":"India",
# # "86667":"Nepal",
# # "86668":"Taiwan",
# # "86693":"Sri Lanka",
# # "86729":"Azerbaijan",
# # "86731":"Kazakhstan",
# # "86772":"Mongolia",
# # "86776":"Kyrgyzstan",
# # "86779":"Brunei Darussalam",
# # "86797":"Laos",
# # "86819":"Bhutan",
# # "86831":"Pakistan",
# # "86851":"Bangladesh",
# # "86938":"Tajikistan",
# # "87077":"North Korea",
# # "88352":"Italy",
# # "88358":"France",
# # "88359":"United Kingdom",
# # "88360":"Russia",
# # "88362":"Spain",
# # "88366":"Czech Republic",
# # "88367":"Turkiye",
# # "88368":"Germany",
# # "88373":"The Netherlands",
# # "88375":"Greece",
# # "88380":"Hungary",
# # "88384":"Austria",
# # "88386":"Ireland",
# # "88402":"Denmark",
# # "88403":"Poland",
# # "88407":"Malta",
# # "88408":"Belgium",
# # "88419":"Iceland",
# # "88420":"Georgia",
# # "88423":"Latvia",
# # "88432":"Norway",
# # "88434":"Finland",
# # "88437":"Switzerland",
# # "88438":"Croatia",
# # "88445":"Sweden",
# # "88449":"Bulgaria",
# # "88455":"Estonia",
# # "88464":"Serbia",
# # "88477":"Lithuania",
# # "88478":"Slovakia",
# # "88486":"Slovenia",
# # "88503":"Belarus",
# # "88527":"Cyprus",
# # "88609":"Albania",
# # "88654":"Montenegro",
# # "88723":"Luxembourg",
# # "88731":"Moldova",
# # "90374":"Canada",
# # "90407":"United States",
# # "90647":"Panama",
# # "90648":"Honduras",
# # "90651":"Costa Rica",
# # "90652":"Guatemala",
# # "90656":"Belize",
# # "90660":"Nicaragua",
# # "90759":"Morocco",
# # "90763":"Egypt",
# # "90764":"South Africa",
# # "91342":"Mexico",
# # "91387":"Australia",
# # "91394":"New Zealand",
# # "91403":"Fiji",
# # "91524":"United Arab Emirates",
# # "91525":"Israel",
# # "91526":"Lebanon",
# # "91530":"Iran",
# # "91534":"Jordan",
# # "91535":"Qatar",
# # "91536":"Oman",
# # "91537":"Kuwait",
# # "91540":"Bahrain",
# # "91548":"Saudi Arabia",
# # "86674":"Rajasthan",
# # "86675":"Maharashtra",
# # "86676":"Hokkaido",
# # "86686":"Karnataka",
# # "86689":"Guangdong",
# # "86691":"Tamil Nadu",
# # "86706":"Uttar Pradesh",
# # "86714":"Kerala",
# # "86716":"West Bengal",
# # "86722":"Jiangsu",
# # "86743":"Fujian",
# # "86805":"Uttarakhand",
# # "86887":"Punjab",
# # "86904":"Odisha",
# # "86937":"Assam",
# # "86988":"Meghalaya",
# # "87236":"Arunachal Pradesh",
# # "87411":"Nagaland",
# # "88406":"Bavaria",
# # "90405":"California",
# # "90409":"Nevada",
# # "90412":"Washington",
# # "90413":"Louisiana",
# # "90419":"Texas",
# # "90420":"Massachusetts",
# # "91388":"New South Wales",
# # "91389":"Queensland",
# # "91397":"South Australia",
# # "91399":"Western Australia",
# # "91406":"Tasmania"
# # "146238":"Amer"
# # "1424":"Bhavnagar",
# # "1060":"Ludhiana",
# "2264":"Rourkela",
# "2346":"Bathinda",



 
#     # Add all your countries/cities
# }

# SUBCOLLECTION_NAME = "top_attractions"

# # Operation Mode
# class OperationMode(Enum):
#     CURATE_ONLY = "curate"      # Non-destructive: writes curation metadata
#     DELETE_MODE = "delete"       # Destructive: deletes low-quality places
#     HYBRID = "hybrid"            # Both: curate first, then delete based on curation

# OPERATION_MODE = OperationMode.CURATE_ONLY

# # Quality Thresholds
# MIN_RATING_DEFAULT = 4.0
# MIN_REVIEWS_DEFAULT = 100
# MIN_RATING_RESTAURANT = 4.5
# MIN_REVIEWS_RESTAURANT = 2000
# MIN_RATING_ABSOLUTE = 3.5      # Hard minimum (instant reject)
# MIN_REVIEWS_ABSOLUTE = 50      # Hard minimum (instant reject)

# # Type-based rules
# ALLOW_RESTAURANTS = False       # If False, restaurants need higher bar
# ALLOW_SHOPPING = True
# ALLOW_HOTELS = False
# FAMOUS_RESTAURANT_OVERRIDE = True  # Allow LLM to save iconic restaurants

# DOWNRANK_TYPES = {
#     "lodging", "real_estate_agency", "car_dealer", "bank", "lawyer",
#     "political", "local_government_office", "school", "university",
#     "hospital", "pharmacy", "supermarket", "convenience_store",
#     "gas_station", "car_rental", "car_repair"
# }

# RESTAURANT_TYPES = {"restaurant", "food", "bar", "cafe", "meal_takeaway", "meal_delivery"}
# SHOPPING_TYPES = {"shopping_mall", "store", "clothing_store", "department_store"}
# HOTEL_TYPES = {"lodging", "hotel", "resort", "motel"}

# # Scoring Thresholds (0-100 scale)
# SCORE_THRESHOLD_KEEP = 70      # Auto-keep if score >= this
# SCORE_THRESHOLD_REVIEW = 55    # Manual review needed
# # Below REVIEW threshold = DEMOTE/DELETE

# # Processing Options
# API_CALL_DELAY = 0.4
# BATCH_SIZE = 450               # Firestore batch write limit
# DRY_RUN = False                 # Set False to write changes
# ONLY_COUNTRIES = None          # e.g., ["1", "25"] or None for all
# SKIP_IF_UNCHANGED = True       # Skip docs with identical curation
# # NEW: process only docs that don't have 'curation' yet
# PROCESS_ONLY_NEW = True

# # Output Files
# CSV_PATH = "curation_report.csv"
# CACHE_PATH = f"llm_cache_{AI_PROVIDER.value}.json"
# DELETE_LOG_PATH = "deleted_attractions.csv"

# # ═══════════════════════════════════════════════════════════════════════════════
# # INITIALIZATION
# # ═══════════════════════════════════════════════════════════════════════════════

# # Firebase
# cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
# try:
#     firebase_admin.get_app()
# except ValueError:
#     firebase_admin.initialize_app(cred)
# db = firestore.client()

# # AI Client
# if AI_PROVIDER == AIProvider.OPENAI:
#     from openai import OpenAI
#     ai_client = OpenAI(api_key=OPENAI_API_KEY)
# elif AI_PROVIDER == AIProvider.ANTHROPIC:
#     import anthropic
#     ai_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# # ═══════════════════════════════════════════════════════════════════════════════
# # TEXT UTILITIES
# # ═══════════════════════════════════════════════════════════════════════════════

# _EMOJI_RE = re.compile(
#     r"[\U0001F1E0-\U0001F1FF]|[\U0001F300-\U0001F5FF]|[\U0001F600-\U0001F64F]|"
#     r"[\U0001F680-\U0001F6FF]|[\U0001F700-\U0001F77F]|[\U0001F780-\U0001F7FF]|"
#     r"[\U0001F800-\U0001F8FF]|[\U0001F900-\U0001F9FF]|[\U0001FA00-\U0001FA6F]|"
#     r"[\U0001FA70-\U0001FAFF]|[\u2600-\u26FF]|[\u2700-\u27BF]"
# )
# _TAG_RE = re.compile(r"<[^>]+>")
# _WS_RE = re.compile(r"\s+")

# def clean_text(s: Optional[str]) -> str:
#     """Clean text from HTML, emojis, and normalize whitespace."""
#     if not s:
#         return ""
#     s = html.unescape(s)
#     s = _TAG_RE.sub("", s)
#     s = _EMOJI_RE.sub("", s)
#     s = s.replace("\u200b", "")
#     s = unicodedata.normalize("NFKC", s)
#     s = _WS_RE.sub(" ", s).strip()
#     return s

# def truncate_text(s: str, max_len: int = 300) -> str:
#     """Truncate text to max length."""
#     return s[:max_len] if len(s) > max_len else s

# # ═══════════════════════════════════════════════════════════════════════════════
# # CACHING
# # ═══════════════════════════════════════════════════════════════════════════════

# _cache: Dict[str, Dict] = {}
# if os.path.exists(CACHE_PATH):
#     try:
#         with open(CACHE_PATH, "r", encoding="utf-8") as f:
#             _cache = json.load(f)
#         print(f"📦 Loaded {len(_cache)} cached LLM responses\n")
#     except Exception:
#         _cache = {}

# def cache_key(data: Dict) -> str:
#     """Generate SHA256 hash for cache key."""
#     return hashlib.sha256(
#         json.dumps(data, sort_keys=True, ensure_ascii=False).encode("utf-8")
#     ).hexdigest()

# def cache_get(key: str) -> Optional[Dict]:
#     return _cache.get(key)

# def cache_put(key: str, value: Dict):
#     _cache[key] = value
#     try:
#         with open(CACHE_PATH, "w", encoding="utf-8") as f:
#             json.dump(_cache, f, ensure_ascii=False, indent=2)
#     except Exception:
#         pass

# # ═══════════════════════════════════════════════════════════════════════════════
# # TYPE CHECKING UTILITIES
# # ═══════════════════════════════════════════════════════════════════════════════

# def is_restaurant(types: List[str]) -> bool:
#     tset = {t.lower() for t in (types or [])}
#     return bool(tset & RESTAURANT_TYPES)

# def is_shopping(types: List[str]) -> bool:
#     tset = {t.lower() for t in (types or [])}
#     return bool(tset & SHOPPING_TYPES)

# def is_hotel(types: List[str]) -> bool:
#     tset = {t.lower() for t in (types or [])}
#     return bool(tset & HOTEL_TYPES)

# def has_downrank_types(types: List[str]) -> bool:
#     tset = {t.lower() for t in (types or [])}
#     return bool(tset & DOWNRANK_TYPES)

# # ═══════════════════════════════════════════════════════════════════════════════
# # HARD RULES FILTERING
# # ═══════════════════════════════════════════════════════════════════════════════

# def apply_hard_rules(name: str, types: List[str], rating: Optional[float],
#                      review_count: Optional[int]) -> Optional[str]:
#     """
#     Apply hard rejection rules before LLM.
#     Returns rejection reason if should be rejected, None otherwise.
#     """
#     r = rating or 0.0
#     n = review_count or 0
    
#     # Absolute minimums
#     if n < MIN_REVIEWS_ABSOLUTE:
#         return f"Too few reviews ({n} < {MIN_REVIEWS_ABSOLUTE})"
    
#     if r < MIN_RATING_ABSOLUTE:
#         return f"Rating too low ({r:.1f} < {MIN_RATING_ABSOLUTE})"
    
#     # Type-based rules
#     if is_hotel(types) and not ALLOW_HOTELS:
#         return "Hotel/lodging not allowed"
    
#     if is_shopping(types) and not ALLOW_SHOPPING:
#         return "Shopping venue not allowed"
    
#     if is_restaurant(types) and not ALLOW_RESTAURANTS:
#         # Stricter rules for restaurants when not allowed
#         if n < MIN_REVIEWS_RESTAURANT or r < MIN_RATING_RESTAURANT:
#             return f"Restaurant doesn't meet strict threshold (R:{r:.1f}, N:{n})"
    
#     return None

# # ═══════════════════════════════════════════════════════════════════════════════
# # HEURISTIC SCORING
# # ═══════════════════════════════════════════════════════════════════════════════

# def calculate_heuristic_score(name: str, types: List[str], rating: Optional[float],
#                                review_count: Optional[int]) -> Tuple[int, List[str]]:
#     """
#     Calculate heuristic score (0-100) based on rating, reviews, and type.
#     Returns (score, list_of_triggered_rules)
#     """
#     score = 50  # Neutral starting point
#     triggers = []
    
#     r = rating or 0.0
#     n = review_count or 0
    
#     # Restaurant handling
#     if is_restaurant(types):
#         if r >= MIN_RATING_RESTAURANT:
#             score += 20
#             triggers.append("high_rating_restaurant")
#         elif r >= MIN_RATING_DEFAULT:
#             score += 5
#             triggers.append("ok_rating_restaurant")
#         else:
#             score -= 20
#             triggers.append("low_rating_restaurant")
        
#         if n >= MIN_REVIEWS_RESTAURANT:
#             score += 25
#             triggers.append("high_reviews_restaurant")
#         elif n >= MIN_REVIEWS_DEFAULT:
#             score += 10
#             triggers.append("ok_reviews_restaurant")
#         else:
#             score -= 25
#             triggers.append("low_reviews_restaurant")
    
#     # Regular attractions
#     else:
#         if r >= 4.5:
#             score += 20
#             triggers.append("excellent_rating")
#         elif r >= MIN_RATING_DEFAULT:
#             score += 15
#             triggers.append("good_rating")
#         elif r >= MIN_RATING_ABSOLUTE:
#             score += 5
#             triggers.append("ok_rating")
#         else:
#             score -= 15
#             triggers.append("low_rating")
        
#         if n >= 1000:
#             score += 25
#             triggers.append("very_high_reviews")
#         elif n >= MIN_REVIEWS_DEFAULT:
#             score += 20
#             triggers.append("high_reviews")
#         elif n >= MIN_REVIEWS_ABSOLUTE:
#             score += 5
#             triggers.append("ok_reviews")
#         else:
#             score -= 20
#             triggers.append("low_reviews")
    
#     # Type penalties
#     if has_downrank_types(types):
#         score -= 20
#         triggers.append("downrank_type")
    
#     # Bound to 0-100
#     score = max(0, min(100, score))
    
#     return score, triggers

# # ═══════════════════════════════════════════════════════════════════════════════
# # LLM ASSESSMENT
# # ═══════════════════════════════════════════════════════════════════════════════

# def assess_with_openai(city: str, name: str, types: List[str], rating: float,
#                        review_count: int, description: str) -> Dict:
#     """Use OpenAI to assess attraction quality with JSON response."""
    
#     system_msg = (
#         "You are a travel expert curator. Evaluate whether a place deserves 'top attraction' "
#         "status for the SPECIFIC city/town (not national level). Consider fame, cultural significance, "
#         "uniqueness, and tourist appeal. Be strict with restaurants - only world-famous or "
#         "city-defining eateries qualify."
#     )
    
#     user_msg = f"""Evaluate this place for {city}'s top attractions:

# Name: {name}
# Types: {', '.join(types[:5]) if types else 'Unknown'}
# Rating: {rating}/5.0 ({review_count:,} reviews)
# Description: {truncate_text(clean_text(description))}

# Return ONLY valid JSON with these exact keys:
# {{
#   "category": "iconic|significant|local|minor",
#   "reason": "<concise explanation>",
#   "confidence": 0.0-1.0,
#   "iconic_restaurant": true|false
# }}

# Category definitions:
# - iconic: World-famous, must-see landmark
# - significant: Important local attraction, well worth visiting
# - local: Neighborhood spot, not a major attraction
# - minor: Generic business, not attraction-worthy
# """
    
#     cache_data = {
#         "provider": "openai",
#         "model": OPENAI_MODEL,
#         "city": city,
#         "name": name,
#         "types": types[:3],
#         "rating": rating,
#         "reviews": review_count
#     }
#     key = cache_key(cache_data)
    
#     cached = cache_get(key)
#     if cached:
#         return cached
    
#     try:
#         response = ai_client.chat.completions.create(
#             model=OPENAI_MODEL,
#             messages=[
#                 {"role": "system", "content": system_msg},
#                 {"role": "user", "content": user_msg}
#             ],
#             temperature=0.2,
#             response_format={"type": "json_object"},
#             max_tokens=300
#         )
        
#         content = response.choices[0].message.content or "{}"
#         result = json.loads(content)
        
#         # Validate and normalize
#         category = str(result.get("category", "local")).lower()
#         if category not in {"iconic", "significant", "local", "minor"}:
#             category = "local"
        
#         normalized = {
#             "category": category,
#             "reason": clean_text(result.get("reason", ""))[:200],
#             "confidence": max(0.0, min(1.0, float(result.get("confidence", 0.5)))),
#             "iconic_restaurant": bool(result.get("iconic_restaurant", False))
#         }
        
#         cache_put(key, normalized)
#         time.sleep(API_CALL_DELAY)
#         return normalized
        
#     except Exception as e:
#         print(f"   ❌ OpenAI Error: {e}")
#         return {
#             "category": "local",
#             "reason": f"LLM error: {str(e)[:100]}",
#             "confidence": 0.3,
#             "iconic_restaurant": False
#         }

# def assess_with_anthropic(city: str, name: str, types: List[str], rating: float,
#                           review_count: int, description: str) -> Dict:
#     """Use Anthropic Claude to assess attraction quality."""
    
#     prompt = f"""Evaluate if this place deserves 'top attraction' status for {city}:

# Name: {name}
# Types: {', '.join(types[:5]) if types else 'Unknown'}
# Rating: {rating}/5.0 ({review_count:,} reviews)
# Description: {truncate_text(clean_text(description))}

# Consider: fame, cultural significance, uniqueness, tourist appeal. Be strict with restaurants - 
# only world-famous or city-defining eateries qualify. Evaluate for THIS CITY specifically, not nationally.

# Return ONLY valid JSON with these exact keys:
# {{
#   "category": "iconic|significant|local|minor",
#   "reason": "<concise explanation>",
#   "confidence": 0.0-1.0,
#   "iconic_restaurant": true|false
# }}

# Category definitions:
# - iconic: World-famous landmark/attraction
# - significant: Important local attraction
# - local: Neighborhood spot, not major attraction
# - minor: Generic business, not attraction-worthy
# """
    
#     cache_data = {
#         "provider": "anthropic",
#         "model": ANTHROPIC_MODEL,
#         "city": city,
#         "name": name,
#         "types": types[:3],
#         "rating": rating,
#         "reviews": review_count
#     }
#     key = cache_key(cache_data)
    
#     cached = cache_get(key)
#     if cached:
#         return cached
    
#     try:
#         message = ai_client.messages.create(
#             model=ANTHROPIC_MODEL,
#             max_tokens=300,
#             temperature=0.2,
#             messages=[{"role": "user", "content": prompt}]
#         )
        
#         content = message.content[0].text
#         result = json.loads(content)
        
#         # Validate and normalize
#         category = str(result.get("category", "local")).lower()
#         if category not in {"iconic", "significant", "local", "minor"}:
#             category = "local"
        
#         normalized = {
#             "category": category,
#             "reason": clean_text(result.get("reason", ""))[:200],
#             "confidence": max(0.0, min(1.0, float(result.get("confidence", 0.5)))),
#             "iconic_restaurant": bool(result.get("iconic_restaurant", False))
#         }
        
#         cache_put(key, normalized)
#         time.sleep(API_CALL_DELAY)
#         return normalized
        
#     except Exception as e:
#         print(f"   ❌ Anthropic Error: {e}")
#         return {
#             "category": "local",
#             "reason": f"LLM error: {str(e)[:100]}",
#             "confidence": 0.3,
#             "iconic_restaurant": False
#         }

# # ═══════════════════════════════════════════════════════════════════════════════
# # DECISION FUSION
# # ═══════════════════════════════════════════════════════════════════════════════

# def fuse_decision(is_rest: bool, heuristic_score: int, llm_category: str,
#                   llm_confidence: float, iconic_restaurant: bool) -> Tuple[str, bool, int]:
#     """
#     Combine heuristic score + LLM assessment into final decision.
#     Returns: (action, is_top_suggested, final_score)
    
#     Actions: KEEP, REVIEW, DEMOTE
#     """
#     # Map LLM category to weight
#     category_weights = {
#         "iconic": 30,
#         "significant": 15,
#         "local": 0,
#         "minor": -20
#     }
#     llm_weight = category_weights.get(llm_category, 0)
    
#     # Confidence modifier (reduce weight if low confidence)
#     confidence_modifier = llm_confidence
#     adjusted_llm_weight = int(llm_weight * confidence_modifier)
    
#     # Fused score
#     fused_score = heuristic_score + adjusted_llm_weight
    
#     # Special handling: iconic restaurant override
#     if (is_rest and FAMOUS_RESTAURANT_OVERRIDE and iconic_restaurant and
#         llm_category in {"iconic", "significant"} and llm_confidence >= 0.7):
#         fused_score = max(fused_score, 75)
    
#     # Bound score
#     fused_score = max(0, min(100, fused_score))
    
#     # Determine action
#     if fused_score >= SCORE_THRESHOLD_KEEP:
#         action = "KEEP"
#         is_top = True
#     elif fused_score >= SCORE_THRESHOLD_REVIEW:
#         action = "REVIEW"
#         is_top = False
#     else:
#         action = "DEMOTE"
#         is_top = False
    
#     return action, is_top, fused_score

# # ═══════════════════════════════════════════════════════════════════════════════
# # MAIN PROCESSING
# # ═══════════════════════════════════════════════════════════════════════════════

# def assess_place(data: Dict, city: str) -> Dict:
#     """
#     Comprehensive assessment of a single place.
#     Returns complete assessment dictionary.
#     """
#     name = data.get("name") or data.get("placeName") or "Unknown"
#     types = data.get("types") or []
#     rating = data.get("rating")
#     review_count = data.get("ratingCount") or data.get("user_ratings_total")
#     description = (data.get("detail_description") or
#                    data.get("g_description") or
#                    data.get("description") or "")
    
#     # Step 1: Check hard rules
#     hard_rule_reason = apply_hard_rules(name, types, rating, review_count)
#     if hard_rule_reason:
#         return {
#             "decision": "REJECT_RULE",
#             "action": "DEMOTE",
#             "is_top_suggested": False,
#             "heuristic_score": 0,
#             "heuristic_triggers": ["hard_rule_reject"],
#             "llm_category": "rule-based",
#             "llm_confidence": 1.0,
#             "llm_reason": hard_rule_reason,
#             "llm_iconic_restaurant": False,
#             "final_score": 0,
#             "rating": rating,
#             "review_count": review_count
#         }
    
#     # Step 2: Calculate heuristic score
#     heuristic_score, triggers = calculate_heuristic_score(name, types, rating, review_count)
    
#     # Step 3: Get LLM assessment
#     r = rating or 0.0
#     n = review_count or 0
    
#     if AI_PROVIDER == AIProvider.OPENAI:
#         llm_result = assess_with_openai(city, name, types, r, n, description)
#     else:
#         llm_result = assess_with_anthropic(city, name, types, r, n, description)
    
#     # Step 4: Fuse decisions
#     action, is_top, final_score = fuse_decision(
#         is_restaurant(types),
#         heuristic_score,
#         llm_result["category"],
#         llm_result["confidence"],
#         llm_result["iconic_restaurant"]
#     )
    
#     return {
#         "decision": "ASSESSED",
#         "action": action,
#         "is_top_suggested": is_top,
#         "heuristic_score": heuristic_score,
#         "heuristic_triggers": triggers,
#         "llm_category": llm_result["category"],
#         "llm_confidence": llm_result["confidence"],
#         "llm_reason": llm_result["reason"],
#         "llm_iconic_restaurant": llm_result["iconic_restaurant"],
#         "final_score": final_score,
#         "rating": rating,
#         "review_count": review_count
#     }

# def process_country(country_id: str, csv_rows: List[List]) -> Dict:
#     """
#     Process all places in a country's top_attractions subcollection.
#     If PROCESS_ONLY_NEW = True, only process docs where 'curation' is missing or {}.
#     Returns a statistics dictionary.
#     """
#     city_name = COUNTRY_NAMES.get(country_id, f"City_{country_id}")

#     print(f"\n{'='*80}")
#     print(f"📍 Processing: {city_name} (ID: {country_id})")
#     print(f"{'='*80}\n")

#     coll = db.collection("allplaces").document(country_id).collection(SUBCOLLECTION_NAME)
#     all_docs = list(coll.stream())

#     if not all_docs:
#         print("   ⚠️  No places found\n")
#         return {
#             "total": 0, "updated": 0, "skipped": 0, "deleted": 0,
#             "keep": 0, "review": 0, "demote": 0
#         }

#     # Determine which docs are uncurated:
#     # - uncurated if 'curation' is None or {} (empty dict)
#     # - curated if 'curation' is a non-empty dict
#     def is_uncurated(data: Dict) -> bool:
#         cur = data.get("curation")
#         return (cur is None) or (isinstance(cur, dict) and len(cur) == 0)

#     docs = all_docs
#     already_curated = 0
#     if PROCESS_ONLY_NEW:
#         filtered = []
#         for d in all_docs:
#             data = d.to_dict() or {}
#             if is_uncurated(data):
#                 filtered.append(d)
#             else:
#                 already_curated += 1
#         print(f"   ℹ️  Found {len(all_docs)} docs | {len(filtered)} uncurated | {already_curated} already curated (skipped)\n")
#         docs = filtered
#         if not docs:
#             # Nothing to process; return stats that reflect pre-filter skip count
#             return {
#                 "total": 0, "updated": 0, "skipped": already_curated, "deleted": 0,
#                 "keep": 0, "review": 0, "demote": 0
#             }

#     stats = {
#         "total": len(docs),
#         "updated": 0,
#         "skipped": already_curated if PROCESS_ONLY_NEW else 0,  # count pre-filtered as skipped
#         "deleted": 0,
#         "keep": 0,
#         "review": 0,
#         "demote": 0
#     }

#     batch = db.batch()
#     batch_ops = 0
#     to_delete = []

#     for idx, doc in enumerate(docs, 1):
#         data = doc.to_dict() or {}
#         name = data.get("name") or data.get("placeName") or "Unknown"

#         print(f"[{idx}/{stats['total']}] {name}")

#         # Assess the place
#         assessment = assess_place(data, city_name)

#         action = assessment["action"]
#         is_top = assessment["is_top_suggested"]

#         # Update action-based stats
#         if action == "KEEP":
#             stats["keep"] += 1
#         elif action == "REVIEW":
#             stats["review"] += 1
#         else:  # DEMOTE
#             stats["demote"] += 1

#         # Display result
#         icon = "✅" if action == "KEEP" else "🔍" if action == "REVIEW" else "❌"
#         print(
#             f"   {icon} {action} | Score: {assessment['final_score']} "
#             f"(H:{assessment['heuristic_score']} + LLM:{assessment['llm_category']}/{assessment['llm_confidence']:.2f})"
#         )
#         print(f"      {assessment['llm_reason']}")

#         # Incremental skip for unchanged (only in CURATE_ONLY, and only if prior curation exists & is non-empty)
#         if SKIP_IF_UNCHANGED and OPERATION_MODE == OperationMode.CURATE_ONLY:
#             old_curation = data.get("curation")
#             if isinstance(old_curation, dict) and len(old_curation) > 0:
#                 if (
#                     old_curation.get("final_action") == action and
#                     old_curation.get("is_top_suggested") == is_top and
#                     old_curation.get("final_score") == assessment["final_score"]
#                 ):
#                     print("      ⏭️  Unchanged, skipping")
#                     stats["skipped"] += 1
#                     continue

#         # Prepare curation payload
#         curation_data = {
#             "curation": {
#                 "is_top_suggested": is_top,
#                 "final_action": action,
#                 "final_score": assessment["final_score"],
#                 "heuristic_score": assessment["heuristic_score"],
#                 "heuristic_triggers": assessment["heuristic_triggers"],
#                 "llm_category": assessment["llm_category"],
#                 "llm_confidence": assessment["llm_confidence"],
#                 "llm_reason": assessment["llm_reason"],
#                 "llm_iconic_restaurant": assessment["llm_iconic_restaurant"],
#                 "curated_at": firestore.SERVER_TIMESTAMP,
#                 "provider": AI_PROVIDER.value,
#                 "model": OPENAI_MODEL if AI_PROVIDER == AIProvider.OPENAI else ANTHROPIC_MODEL
#             }
#         }

#         # Handle operation modes
#         if OPERATION_MODE == OperationMode.CURATE_ONLY:
#             if not DRY_RUN:
#                 batch.update(doc.reference, curation_data)
#                 batch_ops += 1
#                 stats["updated"] += 1

#                 if batch_ops >= BATCH_SIZE:
#                     batch.commit()
#                     print(f"      💾 Committed batch of {batch_ops} docs")
#                     batch = db.batch()
#                     batch_ops = 0
#             else:
#                 print("      🔍 DRY RUN - would update curation")
#                 stats["updated"] += 1

#         elif OPERATION_MODE == OperationMode.DELETE_MODE:
#             if action == "DEMOTE":
#                 to_delete.append(doc)
#                 if not DRY_RUN:
#                     stats["deleted"] += 1
#                 else:
#                     print("      🔍 DRY RUN - would delete")

#         elif OPERATION_MODE == OperationMode.HYBRID:
#             if not DRY_RUN:
#                 batch.update(doc.reference, curation_data)
#                 batch_ops += 1
#                 stats["updated"] += 1

#                 if action == "DEMOTE":
#                     to_delete.append(doc)

#                 if batch_ops >= BATCH_SIZE:
#                     batch.commit()
#                     print(f"      💾 Committed batch of {batch_ops} docs")
#                     batch = db.batch()
#                     batch_ops = 0
#             else:
#                 print("      🔍 DRY RUN - would update curation (hybrid)")
#                 stats["updated"] += 1
#                 if action == "DEMOTE":
#                     print("      🔍 DRY RUN - would delete (hybrid)")

#         # Log to CSV (only for processed docs)
#         csv_rows.append([
#             country_id,
#             city_name,
#             doc.id,
#             name,
#             action,
#             is_top,
#             assessment["final_score"],
#             assessment["heuristic_score"],
#             "|".join(assessment["heuristic_triggers"]),
#             assessment["llm_category"],
#             f"{assessment['llm_confidence']:.2f}",
#             assessment["llm_reason"],
#             assessment.get("rating", "N/A"),
#             assessment.get("review_count", "N/A"),
#             ",".join((data.get("types") or [])[:3]),
#             doc.reference.path
#         ])

#         print()

#     # Commit remaining batch ops
#     if not DRY_RUN and batch_ops > 0:
#         batch.commit()
#         print(f"   💾 Committed final batch of {batch_ops} docs\n")

#     # Handle deletions (DELETE/HYBRID live modes)
#     if to_delete and not DRY_RUN:
#         delete_batch = db.batch()
#         deleted_rows = []
#         for d in to_delete:
#             delete_batch.delete(d.reference)
#             deleted_rows.append([
#                 country_id,
#                 city_name,
#                 d.id,
#                 (d.to_dict() or {}).get("name", "Unknown"),
#                 datetime.now(timezone.utc).isoformat()
#             ])
#         delete_batch.commit()
#         print(f"   🗑️  Deleted {len(to_delete)} places\n")

#         # Log deletions
#         file_exists = os.path.exists(DELETE_LOG_PATH)
#         with open(DELETE_LOG_PATH, "a", newline="", encoding="utf-8") as f:
#             writer = csv.writer(f)
#             if not file_exists or os.path.getsize(DELETE_LOG_PATH) == 0:
#                 writer.writerow(["country_id", "city", "doc_id", "name", "deleted_at"])
#             writer.writerows(deleted_rows)

#     # Summary
#     print(f"{'─'*80}")
#     print(f"Summary for {city_name}:")
#     print(f"  Total considered: {len(all_docs)}")
#     if PROCESS_ONLY_NEW:
#         print(f"  • Already curated (skipped pre-filter): {already_curated}")
#         print(f"  • Uncurated processed: {stats['total']}")
#     else:
#         print(f"  • Processed: {stats['total']}")
#     denom = max(1, stats["total"])  # avoid div-by-zero
#     print(f"  ✅ Keep: {stats['keep']} ({(stats['keep']/denom)*100:.1f}%)")
#     print(f"  🔍 Review: {stats['review']} ({(stats['review']/denom)*100:.1f}%)")
#     print(f"  ❌ Demote: {stats['demote']} ({(stats['demote']/denom)*100:.1f}%)")
#     print(f"  📝 Updated: {stats['updated']}")
#     print(f"  ⏭️  Skipped: {stats['skipped']}")
#     if stats["deleted"] > 0:
#         print(f"  🗑️  Deleted: {stats['deleted']}")
#     print(f"{'─'*80}")

#     return stats




# # def process_country(country_id: str, csv_rows: List[List]) -> Dict:
# #     """
# #     Process all places in a country's top_attractions subcollection.
# #     Returns statistics dictionary.
# #     """
# #     city_name = COUNTRY_NAMES.get(country_id, f"City_{country_id}")
    
# #     print(f"\n{'='*80}")
# #     print(f"📍 Processing: {city_name} (ID: {country_id})")
# #     print(f"{'='*80}\n")
    
# #     coll = db.collection("allplaces").document(country_id).collection(SUBCOLLECTION_NAME)
# #     docs = list(coll.stream())
    
# #     if not docs:
# #         print("   ⚠️  No places found\n")
# #         return {
# #             "total": 0, "updated": 0, "skipped": 0, "deleted": 0,
# #             "keep": 0, "review": 0, "demote": 0
# #         }
    
# #     stats = {
# #         "total": len(docs),
# #         "updated": 0,
# #         "skipped": 0,
# #         "deleted": 0,
# #         "keep": 0,
# #         "review": 0,
# #         "demote": 0
# #     }
    
# #     batch = db.batch()
# #     batch_ops = 0
# #     to_delete = []
    
# #     for idx, doc in enumerate(docs, 1):
# #         data = doc.to_dict() or {}
# #         name = data.get("name") or data.get("placeName") or "Unknown"
        
# #         print(f"[{idx}/{stats['total']}] {name}")
        
# #         # Assess the place
# #         assessment = assess_place(data, city_name)
        
# #         action = assessment["action"]
# #         is_top = assessment["is_top_suggested"]
        
# #         # Update statistics
# #         if action == "KEEP":
# #             stats["keep"] += 1
# #         elif action == "REVIEW":
# #             stats["review"] += 1
# #         else:  # DEMOTE
# #             stats["demote"] += 1
        
# #         # Display result
# #         icon = "✅" if action == "KEEP" else "🔍" if action == "REVIEW" else "❌"
# #         print(f"   {icon} {action} | Score: {assessment['final_score']} "
# #               f"(H:{assessment['heuristic_score']} + LLM:{assessment['llm_category']}/{assessment['llm_confidence']:.2f})")
# #         print(f"      {assessment['llm_reason']}")
        
# #         # Check if unchanged (for incremental processing)
# #         if SKIP_IF_UNCHANGED and OPERATION_MODE == OperationMode.CURATE_ONLY:
# #             old_curation = data.get("curation") or {}
# #             if (old_curation.get("final_action") == action and
# #                 old_curation.get("is_top_suggested") == is_top and
# #                 old_curation.get("final_score") == assessment['final_score']):
# #                 print("      ⏭️  Unchanged, skipping")
# #                 stats["skipped"] += 1
# #                 continue
        
# #         # Prepare curation data
# #         curation_data = {
# #             "curation": {
# #                 "is_top_suggested": is_top,
# #                 "final_action": action,
# #                 "final_score": assessment["final_score"],
# #                 "heuristic_score": assessment["heuristic_score"],
# #                 "heuristic_triggers": assessment["heuristic_triggers"],
# #                 "llm_category": assessment["llm_category"],
# #                 "llm_confidence": assessment["llm_confidence"],
# #                 "llm_reason": assessment["llm_reason"],
# #                 "llm_iconic_restaurant": assessment["llm_iconic_restaurant"],
# #                 "curated_at": firestore.SERVER_TIMESTAMP,
# #                 "provider": AI_PROVIDER.value,
# #                 "model": OPENAI_MODEL if AI_PROVIDER == AIProvider.OPENAI else ANTHROPIC_MODEL
# #             }
# #         }
        
# #         # Handle different operation modes
# #         if OPERATION_MODE == OperationMode.CURATE_ONLY:
# #             if not DRY_RUN:
# #                 batch.update(doc.reference, curation_data)
# #                 batch_ops += 1
# #                 stats["updated"] += 1
                
# #                 if batch_ops >= BATCH_SIZE:
# #                     batch.commit()
# #                     print(f"      💾 Committed batch of {batch_ops} docs")
# #                     batch = db.batch()
# #                     batch_ops = 0
# #             else:
# #                 print("      🔍 DRY RUN - would update curation")
# #                 stats["updated"] += 1
        
# #         elif OPERATION_MODE == OperationMode.DELETE_MODE:
# #             if action == "DEMOTE":
# #                 to_delete.append(doc)
# #                 if not DRY_RUN:
# #                     stats["deleted"] += 1
# #                 else:
# #                     print("      🔍 DRY RUN - would delete")
        
# #         elif OPERATION_MODE == OperationMode.HYBRID:
# #             if not DRY_RUN:
# #                 batch.update(doc.reference, curation_data)
# #                 batch_ops += 1
# #                 stats["updated"] += 1
                
# #                 if action == "DEMOTE":
# #                     to_delete.append(doc)
                
# #                 if batch_ops >= BATCH_SIZE:
# #                     batch.commit()
# #                     print(f"      💾 Committed batch of {batch_ops} docs")
# #                     batch = db.batch()
# #                     batch_ops = 0
        
# #         # Log to CSV
# #         csv_rows.append([
# #             country_id,
# #             city_name,
# #             doc.id,
# #             name,
# #             action,
# #             is_top,
# #             assessment["final_score"],
# #             assessment["heuristic_score"],
# #             "|".join(assessment["heuristic_triggers"]),
# #             assessment["llm_category"],
# #             f"{assessment['llm_confidence']:.2f}",
# #             assessment["llm_reason"],
# #             assessment.get("rating", "N/A"),
# #             assessment.get("review_count", "N/A"),
# #             ",".join(data.get("types", [])[:3]),
# #             doc.reference.path
# #         ])
        
# #         print()
    
# #     # Commit remaining batch
# #     if not DRY_RUN and batch_ops > 0:
# #         batch.commit()
# #         print(f"   💾 Committed final batch of {batch_ops} docs\n")
    
# #     # Handle deletions
# #     if to_delete and not DRY_RUN:
# #         delete_batch = db.batch()
# #         deleted_rows = []
        
# #         for doc in to_delete:
# #             delete_batch.delete(doc.reference)
# #             deleted_rows.append([
# #                 country_id,
# #                 city_name,
# #                 doc.id,
# #                 doc.to_dict().get("name", "Unknown"),
# #                 datetime.now(timezone.utc).isoformat()
# #             ])
        
# #         delete_batch.commit()
# #         print(f"   🗑️  Deleted {len(to_delete)} places\n")
        
# #         # Log deletions
# #         file_exists = os.path.exists(DELETE_LOG_PATH)
# #         with open(DELETE_LOG_PATH, "a", newline="", encoding="utf-8") as f:
# #             writer = csv.writer(f)
# #             if not file_exists or os.path.getsize(DELETE_LOG_PATH) == 0:
# #                 writer.writerow(["country_id", "city", "doc_id", "name", "deleted_at"])
# #             writer.writerows(deleted_rows)
    
# #     # Summary
# #     print(f"{'─'*80}")
# #     print(f"Summary for {city_name}:")
# #     print(f"  Total: {stats['total']}")
# #     print(f"  ✅ Keep: {stats['keep']} ({stats['keep']/stats['total']*100:.1f}%)")
# #     print(f"  🔍 Review: {stats['review']} ({stats['review']/stats['total']*100:.1f}%)")
# #     print(f"  ❌ Demote: {stats['demote']} ({stats['demote']/stats['total']*100:.1f}%)")
# #     print(f"  📝 Updated: {stats['updated']}")
# #     print(f"  ⏭️  Skipped: {stats['skipped']}")
# #     if stats["deleted"] > 0:
# #         print(f"  🗑️  Deleted: {stats['deleted']}")
# #     print(f"{'─'*80}")
    
# #     return stats

# # ═══════════════════════════════════════════════════════════════════════════════
# # MAIN ENTRY POINT
# # ═══════════════════════════════════════════════════════════════════════════════

# def main():
#     print("🚀 Enhanced Top Attractions Curation System")
#     print(f"{'='*80}")
#     print(f"AI Provider: {AI_PROVIDER.value.upper()}")
#     print(f"Model: {OPENAI_MODEL if AI_PROVIDER == AIProvider.OPENAI else ANTHROPIC_MODEL}")
#     print(f"Operation Mode: {OPERATION_MODE.value.upper()}")
#     print(f"Mode: {'DRY RUN ⚠️' if DRY_RUN else 'LIVE ✅'}")
#     print(f"\nQuality Thresholds:")
#     print(f"  Min Rating (Default): {MIN_RATING_DEFAULT} | Min Reviews: {MIN_REVIEWS_DEFAULT}")
#     print(f"  Min Rating (Restaurant): {MIN_RATING_RESTAURANT} | Min Reviews: {MIN_REVIEWS_RESTAURANT}")
#     print(f"  Score Thresholds: KEEP ≥{SCORE_THRESHOLD_KEEP}, REVIEW ≥{SCORE_THRESHOLD_REVIEW}")
#     print(f"\nAllow: Restaurants={ALLOW_RESTAURANTS}, Shopping={ALLOW_SHOPPING}, Hotels={ALLOW_HOTELS}")
#     print(f"{'='*80}\n")
    
#     # Confirmation for destructive operations
#     if not DRY_RUN and OPERATION_MODE in [OperationMode.DELETE_MODE, OperationMode.HYBRID]:
#         confirm = input("⚠️  This will DELETE places. Type 'DELETE' to confirm: ")
#         if confirm != "DELETE":
#             print("❌ Cancelled.")
#             return
    
#     # Select countries
#     countries = ONLY_COUNTRIES or list(COUNTRY_NAMES.keys())
    
#     csv_rows: List[List] = []
#     total_stats = {
#         "total": 0, "updated": 0, "skipped": 0, "deleted": 0,
#         "keep": 0, "review": 0, "demote": 0
#     }
    
#     # Process each country
#     for country_id in countries:
#         if country_id not in COUNTRY_NAMES:
#             print(f"⚠️  Unknown country ID: {country_id}, skipping\n")
#             continue
        
#         stats = process_country(country_id, csv_rows)
        
#         # Aggregate stats
#         for key in total_stats:
#             total_stats[key] += stats[key]
    
#     # Write CSV report
#     if csv_rows:
#         with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
#             writer = csv.writer(f)
#             writer.writerow([
#                 "country_id", "city", "doc_id", "name", "action", "is_top_suggested",
#                 "final_score", "heuristic_score", "heuristic_triggers",
#                 "llm_category", "llm_confidence", "llm_reason",
#                 "rating", "review_count", "types", "path"
#             ])
#             writer.writerows(csv_rows)
#         print(f"\n📊 Report saved: {CSV_PATH}")
    
#     # Final summary
#     print(f"\n{'='*80}")
#     print("🎉 CURATION COMPLETE")
#     print(f"{'='*80}")
#     print(f"Total Places Analyzed: {total_stats['total']}")
#     print(f"✅ Keep: {total_stats['keep']} ({total_stats['keep']/total_stats['total']*100:.1f}%)")
#     print(f"🔍 Review: {total_stats['review']} ({total_stats['review']/total_stats['total']*100:.1f}%)")
#     print(f"❌ Demote: {total_stats['demote']} ({total_stats['demote']/total_stats['total']*100:.1f}%)")
    
#     if OPERATION_MODE in [OperationMode.CURATE_ONLY, OperationMode.HYBRID]:
#         print(f"📝 Updated: {total_stats['updated']}")
#         print(f"⏭️  Skipped (unchanged): {total_stats['skipped']}")
    
#     if OPERATION_MODE in [OperationMode.DELETE_MODE, OperationMode.HYBRID]:
#         print(f"🗑️  Deleted: {total_stats['deleted']}")
#         if not DRY_RUN and total_stats['deleted'] > 0:
#             print(f"📄 Deletion log: {DELETE_LOG_PATH}")
    
#     print(f"💾 LLM Cache: {len(_cache)} entries in {CACHE_PATH}")
#     print(f"{'='*80}")

# if __name__ == "__main__":
#     main()



"""
Enhanced Top Attractions Curation Script
Combines best features from both versions:
- Multi-provider LLM support (OpenAI + Anthropic)
- Intelligent scoring fusion (heuristics + LLM)
- Non-destructive curation mode + optional deletion
- JSON response parsing for reliability
- Incremental processing with caching
- Comprehensive logging and reporting
- NEW: Distance-based filter from parent city coordinates
"""

import os
import re
import csv
import json
import hashlib
import time
import html
import unicodedata
import math
from typing import Dict, Optional, List, Tuple
from datetime import datetime, timezone
from enum import Enum

import firebase_admin
from firebase_admin import credentials, firestore

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

SERVICE_ACCOUNT_PATH = r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"

# AI Provider Selection
class AIProvider(Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"

AI_PROVIDER = AIProvider.OPENAI  # Change to AIProvider.ANTHROPIC if needed

# API Configuration
# Prefer: set via environment variables instead of hardcoding
OPENAI_API_KEY = "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"
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "your-anthropic-key")
OPENAI_MODEL = "gpt-4.1-mini"
ANTHROPIC_MODEL = "claude-sonnet-4-20250514"

# Country/City mapping
COUNTRY_NAMES = {
    # "1060": "Ludhiana",
    # "2264": "Rourkela",
    # "2346": "Bathinda",
#     "581":"Calangute",
# "1312":"Kasaragod",
# "827":"Tirunelveli",
# '1479':'Karwar'
# '1699':'Margao'
# "1659":"Vasco da Gama"
# "1531":"Ahmednagar"
# "1729":"Gaya"
# "1997":"Tiruvannamalai"
# "4276":"Kedarnath"
# "2451": "Ambaji",
# "2976":"Katra",
# "4947":"Gangotri",
# "2562":"Jhansi",
# "2231":"Guruvayur"

# No images comming
# "1664":"Fatehpur Sikri",
# "2150":"Agonda",
# "2084":"Cavelossim",
# "1854":"Saputara",
# "1402":"Chamba",
# "2654":"Valparai",
# "1250":"Mandya",
# "1949":"Greater Noida",
# "1754":"Bharuch",
# "3446":"Palani",
# "1776":"Rajgir",
# "2131":"Solapur",
# "2018":"Cuttack",
# "2562":"Jhansi",
# "1970":"Bishnupur",
# "1759":"Valsad",
# "1604":"Sirsi",
# "1690":"Chittoor",
# "2038":"Kolar",
# "2366":"Balasore",
# "2160":"Amarkantak",
# "1733":"Belgaum",
# "1678":"Sambalpur",
# "1354":"Dindigul",
# "2263":"Theni",
# "1936":"Pathanamthitta",
# "1781":"Hooghly",
# "1660":"Malappuram",
# "1523":"Secunderabad",
# "2123":"Jalpaiguri",
# "1706":"Tumkur",
# "1996":"Tezpur",
#  "1139":"Nagapattinam",

# "2042":"Aizawl",
# "2541":"Anantnag",
# "4237":"Chhatarpur",
# "2252":"Uttarkashi",
# "1862":"Ganjam",
# "2046":"Bilaspur",
# "2596":"Rupnagar",
# "1782":"Bankura",
# "1591":"Jorhat",
# "2615":"Gulbarga",
# "2265":"Erode",
# "2345":"Jowai",
"1480": "Alibaug"
}


    # Add all your countries/cities

SUBCOLLECTION_NAME = "top_attractions"

# Operation Mode
class OperationMode(Enum):
    CURATE_ONLY = "curate"      # Non-destructive: writes curation metadata
    DELETE_MODE = "delete"      # Destructive: deletes low-quality places
    HYBRID = "hybrid"           # Both: curate first, then delete based on curation

OPERATION_MODE = OperationMode.CURATE_ONLY

# Quality Thresholds
MIN_RATING_DEFAULT = 4.0
MIN_REVIEWS_DEFAULT = 100
MIN_RATING_RESTAURANT = 4.5
MIN_REVIEWS_RESTAURANT = 2000
MIN_RATING_ABSOLUTE = 3.5      # Hard minimum (instant reject)
MIN_REVIEWS_ABSOLUTE = 50      # Hard minimum (instant reject)

# Type-based rules
ALLOW_RESTAURANTS = False       # If False, restaurants need higher bar
ALLOW_SHOPPING = True
ALLOW_HOTELS = False
FAMOUS_RESTAURANT_OVERRIDE = True  # Allow LLM to save iconic restaurants

DOWNRANK_TYPES = {
    "lodging", "real_estate_agency", "car_dealer", "bank", "lawyer",
    "political", "local_government_office", "school", "university",
    "hospital", "pharmacy", "supermarket", "convenience_store",
    "gas_station", "car_rental", "car_repair"
}

RESTAURANT_TYPES = {"restaurant", "food", "bar", "cafe", "meal_takeaway", "meal_delivery"}
SHOPPING_TYPES = {"shopping_mall", "store", "clothing_store", "department_store"}
HOTEL_TYPES = {"lodging", "hotel", "resort", "motel"}

# Scoring Thresholds (0-100 scale)
SCORE_THRESHOLD_KEEP = 70      # Auto-keep if score >= this
SCORE_THRESHOLD_REVIEW = 55    # Manual review needed
# Below REVIEW threshold = DEMOTE/DELETE

# Processing Options
API_CALL_DELAY = 0.4
BATCH_SIZE = 450               # Firestore batch write limit
DRY_RUN = False                # Set False to write changes
ONLY_COUNTRIES = None          # e.g., ["1", "25"] or None for all
SKIP_IF_UNCHANGED = True       # Skip docs with identical curation
PROCESS_ONLY_NEW = True        # Process only docs that don't have 'curation' yet

# Distance-based curation
USE_DISTANCE_FILTER = True          # master toggle for distance rule
MAX_CITY_DISTANCE_KM = 100.0        # anything beyond this is DEMOTE (hard rule)

# Output Files
CSV_PATH = "curation_report.csv"
CACHE_PATH = f"llm_cache_{AI_PROVIDER.value}.json"
DELETE_LOG_PATH = "deleted_attractions.csv"

# ═══════════════════════════════════════════════════════════════════════════════
# INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════════════

# Firebase
cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
try:
    firebase_admin.get_app()
except ValueError:
    firebase_admin.initialize_app(cred)
db = firestore.client()

# AI Client
if AI_PROVIDER == AIProvider.OPENAI:
    from openai import OpenAI
    ai_client = OpenAI(api_key=OPENAI_API_KEY)
elif AI_PROVIDER == AIProvider.ANTHROPIC:
    import anthropic
    ai_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ═══════════════════════════════════════════════════════════════════════════════
# TEXT UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

_EMOJI_RE = re.compile(
    r"[\U0001F1E0-\U0001F1FF]|[\U0001F300-\U0001F5FF]|[\U0001F600-\U0001F64F]|"
    r"[\U0001F680-\U0001F6FF]|[\U0001F700-\U0001F77F]|[\U0001F780-\U0001F7FF]|"
    r"[\U0001F800-\U0001F8FF]|[\U0001F900-\U0001F9FF]|[\U0001FA00-\U0001FA6F]|"
    r"[\U0001FA70-\U0001FAFF]|[\u2600-\u26FF]|[\u2700-\u27BF]"
)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

def clean_text(s: Optional[str]) -> str:
    """Clean text from HTML, emojis, and normalize whitespace."""
    if not s:
        return ""
    s = html.unescape(s)
    s = _TAG_RE.sub("", s)
    s = _EMOJI_RE.sub("", s)
    s = s.replace("\u200b", "")
    s = unicodedata.normalize("NFKC", s)
    s = _WS_RE.sub(" ", s).strip()
    return s

def truncate_text(s: str, max_len: int = 300) -> str:
    """Truncate text to max length."""
    return s[:max_len] if len(s) > max_len else s

# ═══════════════════════════════════════════════════════════════════════════════
# CACHING
# ═══════════════════════════════════════════════════════════════════════════════

_cache: Dict[str, Dict] = {}
if os.path.exists(CACHE_PATH):
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            _cache = json.load(f)
        print(f"📦 Loaded {len(_cache)} cached LLM responses\n")
    except Exception:
        _cache = {}

def cache_key(data: Dict) -> str:
    """Generate SHA256 hash for cache key."""
    return hashlib.sha256(
        json.dumps(data, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()

def cache_get(key: str) -> Optional[Dict]:
    return _cache.get(key)

def cache_put(key: str, value: Dict):
    _cache[key] = value
    try:
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(_cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════════════════════
# TYPE CHECKING UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def is_restaurant(types: List[str]) -> bool:
    tset = {t.lower() for t in (types or [])}
    return bool(tset & RESTAURANT_TYPES)

def is_shopping(types: List[str]) -> bool:
    tset = {t.lower() for t in (types or [])}
    return bool(tset & SHOPPING_TYPES)

def is_hotel(types: List[str]) -> bool:
    tset = {t.lower() for t in (types or [])}
    return bool(tset & HOTEL_TYPES)

def has_downrank_types(types: List[str]) -> bool:
    tset = {t.lower() for t in (types or [])}
    return bool(tset & DOWNRANK_TYPES)

# ═══════════════════════════════════════════════════════════════════════════════
# DISTANCE UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Compute great-circle distance between two points on Earth (in kilometers).
    """
    R = 6371.0  # Earth radius in km

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c

# ═══════════════════════════════════════════════════════════════════════════════
# HARD RULES FILTERING
# ═══════════════════════════════════════════════════════════════════════════════

def apply_hard_rules(name: str, types: List[str], rating: Optional[float],
                     review_count: Optional[int]) -> Optional[str]:
    """
    Apply hard rejection rules before LLM.
    Returns rejection reason if should be rejected, None otherwise.
    """
    r = rating or 0.0
    n = review_count or 0

    # Absolute minimums
    if n < MIN_REVIEWS_ABSOLUTE:
        return f"Too few reviews ({n} < {MIN_REVIEWS_ABSOLUTE})"

    if r < MIN_RATING_ABSOLUTE:
        return f"Rating too low ({r:.1f} < {MIN_RATING_ABSOLUTE})"

    # Type-based rules
    if is_hotel(types) and not ALLOW_HOTELS:
        return "Hotel/lodging not allowed"

    if is_shopping(types) and not ALLOW_SHOPPING:
        return "Shopping venue not allowed"

    if is_restaurant(types) and not ALLOW_RESTAURANTS:
        # Stricter rules for restaurants when not allowed
        if n < MIN_REVIEWS_RESTAURANT or r < MIN_RATING_RESTAURANT:
            return f"Restaurant doesn't meet strict threshold (R:{r:.1f}, N:{n})"

    return None

# ═══════════════════════════════════════════════════════════════════════════════
# HEURISTIC SCORING
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_heuristic_score(name: str, types: List[str], rating: Optional[float],
                              review_count: Optional[int]) -> Tuple[int, List[str]]:
    """
    Calculate heuristic score (0-100) based on rating, reviews, and type.
    Returns (score, list_of_triggered_rules)
    """
    score = 50  # Neutral starting point
    triggers: List[str] = []

    r = rating or 0.0
    n = review_count or 0

    # Restaurant handling
    if is_restaurant(types):
        if r >= MIN_RATING_RESTAURANT:
            score += 20
            triggers.append("high_rating_restaurant")
        elif r >= MIN_RATING_DEFAULT:
            score += 5
            triggers.append("ok_rating_restaurant")
        else:
            score -= 20
            triggers.append("low_rating_restaurant")

        if n >= MIN_REVIEWS_RESTAURANT:
            score += 25
            triggers.append("high_reviews_restaurant")
        elif n >= MIN_REVIEWS_DEFAULT:
            score += 10
            triggers.append("ok_reviews_restaurant")
        else:
            score -= 25
            triggers.append("low_reviews_restaurant")

    # Regular attractions
    else:
        if r >= 4.5:
            score += 20
            triggers.append("excellent_rating")
        elif r >= MIN_RATING_DEFAULT:
            score += 15
            triggers.append("good_rating")
        elif r >= MIN_RATING_ABSOLUTE:
            score += 5
            triggers.append("ok_rating")
        else:
            score -= 15
            triggers.append("low_rating")

        if n >= 1000:
            score += 25
            triggers.append("very_high_reviews")
        elif n >= MIN_REVIEWS_DEFAULT:
            score += 20
            triggers.append("high_reviews")
        elif n >= MIN_REVIEWS_ABSOLUTE:
            score += 5
            triggers.append("ok_reviews")
        else:
            score -= 20
            triggers.append("low_reviews")

    # Type penalties
    if has_downrank_types(types):
        score -= 20
        triggers.append("downrank_type")

    # Bound to 0-100
    score = max(0, min(100, score))

    return score, triggers

# ═══════════════════════════════════════════════════════════════════════════════
# LLM ASSESSMENT
# ═══════════════════════════════════════════════════════════════════════════════

def assess_with_openai(city: str, name: str, types: List[str], rating: float,
                       review_count: int, description: str) -> Dict:
    """Use OpenAI to assess attraction quality with JSON response."""

    system_msg = (
        "You are a travel expert curator. Evaluate whether a place deserves 'top attraction' "
        "status for the SPECIFIC city/town (not national level). Consider fame, cultural significance, "
        "uniqueness, and tourist appeal. Be strict with restaurants - only world-famous or "
        "city-defining eateries qualify."
    )

    user_msg = f"""Evaluate this place for {city}'s top attractions:

Name: {name}
Types: {', '.join(types[:5]) if types else 'Unknown'}
Rating: {rating}/5.0 ({review_count:,} reviews)
Description: {truncate_text(clean_text(description))}

Return ONLY valid JSON with these exact keys:
{{
  "category": "iconic|significant|local|minor",
  "reason": "<concise explanation>",
  "confidence": 0.0-1.0,
  "iconic_restaurant": true|false
}}

Category definitions:
- iconic: World-famous, must-see landmark
- significant: Important local attraction, well worth visiting
- local: Neighborhood spot, not a major attraction
- minor: Generic business, not attraction-worthy
"""

    cache_data = {
        "provider": "openai",
        "model": OPENAI_MODEL,
        "city": city,
        "name": name,
        "types": types[:3],
        "rating": rating,
        "reviews": review_count,
    }
    key = cache_key(cache_data)

    cached = cache_get(key)
    if cached:
        return cached

    try:
        response = ai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.2,
            response_format={"type": "json_object"},
            max_tokens=300,
        )

        content = response.choices[0].message.content or "{}"
        result = json.loads(content)

        # Validate and normalize
        category = str(result.get("category", "local")).lower()
        if category not in {"iconic", "significant", "local", "minor"}:
            category = "local"

        normalized = {
            "category": category,
            "reason": clean_text(result.get("reason", ""))[:200],
            "confidence": max(
                0.0, min(1.0, float(result.get("confidence", 0.5)))
            ),
            "iconic_restaurant": bool(result.get("iconic_restaurant", False)),
        }

        cache_put(key, normalized)
        time.sleep(API_CALL_DELAY)
        return normalized

    except Exception as e:
        print(f"   ❌ OpenAI Error: {e}")
        return {
            "category": "local",
            "reason": f"LLM error: {str(e)[:100]}",
            "confidence": 0.3,
            "iconic_restaurant": False,
        }

def assess_with_anthropic(city: str, name: str, types: List[str], rating: float,
                          review_count: int, description: str) -> Dict:
    """Use Anthropic Claude to assess attraction quality."""

    prompt = f"""Evaluate if this place deserves 'top attraction' status for {city}:

Name: {name}
Types: {', '.join(types[:5]) if types else 'Unknown'}
Rating: {rating}/5.0 ({review_count:,} reviews)
Description: {truncate_text(clean_text(description))}

Consider: fame, cultural significance, uniqueness, tourist appeal. Be strict with restaurants - 
only world-famous or city-defining eateries qualify. Evaluate for THIS CITY specifically, not nationally.

Return ONLY valid JSON with these exact keys:
{{
  "category": "iconic|significant|local|minor",
  "reason": "<concise explanation>",
  "confidence": 0.0-1.0,
  "iconic_restaurant": true|false
}}

Category definitions:
- iconic: World-famous landmark/attraction
- significant: Important local attraction
- local: Neighborhood spot, not major attraction
- minor: Generic business, not attraction-worthy
"""

    cache_data = {
        "provider": "anthropic",
        "model": ANTHROPIC_MODEL,
        "city": city,
        "name": name,
        "types": types[:3],
        "rating": rating,
        "reviews": review_count,
    }
    key = cache_key(cache_data)

    cached = cache_get(key)
    if cached:
        return cached

    try:
        message = ai_client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=300,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}],
        )

        content = message.content[0].text
        result = json.loads(content)

        # Validate and normalize
        category = str(result.get("category", "local")).lower()
        if category not in {"iconic", "significant", "local", "minor"}:
            category = "local"

        normalized = {
            "category": category,
            "reason": clean_text(result.get("reason", ""))[:200],
            "confidence": max(
                0.0, min(1.0, float(result.get("confidence", 0.5)))
            ),
            "iconic_restaurant": bool(result.get("iconic_restaurant", False)),
        }

        cache_put(key, normalized)
        time.sleep(API_CALL_DELAY)
        return normalized

    except Exception as e:
        print(f"   ❌ Anthropic Error: {e}")
        return {
            "category": "local",
            "reason": f"LLM error: {str(e)[:100]}",
            "confidence": 0.3,
            "iconic_restaurant": False,
        }

# ═══════════════════════════════════════════════════════════════════════════════
# DECISION FUSION
# ═══════════════════════════════════════════════════════════════════════════════

def fuse_decision(is_rest: bool, heuristic_score: int, llm_category: str,
                  llm_confidence: float, iconic_restaurant: bool) -> Tuple[str, bool, int]:
    """
    Combine heuristic score + LLM assessment into final decision.
    Returns: (action, is_top_suggested, final_score)

    Actions: KEEP, REVIEW, DEMOTE
    """
    # Map LLM category to weight
    category_weights = {
        "iconic": 30,
        "significant": 15,
        "local": 0,
        "minor": -20,
    }
    llm_weight = category_weights.get(llm_category, 0)

    # Confidence modifier (reduce weight if low confidence)
    confidence_modifier = llm_confidence
    adjusted_llm_weight = int(llm_weight * confidence_modifier)

    # Fused score
    fused_score = heuristic_score + adjusted_llm_weight

    # Special handling: iconic restaurant override
    if (
        is_rest
        and FAMOUS_RESTAURANT_OVERRIDE
        and iconic_restaurant
        and llm_category in {"iconic", "significant"}
        and llm_confidence >= 0.7
    ):
        fused_score = max(fused_score, 75)

    # Bound score
    fused_score = max(0, min(100, fused_score))

    # Determine action
    if fused_score >= SCORE_THRESHOLD_KEEP:
        action = "KEEP"
        is_top = True
    elif fused_score >= SCORE_THRESHOLD_REVIEW:
        action = "REVIEW"
        is_top = False
    else:
        action = "DEMOTE"
        is_top = False

    return action, is_top, fused_score

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PROCESSING
# ═══════════════════════════════════════════════════════════════════════════════

def assess_place(
    data: Dict,
    city: str,
    parent_coords: Optional[Tuple[float, float]],
) -> Dict:
    """
    Comprehensive assessment of a single place.
    Returns complete assessment dictionary.
    """
    name = data.get("name") or data.get("placeName") or "Unknown"
    types = data.get("types") or []
    rating = data.get("rating")
    review_count = data.get("ratingCount") or data.get("user_ratings_total")
    description = (
        data.get("detail_description")
        or data.get("g_description")
        or data.get("description")
        or ""
    )

    # Distance from parent city (if enabled)
    distance_km: Optional[float] = None
    if USE_DISTANCE_FILTER and parent_coords is not None:
        # Try common coordinate fields for the place
        place_lat = (
            data.get("latitude")
            or data.get("lat")
            or data.get("geo_lat")
        )
        place_lng = (
            data.get("longitude")
            or data.get("lng")
            or data.get("geo_lng")
        )
        if place_lat is not None and place_lng is not None:
            try:
                distance_km = haversine_km(
                    float(parent_coords[0]),
                    float(parent_coords[1]),
                    float(place_lat),
                    float(place_lng),
                )
            except Exception:
                distance_km = None

    # Distance hard rule (before rating/reviews/LLM)
    if (
        USE_DISTANCE_FILTER
        and distance_km is not None
        and distance_km > MAX_CITY_DISTANCE_KM
    ):
        return {
            "decision": "REJECT_DISTANCE",
            "action": "DEMOTE",
            "is_top_suggested": False,
            "heuristic_score": 0,
            "heuristic_triggers": ["distance_too_far"],
            "llm_category": "distance-rule",
            "llm_confidence": 1.0,
            "llm_reason": (
                f"Too far from parent city ({distance_km:.1f} km > "
                f"{MAX_CITY_DISTANCE_KM:.0f} km). Not suitable as a local "
                f"top attraction for {city}."
            ),
            "llm_iconic_restaurant": False,
            "final_score": 0,
            "rating": rating,
            "review_count": review_count,
            "distance_km": distance_km,
        }

    # Step 1: Check hard rules
    hard_rule_reason = apply_hard_rules(name, types, rating, review_count)
    if hard_rule_reason:
        return {
            "decision": "REJECT_RULE",
            "action": "DEMOTE",
            "is_top_suggested": False,
            "heuristic_score": 0,
            "heuristic_triggers": ["hard_rule_reject"],
            "llm_category": "rule-based",
            "llm_confidence": 1.0,
            "llm_reason": hard_rule_reason,
            "llm_iconic_restaurant": False,
            "final_score": 0,
            "rating": rating,
            "review_count": review_count,
            "distance_km": distance_km,
        }

    # Step 2: Calculate heuristic score
    heuristic_score, triggers = calculate_heuristic_score(
        name, types, rating, review_count
    )

    # Step 3: Get LLM assessment
    r = rating or 0.0
    n = review_count or 0

    if AI_PROVIDER == AIProvider.OPENAI:
        llm_result = assess_with_openai(city, name, types, r, n, description)
    else:
        llm_result = assess_with_anthropic(city, name, types, r, n, description)

    # Step 4: Fuse decisions
    action, is_top, final_score = fuse_decision(
        is_restaurant(types),
        heuristic_score,
        llm_result["category"],
        llm_result["confidence"],
        llm_result["iconic_restaurant"],
    )

    return {
        "decision": "ASSESSED",
        "action": action,
        "is_top_suggested": is_top,
        "heuristic_score": heuristic_score,
        "heuristic_triggers": triggers,
        "llm_category": llm_result["category"],
        "llm_confidence": llm_result["confidence"],
        "llm_reason": llm_result["reason"],
        "llm_iconic_restaurant": llm_result["iconic_restaurant"],
        "final_score": final_score,
        "rating": rating,
        "review_count": review_count,
        "distance_km": distance_km,
    }

def process_country(country_id: str, csv_rows: List[List]) -> Dict:
    """
    Process all places in a country's top_attractions subcollection.
    If PROCESS_ONLY_NEW = True, only process docs where 'curation' is missing or {}.
    Returns a statistics dictionary.
    """
    city_name = COUNTRY_NAMES.get(country_id, f"City_{country_id}")

    # Fetch parent city coordinates once
    parent_doc_ref = db.collection("allplaces").document(country_id)
    parent_snapshot = parent_doc_ref.get()
    parent_lat = parent_snapshot.get("latitude")
    parent_lng = parent_snapshot.get("longitude")
    if parent_lat is not None and parent_lng is not None:
        parent_coords: Optional[Tuple[float, float]] = (
            float(parent_lat),
            float(parent_lng),
        )
        print(f"   🌍 Parent city coords: lat={parent_lat}, lng={parent_lng}")
    else:
        parent_coords = None
        print("   ⚠️  Parent city coords missing; distance filter will be skipped for this city.")

    print(f"\n{'='*80}")
    print(f"📍 Processing: {city_name} (ID: {country_id})")
    print(f"{'='*80}\n")

    coll = db.collection("allplaces").document(country_id).collection(SUBCOLLECTION_NAME)
    all_docs = list(coll.stream())

    if not all_docs:
        print("   ⚠️  No places found\n")
        return {
            "total": 0,
            "updated": 0,
            "skipped": 0,
            "deleted": 0,
            "keep": 0,
            "review": 0,
            "demote": 0,
        }

    # Determine which docs are uncurated:
    # - uncurated if 'curation' is None or {} (empty dict)
    # - curated if 'curation' is a non-empty dict
    def is_uncurated(data: Dict) -> bool:
        cur = data.get("curation")
        return (cur is None) or (isinstance(cur, dict) and len(cur) == 0)

    docs = all_docs
    already_curated = 0
    if PROCESS_ONLY_NEW:
        filtered = []
        for d in all_docs:
            data = d.to_dict() or {}
            if is_uncurated(data):
                filtered.append(d)
            else:
                already_curated += 1
        print(
            f"   ℹ️  Found {len(all_docs)} docs | {len(filtered)} uncurated | "
            f"{already_curated} already curated (skipped)\n"
        )
        docs = filtered
        if not docs:
            # Nothing to process; return stats that reflect pre-filter skip count
            return {
                "total": 0,
                "updated": 0,
                "skipped": already_curated,
                "deleted": 0,
                "keep": 0,
                "review": 0,
                "demote": 0,
            }

    stats = {
        "total": len(docs),
        "updated": 0,
        "skipped": already_curated if PROCESS_ONLY_NEW else 0,
        "deleted": 0,
        "keep": 0,
        "review": 0,
        "demote": 0,
    }

    batch = db.batch()
    batch_ops = 0
    to_delete = []

    for idx, doc in enumerate(docs, 1):
        data = doc.to_dict() or {}
        name = data.get("name") or data.get("placeName") or "Unknown"

        print(f"[{idx}/{stats['total']}] {name}")

        # Assess the place
        assessment = assess_place(data, city_name, parent_coords)

        action = assessment["action"]
        is_top = assessment["is_top_suggested"]

        # Update action-based stats
        if action == "KEEP":
            stats["keep"] += 1
        elif action == "REVIEW":
            stats["review"] += 1
        else:  # DEMOTE
            stats["demote"] += 1

        # Display result
        icon = "✅" if action == "KEEP" else "🔍" if action == "REVIEW" else "❌"
        print(
            f"   {icon} {action} | Score: {assessment['final_score']} "
            f"(H:{assessment['heuristic_score']} + LLM:{assessment['llm_category']}/"
            f"{assessment['llm_confidence']:.2f})"
        )
        print(f"      {assessment['llm_reason']}")
        if assessment.get("distance_km") is not None:
            print(f"      Distance from city: {assessment['distance_km']:.1f} km")

        # Incremental skip for unchanged (only in CURATE_ONLY, and only if prior curation exists & is non-empty)
        if SKIP_IF_UNCHANGED and OPERATION_MODE == OperationMode.CURATE_ONLY:
            old_curation = data.get("curation")
            if isinstance(old_curation, dict) and len(old_curation) > 0:
                if (
                    old_curation.get("final_action") == action
                    and old_curation.get("is_top_suggested") == is_top
                    and old_curation.get("final_score") == assessment["final_score"]
                ):
                    print("      ⏭️  Unchanged, skipping")
                    stats["skipped"] += 1
                    continue

        # Prepare curation payload
        curation_data = {
            "curation": {
                "is_top_suggested": is_top,
                "final_action": action,
                "final_score": assessment["final_score"],
                "heuristic_score": assessment["heuristic_score"],
                "heuristic_triggers": assessment["heuristic_triggers"],
                "llm_category": assessment["llm_category"],
                "llm_confidence": assessment["llm_confidence"],
                "llm_reason": assessment["llm_reason"],
                "llm_iconic_restaurant": assessment["llm_iconic_restaurant"],
                "curated_at": firestore.SERVER_TIMESTAMP,
                "provider": AI_PROVIDER.value,
                "model": OPENAI_MODEL
                if AI_PROVIDER == AIProvider.OPENAI
                else ANTHROPIC_MODEL,
            }
        }

        # Handle operation modes
        if OPERATION_MODE == OperationMode.CURATE_ONLY:
            if not DRY_RUN:
                batch.update(doc.reference, curation_data)
                batch_ops += 1
                stats["updated"] += 1

                if batch_ops >= BATCH_SIZE:
                    batch.commit()
                    print(f"      💾 Committed batch of {batch_ops} docs")
                    batch = db.batch()
                    batch_ops = 0
            else:
                print("      🔍 DRY RUN - would update curation")
                stats["updated"] += 1

        elif OPERATION_MODE == OperationMode.DELETE_MODE:
            if action == "DEMOTE":
                to_delete.append(doc)
                if not DRY_RUN:
                    stats["deleted"] += 1
                else:
                    print("      🔍 DRY RUN - would delete")

        elif OPERATION_MODE == OperationMode.HYBRID:
            if not DRY_RUN:
                batch.update(doc.reference, curation_data)
                batch_ops += 1
                stats["updated"] += 1

                if action == "DEMOTE":
                    to_delete.append(doc)

                if batch_ops >= BATCH_SIZE:
                    batch.commit()
                    print(f"      💾 Committed batch of {batch_ops} docs")
                    batch = db.batch()
                    batch_ops = 0
            else:
                print("      🔍 DRY RUN - would update curation (hybrid)")
                stats["updated"] += 1
                if action == "DEMOTE":
                    print("      🔍 DRY RUN - would delete (hybrid)")

        # Log to CSV (only for processed docs)
        csv_rows.append([
            country_id,
            city_name,
            doc.id,
            name,
            action,
            is_top,
            assessment["final_score"],
            assessment["heuristic_score"],
            "|".join(assessment["heuristic_triggers"]),
            assessment["llm_category"],
            f"{assessment['llm_confidence']:.2f}",
            assessment["llm_reason"],
            assessment.get("rating", "N/A"),
            assessment.get("review_count", "N/A"),
            ",".join((data.get("types") or [])[:3]),
            doc.reference.path,
            assessment.get("distance_km", "N/A"),
        ])

        print()

    # Commit remaining batch ops
    if not DRY_RUN and batch_ops > 0:
        batch.commit()
        print(f"   💾 Committed final batch of {batch_ops} docs\n")

    # Handle deletions (DELETE/HYBRID live modes)
    if to_delete and not DRY_RUN:
        # If you might exceed 450/500 deletes, chunk this in future
        delete_batch = db.batch()
        deleted_rows = []
        for d in to_delete:
            delete_batch.delete(d.reference)
            deleted_rows.append([
                country_id,
                city_name,
                d.id,
                (d.to_dict() or {}).get("name", "Unknown"),
                datetime.now(timezone.utc).isoformat(),
            ])
        delete_batch.commit()
        print(f"   🗑️  Deleted {len(to_delete)} places\n")

        # Log deletions
        file_exists = os.path.exists(DELETE_LOG_PATH)
        with open(DELETE_LOG_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists or os.path.getsize(DELETE_LOG_PATH) == 0:
                writer.writerow(["country_id", "city", "doc_id", "name", "deleted_at"])
            writer.writerows(deleted_rows)

    # Summary
    print(f"{'─'*80}")
    print(f"Summary for {city_name}:")
    print(f"  Total considered: {len(all_docs)}")
    if PROCESS_ONLY_NEW:
        print(f"  • Already curated (skipped pre-filter): {already_curated}")
        print(f"  • Uncurated processed: {stats['total']}")
    else:
        print(f"  • Processed: {stats['total']}")
    denom = max(1, stats["total"])  # avoid div-by-zero
    print(f"  ✅ Keep: {stats['keep']} ({(stats['keep']/denom)*100:.1f}%)")
    print(f"  🔍 Review: {stats['review']} ({(stats['review']/denom)*100:.1f}%)")
    print(f"  ❌ Demote: {stats['demote']} ({(stats['demote']/denom)*100:.1f}%)")
    print(f"  📝 Updated: {stats['updated']}")
    print(f"  ⏭️  Skipped: {stats['skipped']}")
    if stats["deleted"] > 0:
        print(f"  🗑️  Deleted: {stats['deleted']}")
    print(f"{'─'*80}")

    return stats

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("🚀 Enhanced Top Attractions Curation System")
    print(f"{'='*80}")
    print(f"AI Provider: {AI_PROVIDER.value.upper()}")
    print(f"Model: {OPENAI_MODEL if AI_PROVIDER == AIProvider.OPENAI else ANTHROPIC_MODEL}")
    print(f"Operation Mode: {OPERATION_MODE.value.upper()}")
    print(f"Mode: {'DRY RUN ⚠️' if DRY_RUN else 'LIVE ✅'}")
    print(f"\nQuality Thresholds:")
    print(f"  Min Rating (Default): {MIN_RATING_DEFAULT} | Min Reviews: {MIN_REVIEWS_DEFAULT}")
    print(f"  Min Rating (Restaurant): {MIN_RATING_RESTAURANT} | Min Reviews: {MIN_REVIEWS_RESTAURANT}")
    print(f"  Score Thresholds: KEEP ≥{SCORE_THRESHOLD_KEEP}, REVIEW ≥{SCORE_THRESHOLD_REVIEW}")
    print(f"\nAllow: Restaurants={ALLOW_RESTAURANTS}, Shopping={ALLOW_SHOPPING}, Hotels={ALLOW_HOTELS}")
    print(f"Distance Filter: USE_DISTANCE_FILTER={USE_DISTANCE_FILTER}, "
          f"MAX_CITY_DISTANCE_KM={MAX_CITY_DISTANCE_KM}")
    print(f"{'='*80}\n")

    # Confirmation for destructive operations
    if not DRY_RUN and OPERATION_MODE in [OperationMode.DELETE_MODE, OperationMode.HYBRID]:
        confirm = input("⚠️  This will DELETE places. Type 'DELETE' to confirm: ")
        if confirm != "DELETE":
            print("❌ Cancelled.")
            return

    # Select countries
    countries = ONLY_COUNTRIES or list(COUNTRY_NAMES.keys())

    csv_rows: List[List] = []
    total_stats = {
        "total": 0,
        "updated": 0,
        "skipped": 0,
        "deleted": 0,
        "keep": 0,
        "review": 0,
        "demote": 0,
    }

    # Process each country
    for country_id in countries:
        if country_id not in COUNTRY_NAMES:
            print(f"⚠️  Unknown country ID: {country_id}, skipping\n")
            continue

        stats = process_country(country_id, csv_rows)

        # Aggregate stats
        for key in total_stats:
            total_stats[key] += stats[key]

    # Write CSV report
    if csv_rows:
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "country_id",
                "city",
                "doc_id",
                "name",
                "action",
                "is_top_suggested",
                "final_score",
                "heuristic_score",
                "heuristic_triggers",
                "llm_category",
                "llm_confidence",
                "llm_reason",
                "rating",
                "review_count",
                "types",
                "path",
                "distance_km",
            ])
            writer.writerows(csv_rows)
        print(f"\n📊 Report saved: {CSV_PATH}")

    # Final summary
    print(f"\n{'='*80}")
    print("🎉 CURATION COMPLETE")
    print(f"{'='*80}")
    print(f"Total Places Analyzed: {total_stats['total']}")
    if total_stats["total"] > 0:
        print(f"✅ Keep: {total_stats['keep']} ({total_stats['keep']/total_stats['total']*100:.1f}%)")
        print(f"🔍 Review: {total_stats['review']} ({total_stats['review']/total_stats['total']*100:.1f}%)")
        print(f"❌ Demote: {total_stats['demote']} ({total_stats['demote']/total_stats['total']*100:.1f}%)")
    else:
        print("No places processed.")

    if OPERATION_MODE in [OperationMode.CURATE_ONLY, OperationMode.HYBRID]:
        print(f"📝 Updated: {total_stats['updated']}")
        print(f"⏭️  Skipped (unchanged): {total_stats['skipped']}")

    if OPERATION_MODE in [OperationMode.DELETE_MODE, OperationMode.HYBRID]:
        print(f"🗑️  Deleted: {total_stats['deleted']}")
        if not DRY_RUN and total_stats["deleted"] > 0:
            print(f"📄 Deletion log: {DELETE_LOG_PATH}")

    print(f"💾 LLM Cache: {len(_cache)} entries in {CACHE_PATH}")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
