#!/usr/bin/env python3
"""
best_time_to_visit.py (or generate_and_store_travel_info_multi.py)
───────────────────────────────────────

For each (doc_id, label, locationContext) in DESTINATIONS:
 1) Generate a season-by-season guide via OpenAI for locationContext.
 2) Flatten "Regional Highlights" (if nested).
 3) Generate a 1–5 rating for each month for locationContext.
 4) Store under one Firestore doc named "best_time_to_visit":
      └─ label            → short display name (e.g. "Agartala")
      └─ locationContext  → full context (e.g. "Agartala, Tripura, India")
      └─ seasonal         → {Overview, “June to August”, …}
      └─ monthlyRatings   → {Jan:1–5, …, Dec:1–5}
"""

import os
import json
from json.decoder import JSONDecodeError

from openai import OpenAI
import firebase_admin
from firebase_admin import credentials, firestore

# ─── CONFIG ───────────────────────────────────────────────────────────────────
# (doc_id, short_label, full_location_context)
DESTINATIONS = [
    # ("909", "Agartala", "Agartala, Tripura, India"),
    # ("1006", "Gulmarg", "Gulmarg, Jammu and Kashmir, India"),
    # ("472", "Navi Mumbai", "Navi Mumbai, Maharashtra, India"),
    # ("1089", "Bikaner", "Bikaner, Rajasthan, India"),
    # ("1983", "Dwarka", "Dwarka, Gujarat, India"),
    # ("993", "Bhuj", "Bhuj, Gujarat, India"),
    # ("1025", "Ujjain", "Ujjain, Madhya Pradesh, India"),
    # ("1743", "Diu", "Diu, Gujarat, India"),
    # ("1874", "Vrindavan", "Vrindavan, Uttar Pradesh, India"),
    # ("981", "Jammu City", "Jammu City, Jammu and Kashmir, India"),
    # ("805", "Tiruchirappalli", "Tiruchirappalli, Tamil Nadu, India"),
    # ("1015", "Kolhapur", "Kolhapur, Maharashtra, India"),
    # ("2371", "Gokarna", "Gokarna, Karnataka, India"),
    # ("1241", "Alwar", "Alwar, Rajasthan, India"),
    # ("1365", "Bodh Gaya", "Bodh Gaya, Bihar, India"),
    # ("1750", "Somnath", "Somnath, Gujarat, India"),
    # ("536", "Thrissur", "Thrissur, Kerala, India"),
    # ("398", "Nagpur", "Nagpur, Maharashtra, India"),
    # ("2824", "Arpora", "Arpora, Maharashtra, India"),
    # ("1000", "Udupi", "Udupi, Karnataka, India"),
    # ("1808", "Daman", "Daman, Gujarat, India"),
    # ("1215", "Pachmarhi", "Pachmarhi, Madhya Pradesh, India"),
    # ("2791", "Lansdowne", "Lansdowne, Maharashtra, India"),
    # ("2027", "Orchha", "Orchha, Madhya Pradesh, India"),
    # ("527", "Kozhikode", "Kozhikode, Kerala, India"),
    # ("1406", "Jabalpur", "Jabalpur, Madhya Pradesh, India"),
    # ("1667", "Matheran", "Matheran, Maharashtra, India"),
    # ("785", "Allahabad", "Allahabad, Uttar Pradesh, India"),
    # ("1942", "Shimoga", "Shimoga, Karnataka, India"),
    # ("146301", "Khandala", "Khandala, Maharashtra, India"),
    # ("994", "Junagadh", "Junagadh, Gujarat, India"),
    # ("1529", "Chittaurgarh", "Chittaurgarh, Rajasthan, India"),
    # ("2178", "Panchgani", "Panchgani, Maharashtra, India"),
    # ("2149", "Arambol", "Arambol, Goa, India"),
    # ("1480", "Alibaug", "Alibaug, Maharashtra, India"),
    # ("1002", "Mathura", "Mathura, Uttar Pradesh, India"),
    # ("1302", "Kumbakonam", "Kumbakonam, Tamil Nadu, India"),
    # ("1407", "Kalimpong", "Kalimpong, West Bengal, India"),
    # ("1573", "Tawang", "Tawang, Arunachal Pradesh, India"),
    # ("976", "Kanchipuram", "Kanchipuram, Tamil Nadu, India"),
    # ("3369", "Konark", "Konark, Odisha, India"),
    # ("1212", "Vijayawada", "Vijayawada, Andhra Pradesh, India"),
    # ("894", "Rajkot", "Rajkot, Gujarat, India"),
    # ("2711", "Auroville", "Auroville, Tamil Nadu, India"),
    # ("1159", "Bundi", "Bundi, Rajasthan, India"),
    # ("146275", "Anjuna", "Anjuna, Goa, India"),
    # ("1177", "Ranchi", "Ranchi, Jharkhand, India"),
    # ("798", "Raipur", "Raipur, Chhattisgarh, India"),
    # ("714", "Kollam", "Kollam, Kerala, India"),
    # ("1540", "Ponda", "Ponda, Goa, India"),
    # ("1463", "Vellore", "Vellore, Tamil Nadu, India"),
    # ("1444", "Malvan", "Malvan, Maharashtra, India"),
    # ("979", "Palakkad", "Palakkad, Kerala, India"),
    # ("830", "Dehradun", "Dehradun, Uttarakhand, India"),
    # ("1059", "Idukki", "Idukki, Kerala, India"),
    # ("2401", "Silvassa", "Silvassa, Gujarat, India"),
    # ("982", "Ratnagiri", "Ratnagiri, Maharashtra, India"),
    # ("730", "Jamnagar", "Jamnagar, Gujarat, India"),
    # ("2540", "Kumarakom", "Kumarakom, Kerala, India"),
    # ("1473", "Kutch", "Kutch, Gujarat, India"),
    # ("1863", "Kullu", "Kullu, Himachal Pradesh, India"),
    # ("2177", "Gandhinagar", "Gandhinagar, Gujarat, India"),
    # ("1765", "Almora", "Almora, Uttarakhand, India"),
    # ("1763", "Jamshedpur", "Jamshedpur, Jharkhand, India"),
    # ("1135", "Kanpur", "Kanpur, Uttar Pradesh, India"),
    # ("3129", "Digha", "Digha, West Bengal, India"),
    # ("1852", "Kurnool", "Kurnool, Andhra Pradesh, India"),
    # ("849", "Kottayam", "Kottayam, Kerala, India"),
    # ("948", "Satara", "Satara, Maharashtra, India"),
    # ("1013", "Imphal", "Imphal, Manipur, India"),
    # ("1668", "Hassan", "Hassan, Karnataka, India"),
    # ("1442", "Warangal", "Warangal, Telangana, India"),
    # ("1251", "Salem", "Salem, Tamil Nadu, India"),
    # ("2185", "Kargil", "Kargil, Jammu and Kashmir, India"),
    # ("2919", "Badrinath", "Badrinath, Uttarakhand, India"),
    # ("3260", "Bhimtal", "Bhimtal, Uttarakhand, India"),
    # ("909", "Agartala", "Agartala, Tripura, India"),
    # ("1679", "Panchkula", "Panchkula, Haryana, India"),
    # ("923", "Ghaziabad", "Ghaziabad, Uttar Pradesh, India"),
    # ("1163", "Jalandhar", "Jalandhar, Punjab, India"),
    # ("1218", "Kota", "Kota, Rajasthan, India"),
    # ("1331", "Porbandar", "Porbandar, Gujarat, India"),
    # ("1114", "Siliguri", "Siliguri, West Bengal, India"),
    # ("1581", "Hubli-Dharwad", "Hubli-Dharwad, Karnataka, India"),
    # ("2141", "Kohima", "Kohima, Nagaland, India"),
    # ("3466", "Patnitop", "Patnitop, Sikkim, India"),
    # ("1812", "Patiala", "Patiala, Punjab, India"),
    # ("2040", "Kurukshetra", "Kurukshetra, Haryana, India"),
    # ("1418", "Mandi", "Mandi, Himachal Pradesh, India"),
    # ("1306", "Faridabad", "Faridabad, Haryana, India"),
    #  ("349", "Darjeeling", "India"),
    # ("2477", "Auli", "Auli, Uttarakhand, India"),
    # ("86944", "Andaman and Nicobar Islands", "Andaman and Nicobar Islands, India"),
    # ("87277", "Daman and Diu", "Daman and Diu, India"),
    # ("87478", "Dadra and Nagar Haveli", "Dadra and Nagar Haveli, India"),
    # ("88144", "Lakshadweep", "Lakshadweep, India"),
    # ("2643", "Ayodhya", "India"),
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
#     ("581","Calangute","India"),   
#     ("1597","Sohra","India"),
#     ("7268","Sadri","India"),
# ("8658","Ajanta","India"),
# ("6474","Sinquerim","India"),   
# ("3639","Colva","India)",   
# 2084	Cavelossim	India
# 3543	Bharatpur	India
# 2941	Benaulim	India
    # ("8658","Ajanta","India"),
    # ("6474", "Sinquerim", "India"),
    # ("3639", "Colva", "India"),
    # ("2084", "Cavelossim", "India"),
    # ("3543", "Bharatpur", "India"),
    # ("2941", "Benaulim", "India"),
    # ("2231", "Guruvayur", "India"),
    # ("4705", "Rajsamand", "India"),
    # ("5592", "Varca", "India"),
    # ("2644", "Morjim", "India"),
    # ("4957", "Mararikulam", "India"),
    # ("1854", "Saputara", "India"),
    # ("4656", "Lachung", "India"),
    # ("1402", "Chamba", "India"),
    # ("3110", "Badami", "India"),
    # ("7541", "Belur", "India"),
    # ("1083", "Mandu", "India"),
    # ("2776", "Chikkaballapur", "India"),
    # ("5126", "Vythiri", "India"),
    # ("1555", "Bijapur", "India"),
    # ("7167", "Abhaneri", "India"),
    # ("3089", "Mandrem", "India"),
    # ("3354", "Kausani", "India"),
    # ("146272", "Naggar", "India"),
    # ("7513", "Kanoi", "India"),
    # ("8421", "Chapora", "India"),
    # ("3127", "Mukteshwar", "India"),
    # ("4594", "Athirappilly", "India"),
    # ("4414", "Dona Paula", "India"),
    # ("4639", "Bandipur", "India"),
    # ("1312", "Kasaragod", "India"),
    # ("827", "Tirunelveli", "India"),
    # ("1436", "Murshidabad", "India"),
    # ("2885", "Srirangapatna", "India"),
    # ("1479", "Karwar", "India"),
    # ("146303", "Kumily", "India"),
    # ("2731", "Ranikhet", "India"),
    # ("9024", "Mandarmani", "India"),
    # ("2654", "Valparai", "India"),
    # ("4744", "Halebid", "India"),
    # ("8767", "Bhimashankar", "India"),
    # ("4995", "Mawlynnong", "India"),
    # ("3635", "Nathdwara", "India"),
    # ("1561", "Kangra", "India"),
    # ("2692", "Ravangla", "India"),
    # ("2760", "Dapoli", "India"),
    # ("5464", "Kashid", "India"),
    # ("4655", "Velankanni", "India"),
    # ("8064", "Deshnoke", "India"),
    # ("1250", "Mandya", "India"),
    # ("3404", "Sangla", "India"),
    # ("3710", "Srisailam", "India"),
    # ("2936", "Thiruvannamalai", "India"),
    # ("6608", "Belakavadi", "India"),
    # ("1699", "Margao", "India"),
    # ("3197", "Vagamon", "India"),
    # ("6897", "Utorda", "India"),
    # ("2935", "Yelagiri", "India"),
    # ("2336", "Chamoli", "India"),
    # ("7824", "Malsi", "India"),
    # ("1183", "Raigad", "India"),
    # ("5492", "Lepakshi", "India"),
    # ("7536", "Kottagudi", "India"),
    # ("5776", "Belur", "India"),
    # ("3230", "Murdeshwar", "India"),
    # ("146277", "Candolim", "India"),
    # ("4195", "Dandeli", "India"),
    # ("5422", "Dhanaulti", "India"),
    # ("2609", "Santiniketan", "India"),
    # ("3187", "Mandvi", "India"),
    # ("6377", "Magadi", "India"),
    # ("2121", "Deoghar", "India"),
    # ("4238", "Mandla", "India"),
    # ("4467", "Kaushambi", "India"),
    # ("3419", "Chidambaram", "India"),
    # ("5833", "Daulatabad", "India"),
    # ("1659", "Vasco da Gama", "India"),
    # ("1949", "Greater Noida", "India"),
    # ("959", "Midnapore", "India"),
    # ("1531", "Ahmednagar", "India"),
    # ("5779", "Bhagamandala", "India"),
    # ("1729", "Gaya", "India"),
    # ("4528", "Kaza", "India"),
    # ("1754", "Bharuch", "India"),
    # ("3446", "Palani", "India"),
    # ("1776", "Rajgir", "India"),
    # ("4692", "Cansaulim", "India"),
    # ("5460", "Diveagar", "India"),
    # ("6939", "Malpe", "India"),
    # ("3691", "Majorda", "India"),
    # ("3231", "Kollur", "India"),
    # ("4227", "Dharmapuri", "India"),
    # ("5095", "Hospet", "India"),
    # ("1997", "Tiruvannamalai", "India"),
    # ("1999", "Maheshwar", "India"),
    # ("2843", "Namchi", "India"),
    # ("4211", "Bekal", "India"),
    # ("6012", "Muttukadu", "India"),
    # ("2839", "Sonamarg", "India"),
    # ("3148", "Omkareshwar", "India"),
    # ("3487", "Subramanya", "India"),
    # ("2736", "Bidar", "India"),
    # ("2131", "Solapur", "India"),
    # ("2575", "Rajahmundry", "India"),
    # ("3630", "Shrivardhan", "India"),
    # ("7929", "Kas", "India"),
    # ("2660", "Chitradurga", "India"),
    # ("4933", "Narkanda", "India"),
    # ("2018", "Cuttack", "India"),
    # ("3396", "Ambaji", "India"),
    # ("2562", "Jhansi", "India"),
    # ("2707", "Sanchi", "India"),
    # ("1970", "Bishnupur", "India"),
    # ("3192", "Munsiyari", "India"),
    # ("3376", "Manipal", "India"),
    # ("6910", "Shingnapur", "India"),
    # ("7184", "Sasan Gir", "India"),
    # ("1759", "Valsad", "India"),
    # ("1690", "Chittoor", "India"),
    # ("1604", "Sirsi", "India"),
    # ("3890", "Pattadakal", "India"),
    # ("2038", "Kolar", "India"),
    # ("2366", "Balasore", "India"),
    # ("7362", "Naukuchiatal", "India"),
    # ("2160", "Amarkantak", "India"),
    # ("3167", "Srikalahasti", "India"),
    # ("1792", "Meerut", "India"),
    # ("1733", "Belgaum", "India"),
    # ("5941", "Modhera", "India"),
    # ("4276", "Kedarnath", "India"),
    # ("2664", "Durgapur", "India"),
    # ("1678", "Sambalpur", "India"),
    # ("1354", "Dindigul", "India"),
    # ("3064", "Puttaparthi", "India"),
    # ("1730", "Gorakhpur", "India"),
    # ("6609", "Dabguli", "India"),
    # ("4524", "Betalbatim", "India"),
    # ("2374", "Mapusa", "India"),
    # ("4652", "Murud", "India"),
    # ("2263", "Theni", "India"),
    # ("4275", "Kotagiri", "India"),
    # ("3304", "Igatpuri", "India"),
    # ("1936", "Pathanamthitta", "India"),
    # ("3115", "Jagdalpur", "India"),
    # ("1781", "Hooghly", "India"),
    # ("1660", "Malappuram", "India"),
    # ("1523", "Secunderabad", "India"),
    # ("146284", "Chinnakanal", "India"),
    # ("3349", "Dhanbad", "India"),
    # ("8980", "Dawki", "India"),
    # ("2365", "Mohali", "India"),
    # ("2976", "Katra", "India"),
    # ("2990", "Sringeri", "India"),
    # ("2123", "Jalpaiguri", "India"),
    # ("1706", "Tumkur", "India"),
    # ("2643", "Ayodhya", "India"),
    # ("8908", "Kannan Devan Hills", "India"),
    # ("4568", "Chakrata", "India"),
    # ("4760", "Manikaran", "India"),
    # ("4318", "Dharmasthala", "India"),
    # ("6149", "Lavasa", "India"),
    # ("146305", "Patnem", "India"),
    # ("7175", "Jwalamukhi", "India"),
    # ("3118", "Purulia", "India"),
    # ("146304", "Palolem", "India"),
    # ("5027", "Sakleshpur", "India"),
    # ("2953", "Nalanda", "India"),
    # ("4340", "Osian", "India"),
    # ("2877", "Chitrakoot", "India"),
    # ("1996", "Tezpur", "India"),
    # ("1793", "Nellore", "India"),
    # ("1139", "Nagapattinam", "India"),
    # ("3900", "Pandharpur", "India"),
    # ("4536", "Cherai Beach", "India"),
    # ("6841", "Ukhimath", "India"),
    # ("7306", "Hunsur", "India"),
    # ("4315", "New Tehri", "India"),
    # ("6969", "Naldehra", "India"),
    # ("2042", "Aizawl", "India"),
    # ("3608", "Virar", "India"),
    # ("2541", "Anantnag", "India"),
    # ("2778", "Bellary", "India"),
    # ("7044", "Hemis", "India"),
    # ("6037", "Pinjore", "India"),
    # ("6746", "Baratang Island", "India"),
    # ("4237", "Chhatarpur", "India"),
    # ("4520", "Dabolim", "India"),
    # ("7159", "Gandikota", "India"),
    # ("6706", "Anjarle", "India"),
    # ("2252", "Uttarkashi", "India"),
    # ("8986", "Salasar", "India"),
    # ("1862", "Ganjam", "India"),
    # ("3299", "Anand", "India"),
    # ("4240", "Sikar", "India"),
    # ("6107", "Nawalgarh", "India"),
    # ("2743", "Gondia", "India"),
    # ("3401", "Nanded", "India"),
    # ("4947", "Gangotri", "India"),
    # ("3150", "Champaner", "India"),
    # ("8350", "Taoru", "India"),
    # ("2046", "Bilaspur", "India"),
    # ("2806", "Sangli", "India"),
    # ("4221", "Bogmalo", "India"),
    # ("4603", "Kalyan", "India"),
    # ("2925", "Courtallam", "India"),
    # ("4181", "Palitana", "India"),
    # ("2596", "Rupnagar", "India"),
    # ("4274", "Ranakpur", "India"),
    # ("2657", "Nalgonda", "India"),
    # ("3405", "Kumbhalgarh", "India"),
    # ("2677", "Adilabad", "India"),
    # ("7272", "Deeg", "India"),
    # ("2811", "Namakkal", "India"),
    # ("2909", "Mehsana", "India"),
    # ("5666", "Kakkabe", "India"),
    # ("5246", "Sanguem", "India"),
    # ("8571", "Channarayapatna", "India"),
    # ("2959", "Mirzapur", "India"),
    # ("7774", "Thirunageswaram", "India"),
    # ("3907", "Thenmala", "India"),
    # ("4300", "Pavagadh", "India"),
    # ("3620", "Kamshet", "India"),
    # ("3590", "Kakinada", "India"),
    # ("4497", "Araku Valley", "India"),
    # ("7200", "Sattal", "India"),
    # ("2828", "Pimpri-Chinchwad", "India"),
    # ("3904", "Agumbe", "India"),
    # ("5043", "Junnar", "India"),
    # ("2867", "Kushinagar", "India"),
    # ("1782", "Bankura", "India"),
    # ("2615", "Gulbarga", "India"),
    # ("2833", "Chalakudy", "India"),
    # ("5562", "Thiruchendur", "India"),
    # ("146319", "Joshimath", "India"),
    # ("3906", "Yuksom", "India"),
    # ("4910", "Mormugao", "India"),
    # ("8994", "Talacauvery", "India"),
    # ("3762", "Karkala", "India"),
    # ("3764", "Bomdila", "India"),
    # ("3629", "Panvel", "India"),
    # ("3636", "Asansol", "India"),
    # ("2373", "Nahan", "India"),
    # ("2617", "Brahmapur", "India"),
    # ("1591", "Jorhat", "India"),
    # ("146268", "Zirakpur", "India"),
    # ("3994", "Tarkarli", "India"),
    # ("5264", "Nuvem", "India"),
    # ("4204", "Mirik", "India"),
    # ("2265", "Erode", "India"),
    # ("4071", "Kundapur", "India"),
    # ("5662", "Bakkhali", "India"),
    # ("9569", "Kaladhungi", "India"),
    # ("2345", "Jowai", "India"),
    # ("6250", "Tarapith", "India"),
    # ("4642", "Channapatna", "India"),
    # ("6196", "Sarahan", "India"),
    # ("4415", "Ramnagar", "India"),
    # ("4569", "Guhagar", "India"),
    # ("2458", "Malda", "India"),
    # ("7430", "Ramnagar", "India"),
    # ("1532", "Amravati", "India"),
    # ("3338", "Panipat", "India"),
    # ("2910", "Guntur", "India"),
    # ("6336", "Sariska", "India"),
    # ("3125", "Bareilly", "India"),
    # ("3748", "Keylong", "India"),
    # ("4852", "Tapovan", "India"),
    # ("6444", "Geyzing", "India"),
    # ("7158", "Yana", "India"),
    # ("2972", "Aritar", "India"),
    # ("5843", "Gudalur", "India"),
    # ("1481", "Sirohi", "India"),
    # ("4896", "Kalady", "India"),
    # ("3371", "Karauli", "India"),
    # ("3619", "Chamarajanagar", "India"),
    # ("4588", "Narmada", "India"),
    # ("4790", "Sonipat", "India"),
    # ("5230", "Lonar", "India"),
    # ("5363", "Kanakapura", "India"),
    # ("6338", "Dahanu", "India"),
    # ("3588", "Karnal", "India"),
    # ("3382", "Vasai", "India"),
    # ("5974", "Ashvem Beach", "India"),
    # ("3082", "Cuddalore", "India"),
    # ("3839", "Pudukkottai", "India"),
    # ("2292", "Ambala", "India"),
    # ("3394", "Kheda", "India"),
    # ("5088", "Nagercoil", "India"),
    # ("6265", "Loutolim", "India"),
    # ("3157", "Cooch Behar", "India"),
    # ("4691", "Yamunotri", "India"),
    # ("5459", "Covelong", "India"),
    # ("6726", "North Paravur", "India"),
    # ("4405", "Bordi", "India"),
    # ("1917", "Jalgaon", "India"),
    # ("2476", "Barmer", "India"),
    # ("5736", "Kodanad", "India"),
    # ("7918", "Dabaspete", "India"),
    # ("4670", "Somvarpet", "India"),
    # ("5525", "Tiruchendur", "India"),
    # ("2271", "Pali", "India"),
    # ("2783", "Durg", "India"),
    # ("5365", "Mantralayam", "India"),
    # ("4998", "Bhadrachalam", "India"),
    # ("2781", "Kurseong", "India"),
    # ("4353", "Rohtak", "India"),
    # ("6277", "Vikarabad", "India"),
    # ("2610", "Jhunjhunu", "India"),
    # ("4271", "Bhilai", "India"),
    # ("5012", "Datia", "India"),
    # ("9099", "Ramgarh", "India"),
    # ("3838", "Dausa", "India"),
    # ("7952", "Someshwar", "India"),
    # ("2662", "Vengurla", "India"),
    # ("8610", "Alchi", "India"),
    # ("3355", "Kolad", "India"),
    # ("3568", "Bokaro Steel City", "India"),
    # ("5627", "Baramati", "India"),
    # ("5850", "Nadia", "India"),
    # ("3373", "Haldwani", "India"),
    # ("3345", "Itanagar", "India"),
    # ("6692", "Tharangambadi", "India"),
    # ("3099", "Aligarh", "India"),
    # ("5020", "Bambolim", "India"),
    # ("2404", "Bilaspur", "India"),
    # ("3763", "Tinsukia", "India"),
    # ("5017", "Devprayag", "India"),
    # ("6182", "Majuli", "India"),
    # ("6442", "Bhojpur", "India"),
    # ("3529", "Thiruvalla", "India"),
    # ("4218", "Bolpur", "India"),
    # ("4604", "Srivilliputhur", "India"),
    # ("7646", "Karnala", "India"),
    # ("2992", "Hosur", "India"),
    # ("3680", "Giridih", "India"),
    # ("6736", "Nalsarovar", "India"),
    # ("8340", "Kufri", "India"),
    # ("6802", "Bapatla", "India"),
    # ("7673", "Hazira", "India"),
    # ("1892", "Thalassery", "India"),
    # ("1947", "Nagaur", "India"),
    # ("3476", "Sibsagar", "India"),
    # ("5019", "Kushalnagar", "India"),
    # ("9457", "Kammasandra", "India"),
    # ("1883", "Tiruppur", "India"),
    # ("2651", "Vidisha", "India"),
    # ("2636", "Anantapur", "India"),
    # ("9547", "Hunder", "India"),
    # ("3353", "Navsari", "India"),
    # ("2599", "Thiruvarur", "India"),
    # ("3067", "Una", "India"),
    # ("3092", "Yellapur", "India"),
    # ("3286", "Kodungallur", "India"),
    # ("2784", "Faizabad", "India"),
    # ("6215", "Chandor", "India"),
    # ("8949", "Thattekad", "India"),
    # ("7331", "Nagarjuna Sagar", "India"),
    # ("2738", "Pathankot", "India"),
    # ("4485", "Nileshwar", "India"),
    # ("4679", "Nelliyampathy", "India"),
    # ("5763", "Karaikal", "India"),
    # ("6528", "Peermade", "India"),
    # ("7416", "Gangolihat", "India"),
    # ("4096", "Talakad", "India"),
    # ("3991", "Chiplun", "India"),
    # ("2128", "Wardha", "India"),
    # ("4590", "Karjat Town", "India"),
    # ("5178", "Jaunpur", "India"),
    # ("7275", "Shegaon", "India"),
    # ("3060", "Mahbubnagar", "India"),
    # ("6273", "Mukutmanipur", "India"),
    # ("6468", "Thariyode", "India"),
    # ("7255", "Diskit", "India"),
    # ("9354", "Temi", "India"),
    # ("2388", "Chanderi", "India"),
    # ("6664", "Tenkasi", "India"),
    # ("3631", "Dimapur", "India"),
    # ("4628", "Buldana", "India"),
    # ("7319", "Salangpur", "India"),
    # ("1985", "Munger", "India"),
    # ("3860", "Panna", "India"),
    # ("6867", "Zuluk", "India"),
    # ("4020", "Hogenakkal", "India"),
    # ("4631", "Chandannagar", "India"),
    # ("8037", "Lamayuru", "India"),
    # ("5209", "Nilambur", "India"),
    # ("1787", "Keonjhar", "India"),
    # ("3661", "Chandrapur", "India"),
    # ("4027", "Burhanpur", "India"),
    # ("4891", "Medak", "India"),
    # ("5765", "Annavaram", "India"),
    # ("9510", "Gingee", "India"),
    # ("2780", "Muzaffarpur", "India"),
    # ("4225", "Tuljapur", "India"),
    # ("1060","Ludhiana","India"),
    #     ('581','Calangute','India'),
    # ('7268','Sadri','India'),
    # ('1664','Fatehpur Sikri','India')
    # ('2150','Agonda','India'),
    # ('1424','Bhavnagar', 'India'),
    # ('2083','Howrah','India'),
    # ('2264','Rourkela','India'),
    # ('2346','Bathinda','India')
("60831", "Hammond", "United States"),
    ("131457", "Casablanca", "Chile"),
    ("248", "Lhasa", "China"),
    ("58455", "Athens", "United States"),

    

]

SERVICE_ACCOUNT_JSON = (
    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
    r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
)

OPENAI_MODEL = "gpt-4.1-mini"

# ─── INITIALIZE OPENAI & FIRESTORE ────────────────────────────────────────────
api_key = "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"
if not api_key:
    raise RuntimeError("Missing OPENAI_API_KEY environment variable")

client = OpenAI(api_key=api_key)

if not firebase_admin._apps:
    cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
    firebase_admin.initialize_app(cred)
db = firestore.client()


# ─── SECTION GENERATORS ────────────────────────────────────────────────────────
def generate_guide(location_context: str) -> dict:
    """
    Generate a season-by-season guide as JSON with these top-level keys:
      "Overview",
      "December to February",
      "March to May",
      "June to August",
      "September to November",
      "Regional Highlights",
      "Travel Tip"
    for the given location_context (e.g. "Agartala, Tripura, India").
    """
    system_msg = "You are a JSON generator. Output MUST BE valid JSON and nothing else."
    user_prompt = f"""
Generate a JSON object with exactly these top-level keys:
  "Overview",
  "December to February",
  "March to May",
  "June to August",
  "September to November",
  "Regional Highlights",
  "Travel Tip"

Fill each with detailed travel-guide text for the destination {location_context}.
Describe climate, crowds, and what travelers can do in each season, taking into
account that this place is located in its parent state/region and country.
Do NOT output markdown, code fences, or any extra keys."""
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
        print(f"⚠️ Failed to parse guide JSON for {location_context}. Raw response:\n{content}")
        raise


def flatten_guide(raw: dict) -> dict:
    """
    If any values are nested dicts (e.g. inside "Regional Highlights"),
    flatten them into "Parent – Child" keys. Otherwise pass through.
    """
    flat = {}
    for key, val in raw.items():
        if isinstance(val, dict):
            for subkey, subval in val.items():
                flat[f"{key} – {subkey}"] = subval
        else:
            flat[key] = val
    return flat


def generate_monthly_ratings(location_context: str) -> dict:
    """
    Rate each calendar month (Jan–Dec) for best time to visit location_context
    on a scale of 1 (worst) to 5 (best).
    """
    system_msg = "You are a JSON-only generator. Output MUST BE valid JSON."
    user_prompt = f"""
Rate each calendar month (Jan–Dec) for best time to visit {location_context}
on a scale of 1 (worst) to 5 (best), considering its climate, weather patterns,
and tourism seasons within its parent region and country.

Output only a JSON object with keys:
"Jan", "Feb", "Mar", "Apr", "May", "Jun",
"Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
and integer values 1–5."""
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
        print(f"⚠️ Failed to parse monthly-ratings JSON for {location_context}. Raw response:\n{content}")
        raise


# ─── MAIN WORKFLOW ─────────────────────────────────────────────────────────────
def main():
    for doc_id, label, context in DESTINATIONS:
        print(f"\n🛠 Processing {label} (doc {doc_id}) – context: {context} …")

        # 1. Build the “best time to visit” pieces
        raw_guide = generate_guide(context)
        flat_guide = flatten_guide(raw_guide)
        monthly_ratings = generate_monthly_ratings(context)

        # 2. Combine into one payload for Firestore
        best_time_payload = {
            "label": label,
            "locationContext": context,
            "seasonal": flat_guide,
            "monthlyRatings": monthly_ratings,
        }

        # 3. Store in Firestore under travelInfo subcollection
        ti_coll = db.collection("allplaces").document(doc_id).collection("travelInfo")
        print(f"☁️ Writing best_time_to_visit to allplaces/{doc_id}/travelInfo/best_time_to_visit")
        ti_coll.document("best_time_to_visit").set(best_time_payload)

        print("✅ Done.")

    print("\n🎉 All destinations processed.")


if __name__ == "__main__":
    main()





# #!/usr/bin/env python3
# """
# generate_and_store_travel_info_multi.py
# ───────────────────────────────────────

# For each (doc_id, country_name) in COUNTRIES:
#  1) Generate a season-by-season guide via OpenAI.
#  2) Flatten "Regional Highlights".
#  3) Generate a 1–5 rating for each month.
#  4) Store under one Firestore doc named "best_time_to_visit":
#       └─ seasonal        → {Overview, “June to August”, …}
#       └─ monthlyRatings  → {Jan:1–5, …, Dec:1–5}
#  5) (Placeholders) Store other sections as their own docs:
#       ├─ things_to_know
#       ├─ ways_to_get_around
#       └─ visa_requirements
# """

# import os
# import json
# from json.decoder import JSONDecodeError

# import openai
# import firebase_admin
# from firebase_admin import credentials, firestore

# # ─── CONFIG ───────────────────────────────────────────────────────────────────
# COUNTRIES = [
     

#     # New entries you just provided (Various Provinces/Governorates)

   
#     # ("131392", "Salento"),
#     # ("186", "Kushiro"),
#     # ("79348", "Swakopmund"),
#     # ("346", "Shimoda"),
#     # ("59317", "Cripple Creek"),
#     # ("81251", "Trois-Ilets"),
#     # ("83361", "Papetoai"),
#     # ("79331", "Ouarzazate"),
#     # ("79335", "Windhoek"),
#     # ("79522", "Midoun"),
#     # ("392", "Mirissa"),
#     # ("83076", "South Perth"),
#     # ("58098", "Vernon"),
#     # ("10658", "Leeuwarden"),
#     # ("79608", "Storms River"),
#     # ("13208", "Lisbon"),
#     # ("82822", "Arrowtown"),
#     # ("11009", "Recanati"),
#     # ("519", "Tangerang"),
#     # ("81969", "Taxco"),
#     # ("58566", "Montauk"),
#     # ("14462", "Bermeo"),
#     # ("1015", "Kolhapur"),
#     # ("59224", "Hill City"),
#     # ("60594", "King of Prussia"),
#     # ("131679", "Mateiros"),
#     # ("154", "Odawara"),
#     # ("258", "Quezon City")
#     # ("131175", "Itacare"),
#     # ("131327", "Bombinhas"),
#     # ("58309", "Hilo"),
#     # ("9827", "Cannes"),
#     # ("131140", "Puno"),
#     # ("131107", "Santa Marta"),
#     # ("110", "Hakodate"),
#     # ("58211", "Fort Myers"),
#     # ("256", "Srinagar"),
#     # ("131131", "Sao Luis"),
#     # ("43", "Colombo"),
#     # ("81918", "Puebla"),
#     # ("79308", "Casablanca"),
#     #  ("9887", "Cadiz"),
#     # ("131447", "Caldas Novas"),
#     # ("58345", "Wisconsin Dells"),
#     # ("10134", "Burgos"),
#     # ("81219", "Puerto Plata"),
#     # ("9769", "Kaliningrad"),
#     # ("131145", "Blumenau"),
#     # ("480", "Ooty (Udhagamandalam)"),
#     # ("10241", "Playa Blanca"),
#     # ("12491", "Mdina"),
#     # ("58213", "Omaha")
#     # ("85939", "Dubai"),
#     # ("9630", "Florence"),
#     # ("9636", "Edinburgh"),
#     # ("82574", "Sydney"),
#     # ("15", "Hong Kong"),
#     # ("9618", "St. Petersburg"),
#     # ("2", "Kyoto"),
#     # ("9637", "Athens"),
#     # ("58150", "San Diego"),
#     # ("9643", "Copenhagen"),
#     # ("11", "Siem Reap"),
#     # ("9635", "Naples"),
#     # ("9649", "Brussels"),
#     # ("81905", "Playa del Carmen"),
#     # ("9707", "York"),
#     # ("131100", "Natal"),
#     # ("3", "Osaka"),
#     # ("131086", "Florianopolis"),
#     # ("18", "Taipei"),
#     # ("131118", "Porto Seguro"),
#     # ("7865", "Mumbai"),
#     # ("82576", "Auckland"),
#     # ("9745", "Bruges"),
#     # ("58186", "St. Augustine"),
#     # ("9798", "Blackpool"),
#     # ("58183", "Branson"),
#     # ("22", "Ubud"),
#     # ("78744", "Panama City"),
#     # ("24", "Jaipur"),
#     # ("9656", "Reykjavik"),
#     # ("82578", "Gold Coast"),
#     # ("58170", "Charleston"),
#     # ("53", "Kathu"),
#     # ("9711", "Verona"),
#     # ("10413", "Marne-la-Vallee"),
#     # ("58175", "Saint Louis"),
#     # ("9683", "Genoa"),
#     # ("9666", "Birmingham"),
#     # ("9670", "Lyon"),
#     # ("9735", "Funchal"),
#     # ("80", "Pattaya"),
#     # ("9730", "Bath"),
#     # ("81904", "Cancun"),
#     # ("81180", "Punta Cana"),
#     # ("9654", "Palermo"),
#     # ("9679", "Tallinn"),
#     # ("131150", "Ipojuca"),
#     # ("131090", "San Carlos de Bariloche"),
#     # ("81909", "Tulum"),
#     # ("21", "Hoi An"),
#     # ("19", "Chiang Mai"),
#     # ("131083", "Mendoza"),
#     # ("9767", "Pisa"),
#     # ("131078", "Quito"),
#     # ("131132", "Petropolis"),
#     # ("37", "Kuta"),
#     # ("131115", "Maceio"),
#     # ("79", "Krabi Town"),
#     # ("131134", "Jijoca de Jericoacoara"),
#     # ("28", "Taito"),
#     # ("131094", "Paraty"),
#     # ("9712", "Cardiff"),
#     # ("9792", "Maspalomas"),
#     # ("82593", "Rotorua"),
#     # ("9826", "Adeje"),
#     # ("9800", "Syracuse"),
#     # ("9751", "Strasbourg"),
#     # ("9663", "Bordeaux"),
#     # ("9763", "Paphos"),
#     # ("82585", "Canberra"),
#     # ("9802", "Siena"),
#     # ("9696", "Bratislava"),
#     # ("23", "Minato"),
#     # ("10219", "Windsor"),
#     # ("29", "Sapporo"),
#     # ("9803", "Portsmouth"),
#     # ("9668", "Zurich"),
#     # ("85940", "Tel Aviv"),
#     # ("10061", "Versailles"),
#     # ("38", "Shibuya"),
#     # ("58187", "Sarasota"),
#     # ("9801", "Bergen"),
#     # ("131079", "Medellin"),
#     # ("184", "Luang Prabang"),
#     # ("79309", "Sharm El Sheikh"),
#     # ("9850", "Chester"),
#     # ("131318", "Machu Picchu"),
#     # ("20", "Yokohama"),
#     # ("58049", "Ottawa"),
#     # ("81226", "Varadero"),
#     # ("9724", "Geneva"),
#     # ("58182", "Greater Palm Springs"),
#     # ("10008", "Lucerne"),
#     # ("82580", "Perth"),
#     # ("9690", "Padua"),
#     # ("9889", "Benalmadena"),
#     # ("82581", "Adelaide"),
#     # ("9723", "Oxford"),
#     # ("9836", "Killarney"),
#     # ("9677", "Sochi"),
#     # ("58191", "Albuquerque"),
#     # ("51", "Phuket Town"),
#     # ("32", "Shinjuku"),
#     # ("9765", "Albufeira"),
#     # ("42", "Chiyoda"),
#     # ("59", "Hiroshima"),
#     # ("131093", "Angra Dos Reis"),
#     # ("9840", "Lucca"),
#     # ("9689", "Leeds"),
#     # ("81194", "Nassau"),
#     # ("10046", "Ravenna"),
#     # ("9687", "Belgrade"),
#     # ("131117", "Ouro Preto"),
#     # ("9725", "The Hague"),
#     # ("9691", "Trieste"),
#     # ("9930", "Stratford-upon-Avon"),
#     # ("9694", "Split"),
#     # ("9891", "Matera"),
#     # ("9935", "Taormina"),
#     # ("9998", "Llandudno"),
#     # ("79306", "Johannesburg"),
#     # ("11769", "Lindos"),
#     # ("58198", "Anchorage"),
#     # ("9709", "Toulouse"),
#     # ("9676", "Sofia"),
#     # ("9838", "Santiago de Compostela"),
#     # ("9945", "Assisi"),
#     # ("9756", "Rimini"),
#     # ("30", "Nagoya"),
#     # ("9799", "Nantes"),
#     # ("41", "New Taipei"),
#     # ("10074", "Agrigento"),
#     # ("176", "Karon"),
#     # ("58062", "Niagara-on-the-Lake"),
#     # ("9903", "Carcassonne"),
#     # ("131478", "Mata de Sao Joao"),
#     # ("78752", "La Fortuna de San Carlos"),
#     # ("9713", "Newcastle upon Tyne"),
#     # ("34", "Kobe"),
#     # ("79303", "Nairobi"),
#     # ("9919", "Avignon"),
#     # ("131360", "Maragogi"),
#     # ("82594", "Dunedin"),
#     # ("31", "Fukuoka"),
#     # ("10064", "Weymouth"),
#     # ("10235", "Bled"),
#     # ("10211", "Vila Nova de Gaia"),
#     # ("118", "Kandy"),
#     # ("9937", "Scarborough"),
#     # ("9861", "Innsbruck"),
#     # ("9917", "Lincoln"),
#     # ("9727", "Thessaloniki"),
#     # ("9734", "Galway"),
#     # ("98", "Bophut"),
#     # ("7865", "Mumbai"),
#     # ("24", "Jaipur"),
#     # # ("68", "Kochi"),
#     # ("412", "Munnar"),
#     # ("297", "Leh"),
#     # ("313", "Manali Tehsil"),
#     # ("428", "Shimla"),
#     # ("842", "Kodaikanal"),
#     # ("405", "Dharamsala"),
#     # ("304", "Chandigarh"),
#     # ("753", "Thekkady"),
#     # ("701", "Lonavala"),
#     # ("329", "Bardez"),
#     # ("783", "Haridwar"),
#     # ("1868", "Baga"),
#     # ("787", "Chikmagalur"),
#     # ("814", "Canacona"),
#     # ("2253", "Pahalgam"),
#     # ("956", "Ajmer"),
#     # ("461", "Kannur"),
#     # ("541", "Thane"),
#     # ("2761", "Ganpatipule"),
#     # ("2397", "Kasauli"),
#     # ("3684", "Kandaghat Tehsil"),
#     # ("1673", "Palampur"),








#     # …add more (doc_id, country_name) pairs as needed…
# ]

# SERVICE_ACCOUNT_JSON = (
#     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project"
#     r"\mycasavsc-firebase-adminsdk-u26li-ff3db6bf13.json"
# )
# OPENAI_MODEL = "gpt-4o-mini"
# OUTPUT_DIR    = "./"  # where per-country JSON files go

# # ─── INITIALIZE OPENAI & FIRESTORE ────────────────────────────────────────────
# openai.api_key = "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"
# if not openai.api_key:
#     raise RuntimeError("Missing OPENAI_API_KEY environment variable")

# if not firebase_admin._apps:
#     cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
#     firebase_admin.initialize_app(cred)
# db = firestore.client()


# # ─── SECTION GENERATORS ────────────────────────────────────────────────────────
# def generate_guide(country: str) -> dict:
#     system_msg = "You are a JSON generator. Output MUST BE valid JSON and nothing else."
#     user_prompt = f"""
# Generate a JSON object with exactly these top‐level keys:
#   "Overview",
#   "December to February",
#   "March to May",
#   "June to August",
#   "September to November",
#   "Regional Highlights",
#   "Travel Tip"

# Fill each with travel-guide text for {country}.
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
#         print(f"⚠️ Failed to parse guide JSON for {country}. Raw response:\n{content}")
#         raise


# def flatten_guide(raw: dict) -> dict:
#     flat = {}
#     for key, val in raw.items():
#         if isinstance(val, dict):
#             for subkey, subval in val.items():
#                 flat[f"{key} – {subkey}"] = subval
#         else:
#             flat[key] = val
#     return flat


# def generate_monthly_ratings(country: str) -> dict:
#     system_msg = "You are a JSON-only generator. Output MUST BE valid JSON."
#     user_prompt = f"""
# Rate each calendar month (Jan–Dec) for best time to visit {country} on a scale of 1 (worst) to 5 (best).
# Output only a JSON object with keys "Jan", "Feb", …, "Dec" and integer values 1–5."""
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
#         print(f"⚠️ Failed to parse monthly-ratings JSON for {country}. Raw response:\n{content}")
#         raise


# # ─── (OPTIONAL) STUBS FOR OTHER SECTIONS ────────────────────────────────────────
# # def generate_things_to_know(country: str) -> dict:
# #     # Your prompt for “Things to know” goes here...
# #     return {}

# # def generate_ways_to_get_around(country: str) -> dict:
# #     # Prompt for transportation tips...
# #     return {}

# # def generate_visa_requirements(country: str) -> dict:
# #     # Prompt for visa rules...
# #     return {}


# # ─── MAIN WORKFLOW ─────────────────────────────────────────────────────────────
# def main():
#     for doc_id, country in COUNTRIES:
#         print(f"\n🛠 Processing {country} (doc {doc_id})…")

#         # 1. Build the two “best time to visit” pieces
#         raw_guide       = generate_guide(country)
#         flat_guide      = flatten_guide(raw_guide)
#         monthly_ratings = generate_monthly_ratings(country)

#         # 2. Combine into one payload for Firestore & local JSON
#         best_time_payload = {
#             "seasonal":       flat_guide,
#             "monthlyRatings": monthly_ratings
#         }

#         # 3. Write local JSON for inspection
#         # full_payload = {
#         #     "best_time_to_visit": best_time_payload,
#         #     # "things_to_know":      generate_things_to_know(country),
#         #     # "ways_to_get_around":  generate_ways_to_get_around(country),
#         #     # "visa_requirements":   generate_visa_requirements(country),
#         # }
#         # fname = f"{country.lower().replace(' ', '_')}_travel_info.json"
#         # path  = os.path.join(OUTPUT_DIR, fname)
#         # print(f"💾 Writing JSON to {path}")
#         # with open(path, "w", encoding="utf-8") as f:
#         #     json.dump(full_payload, f, ensure_ascii=False, indent=2)

#         # 4. Store in Firestore under travelInfo subcollection
#         ti_coll = db.collection("allplaces").document(doc_id).collection("travelInfo")

#         print(f"☁️ Writing best_time_to_visit to allplaces/{doc_id}/travelInfo/best_time_to_visit")
#         ti_coll.document("best_time_to_visit").set(best_time_payload)

#         # 5. (Optional) Write other sections
#         # print("☁️ Writing things_to_know…")
#         # ti_coll.document("things_to_know").set(generate_things_to_know(country))
#         #
#         # print("☁️ Writing ways_to_get_around…")
#         # ti_coll.document("ways_to_get_around").set(generate_ways_to_get_around(country))
#         #
#         # print("☁️ Writing visa_requirements…")
#         # ti_coll.document("visa_requirements").set(generate_visa_requirements(country))

#         print("✅ Done.")

# if __name__ == "__main__":
#     main()




