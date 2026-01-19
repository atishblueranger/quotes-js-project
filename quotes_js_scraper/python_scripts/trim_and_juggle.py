import json
import os

# ── 1) CONFIGURE ────────────────────────────────────────────────────────────────

# List the exact JSON files you want to process:
FILE_LIST = [
    
     
        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\orlando_attractions.json",
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\granada_attractions.json", #Less than 20
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\brasilia_attractions.json", #Less than 20
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\salvador_attractions.json", #Less than 20
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\niagara-falls_attractions.json", #Less than 20
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\montevideo_attractions.json", #Less than 27
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\shanghai_attractions.json", 
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\atlanta_attractions.json",  #Less than 27
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\stockholm_attractions.json",
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\san-antonio_attractions.json",
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cape-town_attractions.json",
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\philadelphia_attractions.json",
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\dubrovnik_attractions.json",
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\mumbai-bombay_attractions.json",
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cairo_attractions.json",
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\rotterdam_attractions.json",
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\georgetown_attractions.json",     
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\colonia-del-sacramento_attractions.json", #Less than 20 : 13
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\el-calafate_attractions.json", # Less than 20 : 3
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\zaragoza_attractions.json", # Less than 20 : 19
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kansas-city_attractions.json", #Less than 20 : 11
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\vilnius_attractions.json", 
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\dresden_attractions.json", #Less than 20 : 22

        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\calgary_attractions.json", # Less than 20 : 10
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\san-sebastian_attractions.json", # Less than 20 : 17
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\doha_attractions.json", # Less than 20 : 29


        
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\luxor_attractions.json",#Done
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kolkata-calcutta_attractions.json", #Done
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lahaina_attractions.json", #Less than 20 : 22
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\danang_attractions.json", #Less than 20 : 6
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\pompeii_attractions.json", #Less than 20 : 31  
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\catania_attractions.json", #Less than 20 : 30
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\antalya_attractions.json", #Less than 20 : 22
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\fort-lauderdale_attractions.json", #Less than 20 : 9
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\praia-da-pipa_attractions.json", #Less than 20 : 6
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kyiv_attractions.json",
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\hobart_attractions.json",

        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\nha-trang_attractions.json", #Less than 20 : 15
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ljubljana_attractions.json", #Less than 20 : 40
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tbilisi_attractions.json", #Done
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cincinnati_attractions.json", #Less than 20 : 8
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\salamanca_attractions.json", #Less than 20 : 18
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\zagreb_attractions.json", #Done
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\melaka_attractions.json", #Done
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\alicante_attractions.json", #Less than 20 : 12
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\petra_attractions.json", #Less than 20 : 6
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cairns_attractions.json", #Less than 20 : 21
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\hue_attractions.json",#Done
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\wroclaw_attractions.json", #Less than 20 : 24
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\stuttgart_attractions.json", #Less than 20 : 28
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\mar-del-plata_attractions.json", #Less than 20 : 6
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cabo-san-lucas_attractions.json", #Less than 20 : 5
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\newport_attractions.json",# Less than 20 : 29
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\milwaukee_attractions.json", #Done
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\nuremberg_attractions.json", # Less than 20 : 25
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\moab_attractions.json", # Less than 20 : 6
        # r"C:\dev\python_runs\scrapy_se
        # 
        # 
        # 
        # lenium\quotes-js-project\quotes_js_scraper\darwin_attractions.json", # Less than 20 : 26
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\giza-pyramids_attractions.json", # Less than 20 : 19
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\segovia_attractions.json", # Less than 20 : 20
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ghent_attractions.json", #Done
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\hua-hin_attractions.json", # Less than 20 : 16
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\halifax_attractions.json", # Less than 20 : 16
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\busan_attractions.json", # Less than 20 : 25
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\chennai-madras_attractions.json", # Less than 20 : 27



        
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\kanazawa_attractions.json", # Less than 20 : 15
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\indianapolis_attractions.json", # Less than 20 : 11
        # r"C:\dev\python_runs\scrapy_selenium\quotes_js-project\quotes_js_scraper\python_scripts\bergamo_attractions.json", # Less than 20 : 22
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\asheville_attractions.json", # Less than 20 : 4
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\santo-domingo_attractions.json", # Less than 20 : 40
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\salt-lake-city_attractions.json", # Less than 20 : 26
            # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\rhodes-town_attractions.json", # DONE
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\st-petersburg_attractions.json", # Less than 20 : 8
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\arequipa_attractions.json", # Less than 20 : 27
            # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\naples_attractions.json", # Done
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\las-palmas-de-gran-canaria_attractions.json", # Less than 20 : 14
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\detroit_attractions.json", # Less than 20 : 20
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\minneapolis_attractions.json", # Less than 20 : 16
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\naha_attractions.json", # Less than 8 : 40 
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\guangzhou_attractions.json", # Less than 20 : 26
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\pune_attractions.json", # Less than 20 : 8
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\heidelberg_attractions.json", # Less than 20 : 15
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\oklahoma_attractions.json", # Less than 40 : 40
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\ronda_attractions.json", # Less than 20 : 19
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\chiang-rai_attractions.json", # Less than 20 : 19
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\kailua-kona_attractions.json" # Less than 20 : 13
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\manila_attractions.json", 
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\marmaris_attractions.json", # Less than 20 : 7
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\evora_attractions.json",# Less than 20 : 27
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\lake-louise_attractions.json", # Less than 20 : 6
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\cordoba_attractions.json", # Less than 20 : 19
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\san-andres_attractions.json", # Less than 20 : 7
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\fort-worth_attractions.json", # Less than 20 : 17
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\chengdu_attractions.json", # Less than 20 : 28
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\charlotte_attractions.json", #Less than 20 : 13
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\nagasaki_attractions.json",
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\coimbra_attractions.json", # Less than 20 : 26
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\varanasi_attractions.json", # Less than 20 : 30
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\santa-barbara_attractions.json", # Less than 20 : 21
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\baku-baki_attractions.json", 
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\hangzhou_attractions.json", 
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\nerja_attractions.json", #Less than 20 : 6
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\richmond_attractions.json", #Less than 20 : 9
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\edmonton_attractions.json", #Less than 20 : 13
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\la-paz_attractions.json", #Less than 20 : 7
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\jodhpur_attractions.json", #Less than 20 : 12
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\guadalajara_attractions.json",
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\yerevan_attractions.json", # Less than 20 : 23
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\hilo_attractions.json", # Less than 20 : 29
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\cannes_attractions.json", # Less than 20 : 11 
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\puno_attractions.json", # Less than 20 : 6   
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\santa-marta_attractions.json", # Less than 20 : 6
    #     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\srinagar_attractions.json", # Less than 20 : 27
    #     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\puebla_attractions.json", # Less than 20 : 16
    #     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\burgos_attractions.json", # Less than 20 : 16
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\puerto-plata_attractions.json", # Less than 20 : 9
        #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\ooty-udhagamandalam_attractions.json", # Done
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\playa-blanca_attractions.json", # Less than 20 : 3
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\mdina_attractions.json", # Less than 20 : 16
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\jaisalmer_attractions.json", # Less than 20 : 17
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\guanajuato_attractions.json", # Less than 20 : 21
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\goteborg_attractions.json", # Less than 20 : 15
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\kota-kinabalu_attractions.json", # Less than 20 : 16
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\amritsar_attractions.json", # Less than 20 : 18
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\python_scripts\bandung_attractions.json", # Less than 20 : 6
    #     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\bern_attractions.json", # Less than 20 : 16
    #     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\basel_attractions.json", # Less than 20 : 25
    #     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\manali_attractions.json", # Less than 20 : 12 
    #     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\san-jose_attractions.json", # Less than 20 : 10
    #     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\nottingham_attractions.json", # Less than 20 : 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\puerto-varas_attractions.json", # Less than 20 : 2
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\santander_attractions.json", # Less than 20 : 25
    #     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\flagstaff_attractions.json", # Less than 20 : 9
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sarajevo_attractions.json", # Less than 20 : 36
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tehran_attractions.json", # Less than 20 : 40
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cebu-city_attractions.json", # Less than 20 : 13
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\vientiane_attractions.json", # Less than 20 : 20
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\guayaquil_attractions.json", # Less than 20 : 35
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\oia_attractions.json", # Less than 20 : 2
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\potsdam_attractions.json", # Less than 20 : 26
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\beirut_attractions.json", # Less than 20 : 26
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\winnipeg_attractions.json", # Less than 20 : 26
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\taupo_attractions.json", # Less than 20 : 11
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\darjeeling_attractions.json", # Less than 20 : 11
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\suzhou_attractions.json", # Less than 20 : 36
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sacramento_attractions.json", # Less than 20 : 11
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kuching_attractions.json", # Less than 20 : 40
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\minsk_attractions.json", # Less than 20 : 6
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\pokhara_attractions.json", # Less than 20 : 15
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sigiriya_attractions.json", # Less than 20 : 14
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\girona_attractions.json", # Less than 20 : 12
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lviv_attractions.json", # Done
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\maastricht_attractions.json", # Less than 20 : 13
        # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\xiamen_attractions.json", # Less than 20 : 13
    #     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\vicenza_attractions.json", # Less than 20 : 14
    #     r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tarragona_attractions.json", # Less than 20 : 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\johor-bahru_attractions.json", # Less than 20 : 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ciutadella_attractions.json", # Less than 20 : 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\shenzhen_attractions.json", # Less than 20 : 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\reno_attractions.json", # Less than 20 : 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\portland_attractions.json", # Less than 20 : 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\corralejo_attractions.json", # Less than 20 : 3
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\whistler_attractions.json", # Less than 20 : 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ella_attractions.json", # Less than 20 : 2
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\park-city_attractions.json", # Less than 20 : 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sanur_attractions.json", # Less than 20 : 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\braga_attractions.json", # Less than 20 : 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\newport_attractions.json", # Less than 20 : 29
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\punta-arenas_attractions.json", # Less than 20 : 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\skopje_attractions.json", # Less than 20 : 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\santa-cruz-de-tenerife_attractions.json", # Less than 20 : 17
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\zermatt_attractions.json", # Less than 20 : 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lonely_planet_data\lonelyplanet_india_thiruvananthapuram-trivandrum_attractions.json", # Less than 20 : 15
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\utrecht-city_attractions.json", # Less than 20 : 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\nanjing_attractions.json", # Less than 20 : 29
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\santa-cruz-de-tenerife_attractions.json", # Less than 20 : 17
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cartagena_attractions.json", # Less than 20 : 16
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\seogwipo_attractions.json", # Less than 20 : 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\san-jose_attractions.json", # Less than 20 : 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\trier_attractions.json", # Less than 20 : 22
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\iquique_attractions.json", # Less than 20 : 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cuenca_attractions.json", # Less than 20 : 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\brescia_attractions.json", # Less than 20 : 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\puerto-ayora_attractions.json", # Less than 20 : 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\acapulco_attractions.json", # Less than 20 : 25
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\rovinj_attractions.json", # Less than 20 : 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\batumi_attractions.json", # Less than 20 : 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\amman_attractions.json", # Less than 20 : 26
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\vladivostok_attractions.json", # Less than 20 : 15
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lijiang_attractions.json", # Less than 20 : 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cambridge_attractions.json", # Less than 20 : 25
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\uyuni_attractions.json", # Less than 20 : 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sirmione_attractions.json", # Less than 20 : 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\corfu-town_attractions.json", # Less than 20 : 28
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\monestir-de-montserrat_attractions.json", # Less than 20 : 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\surabaya_attractions.json", # Less than 20 : 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\nuwara-eliya_attractions.json", # Less than 20 : 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\vigo_attractions.json", # Less than 20 : 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\buffalo_attractions.json", # Less than 20 : 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\puerto-madryn_attractions.json", # Less than 20 : 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\palenque_attractions.json", 
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\puerto-ayora_attractions.json", 
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\caracas_attractions.json", # Less than 20 : length 24
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sanya_attractions.json", # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\salerno_attractions.json", # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\rovaniemi_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\valladolid_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\mostar_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\thimphu_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cafayate_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\boulder_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\aspen_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\aachen_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\broome_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\garmisch-partenkirchen_attractions.json", # Less than 20 : 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\haifa_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\takayama_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\salerno_attractions.json", # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\rovaniemi_attractions.json", # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\valladolid_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\mostar_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\thimphu_attractions.json",
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cafayate_attractions.json",
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\boulder_attractions.json", # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\aspen_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\aachen_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\broome_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\garmisch-partenkirchen_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\haifa_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\takayama_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\alexandria_attractions.json", # Less than 20 : length 15
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\matsumoto_attractions.json", # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\stavanger_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kos-town_attractions.json", # Less than 20 : length 17
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\pretoria_attractions.json", # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\seminyak_attractions.json", # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\zakopane_attractions.json", # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\manama_attractions.json", # Less than 20 : length 22 
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\budva_attractions.json", # Less than 20 : length 20
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\chamarel_attractions.json", # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tirana_attractions.json", # Less than 20 : 20
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\boise_attractions.json", # Less than 20 : 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\bursa_attractions.json", 
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\puri_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\wanaka_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\pamplona_attractions.json" # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\trondheim_attractions.json" # Less than 20 : length 23
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ajaccio_attractions.json" # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\hampi_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\mussoorie_attractions.json" # Less than 20 : 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\murcia_attractions.json" # Less than 20 : 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sandakan_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\marsa-alam_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kumamoto_attractions.json" # Less than 20 : length 15
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\almaty_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tofino-and-around_attractions.json" # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\bend_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\riyadh_attractions.json" # Less than 20 : length 17
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\la-paz_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ipoh_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\port-vila_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\guatemala-city_attractions.json" # Less than 20 : length 26
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\providence_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\dalian_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\oakland_attractions.json" # Less than 20 : length 16
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\toulon_attractions.json" # Less than 20 : length 3
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kingston_attractions.json" # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tangier_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\karlovy-vary_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\koblenz_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\madurai_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\berchtesgaden_attractions.json" # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\delphi_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\seward_attractions.json" # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cuenca_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ketchikan_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\la-serena_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kailua_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\delft_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cienfuegos_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\alexandria_attractions.json" # Less than 20 : length 15
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sitges_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\haerbin_attractions.json" # Less than 20 : length 16
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ohrid_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\rishikesh_attractions.json" # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\trujillo_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\brno_attractions.json" # Less than 20 : length 22
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\baguio_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\toledo_attractions.json" # Less than 20 : length 22
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\villa-de-leyva_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\faro_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\haarlem_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\jeddah_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sapa_attractions.json" # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\wulingyuan-and-zhangjiajie_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lucknow_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\nasik_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\semarang_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\bamberg_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\burlington_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\valladolid_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\guilin_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\trogir_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\hallstatt_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sopot_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sinaia_attractions.json" # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sukhothai_attractions.json" # Less than 20 : length 22
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\zacatecas_attractions.json" # Less than 20 : length 18
    # r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\gyeongju_attractions.json" ,
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sagres_attractions.json" #Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\perth_attractions.json" # Less than 20 : length 25
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\bonn_attractions.json" # Less than 20 : length 15
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kaunas_attractions.json" # Less than 20 : length 40
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\beppu_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sibiu_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\iquitos_attractions.json" # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\fort-de-france_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ostend_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\pavia_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tarifa_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\samarkand_attractions.json" # Less than 20 : length 26
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tashkent_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tashkent_attractions.json" ,# Less than 20 : length 31
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\yulara-ayers-rock-resort_attractions.json" # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\madaba_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\aveiro_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\santiago-de-cuba_attractions.json" ,# Less than 20 : length 40
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\wuhan_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\addis-ababa_attractions.json" # Less than 20 : length 27
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\constance_attractions.json" # Less than 20 : length 16  
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\mecca_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\pushkar_attractions.json" # Less than 20 : length 11
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\saint-john_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\arica_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\dunedin_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\el-paso_attractions.json" # Less than 20 : length 15
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\plovdiv_attractions.json" ,# Less than 20 : length 31
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cremona_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ixtapa_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\whitehorse_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tianjin_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\varna_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\augsburg_attractions.json" # Less than 20 : length 11
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tunis_attractions.json" ,# Less than 20 : length 40
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\guwahati_attractions.json" # Less than 20 : length 15
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\khajuraho_attractions.json" # Less than 20 : length 32
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\chefchaouen_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cavtat_attractions.json" # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\medan_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cluj-napoca_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\port-louis_attractions.json" # Less than 20 : length 23
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\florence_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\singaraja_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kigali_attractions.json" # Less than 20 : length 7

    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sousse_attractions.json" # Less than 20 : length 11
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\gaziantep-antep_attractions.json"  # Less than 20 : length 16
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\san-salvador_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\morelia_attractions.json" # Less than 20 : length 24
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\new-haven_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\battambang_attractions.json" # Less than 20 : length 22
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kovalam_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sucre_attractions.json" # Less than 20 : length 24
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\bukhara_attractions.json" # Less than 20 : length 35
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\trani_attractions.json" # Less than 20 : length 4

    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\berkeley_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\santa-cruz_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\algiers_attractions.json" # Less than 20 : length 17
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\zihuatanejo_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\pontevedra_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\bol_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kep_attractions.json" # Less than 20 : length 11
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\syracuse_attractions.json" # Less than 20 : length 27
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\hanalei_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lhasa_attractions.json" ,# Less than 20 : length 40
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kunming_attractions.json" # Less than 20 : length 24
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\campeche_attractions.json" # Less than 20 : length 23
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\wiesbaden_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\nazareth_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\nadi_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ulm_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\andorra-la-vella_attractions.json" # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\campeche_attractions.json" # Less than 20 : length 23
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tampere_attractions.json" # Less than 20 : length 16
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\san-juan_attractions.json" ,# Less than 20 : length 40
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\udine_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\barranquilla_attractions.json" # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\linz_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ponce_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\mannheim_attractions.json" # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kampot_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\turku_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\solo_attractions.json" # Less than 20 : length 6



    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\incheon_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\bhaktapur_attractions.json" ,# Less than 20 : length 40
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\apia_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cuernavaca_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\unawatuna_attractions.json" # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\skagen_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\vigan_attractions.json" # Less than 20 : length 11
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\accra_attractions.json" # Less than 20 : length 16
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\bastia_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\groningen-city_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\trabzon_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\matanzas_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\puerto-escondido_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\hermanus_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\bucaramanga_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\akureyri_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\klagenfurt_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\edirne_attractions.json" # Less than 20 : length 16
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\szczecin_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\diu_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\meknes_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\trujillo_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\passau_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\piran_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\paramaribo_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\karachi_attractions.json" # Less than 20 : length 22
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\belize-city_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\santa-cruz_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lahore_attractions.json" ,# Less than 20 : length 32
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\dover_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\brindisi_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\podgorica_attractions.json" # Less than 20 : length 20
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\dar-es-salaam_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\vang-vieng_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\uppsala_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\swakopmund_attractions.json" # Less than 20 : length 24
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\windhoek_attractions.json" # Less than 20 : length 20
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tijuana_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\eger_attractions.json" # Less than 20 : length 20
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lund_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\bishkek_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\vaduz_attractions.json" # Less than 20 : length 2
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\jerash_attractions.json" # Less than 20 : length 16
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\antananarivo_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\maputo_attractions.json" # Less than 20 : length 21
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lalibela_attractions.json" # Less than 20 : length 23
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\male_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kutaisi_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\dahab_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\trincomalee_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\nijmegen_attractions.json" # Less than 20 : length 11
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\jaffna_attractions.json" # Less than 20 : length 17
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\vung-tau_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\dunhuang_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\santa-fe_attractions.json" # Less than 20 : length 26
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\athens_attractions.json" ,# Less than 20 : length 40
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\aqaba_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\viseu_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\burlington_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\hanover_attractions.json" # Less than 20 : length 23
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\katowice_attractions.json" # Less than 20 : length 11
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\suwon_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\versailles_attractions.json" # Less than 20 : length 17
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\manchester_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\badajoz_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\daegu_attractions.json" # Less than 20 : length 16
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\newcastle_attractions.json" # Less than 20 : length 15
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\charleston_attractions.json" # Less than 20 : length 29
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\hat-yai_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\rijeka_attractions.json" # Less than 20 : length 11
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\jeonju_attractions.json" # Less than 20 : length 17
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\udon-thani_attractions.json" # Less than 20 : length 11
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kansas-city_attractions.json" # Less than 20 : length 11

#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kampala_attractions.json" # Less than 20 : length 21
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\hondarribia_attractions.json" # Less than 20 : length 10
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\yellowknife_attractions.json" # Less than 20 : length 10
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\manzanillo_attractions.json" # Less than 20 : length 8
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\diani-beach_attractions.json" # Less than 20 : length 11
#        r"C:\dev\python_runs\scrapy_selah\quotes_js_scraper\quotes_js_scraper\ayacucho_attractions.json" # Less than 20 : length 11
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lago_attractions.json" # Less than 20 : length 11
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\khiva_attractions.json" # Less than 20 : length 11
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\aljezur_attractions.json" # Less than 20 : length 11
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\uvita_attractions.json" # Less than 20 : length 11
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\banda-aceh_attractions.json" # Less than 20 : length 11
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ayacucho_attractions.json" # Less than 20 : length 11
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lagos_attractions.json" # Less than 20 : length 15
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\khiva_attractions.json" # Less than 20 : length 22
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\aljezur_attractions.json" # Less than 20 : length 9
#        r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\banda-aceh_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\phitsanulok_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\verona_attractions.json" # Less than 20 : length 20
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\burgas_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\york_attractions.json" # Less than 20 : length 18
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kingston_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\nong-khai_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\cape-coast_attractions.json" # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\pereira_attractions.json" # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\esztergom_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sydney_attractions.json", # Less than 20 : length 40
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ulcinj_attractions.json" # Less than 20 : length 17
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ceuta-sebta_attractions.json" # Less than 20 : length 17
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kratie_attractions.json" # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\santa-cruz-de-la-palma_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\prachuap-khiri-khan_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\baltimore_attractions.json" # Less than 20 : length 24
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\bristol_attractions.json" # Less than 20 : length 27
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\syracuse_attractions.json" # Less than 20 : length 20
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\hamburg_attractions.json" ,# Less than 20 : length 40
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\chur_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\oakland_attractions.json" # Less than 20 : length 16
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\gaborone_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\trinidad_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\arcos-de-la-frontera_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\churchill_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\churchill_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\nagoya_attractions.json" # Less than 20 : length 23
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\geneva_attractions.json" # Less than 20 : length 25
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\koper_attractions.json" # Less than 20 : length 25
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\melilla_attractions.json" # Less than 20 : length 15
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\port-moresby_attractions.json" # Less than 20 : length 21
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\banjul_attractions.json" # Less than 20 : length 15
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\huaraz_attractions.json" # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\san-gil_attractions.json" # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\yakutsk_attractions.json" # Less than 20 : length 13
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\lusaka_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\carrapateira_attractions.json" # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\balikpapan_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\manzanillo_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\sorrento_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\mataram_attractions.json" # Less than 20 : length 4
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kashgar_attractions.json" # Less than 20 : length 9
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ubon-ratchathani_attractions.json" # Less than 20 : length 21
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\varna_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ashgabat_attractions.json" # Less than 20 : length 16
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\asilah_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\chetumal_attractions.json" # Less than 20 : length 5
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\panajachel_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\guiyang_attractions.json" # Less than 20 : length 6
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\bath_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\leeds_attractions.json" # Less than 20 : length 8
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\panama-city_attractions.json", # Less than 20 : length 40
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\queenstown_attractions.json" # Less than 20 : length 7
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\austin_attractions.json" # Less than 20 : length 26
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\toledo_attractions.json" # Less than 20 : length 22
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\loja_attractions.json" # Less than 20 : length 15
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\quetzaltenango-xela_attractions.json" # Less than 20 : length 15
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\khartoum_attractions.json" # Less than 20 : length 19
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\ruse_attractions.json" # Less than 20 : length 12
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\kota-bharu_attractions.json" # Less than 20 : length 10
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\khartoum_attractions.json" # Less than 20 : length 14
    #    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\khartoum_attractions.json" # Less than 20 : length 14
    r"C:\dev\python_runs\scrapy_selenium\quotes-js-project\quotes_js_scraper\tokyo_attractions.json"

]      

MAX_ITEMS = 20  # or 21 if you prefer


# ── 2) SWAP BASED ON THE "index" FIELD ──────────────────────────────────────────

def swap_by_field_index(items):
    """
    For each group of three consecutive index-values (1,2,3), (4,5,6), etc.,
    swap the objects whose field 'index' == start and 'index' == start+2,
    then swap their 'index' values internally.
    """
    # build a quick lookup
    lookup = {item["index"]: item for item in items}
    
    max_idx = max(lookup)

    # walk through groups: 1→3, 4→6, 7→9, ...
    for start in range(1, max_idx + 1, 3):
        end = start + 2
        if start in lookup and end in lookup:
            a = lookup[start]
            b = lookup[end]
            # swap their index fields
            a["index"], b["index"] = b["index"], a["index"]

    # now sort the list by the (updated) 'index'
    return sorted(items, key=lambda x: x["index"])


# ── 3) PROCESS EACH FILE ───────────────────────────────────────────────────────

def process_file(path):
    # load the array of attractions
    with open(path, "r", encoding="utf-8") as f:
        items = json.load(f)

    # 3.1 swap by the 'index' field
    items = swap_by_field_index(items)

    # 3.2 truncate to MAX_ITEMS
    items = items[:MAX_ITEMS]

    # 3.3 write out to a new file alongside the original
    base, ext = os.path.splitext(path)
    out_path = f"{base}_top{MAX_ITEMS}{ext}"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=4)

    print(f"Processed {os.path.basename(path)} → {len(items)} items → {os.path.basename(out_path)}")


if __name__ == "__main__":
    for filepath in FILE_LIST:
        if os.path.exists(filepath):
            process_file(filepath)
        else:
            print(f"[!] File not found: {filepath}")



