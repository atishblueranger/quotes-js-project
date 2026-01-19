import scrapy
import json
from w3lib.html import remove_tags

class MultiCountryAttractionsSpider(scrapy.Spider):
    name = "multi_country_attractions"
    custom_settings = {
        'USER_AGENT': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/91.0.4472.124 Safari/537.36'
        ),
        'DOWNLOAD_DELAY': 1,
            # enable our pipeline
            # 'ITEM_PIPELINES': {
            #     'myproject.pipelines.CountryJsonWriterPipeline': 300,
            # },
    }

    def start_requests(self):
        urls = [
          

# "https://www.lonelyplanet.com/italy/tropea/attractions",
    # "https://www.lonelyplanet.com/india/uttarakhand-uttaranchal/mussoorie/attractions",
    # "https://www.lonelyplanet.com/spain/murcia/attractions",
    # "https://www.lonelyplanet.com/malaysia/malaysian-borneo-sabah/sand    akan/attractions",
    # "https://www.lonelyplanet.com/egypt/red-sea-coast/marsa-alam/attractions",
    # "https://www.lonelyplanet.com/japan/kyushu/kumamoto/attractions",
    # "https://www.lonelyplanet.com/kazakhstan/almaty/attractions",
    # "https://www.lonelyplanet.com/canada/british-columbia/tofino-and-around/attractions",
    # "https://www.lonelyplanet.com/usa/oregon/bend/attractions",
    # "https://www.lonelyplanet.com/saudi-arabia/riyadh/attractions",
    # "https://www.lonelyplanet.com/mexico/baja-california/la-paz/attractions",
    # "https://www.lonelyplanet.com/malaysia/peninsular-malaysia-west-coast/ipoh/attractions",
    # "https://www.lonelyplanet.com/vanuatu/efate/port-vila/attractions",
    # "https://www.lonelyplanet.com/guatemala/guatemala-city/attractions",
    # "https://www.lonelyplanet.com/germany/bavarian-alps/berchtesgaden/attractions",
    # "https://www.lonelyplanet.com/greece/delphi/attractions",
    # "https://www.lonelyplanet.com/usa/alaska/seward/attractions",
    # "https://www.lonelyplanet.com/thailand/mae-hong-son-province/pai/attractions",
    # "https://www.lonelyplanet.com/usa/new-england/providence/attractions",
    # "https://www.lonelyplanet.com/spain/castilla-la-mancha/cuenca/attractions",
    # "https://www.lonelyplanet.com/mongolia/ulaanbaatar/attractions",
    # "https://www.lonelyplanet.com/usa/alaska/ketchikan/attractions",
    # "https://www.lonelyplanet.com/chile/northern-chile/la-serena/attractions",
    # "https://www.lonelyplanet.com/usa/hawaii/kailua/attractions",
    # "https://www.lonelyplanet.com/the-netherlands/the-randstad/delft/attractions",
    # "https://www.lonelyplanet.com/cuba/cienfuegos/attractions",
    # "https://www.lonelyplanet.com/usa/virginia/alexandria/attractions",
    # "https://www.lonelyplanet.com/spain/sitges/attractions",
    # "https://www.lonelyplanet.com/china/heilongjiang/haerbin/attractions",
    # "https://www.lonelyplanet.com/macedonia/southern-macedonia/ohrid/attractions",

# "https://www.lonelyplanet.com/china/liaoning/dalian/attractions",
# "https://www.lonelyplanet.com/peru/cuzco-and-the-sacred-valley/ollantaytambo/attractions",
# "https://www.lonelyplanet.com/usa/california/oakland/attractions",
# "https://www.lonelyplanet.com/france/cote-dazur/toulon/attractions",
# "https://www.lonelyplanet.com/canada/ontario/kingston/attractions",
# "https://www.lonelyplanet.com/morocco/the-mediterranean-coast-and-the-rif/tangier/attractions",
# "https://www.lonelyplanet.com/czech-republic/bohemia/karlovy-vary/attractions",
# "https://www.lonelyplanet.com/germany/koblenz/attractions",
# "https://www.lonelyplanet.com/india/tamil-nadu/madurai/attractions",
# "https://www.lonelyplanet.com/india/uttarakhand-uttaranchal/rishikesh/attractions",
# "https://www.lonelyplanet.com/peru/north-coast/trujillo/attractions",
# "https://www.lonelyplanet.com/czech-republic/moravia/brno/attractions",
# "https://www.lonelyplanet.com/philippines/north-luzon/baguio/attractions",
# "https://www.lonelyplanet.com/spain/castilla-la-mancha/toledo/attractions",
# "https://www.lonelyplanet.com/colombia/north-of-bogota/villa-de-leyva/attractions",
# "https://www.lonelyplanet.com/portugal/the-algarve/faro/attractions",
# "https://www.lonelyplanet.com/the-netherlands/the-randstad/haarlem/attractions",
# "https://www.lonelyplanet.com/saudi-arabia/hejaz/jeddah/attractions",
# "https://www.lonelyplanet.com/vietnam/northwest-vietnam/sapa/attractions",
# "https://www.lonelyplanet.com/china/hunan/wulingyuan-and-zhangjiajie/attractions",
# "https://www.lonelyplanet.com/india/uttar-pradesh/lucknow/attractions",
# "https://www.lonelyplanet.com/australia/new-south-wales/newcastle/attractions",
# "https://www.lonelyplanet.com/india/maharashtra/nasik/attractions",
# "https://www.lonelyplanet.com/indonesia/semarang/attractions",
# "https://www.lonelyplanet.com/chile/the-lakes-district/puerto-montt/attractions",
    # "https://www.lonelyplanet.com/seychelles/mahe/victoria/attractions",
    # "https://www.lonelyplanet.com/germany/bavaria/bamberg/attractions",
    # "https://www.lonelyplanet.com/usa/new-england/burlington/attractions",
    # "https://www.lonelyplanet.com/spain/valladolid/attractions",
    # "https://www.lonelyplanet.com/china/guangxi/guilin/attractions",
    # "https://www.lonelyplanet.com/croatia/dalmatia/trogir/attractions",
    # "https://www.lonelyplanet.com/austria/salzkammergut/hallstatt/attractions",
    # "https://www.lonelyplanet.com/poland/pomerania/sopot/attractions",
    # "https://www.lonelyplanet.com/romania/transylvania/sinaia/attractions",
    # "https://www.lonelyplanet.com/thailand/sukhothai-province/sukhothai/attractions",
    # "https://www.lonelyplanet.com/france/cassis/attractions",
    # "https://www.lonelyplanet.com/mexico/northern-central-highlands/zacatecas/attractions",
    # "https://www.lonelyplanet.com/south-korea/gyeongsangbuk-do/gyeongju/attractions",
    # "https://www.lonelyplanet.com/portugal/the-algarve/sagres/attractions",
    # "https://www.lonelyplanet.com/australia/western-australia/perth/attractions",
    # "https://www.lonelyplanet.com/germany/north-rhine-westphalia/bonn/attractions",
    # "https://www.lonelyplanet.com/lithuania/central-lithuania/kaunas/attractions",
    # "https://www.lonelyplanet.com/japan/kyushu/beppu/attractions",
    # "https://www.lonelyplanet.com/romania/transylvania/sibiu/attractions",
    # "https://www.lonelyplanet.com/peru/amazon-basin/iquitos/attractions",
    # "https://www.lonelyplanet.com/martinique/fort-de-france/attractions",
    # "https://www.lonelyplanet.com/belgium/ostend/attractions",
    # "https://www.lonelyplanet.com/italy/lombardy-and-the-lakes/pavia/attractions",
    # "https://www.lonelyplanet.com/spain/andalucia/tarifa/attractions",
    # "https://www.lonelyplanet.com/uzbekistan/central-uzbekistan/samarkand/attractions",
    # "https://www.lonelyplanet.com/uzbekistan/tashkent/attractions",
# "https://www.lonelyplanet.com/australia/northern-territory/yulara-ayers-rock-resort/attractions",
# "https://www.lonelyplanet.com/jordan/kings-highway/madaba/attractions",
# "https://www.lonelyplanet.com/usa/new-england/newport/attractions",
# "https://www.lonelyplanet.com/portugal/aveiro/attractions",
# "https://www.lonelyplanet.com/cuba/eastern-cuba/santiago-de-cuba/attractions",
# "https://www.lonelyplanet.com/china/hubei/wuhan/attractions",
# "https://www.lonelyplanet.com/ethiopia/addis-ababa/attractions",
# "https://www.lonelyplanet.com/germany/lake-constance/constance/attractions",
# "https://www.lonelyplanet.com/saudi-arabia/mecca/attractions",
# "https://www.lonelyplanet.com/india/rajasthan/pushkar/attractions",
# "https://www.lonelyplanet.com/spain/ribadeo/attractions",
# "https://www.lonelyplanet.com/canada/new-brunswick/saint-john/attractions",
# "https://www.lonelyplanet.com/chile/northern-chile/arica/attractions",
# "https://www.lonelyplanet.com/new-zealand/dunedin-and-otago/dunedin/attractions",
# "https://www.lonelyplanet.com/usa/texas/el-paso/attractions",
# "https://www.lonelyplanet.com/bulgaria/plovdiv-and-rodopi-mountains/plovdiv/attractions",
# "https://www.lonelyplanet.com/italy/lombardy-and-the-lakes/cremona/attractions",
# "https://www.lonelyplanet.com/mexico/central-pacific-coast/ixtapa/attractions",
# "https://www.lonelyplanet.com/canada/yukon-territory/whitehorse/attractions",
# "https://www.lonelyplanet.com/china/tianjin/attractions",
# "https://www.lonelyplanet.com/bulgaria/black-sea-coast/varna/attractions",
# "https://www.lonelyplanet.com/italy/vernazza/attractions",
# "https://www.lonelyplanet.com/germany/bavaria/augsburg/attractions",
# "https://www.lonelyplanet.com/mexico/yucatan-peninsula/puerto-morelos/attractions",
# "https://www.lonelyplanet.com/tunisia/tunis/attractions",
# "https://www.lonelyplanet.com/india/northeast-states/guwahati/attractions",
# "https://www.lonelyplanet.com/the-netherlands/eindhoven/attractions",
# "https://www.lonelyplanet.com/india/madhya-pradesh-and-chhattisgarh/khajuraho/attractions",
# "https://www.lonelyplanet.com/morocco/the-mediterranean-coast-and-the-rif/chefchaouen/attractions",
# "https://www.lonelyplanet.com/switzerland/grindelwald/attractions",
# "https://www.lonelyplanet.com/croatia/cavtat/attractions",
# "https://www.lonelyplanet.com/indonesia/sumatra/medan/attractions",
# "https://www.lonelyplanet.com/romania/transylvania/cluj-napoca/attractions",
# "https://www.lonelyplanet.com/mauritius/port-louis/attractions",
# "https://www.lonelyplanet.com/italy/florence/attractions",
# "https://www.lonelyplanet.com/indonesia/singaraja/attractions",
# "https://www.lonelyplanet.com/rwanda/kigali/attractions",
# "https://www.lonelyplanet.com/tunisia/central-tunisia/sousse/attractions",
# "https://www.lonelyplanet.com/turkey/central-anatolia/gaziantep-antep/attractions",
# "https://www.lonelyplanet.com/el-salvador/san-salvador/attractions",
# "https://www.lonelyplanet.com/mexico/western-central-highlands/morelia/attractions",
# "https://www.lonelyplanet.com/usa/new-england/new-haven/attractions",
# "https://www.lonelyplanet.com/cambodia/northwestern-cambodia/battambang/attractions",
# "https://www.lonelyplanet.com/india/kerala/kovalam/attractions",
# "https://www.lonelyplanet.com/costa-rica/caribbean-coast/cahuita/attractions",
# "https://www.lonelyplanet.com/bolivia/the-southwest/sucre/attractions",
# "https://www.lonelyplanet.com/uzbekistan/central-uzbekistan/bukhara/attractions",
# "https://www.lonelyplanet.com/seychelles/mahe/victoria/attractions",
# "https://www.lonelyplanet.com/usa/virginia/richmond/attractions",

# "https://www.lonelyplanet.com/italy/puglia/trani/attractions",
# "https://www.lonelyplanet.com/usa/california/berkeley/attractions",
# "https://www.lonelyplanet.com/usa/california/santa-cruz/attractions",
# "https://www.lonelyplanet.com/algeria/algiers/attractions",
# "https://www.lonelyplanet.com/mexico/central-pacific-coast/zihuatanejo/attractions",
# "https://www.lonelyplanet.com/spain/pontevedra/attractions",
# "https://www.lonelyplanet.com/croatia/bol/attractions",
# "https://www.lonelyplanet.com/cambodia/south-coast/kep/attractions",
# "https://www.lonelyplanet.com/italy/sicily/syracuse/attractions",

# "https://www.lonelyplanet.com/usa/hawaii/hanalei/attractions",
# "https://www.lonelyplanet.com/china/tibet/lhasa/attractions",
# "https://www.lonelyplanet.com/peru/cuzco-and-the-sacred-valley/aguas-calientes/attractions",
# "https://www.lonelyplanet.com/china/yunnan/kunming/attractions",
# "https://www.lonelyplanet.com/mexico/yucatan-peninsula/campeche/attractions",
# "https://www.lonelyplanet.com/germany/hesse/wiesbaden/attractions",
# "https://www.lonelyplanet.com/israel-and-the-palestinian-territories/galilee/nazareth/attractions",
# "https://www.lonelyplanet.com/fiji/viti-levu/nadi/attractions",
# "https://www.lonelyplanet.com/germany/baden-wurttemberg/ulm/attractions",
# "https://www.lonelyplanet.com/andorra/andorra-la-vella/attractions",
# "https://www.lonelyplanet.com/mexico/yucatan-peninsula/campeche/attractions",
# "https://www.lonelyplanet.com/finland/southwestern-finland/tampere/attractions",
# "https://www.lonelyplanet.com/puerto-rico/san-juan/attractions",
# "https://www.lonelyplanet.com/italy/friuli-venezia-giulia/udine/attractions",
# "https://www.lonelyplanet.com/oman/dhofar/salalah/attractions",
# "https://www.lonelyplanet.com/colombia/barranquilla/attractions",
# "https://www.lonelyplanet.com/austria/the-danube-valley/linz/attractions",
# "https://www.lonelyplanet.com/puerto-rico/southern-and-western-puerto-rico/ponce/attractions",
# "https://www.lonelyplanet.com/germany/baden-wurttemberg/mannheim/attractions",
# "https://www.lonelyplanet.com/cambodia/south-coast/kampot/attractions",
# "https://www.lonelyplanet.com/finland/south-coast/turku/attractions",
# "https://www.lonelyplanet.com/indonesia/java/solo/attractions",
# "https://www.lonelyplanet.com/china/yunnan/dali/attractions",

# "https://www.lonelyplanet.com/south-korea/gyeonggi-do/incheon/attractions",
# "https://www.lonelyplanet.com/nepal/around-the-kathmandu-valley/bhaktapur/attractions",
# "https://www.lonelyplanet.com/usa/virginia/richmond/attractions",
# "https://www.lonelyplanet.com/samoa/apia/attractions",
# "https://www.lonelyplanet.com/mexico/south-of-mexico-city/cuernavaca/attractions",
# "https://www.lonelyplanet.com/sri-lanka/the-south/unawatuna/attractions",
# "https://www.lonelyplanet.com/denmark/jutland/skagen/attractions",
# "https://www.lonelyplanet.com/dominican-republic/peninsula-de-samana/las-terrenas/attractions",
# "https://www.lonelyplanet.com/philippines/north-luzon/vigan/attractions",
# "https://www.lonelyplanet.com/ghana/accra/attractions",
# "https://www.lonelyplanet.com/france/corsica/bastia/attractions",
# "https://www.lonelyplanet.com/the-netherlands/the-north-and-east/groningen-city/attractions",
# "https://www.lonelyplanet.com/turkey/the-black-sea-and-northeastern-anatolia/trabzon/attractions",
# "https://www.lonelyplanet.com/cuba/matanzas/attractions",
# "https://www.lonelyplanet.com/italy/riomaggiore/attractions",
# "https://www.lonelyplanet.com/italy/the-italian-lakes/bellagio/attractions",
# "https://www.lonelyplanet.com/ireland/county-cork/kinsale/attractions",
# "https://www.lonelyplanet.com/greece/athens/piraeus/attractions",
# "https://www.lonelyplanet.com/mexico/oaxaca-state/puerto-escondido/attractions",
# "https://www.lonelyplanet.com/south-africa/hermanus/attractions",
# "https://www.lonelyplanet.com/colombia/north-of-bogota/bucaramanga/attractions",
# "https://www.lonelyplanet.com/italy/agropoli/attractions",
# "https://www.lonelyplanet.com/iceland/the-north/akureyri/attractions",
# "https://www.lonelyplanet.com/colombia/manizales/attractions",
# "https://www.lonelyplanet.com/austria/the-south/klagenfurt/attractions",
# "https://www.lonelyplanet.com/turkey/edirne/attractions",
# "https://www.lonelyplanet.com/poland/pomerania/szczecin/attractions",
# "https://www.lonelyplanet.com/india/gujarat/diu/attractions",
# "https://www.lonelyplanet.com/morocco/the-mediterranean-coast-and-the-rif/meknes/attractions",
# "https://www.lonelyplanet.com/peru/north-coast/trujillo/attractions",
# "https://www.lonelyplanet.com/france/collioure/attractions",
# "https://www.lonelyplanet.com/canary-islands/fuerteventura/el-cotillo/attractions",
# "https://www.lonelyplanet.com/india/uttar-pradesh/fatehpur-sikri/attractions",
# "https://www.lonelyplanet.com/germany/bavaria/passau/attractions",
# "https://www.lonelyplanet.com/slovenia/karst-and-coast/piran/attractions",
# "https://www.lonelyplanet.com/italy/liguria-piedmont-and-valle-daosta/portofino/attractions",
# "https://www.lonelyplanet.com/mexico/baja-california/ensenada/attractions",
# "https://www.lonelyplanet.com/the-guianas/suriname/paramaribo/attractions",
# "https://www.lonelyplanet.com/pakistan/sindh/karachi/attractions",
# "https://www.lonelyplanet.com/belize/belize-city/attractions",
# "https://www.lonelyplanet.com/usa/california/santa-cruz/attractions",
# "https://www.lonelyplanet.com/pakistan/punjab/lahore/attractions",
# "https://www.lonelyplanet.com/usa/california/sausalito/attractions",
# "https://www.lonelyplanet.com/england/southeast-england/dover/attractions",
# "https://www.lonelyplanet.com/italy/puglia/brindisi/attractions",

# "https://www.lonelyplanet.com/montenegro/podgorica/attractions",
# "https://www.lonelyplanet.com/tanzania/dar-es-salaam/attractions",
# "https://www.lonelyplanet.com/laos/northern-laos/vang-vieng/attractions",
# "https://www.lonelyplanet.com/sweden/svealand/uppsala/attractions",
# "https://www.lonelyplanet.com/namibia/western-namibia/swakopmund/attractions",
# "https://www.lonelyplanet.com/namibia/windhoek/attractions",

# "https://www.lonelyplanet.com/portugal/lisbon/attractions",
# "https://www.lonelyplanet.com/mexico/south-of-mexico-city/taxco/attractions",
# "https://www.lonelyplanet.com/mexico/baja-california/tijuana/attractions",
# "https://www.lonelyplanet.com/hungary/northeastern-hungary/eger/attractions",
# "https://www.lonelyplanet.com/sweden/skane/lund/attractions",
# "https://www.lonelyplanet.com/kyrgyzstan/bishkek/attractions",
# "https://www.lonelyplanet.com/mexico/baja-california/loreto/attractions",
# "https://www.lonelyplanet.com/liechtenstein/vaduz/attractions",
# "https://www.lonelyplanet.com/jordan/jerash-and-the-north/jerash/attractions",
# "https://www.lonelyplanet.com/madagascar/antananarivo/attractions",
# "https://www.lonelyplanet.com/mozambique/maputo/attractions",
# "https://www.lonelyplanet.com/ethiopia/northern-ethiopia/lalibela/attractions",
# "https://www.lonelyplanet.com/italy/sicily/milazzo/attractions",
# "https://www.lonelyplanet.com/maldives/male/attractions",
# "https://www.lonelyplanet.com/canada/vancouver/attractions",
# "https://www.lonelyplanet.com/georgia/western-georgia/kutaisi/attractions",
# "https://www.lonelyplanet.com/egypt/sinai/dahab/attractions",
# "https://www.lonelyplanet.com/sri-lanka/the-east/trincomalee/attractions",
# "https://www.lonelyplanet.com/the-netherlands/nijmegen/attractions",
# "https://www.lonelyplanet.com/sri-lanka/jaffna-and-the-north/jaffna/attractions",
# "https://www.lonelyplanet.com/vietnam/around-ho-chi-minh-city/vung-tau/attractions",
# "https://www.lonelyplanet.com/china/gansu/dunhuang/attractions",
# "https://www.lonelyplanet.com/usa/santa-fe/attractions",
# "https://www.lonelyplanet.com/italy/liguria-piedmont-and-valle-daosta/savona/attractions",
# "https://www.lonelyplanet.com/usa/new-england/newport/attractions",
# "https://www.lonelyplanet.com/egypt/el-gouna/attractions",
# "https://www.lonelyplanet.com/greece/athens/attractions",
# "https://www.lonelyplanet.com/mexico/central-pacific-coast/sayulita/attractions",
# "https://www.lonelyplanet.com/malaysia/peninsular-malaysia-west-coast/georgetown/attractions",
# "https://www.lonelyplanet.com/jordan/petra-and-the-south/aqaba/attractions",
# "https://www.lonelyplanet.com/portugal/viseu/attractions",
# "https://www.lonelyplanet.com/usa/new-england/burlington/attractions",
# "https://www.lonelyplanet.com/india/madhya-pradesh-and-chhattisgarh/orchha/attractions",
# "https://www.lonelyplanet.com/nicaragua/managua/attractions",
# "https://www.lonelyplanet.com/italy/sardinia/cala-gonone-and-around/attractions",
# "https://www.lonelyplanet.com/germany/lower-saxony/hanover/attractions",
# "https://www.lonelyplanet.com/poland/katowice/attractions",
# "https://www.lonelyplanet.com/south-korea/gyeonggi-do/suwon/attractions",
# "https://www.lonelyplanet.com/france/versailles/attractions",
# "https://www.lonelyplanet.com/england/northwest-england/manchester/attractions",
# "https://www.lonelyplanet.com/spain/badajoz/attractions",

# "https://www.lonelyplanet.com/south-korea/gyeongsangbuk-do/daegu/attractions",
# "https://www.lonelyplanet.com/australia/new-south-wales/newcastle/attractions",
# "https://www.lonelyplanet.com/usa/the-south/charleston/attractions",
# "https://www.lonelyplanet.com/thailand/lower-southern-gulf/hat-yai/attractions",
# "https://www.lonelyplanet.com/panama/chiriqui-province/boquete/attractions",
# "https://www.lonelyplanet.com/the-guianas/guyana/georgetown/attractions",
# "https://www.lonelyplanet.com/croatia/gulf-of-kvarner/rijeka/attractions",
# "https://www.lonelyplanet.com/germany/kiel/attractions",
# "https://www.lonelyplanet.com/south-korea/jeollabuk-do/jeonju/attractions",
# "https://www.lonelyplanet.com/thailand/udon-thani-province/udon-thani/attractions",
# "https://www.lonelyplanet.com/usa/great-plains/kansas-city/attractions",
# "https://www.lonelyplanet.com/uganda/kampala/attractions",
# "https://www.lonelyplanet.com/spain/basque-country/hondarribia/attractions",
# "https://www.lonelyplanet.com/england/northwest-england/manchester/attractions",
# "https://www.lonelyplanet.com/canada/northwest-territories/yellowknife/attractions",
# "https://www.lonelyplanet.com/mexico/central-pacific-coast/manzanillo/attractions",
# "https://www.lonelyplanet.com/kenya/diani-beach/attractions",
# "https://www.lonelyplanet.com/the-guianas/guyana/georgetown/attractions",
# "https://www.lonelyplanet.com/peru/central-highlands/ayacucho/attractions",
# "https://www.lonelyplanet.com/portugal/the-algarve/lagos/attractions",
# "https://www.lonelyplanet.com/uzbekistan/khorezm/khiva/attractions",
# "https://www.lonelyplanet.com/portugal/aljezur/attractions",
# "https://www.lonelyplanet.com/costa-rica/central-pacific-coast/uvita/attractions",
# "https://www.lonelyplanet.com/indonesia/sumatra/banda-aceh/attractions"

    #  "https://www.lonelyplanet.com/china/hunan/changsha/attractions",
    #  "https://www.lonelyplanet.com/mexico/baja-california/loreto/attractions",
    #  "https://www.lonelyplanet.com/indonesia/bali/padangbai/attractions",
    #  "https://www.lonelyplanet.com/chile/northern-patagonia/coyhaique/attractions",
    #  "https://www.lonelyplanet.com/fiji/viti-levu/suva/attractions",
    #  "https://www.lonelyplanet.com/malaysia/malaysian-borneo-sabah/semporna-and-pulau-sipadan/attractions",
    #  "https://www.lonelyplanet.com/north-korea/pyongyang/attractions",
    #  "https://www.lonelyplanet.com/england/eastern-england/cambridge/attractions",
    #  "https://www.lonelyplanet.com/belize/western-belize/san-ignacio-cayo/attractions",
    #  "https://www.lonelyplanet.com/honduras/tegucigalpa/attractions",
    #  "https://www.lonelyplanet.com/switzerland/geneva/attractions",
    #  "https://www.lonelyplanet.com/malaysia/malaysian-borneo-sarawak/miri/attractions",
    #  "https://www.lonelyplanet.com/thailand/lower-southern-gulf/surat-thani/attractions",
    #  "https://www.lonelyplanet.com/slovenia/maribor/attractions",
    #  "https://www.lonelyplanet.com/dominican-republic/north-coast/cabarete/attractions",
    #  "https://www.lonelyplanet.com/hungary/the-danube-bend/szentendre/attractions",
    #  "https://www.lonelyplanet.com/sweden/norrland/abisko/attractions",
    #  "https://www.lonelyplanet.com/usa/california/berkeley/attractions",
    #  "https://www.lonelyplanet.com/mexico/central-gulf-coast/xalapa/attractions",
    #  "https://www.lonelyplanet.com/usa/california/san-jose/attractions",
    #  "https://www.lonelyplanet.com/spain/andalucia/vejer-de-la-frontera/attractions",
    #  "https://www.lonelyplanet.com/russia/russian-far-east/petropavlovsk-kamchatsky/attractions",
    #  "https://www.lonelyplanet.com/greece/crete/agios-nikolaos/attractions",
    #  "https://www.lonelyplanet.com/colombia/amazon-basin/leticia/attractions",
    #  "https://www.lonelyplanet.com/the-guianas/guyana/georgetown/attractions",
    #  "https://www.lonelyplanet.com/mexico/oaxaca-state/mazunte/attractions",
    #  "https://www.lonelyplanet.com/croatia/motovun/attractions",
    #  "https://www.lonelyplanet.com/spain/valencia-and-murcia/valencia/attractions",
    #  "https://www.lonelyplanet.com/albania/southern-albania/saranda/attractions",
    #  "https://www.lonelyplanet.com/france/villefranche-sur-mer/attractions",
    #  "https://www.lonelyplanet.com/spain/calella-de-palafrugell/attractions",
    #  "https://www.lonelyplanet.com/puerto-rico/aguadilla/attractions",
    #  "https://www.lonelyplanet.com/usa/great-lakes/cleveland/attractions",
    #  "https://www.lonelyplanet.com/oman/al-hamra/attractions",
    #  "https://www.lonelyplanet.com/croatia/makarska/attractions",
    #  "https://www.lonelyplanet.com/tajikistan/dushanbe/attractions",
    #  "https://www.lonelyplanet.com/spain/valldemossa/attractions",
    #  "https://www.lonelyplanet.com/germany/berlin/attractions",
    #  "https://www.lonelyplanet.com/malaysia/taiping/attractions",
    #  "https://www.lonelyplanet.com/colombia/minca/attractions",
    #  "https://www.lonelyplanet.com/malaysia/peninsular-malaysia-east-coast/kuantan/attractions",
    #  "https://www.lonelyplanet.com/peru/amazon-basin/puerto-maldonado/attractions",
    #  "https://www.lonelyplanet.com/usa/nashville/attractions",
    #  "https://www.lonelyplanet.com/new-zealand/wellington/attractions",
# "https://www.lonelyplanet.com/germany/mittenwald/attractions",
# "https://www.lonelyplanet.com/thailand/phitsanulok-province/phitsanulok/attractions",
# "https://www.lonelyplanet.com/italy/the-veneto/verona/attractions",
# "https://www.lonelyplanet.com/seychelles/mahe/victoria/attractions",
# "https://www.lonelyplanet.com/bulgaria/black-sea-coast/burgas/attractions",
# "https://www.lonelyplanet.com/england/yorkshire/york/attractions",
# "https://www.lonelyplanet.com/canada/ontario/kingston/attractions",
# "https://www.lonelyplanet.com/thailand/nong-khai-province/nong-khai/attractions",
# "https://www.lonelyplanet.com/ghana/the-coast/cape-coast/attractions",
# "https://www.lonelyplanet.com/colombia/pereira/attractions",
# "https://www.lonelyplanet.com/hungary/the-danube-bend/esztergom/attractions",
# "https://www.lonelyplanet.com/australia/sydney/attractions",
# "https://www.lonelyplanet.com/the-guianas/guyana/georgetown/attractions",
# "https://www.lonelyplanet.com/montenegro/coastal-montenegro/ulcinj/attractions",
# "https://www.lonelyplanet.com/morocco/the-mediterranean-coast-and-the-rif/ceuta-sebta/attractions",
# "https://www.lonelyplanet.com/cambodia/northeastern-cambodia/kratie/attractions",
# "https://www.lonelyplanet.com/canary-islands/la-palma/santa-cruz-de-la-palma/attractions",
# "https://www.lonelyplanet.com/thailand/upper-southern-gulf/prachuap-khiri-khan/attractions",
# "https://www.lonelyplanet.com/usa/maryland/baltimore/attractions",
# "https://www.lonelyplanet.com/england/southwest-england/bristol/attractions",
# "https://www.lonelyplanet.com/italy/sicily/syracuse/attractions",
# "https://www.lonelyplanet.com/germany/hamburg/attractions",
# "https://www.lonelyplanet.com/switzerland/chur/attractions",
# "https://www.lonelyplanet.com/portugal/the-algarve/monchique/attractions",
# "https://www.lonelyplanet.com/usa/california/oakland/attractions",
#  "https://www.lonelyplanet.com/guatemala/the-highlands-quiche/chichicastenango/attractions",
# # "https://www.lonelyplanet.com/england/southwest-england/bristol/attractions",
# # "https://www.lonelyplanet.com/botswana/gaborone/attractions",
# # "https://www.lonelyplanet.com/cuba/central-cuba/trinidad/attractions",
# "https://www.lonelyplanet.com/spain/andalucia/arcos-de-la-frontera/attractions",
# # "https://www.lonelyplanet.com/austria/vienna/attractions",
# "https://www.lonelyplanet.com/canada/manitoba/churchill/attractions",
#  "https://www.lonelyplanet.com/japan/central-honshu/nagoya/attractions",
# "https://www.lonelyplanet.com/slovenia/karst-and-coast/koper/attractions",
# "https://www.lonelyplanet.com/morocco/the-mediterranean-coast-and-the-rif/melilla/attractions",
# "https://www.lonelyplanet.com/papua-new-guinea/port-moresby/attractions",
# "https://www.lonelyplanet.com/the-gambia/banjul/attractions",
# "https://www.lonelyplanet.com/guatemala/the-highlands-lago-de-atitlan/san-pedro-la-laguna/attractions",
# "https://www.lonelyplanet.com/peru/huaraz-and-the-cordilleras/huaraz/attractions",
# "https://www.lonelyplanet.com/colombia/north-of-bogota/san-gil/attractions",
# "https://www.lonelyplanet.com/cuba/eastern-cuba/baracoa/attractions",
# "https://www.lonelyplanet.com/switzerland/geneva/attractions",
# "https://www.lonelyplanet.com/russia/russian-far-east/yakutsk/attractions",
# "https://www.lonelyplanet.com/zambia/lusaka/attractions",
# "https://www.lonelyplanet.com/portugal/carrapateira/attractions",
# "https://www.lonelyplanet.com/portugal/odeceixe/attractions",
# "https://www.lonelyplanet.com/indonesia/kalimantan/balikpapan/attractions",
# "https://www.lonelyplanet.com/mexico/central-pacific-coast/manzanillo/attractions",
# "https://www.lonelyplanet.com/italy/campania/sorrento/attractions",
# "https://www.lonelyplanet.com/indonesia/nusa-tenggara/mataram/attractions",

# "https://www.lonelyplanet.com/usa/california/santa-cruz/attractions",
# "https://www.lonelyplanet.com/usa/california/santa-cruz/attractions",
# "https://www.lonelyplanet.com/china/xinjiang/kashgar/attractions",
# "https://www.lonelyplanet.com/thailand/ubon-ratchathani-province/ubon-ratchathani/attractions",
# "https://www.lonelyplanet.com/bulgaria/black-sea-coast/varna/attractions",
# "https://www.lonelyplanet.com/turkmenistan/ashgabat/attractions",
# "https://www.lonelyplanet.com/morocco/the-atlantic-coast/asilah/attractions",
# "https://www.lonelyplanet.com/mexico/yucatan-peninsula/chetumal/attractions",
# "https://www.lonelyplanet.com/guatemala/the-highlands-lago-de-atitlan/panajachel/attractions",
# "https://www.lonelyplanet.com/china/guizhou/guiyang/attractions",
# "https://www.lonelyplanet.com/england/southeast-england/dover/attractions",
# "https://www.lonelyplanet.com/england/southwest-england/bath/attractions",c
# "https://www.lonelyplanet.com/italy/corniglia/attractions",
# "https://www.lonelyplanet.com/england/yorkshire/leeds/attractions",
# "https://www.lonelyplanet.com/usa/austin/attractions",
# "https://www.lonelyplanet.com/indonesia/kerobokan/attractions",
# "https://www.lonelyplanet.com/ecuador/the-southern-highlands/loja/attractions",
# "https://www.lonelyplanet.com/panama/panama-city/attractions",
# "https://www.lonelyplanet.com/england/southeast-england/rye/attractions",,
# "https://www.lonelyplanet.com/new-zealand/queenstown-and-wanaka/queenstown/attractions",
# "https://www.lonelyplanet.com/bosnia-hercegovina/trebinje/attractions",
# "https://www.lonelyplanet.com/spain/castilla-la-mancha/toledo/attractions",
# "https://www.lonelyplanet.com/guatemala/western-highlands/quetzaltenango-xela/attractions",
# "https://www.lonelyplanet.com/sudan/khartoum/attractions",
# "https://www.lonelyplanet.com/bulgaria/central-balkans/ruse/attractions",
# "https://www.lonelyplanet.com/malaysia/peninsular-malaysia-east-coast/kota-bharu/attractions",
#  "https://www.lonelyplanet.com/usa/florida/orlando/attractions",
 "https://www.lonelyplanet.com/japan/tokyo/attractions",


        ]
        for url in urls:
            yield scrapy.Request(url, callback=self.parse_main)

    def parse_main(self, response):
        # derive country slug (e.g. “india”, “egypt”)
        country = response.url.rstrip("/").split("/")[-2]

        json_text = response.css('script#__NEXT_DATA__::text').get()
        
        data = json.loads(json_text) if json_text else {}
        items = data.get("props", {}).get("pageProps", {}).get("pois", {}).get("items", [])
        city_names = response.xpath(
            '//p[contains(@class,"text-sm") and contains(@class,"font-semibold") '
            'and contains(@class,"uppercase")]/text()'
        ).getall()

        for idx, item in enumerate(items, 1):
            city = city_names[idx-1].strip() if idx-1 < len(city_names) else None
            slug = item.get("slug")
            detail_url = f"https://www.lonelyplanet.com/{slug}" if slug else None

            attraction = {
                "country":           country,
                "city":              city,
                "index":             idx,
                "name":              item.get("title"),
                "latitude":          item.get("coordinates", {}).get("lat"),
                "longitude":         item.get("coordinates", {}).get("lon"),
                "excerpt":           item.get("excerpt", ""),
                "source_url":        detail_url,
                "image_url":         (item.get("images") or [{}])[0].get("url"),
                "detail_description": "",
            }

            if detail_url:
                yield scrapy.Request(
                    detail_url,
                    callback=self.parse_detail,
                    meta={"attraction": attraction},
                    dont_filter=True,
                )
            else:
                yield attraction

    def parse_detail(self, response):
        attraction = response.meta["attraction"]
        json_text = response.css('script#__NEXT_DATA__::text').get()
        data = json.loads(json_text) if json_text else {}
        editorial = data.get("props", {}).get("pageProps", {}).get("editorial", "")
        attraction["detail_description"] = remove_tags(editorial) if editorial else ""
        yield attraction

# import scrapy
# import json
# from w3lib.html import remove_tags

# class LonelyPlanetIndianAttractionsSpider(scrapy.Spider):
#     name = "lonelyplanet_indian_attractions"
    
#     custom_settings = {
#         'USER_AGENT': (
#             'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
#             'AppleWebKit/537.36 (KHTML, like Gecko) '
#             'Chrome/91.0.4472.124 Safari/537.36'
#         ),
#         'DOWNLOAD_DELAY': 1,
#     }

#     def __init__(self, *args, **kwargs):
#         super(LonelyPlanetIndianAttractionsSpider, self).__init__(*args, **kwargs)
#         self.attractions = []       # Accumulate all attraction records
#         self.attraction_index = 1   # Running index (Sno)

#     def start_requests(self):
#         # Starting URL for Mumbai attractions
#         start_url = "https://www.lonelyplanet.com/india/attractions"
#         self.logger.info("Starting attractions scrape from: %s", start_url)
#         yield scrapy.Request(url=start_url, callback=self.parse_main)

#     def parse_main(self, response):
#         """
#         Parse the main attractions page.
#         Extract JSON data from the __NEXT_DATA__ script,
#         then loop over each attraction item.
#         """
#         json_text = response.css('script#__NEXT_DATA__::text').get()
#         if not json_text:
#             self.logger.error("No JSON found in the main page.")
#             return

#         try:
#             data = json.loads(json_text)
#         except json.JSONDecodeError as e:
#             self.logger.error("JSON decode error: %s", e)
#             return

#         # Access the attractions list; expected structure:
#         # data["props"]["pageProps"]["pois"]["items"]
#         try:
#             items = data["props"]["pageProps"]["pois"]["items"]
#         except (KeyError, TypeError) as e:
#             self.logger.error("Error accessing attractions items: %s", e)
#             return

#         self.logger.info("Found %d attractions on the main page.", len(items))
#         for item in items:
#             # Extract coordinates
#             coordinates = item.get("coordinates", {})
#             lat = coordinates.get("lat")
#             lon = coordinates.get("lon")
            
#             # Extract excerpt (short summary)
#             excerpt = item.get("excerpt", "")
            
#             # Retrieve slug and title from the top level fields.
#             slug = item.get("slug")  # e.g., "india/delhi/greater-delhi-gurgaon-gurugram/attractions/..."
#             name = item.get("title", "Unknown Attraction")
            
#             # Build the detail URL by prefixing with LonelyPlanet base URL if slug exists
#             detail_url = "https://www.lonelyplanet.com/" + slug if slug else None

#             # Extract image URL from the images list (if available)
#             image_url = None
#             images = item.get("images", [])
#             if images and isinstance(images, list):
#                 first_image = images[0]
#                 image_url = first_image.get("url")

#             # Build the attraction record with a running index
#             attraction = {
#                 "index": self.attraction_index,
#                 "name": name,
#                 "latitude": lat,
#                 "longitude": lon,
#                 "excerpt": excerpt,
#                 "detail_url": detail_url,
#                 "image_url": image_url,
#                 "detail_description": ""  # To be filled from detail page
#             }
#             self.attraction_index += 1

#             if detail_url:
#                 # Request the detail page to extract the full description
#                 yield scrapy.Request(
#                     url=detail_url,
#                     callback=self.parse_detail,
#                     meta={"attraction": attraction},
#                     dont_filter=True
#                 )
#             else:
#                 self.attractions.append(attraction)
#                 yield attraction

#     def parse_detail(self, response):
#         """
#         Parse the detail page for an individual attraction.
#         Extract the JSON and then get the detailed description (editorial)
#         from the embedded JSON. Clean the HTML tags from the description.
#         """
#         attraction = response.meta.get("attraction", {})
#         json_text = response.css('script#__NEXT_DATA__::text').get()
#         if not json_text:
#             self.logger.error("No JSON found on detail page: %s", response.url)
#             yield attraction
#             return


#         try:
#             data = json.loads(json_text)
#         except json.JSONDecodeError as e:
#             self.logger.error("JSON decode error on detail page %s: %s", response.url, e)
#             yield attraction
#             return

#         # Expected structure: data["props"]["pageProps"]["editorial"]
#         editorial = data.get("props", {}).get("pageProps", {}).get("editorial", "")
#         detail_description = remove_tags(editorial) if editorial else ""
#         attraction["detail_description"] = detail_description

#         self.attractions.append(attraction)
#         yield attraction

#     def closed(self, reason):
#         """
#         When the spider finishes, dump all collected attractions into a JSON file.
#         """
#         output_file = "lonelyplanet_india__attractions.json"
#         with open(output_file, "w", encoding="utf-8") as f:
#             json.dump(self.attractions, f, ensure_ascii=False, indent=4)
#         self.logger.info("Saved %d attractions to %s", len(self.attractions), output_file)

