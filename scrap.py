import pandas as pd
from bs4 import BeautifulSoup
import requests
from csv import writer
import os
from datetime import datetime
import time
import random
import pymysql
import json

# !pip install fake-useragent

url_ref = "https://www.leboncoin.fr"
url = "https://www.leboncoin.fr/_immobilier_/offres/ile_de_france/"

headers = [{'User-Agent': 'Mozilla/5.0 (Linux; Android 8.0.0; SM-G960F Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.84 Mobile Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 7.0; SM-G892A Build/NRD90M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/60.0.3112.107 Mobile Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 7.0; SM-G930VC Build/NRD90M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/58.0.3029.83 Mobile Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0.1; SM-G935S Build/MMB29K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 5.1.1; SM-G928X Build/LMY47X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.83 Mobile Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0.1; Nexus 6P Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.83 Mobile Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 7.1.1; G8231 Build/41.2.A.0.219; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/59.0.3071.125 Mobile Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0.1; E6653 Build/32.2.A.0.253) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.98 Mobile Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; HTC One X10 Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/61.0.3163.98 Mobile Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; HTC One M9 Build/MRA58K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.98 Mobile Safari/537.3'},
{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Mobile/15E148 Safari/604.1'},
{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/69.0.3497.105 Mobile/15E148 Safari/605.1'},
{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/13.2b11866 Mobile/16A366 Safari/605.1.15'},
{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1'},
{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1'},
{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A5370a Safari/604.1'},
{'User-Agent': 'Mozilla/5.0 (iPhone9,3; U; CPU iPhone OS 10_0_1 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/14A403 Safari/602.1'},
{'User-Agent': 'Mozilla/5.0 (iPhone9,4; U; CPU iPhone OS 10_0_1 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/14A403 Safari/602.1'},
{'User-Agent': 'Mozilla/5.0 (Apple-iPhone7C2/1202.466; U; CPU like Mac OS X; en) AppleWebKit/420+ (KHTML, like Gecko) Version/3.0 Mobile/1A543 Safari/419.3'},
{'User-Agent': 'Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; RM-1127_16056) AppleWebKit/537.36(KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10536'},
{'User-Agent': 'Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 950) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Mobile Safari/537.36 Edge/13.1058'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 7.0; Pixel C Build/NRD90M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/52.0.2743.98 Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0.1; SGP771 Build/32.2.A.0.253; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/52.0.2743.98 Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0.1; SHIELD Tablet K1 Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 7.0; SM-T827R4 Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.116 Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 5.0.2; SAMSUNG SM-T550 Build/LRX22G) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/3.3 Chrome/38.0.2125.102 Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 4.4.3; KFTHWI Build/KTU84M) AppleWebKit/537.36 (KHTML, like Gecko) Silk/47.1.79 like Chrome/47.0.2526.80 Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 5.0.2; LG-V410/V41020c Build/LRX22G) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/34.0.1847.118 Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'},
{'User-Agent': 'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9'},
{'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1'},
{'User-Agent': 'Mozilla/5.0 (CrKey armv7l 1.5.16041) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.0 Safari/537.36'},
{'User-Agent': 'Mozilla/5.0 (Linux; U; Android 4.2.2; he-il; NEO-X5-116A Build/JDQ39) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Safari/534.30'},
{'User-Agent': 'Mozilla/5.0 (Linux; Android 5.1; AFTS Build/LMY47O) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/41.99900.2250.0242 Safari/537.36'},
{'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 6.0.1; Nexus Player Build/MMB29T)'},
{'User-Agent': 'AppleTV6,2/11.1'},
{'User-Agent': 'AppleTV5,3/9.1.1'},
{'User-Agent': 'Mozilla/5.0 (Nintendo WiiU) AppleWebKit/536.30 (KHTML, like Gecko) NX/3.0.4.2.12 NintendoBrowser/4.3.1.11264.US'},
{'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; XBOX_ONE_ED) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393'},
{'User-Agent': 'Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Xbox; Xbox One) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Mobile Safari/537.36 Edge/13.10586'},
{'User-Agent': 'Mozilla/5.0 (PlayStation 4 3.11) AppleWebKit/537.73 (KHTML, like Gecko)'},
{'User-Agent': 'Mozilla/5.0 (PlayStation Vita 3.61) AppleWebKit/537.73 (KHTML, like Gecko) Silk/3.2'}
]
url_page = "https://www.leboncoin.fr/_immobilier_/offres/ile_de_france/p-{}/"



def append_list_as_row(file_path, l):
    script_dir = os.path.dirname(__file__)
    abs_file_path = os.path.join(script_dir, file_path)
    with open(abs_file_path, 'a+', newline='', encoding='utf-8') as write_obj:
        csv_writer = writer(write_obj)
        csv_writer.writerow(l)

def scrap_page(url, date):
    header = random.choice(headers)
    response = requests.get(url, headers=header)
    print(url)
    if (response.status_code != 200):
        print(response.status_code)
        return
    soup = BeautifulSoup(response.text, 'html.parser')

    list_immo = []
    for a in soup.find_all('a', href=True):
        list_immo.append(a['href'])


    list_immo = [x for x in list_immo if "ventes_immobilieres" in x]
    list_immo = [x for x in list_immo if not "offres" in x]

    conn = create_conn()
    
    for elm in list_immo:
        time.sleep(3)
        # store all info inside values_col and append to file
        values_col = []
        values_col.append(date)

        build_url = url_ref + elm
        print(build_url)
        header = random.choice(headers)
        response_immo = requests.get(build_url, headers=header)
        if (response_immo.status_code != 200):
            print(response_immo.status_code)
            continue
        soup_immo = BeautifulSoup(response_immo.text, 'html.parser')

        # Description
        description = soup_immo.find("span", class_="_1fFkI").text
        description = description.replace(',', ' ')
        description = description.replace('\n', ' ')
        # print(description)
        # Price
        try:
            price = int(soup_immo.find("span", class_="_3Ce01 _3gP8T _25LNb _35DXM").text.replace("€", "").replace(" ", ""))
            # print(price)
        except:
            print(build_url + " => No price")
            continue

        
        # Localisation city + zip code
        localisation_all = soup_immo.find_all("h2", class_="Roh2X _3c6yv _25dUs _21rqc _3QJkO _1hnil _1-TTU _35DXM")[2]

        city = localisation_all.text.split("(")[0]
        # print(city)
        zip_code = localisation_all.text.split("(")[-1].split(")")[0]
        # print(zip_code)


        values_col.append(build_url)
        values_col.append(description)
        values_col.append(price)
        values_col.append(city)
        values_col.append(zip_code)

        div_key_value = soup_immo.find_all("p", class_="_2k43C _1pHkp _137P- P4PEa _3j0OU")

        for div in div_key_value:
            if (div.text == "Type de bien"):
                type_bien = div.findNext("p").text
                if type_bien == "Maison":
                    type_bien = 1
                elif type_bien == "Appartement":
                    type_bien = 2
                else:
                    print(type_bien)
                    break
                values_col.append(type_bien)
            if (div.text == "Surface"):
                values_col.append(div.findNext("p").text.split(" ")[0])
            if (div.text == "Pièces"):
                values_col.append(div.findNext("p").text)

        # Writte to csv
        send_to_rds(values_col, conn)
        append_list_as_row("data/{}.csv".format(date), values_col)



def run_scrapping():
    date = datetime.now().strftime("%Y-%m-%d")
    size = 100
    for i in range(2, size):
        scrap_page(url_page.format(i), date)
        print(str(i) + " / " + str(size))


def create_conn():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    f = open(os.path.join(dir_path, "aws_keys"), "r")
    keys = f.read().split("\n")
    return pymysql.connect(
        host= keys[2],
        user=keys[3],
        password=keys[4],
        port=int(keys[5]))

def send_to_rds(data, conn):
    if (len(data) < 7):
        return
    cursor = conn.cursor()
    header_data = ["date_mutation", "code_postal", "valeur_fonciere", "code_type_local", "surface_reelle_bati", "nombre_pieces_principales", "surface_terrain", "longitude", "latitude", "message"]
    header_data = ','.join(header_data)
    insert_data = []
    insert_data.append(data[0])
    insert_data.append(data[5])
    insert_data.append(data[3])
    insert_data.append(data[6])
    insert_data.append(data[7])
    insert_data.append(data[8])
    insert_data.append(data[7])
    pos = get_coord_from_address(data[5])
    insert_data.append(pos[0])
    insert_data.append(pos[1])
    insert_data.append(data[2])
    print(insert_data)
    sql = "REPLACE INTO predimmo.data(" + header_data + ") VALUES (" + "%s,"*(len(insert_data)-1) + "%s)"
    print(sql)
    cursor.execute(sql, tuple(insert_data))
    conn.commit()


def get_coord_from_address(code_postal, adresse=None):
    headers = {"Content-Type": "application/json"}
    if adresse != None:
        url = str(("http://api-adresse.data.gouv.fr/search/?q=" + str(adresse) + "&postcode=" + str(code_postal)))
    else:
        url = str(("http://api-adresse.data.gouv.fr/search/?q=" + str(code_postal)))
    r = requests.get(url, headers=headers, data="")
    js = json.loads(r.text)
    x = js['features'][0]['geometry']['coordinates']
    longitude = x[0]
    latitude = x[1]
    pos = []
    pos.append(longitude)
    pos.append(latitude)
    return pos