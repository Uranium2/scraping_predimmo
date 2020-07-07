import os
import json
import time
import random
import pymysql
import requests
import pandas as pd

from csv import writer
from bs4 import BeautifulSoup
from datetime import datetime
from fake_headers import Headers


url_ref = "https://www.leboncoin.fr"
url = "https://www.leboncoin.fr/recherche/?category=8&locations=Paris__48.85790400439862_2.358842071208555_10000"

header = Headers(headers=True)
url_page = "https://www.leboncoin.fr/recherche/?category=8&locations=Paris__48.85790400439862_2.358842071208555_10000/p-{}/"



def append_list_as_row(file_path, l):
    """ Add a list as row in a "filepath".csv

    Args:
        file_path [Stringtype]: Set the filepath.
        l [List]: Set the list to append to the "filepath".csv.
    """
    script_dir = os.path.dirname(__file__)
    abs_file_path = os.path.join(script_dir, file_path)
    
    with open(abs_file_path, 'a+', newline='', encoding='utf-8') as write_obj:
        csv_writer = writer(write_obj)
        csv_writer.writerow(l)


def scrap_page(url, date):
    """ Scrap a specific URL with specific date.

    Args:
        url [String]: Get the scrap page URL.
        date [Int]: Get the scrap date.

    Write [.csv]: Write scrapping in CSV.
    """
    response = requests.get(url, headers=header.generate())
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

    random.shuffle(list_immo)

    for elm in list_immo:
        time.sleep(random.randint(5, 15))

        # Store all info inside values_col and append to file
        values_col = []
        values_col.append(date)

        build_url = url_ref + elm
        print(build_url)
        response_immo = requests.get(build_url, headers=header.generate())
        if (response_immo.status_code != 200):
            print(response_immo.status_code)
            continue
        soup_immo = BeautifulSoup(response_immo.text, 'html.parser')

        # Description
        description = soup_immo.find("span", class_="_1fFkI").text
        description = description.replace(',', ' ')
        description = description.replace('\n', ' ')
        
        # Price
        try:
            price = int(soup_immo.find("span", class_="_3Ce01 _3gP8T _25LNb _35DXM").text.replace("€", "").replace(" ", ""))
        except:
            print(build_url + " => No price")
            continue

        
        # City & zip code location
        localisation_all = soup_immo.find_all("h2", class_="Roh2X _3c6yv _25dUs _21rqc _3QJkO _1hnil _1-TTU _35DXM")[2]

        city = localisation_all.text.split("(")[0]
        zip_code = localisation_all.text.split("(")[-1].split(")")[0]

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

        # Write to csv
        send_to_rds(values_col, conn)
        append_list_as_row("data/{}.csv".format(date), values_col)


def run_scrapping():
    """ Start scrapping on specific URL page and date.
    """
    date = datetime.now().strftime("%Y-%m-%d")
    size = 100
    r = list(range(size))
    random.shuffle(r)
    for i in r:
        scrap_page(url_page.format(i), date)
        print(str(i) + " / " + str(size))


def create_conn():
    """ Create connection to RDS wyth PyMySQL. 

    Returns:
        pymysql [String]: SQL Credentials.
    """
    dir_path = os.path.dirname(os.path.realpath(__file__))
    f = open(os.path.join(dir_path, "aws_keys"), "r")
    keys = f.read().split("\n")

    return pymysql.connect(
        host= keys[2],
        user=keys[3],
        password=keys[4],
        port=int(keys[5]))


def send_to_rds(data, conn):
    """ Upload a dataframe in RDS by PyMySQL.

    Args:
        data [Dataframe]: Get a dataframe to upload.
        conn [String]: SQL Statement to RDS.
    """
    if (len(data) < 9):
        return
    cursor = conn.cursor()
    header_data = ["date_mutation", "code_postal", "valeur_fonciere", "code_type_local", "surface_reelle_bati", "nombre_pieces_principales","surface_terrain","longitude","latitude","message"]
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
    sql = "REPLACE INTO predimmo.data_django(" + header_data + ") VALUES (" + "%s,"*(len(insert_data)-1) + "%s)"
    print(sql)
    cursor.execute(sql, tuple(insert_data))
    conn.commit()


def get_coord_from_address(code_postal, adresse=None):
    """ Get longitude and latitude from an address.

    Args:
        code_postal [String]: Get zip code.
        adresse [String] (optional): Get address (Defaults to None).

    Returns:
        pos [List]: This is a list of longitude and latitude.
    """
    headers = {"Content-Type": "application/json"}
    if adresse != None:
        url = str(("http://api-adresse.data.gouv.fr/search/?q=" + str(adresse) + "&postcode=" + str(code_postal)))
    else:
        url = str(("http://api-adresse.data.gouv.fr/search/?q=" + str(code_postal)))

    r = requests.get(url, headers=header.generate(), data="")
    js = json.loads(r.text)
    x = js['features'][0]['geometry']['coordinates']
    longitude = x[0]
    latitude = x[1]
    
    pos = []
    pos.append(longitude)
    pos.append(latitude)

    return pos
