import csv
import datetime
import json
import os
import threading
import time
import traceback

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook

headers = ["id", "Ime/Naziv", "Adresa", "Telefon", "Fax", "Mobitel", "E-mail", "Jezici", "Verbena", "Odvjetnici",
           "Vježbenici", "Zaposlen/a u", "URL"]
hok = "https://www.hok-cba.eu/imenik/"
out = 'out-hok-cba.csv'
outxl = 'out-hok-cba.xlsx'
error = 'error-hok-cba.txt'
encoding = 'cp1250'
lock = threading.Lock()
threadcount = 5
semaphore = threading.Semaphore(threadcount)
scraped = []
total = 0
threads = []


def scrape(id):
    global scraped
    with semaphore:
        purl = f'{hok}pregled/{id}'
        print("Working on", purl)
        try:
            soup = BeautifulSoup(requests.get(purl).content, 'lxml')
            data = {
                "id": id,
                'Ime/Naziv': soup.find_all('h1')[1].text,
                "URL": purl
            }
            dts = soup.find_all('dt')
            dds = soup.find_all('dd')
            for dt, dd in zip(dts, dds):
                data[dt.text.strip()] = dd.text.strip()
            for h2 in soup.find_all('h2')[1:]:
                data[h2.text.strip()] = ""
                sib = h2.find_next_sibling()
                if sib.name == "a":
                    data[h2.text.strip()] += f"{sib.text.strip()} (https://www.hok-cba.eu/{sib['href']})"
                else:
                    for a in sib.find_all('a'):
                        data[h2.text.strip()] += f"{a.text.strip()} (https://www.hok-cba.eu/{a['href']})\n"
                    data[h2.text.strip()] = data[h2.text.strip()][:-1]
            print(f"{len(scraped)}/{total}", json.dumps(data, indent=4))
            append(data)
            scraped.append(id)
        except:
            traceback.print_exc()
            with open(error, 'a') as efile:
                efile.write(f"{id}\n")


def spawn(i):
    page = BeautifulSoup(requests.get(f"{hok}/?page={i}").content, 'lxml')
    print("Page", i)
    for a in page.find_all('a', text='Prikaži'):
        id = a['href'].split("/")[-1]
        if id not in scraped:
            t = threading.Thread(target=scrape, args=(id,))
            t.start()
            threads.append(t)
            time.sleep(0.1)
        else:
            print("Already scraped", id)


def main():
    # scrape(15761)
    # return
    global scraped, total, threads
    logo()
    if not os.path.isfile(out):
        with open(out, 'w', encoding=encoding, newline='') as outfile:
            csv.DictWriter(outfile, fieldnames=headers).writeheader()
    if os.path.isfile(error):
        print("Working on", error)
        threads = []
        with open(error) as efile:
            for line in efile:
                t = threading.Thread(target=scrape, args=(line.strip(),))
                t.start()
                threads.append(t)
        for thread in threads:
            thread.join()
        print("Done with error file")
    with open(out) as outfile:
        scraped = [line['id'] for line in csv.DictReader(outfile)]
    print('Loading data...')
    page = BeautifulSoup(requests.get(hok).content, 'lxml')
    total = int(page.find('p').text.split()[-1])
    print(f"Scraped: {len(scraped)}\nTotal: {total}")
    print("Already scraped data", scraped)
    for i in range(1, int(total / 10) + 2):
        threading.Thread(spawn(i)).start()
        time.sleep(0.1)
    for thread in threads:
        thread.join()
    print("Done with scraping.")
    cvrt()
    print("Done with conversion.")
    print("All done!")


def append(data):
    with lock:
        with open(out, 'a', encoding=encoding, newline='') as outfile:
            csv.DictWriter(outfile, fieldnames=headers).writerow(data)


def cvrt():
    wb = Workbook()
    ws = wb.active
    with open(out, 'r', encoding=encoding) as f:
        for row in csv.reader(f):
            ws.append(row)
    wb.save(outxl)


def logo():
    os.system('color 0a')
    print(f"""
    .__            __                   ___.           
    |  |__   ____ |  | __           ____\_ |__ _____   
    |  |  \ /  _ \|  |/ /  ______ _/ ___\| __ \\\\__  \  
    |   Y  (  <_> )    <  /_____/ \  \___| \_\ \/ __ \_
    |___|  /\____/|__|_ \          \___  >___  (____  /
         \/            \/              \/    \/     \/ 
==============================================================
                   hok-cba.eu scraper by:
            https://github.com/evilgenius786
==============================================================
[+] Multithreaded (Thread count: {threadcount})
[+] Without browser
[+] Super fast
[+] Resumable
[+] Check duplicates
________________________________________________
""")


if __name__ == '__main__':
    main()
