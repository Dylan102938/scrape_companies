import pandas as pd
from bs4 import BeautifulSoup as bs
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import requests
import urllib
import re
import psycopg2
import numpy as np
import math
from func_timeout import func_set_timeout
from time import sleep
import sys



# reading dataframe
conn = psycopg2.connect("dbname=company_repository user=postgres password=Dylan5188")
df = pd.read_csv("companies_condensed.csv").iloc[1142:50000]
print("# of companies:", df.shape[0])


# search patterns
link_patterns = [
    ".*about.*",
    ".*product.*",
    ".*solution.*",
    ".*company.*",
    ".*customer.*",
    ".*learn.*",
    ".*api.*",
    ".*explore.*",
    ".*technology.*",
    ".*feature.*",
    ".*press.*",
    ".*platform.*",
    ".*business.*",
    ".*offering.*"
]

exclude_patterns = [
    ".*terms.*of.*",
    ".*policy.*",
    ".*privacy.*",
    ".*disclos.*",
    ".*facebook.*",
    ".*linkedin.*",
    ".*twitter.*",
    ".*instagram.*"
]

headers = {
    'accept': 'text/html,application/xhtml+xml',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.8',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'
}


# defining getting text functions
def get_text_from_single_page(url, min_passage_length=5):
    try:
        # print("Checking:", url)
        resp = requests.get(url, headers=headers, timeout=3)

        soup = bs(resp.text, 'html.parser')
        elements = soup.find_all("h1") + soup.find_all("h2") + soup.find_all("h3") + soup.find_all("h4") + soup.find_all("p")

        return set([re.sub('[^0-9A-Za-z:,.?!;%$+-\]] ', '', url + " [SEP] " + elm.text.strip()) for elm in elements if len(elm.text.strip().split()) >= min_passage_length and elm.text.strip() != ''])
    except:
        return set()

@func_set_timeout(90)
def get_text(url, min_passage_length=5, max_extra_passages=30):
    resp = requests.get(url, headers=headers, timeout=3)
    soup = bs(resp.text, 'html.parser')
    evaluated_url = resp.url

    links = set([evaluated_url])
    remaining = set([])
    for a in soup.find_all('a'):
        try:
            link = a['href']
            if "http" not in link:
                link = urllib.parse.urljoin(evaluated_url, link)
            if "mailto" in a['href'] or "#" in a['href']:
                continue

            if any([re.match(pattern, link) for pattern in link_patterns]):
                links.add(link)
            elif not any([re.match(pattern, link) for pattern in exclude_patterns]) and len(remaining) < max_extra_passages:
                remaining.add(link)
        except:
            pass

    links |= remaining 

    text_set = set()
    it = iter(links)
    lock = Lock()
    with ThreadPoolExecutor(max_workers=32) as executor:
        for res in executor.map(get_text_from_single_page, it):
            with lock:
                text_set |= res
    
    return list(text_set), evaluated_url


# process single row
def process_row(tpl):
    _, batch = tpl
    try:
        print("Scraping:", batch['name'], "with domain name", batch['domain'])
        res, evaluated_url = get_text('https://' + batch['domain'])
        print("Finished:", batch['name'], "with domain name", batch['domain'])

        print("Writing to database:", batch['name'], "with domain name", batch['domain'])
        cur = conn.cursor()
        sql = "INSERT INTO pdl_companies (base_url, company, url, contents) VALUES (%s, %s, %s, %s);"
        for content in res:
            url = content.split(" [SEP] ")[0]
            cur.execute(sql, (evaluated_url, batch['name'], url, content))

        conn.commit()
        cur.close()
        print("Finished writing to database:", batch['name'], 'with domain name', batch['domain'])
    except:
        print("Error occured for", batch['name'], "skipping...")


# start scraping
counter = 0
batch_size = 4
# offset = int(sys.argv[1])


# parallelize processing
# with ThreadPoolExecutor(max_workers=batch_size) as executor:
#     for i, batch in executor.map(process_row, df.iterrows()):
#         print("Finished row:", i)

for tpl in df.iterrows():
    process_row(tpl)
