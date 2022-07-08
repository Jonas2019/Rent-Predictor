import pandas as pd
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from re import search

def main():
    df = pd.read_csv('Data_ETL/data_frame.csv')
    df['time'] = pd.to_datetime(df['time'])
    cutoff_time = df['time'].max()
    startingUrl = "https://vancouver.craigslist.org/search/apa?query=ubc&min_price=&max_price=&availabilityMode=0&sale_date=all+dates"
    HEADER = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '3600',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }
    req = requests.get(startingUrl, HEADER)
    soup = BeautifulSoup(req.content, 'html.parser')
    # find all rows
    resultRows = soup.find_all("li", {"class": "result-row"})
    data = []
    for result in resultRows:
        row = {}
        time = result.find("time", {"class": "result-date"})['datetime']
        format_date = datetime.strptime(time, '%Y-%m-%d %H:%M')
        if format_date <= cutoff_time:
            break
        else:
            header = result.find("h3", {"class": "result-heading"}).find("a")
            name = header.text
            href = header['href']
            price = result.find("span", {"class": "result-price"}).text
            bdr = result.find("span", {"class": "housing"})
            if bdr:
                bdr = bdr.text
                if search("br", bdr):
                    bdr = int(bdr[bdr.find(' ')+len(' '):bdr.rfind('br')])
                else:
                    bdr = 1
            else:
                bdr = 1
            row['name'] = name
            row['href'] = href
            row['time'] = time
            row['price'] = price
            row['bedroom'] = bdr
            data.append(row)
    df = pd.DataFrame(data)
    print(df.shape)




if __name__ == '__main__':
    main()

