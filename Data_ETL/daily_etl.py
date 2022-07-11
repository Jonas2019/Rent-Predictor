from attr import asdict
import pandas as pd
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from re import search
from time import sleep
import random


def read_mongo(env_key):
    mongo_uri = 'mongodb+srv://JW:{}@cluster0.q4g0hww.mongodb.net/?retryWrites=true&w=majority'.format(
        env_key)
    client = MongoClient(mongo_uri)
    collection = client.RentPredictorDatabase.RentPredictorCollection
    df = pd.DataFrame(list(collection.find({})))
    df.drop('_id', axis=1)
    return df


def get_newest_rents(startingUrl, HEADER, cutoff_time):
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
    return df


def get_location(df):
    hrefs = df['href'].tolist()
    location = []
    for index in range(0, len(hrefs)):
        req = requests.get(hrefs[index], headers={'User-Agent': 'Custom'})
        sleep(random.randint(3, 7))
        if req.status_code == 200:
            soup = BeautifulSoup(req.content, 'html.parser')
            result = soup.find("div", {"class": "mapaddress"})
            # If listing does specifies map address
            if result is not None:
                location.append(result.text)
            # If listing does not specify map address
            else:
                location.append('location not found')
        else:
            print("unexpected status code {}".format(req.status_code))
            break
    return location


def get_longtitude_lattitude(df):
    mapquest_key = os.getenv('MAPQUEST_KEY')
    locations = df['location'].tolist()
    lattitudes = []
    longitudes = []
    for index, ele in enumerate(df['location']):
        if ele != 'location not found':
            maprequest_api_url = "http://open.mapquestapi.com/geocoding/v1/address?key={}&location={}".format(
                mapquest_key, locations[index] + ',BC,Canada')
            response = requests.get(maprequest_api_url)
            data = response.json()
            data = data['results'][0]['locations'][0]['latLng']
            lat = data['lat']
            lng = data['lng']
            lattitudes.append(lat)
            longitudes.append(lng)
        else:
            lattitudes.append(0)
            longitudes.append(0)
    return longitudes, lattitudes


def get_valid_address(df):
    validation_location = (df['longitudes'] < - 110) & (df['lattitudes'] > 45)
    return validation_location


def add_geo_info(df):
    print("Starting to run geolocation ETLs for {} rows".format(df.shape[0]))
    locations = get_location(df)
    df['location'] = locations
    print("Location ETL finished")
    longitudes, lattitudes = get_longtitude_lattitude(df)
    print("Geolocation ETL finished")
    df['longitudes'] = longitudes
    df['lattitudes'] = lattitudes
    validation_location = get_valid_address(df)
    df['validation_location'] = validation_location
    return df


def write_to_mongo(df):
    mongo_uri = 'mongodb+srv://JW:{}@cluster0.q4g0hww.mongodb.net/?retryWrites=true&w=majority'.format(
        os.getenv('MONGODB_USR_PASSWORD'))
    client = MongoClient(mongo_uri)
    collection = client.RentPredictorDatabase.RentPredictorCollection
    collection.insert_many(df.to_dict('records'))


def main():
    load_dotenv()
    df = read_mongo(os.getenv('MONGODB_USR_PASSWORD'))
    df['time'] = pd.to_datetime(df['time'])
    cutoff_time = df['time'].max()
    del df

    startingUrl = "https://vancouver.craigslist.org/search/apa?query=ubc&min_price=&max_price=&availabilityMode=0&sale_date=all+dates"
    HEADER = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '3600',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }
    df = get_newest_rents(startingUrl=startingUrl,
                          HEADER=HEADER, cutoff_time=cutoff_time)
    df = add_geo_info(df)
    df.to_csv('daily_data_frame.csv', index=False)
    write_to_mongo(df)


def write_from_local_csv(filename):
    df = pd.read_csv(filename)
    load_dotenv()
    write_to_mongo(df)


def check_newest_records():
    load_dotenv()
    df = read_mongo(os.getenv('MONGODB_USR_PASSWORD'))
    df['time'] = pd.to_datetime(df['time'])
    cutoff_time = df['time'].max()
    del df
    startingUrl = "https://vancouver.craigslist.org/search/apa?query=ubc&min_price=&max_price=&availabilityMode=0&sale_date=all+dates"
    HEADER = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '3600',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }
    df = get_newest_rents(startingUrl=startingUrl,
                          HEADER=HEADER, cutoff_time=cutoff_time)
    print(df.shape)


if __name__ == '__main__':
    main()
    # write_from_local_csv('daily_data_frame.csv')
    # check_newest_records()
