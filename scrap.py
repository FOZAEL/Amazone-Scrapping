"""
	DB SCHEMA
	CREATE TABLE `scrapper_amazon` (
		`id` INT(222) AUTO_INCREMENT,
		`product_title` TEXT(250),
		`link` TEXT(500),
		`image_link` TEXT(200),
		`current_price` TEXT(500),
		`scraped_date` timestam,
		`asin` TEXT,
		PRIMARY KEY (`id`)
	);
"""

from typing import List
import requests
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import pandas as pd
import argparse
from urllib.parse import urlparse
import concurrent.futures
from datetime import date
import mysql.connector
from datetime import datetime
import time
import random

now = datetime.now()
formatted_date = now.strftime('%Y-%m-%d')


# def insertRecord(data):  # data a tuple of your data
#     global mydb


def insert_varibles_into_table(product_title, link, image_link, current_price, scraped_date, asin):
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='amz',
                                             user='main',
                                             password='123')
        cursor = connection.cursor()
        mySql_insert_query = """INSERT INTO scap (product_title, link, image_link, current_price, scraped_date, asin) 
                                VALUES (%s, %s, %s, %s, %s, %s) """

        record = (product_title, link, image_link, current_price, scraped_date, asin)
        cursor.execute(mySql_insert_query, record)
        connection.commit()
        print("Record inserted successfully into scap table")

    except mysql.connector.Error as error:
        print("Failed to insert into MySQL table {}".format(error))

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def GET_UA():
    uastrings = ["Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36",\
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0",\
                "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/600.1.17 (KHTML, like Gecko) Version/7.1 Safari/537.85.10",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",\
                "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36"\
                ]

    return random.choice(uastrings)


def getdata(url,userAgent):
    time.sleep(5)
    headers = {'User-Agent': userAgent}
    r = s.get(url, headers=headers)
    print(f"Code Url :{r.status_code}\n{url[70:]}\nUserAgent :{userAgent}")
    soup = BeautifulSoup(r.text, 'html.parser')
    return soup


def getdeals(soup):
    products = soup.find_all('div', {'data-component-type': 's-search-result'})
    for item in products:
        title = item.find(
            'a', {'class': 'a-link-normal a-text-normal'}).text.strip()
        link = item.find('a', {'class': 'a-link-normal a-text-normal'})['href']
        imagelink = item.find('img', {'class': 's-image'})['src']
        asins = []

        if item.attrs['data-asin'] != '':
            asins.append(item.attrs['data-asin'])

        try:
            saleprice = float(item.find_all(
                'span', {'class': 'a-offscreen'})[0].text.replace('₹', '').replace(',', '').strip())
        except:
            saleprice = soup.find(
                'span', {'class': 'a-offscreen'}).text.replace('₹', '').replace(',', '').strip()

    return title,link,imagelink,saleprice,asins


def getnextpage(soup):
    next_ = soup.select_one("li.a-last a")
    if not next_:
        next_ = soup.select_one(".s-pagination-next")
        if not next_:
            return

    return "https://www.amazon.in" + next_["href"]


s = requests.Session()

url = f'https://www.amazon.in/s?i=apparel&bbn=1571271031&rh=n%3A1571271031&dc&fs=true&qid=1627938579&ref=sr_ex_n_1'
User= GET_UA()

while True:

    soup = getdata(url,User)
    t=getdeals(soup)
    insert_varibles_into_table(t[0],t[1],t[2],t[3],formatted_date,t[4][0])

    url = getnextpage(soup)
    if not url:
        break
    else:
        print(url)