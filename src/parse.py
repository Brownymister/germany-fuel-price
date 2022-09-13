import datetime
import os

import mysql.connector
import pandas as pd
import requests
import schedule
from bs4 import BeautifulSoup
from dotenv import load_dotenv


class Parse:

    url = "https://www.billig-tanken.de/super-e10/"
    cities = [
        "Berlin",
        "Stuttgart",
        "Muenchen",
        "Potsdam",
        "Bremen",
        "Wiesbaden",
        "Schwerin",
        "Hannover",
        "Duesseldorf",
        "Mainz",
        "Saarbruecken",
        "Dresden",
        "Magdeburg",
        "Kiel",
        "Erfurt",
    ]

    prices: dict[int, int] = {}

    def __init__(self) -> None:
        self.scrape_all_prices()
        schedule.every(2).hours.do(self.scrape_all_prices)

        while True:
            schedule.run_pending()

    def create_table_if_not_exsits(self, mydb):
        sql = """CREATE TABLE IF NOT EXISTS prices (date varchar(255) PRIMARY KEY,Berlin float,Stuttgart float,Muenchen float,Potsdam float,Bremen float,Wiesbaden float,Schwerin float,Hannover float,Duesseldorf float,Mainz float,Saarbruecken float,Dresden float,Magdeburg float,Kiel float,Erfurt float);"""
        mycursor = mydb.cursor()
        mycursor.execute(sql)

    def scrape_all_prices(self):
        for city in self.cities:
            self.get_price(city)

        self.commit_to_db()
        print(self.prices)

    def get_price(self, city):
        request_prices = self.request_prices(self.url + city)
        soup = BeautifulSoup(request_prices, "html.parser")
        price_table = soup.find_all(class_="item")

        prices = []

        for price in price_table:
            if price.find(class_="typ").find("a").text == "Super E10":
                currant_price = float(
                    price.find(class_="price fav_price").text.replace(",", ".")
                )
                prices.append(currant_price)

        print(city)

        price_average = pd.DataFrame(prices)[0].mean()
        self.prices[city] = price_average

    def request_prices(self, url):
        prices = requests.get(url)
        return prices.content

    def commit_to_db(self):
        load_dotenv()
        mydb = mysql.connector.connect(
            host=os.environ.get("DBHOST"),
            user=os.environ.get("DBUSER"),
            password=os.environ.get("PASSWORD"),
            database=os.environ.get("DB"),
        )
        self.create_table_if_not_exsits(mydb)
        commit = f"""INSERT INTO prices
        (date,Berlin,Stuttgart,Muenchen,Potsdam,Bremen,Wiesbaden,Schwerin,Hannover,
        Duesseldorf,Mainz,Saarbruecken,Dresden,Magdeburg,Kiel,Erfurt) VALUES
        ("{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}",{self.prices["Berlin"]},{self.prices["Stuttgart"]}
        ,{self.prices["Muenchen"]},{self.prices["Potsdam"]},{self.prices["Bremen"]},{self.prices["Wiesbaden"]}
        ,{self.prices["Schwerin"]} ,{self.prices["Hannover"]} ,{self.prices["Duesseldorf"]} ,{self.prices["Mainz"]}
        ,{self.prices["Saarbruecken"]} ,{self.prices["Dresden"]} ,{self.prices["Magdeburg"]} ,{self.prices["Kiel"]}
        ,{self.prices["Erfurt"]}
        )
        """
        mycursor = mydb.cursor()
        mycursor.execute(commit)
        mydb.commit()


Parse()
