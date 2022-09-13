import copy
import datetime
import os
import sys
from tracemalloc import start

import matplotlib.pyplot as plt
import mysql.connector
import numpy as np
import pandas as pd
import seaborn as sns
from dotenv import load_dotenv

sns.set_theme()


class Evaluate:

    cities = [
        "date",
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

    reformated_prices = []

    def __init__(self, start_date="", end_date="") -> None:
        self.start_date = start_date
        self.end_date = end_date
        load_dotenv()
        mydb = mysql.connector.connect(
            host=os.environ.get("DBHOST"),
            user=os.environ.get("DBUSER"),
            password=os.environ.get("PASSWORD"),
            database=os.environ.get("DB"),
        )
        get_prices = self.get_all_data_from_db(mydb)

        if self.start_date == "" and self.end_date == "":
            self.reformat_data(get_prices)
        else:
            self.get_data_by_time_spand(
                get_prices,
                datetime.datetime.strptime(self.start_date, "%m-%d-%Y"),
                datetime.datetime.strptime(self.end_date, "%m-%d-%Y"),
            )

        self.plot_prcie_history()

    def get_data_by_time_spand(self, get_prices, start_date, end_date):
        selected_prices = []
        for day in get_prices:
            created_at = datetime.datetime.strptime(day[0], "%Y-%m-%d %H:%M:%S")
            if start_date <= created_at <= end_date:
                selected_prices.append(day)

        for day in selected_prices:
            price_of_day = {}
            for (price, param) in zip(day, self.cities):
                if param == "date":
                    price_of_day[param] = datetime.datetime.strptime(
                        price, "%Y-%m-%d %H:%M:%S"
                    )
                else:
                    price_of_day[param] = float(price)
            self.reformated_prices.append(price_of_day)

    def plot_prcie_history(self):
        self.prices = pd.DataFrame(self.reformated_prices)
        self.prices.to_csv("./out/prices.csv")
        self.prices = self.prices.set_index("date")
        self.prices.plot(colormap="nipy_spectral", figsize=(10, 7))
        plt.legend(loc="upper center", bbox_to_anchor=(0.5, -0.08), ncol=5)
        plt.savefig(f"./out/oilprice{self.start_date}{self.end_date}.png")
        plt.close()

    def reformat_data(self, get_prices):
        for day in get_prices:
            price_of_day = {}
            for (price, param) in zip(day, self.cities):
                if param == "date":
                    price_of_day[param] = datetime.datetime.strptime(
                        price, "%Y-%m-%d %H:%M:%S"
                    )
                else:
                    price_of_day[param] = float(price)
            self.reformated_prices.append(price_of_day)

    def get_all_data_from_db(self, mydb):
        sql = "SELECT * FROM prices"
        mycursor = mydb.cursor()
        mycursor.execute(sql)
        get_prices = mycursor.fetchall()
        return get_prices


if (0 <= 2) and (2 < len(sys.argv)):
    start_date = str(sys.argv[1])
    end_date = str(sys.argv[2])

    Evaluate(start_date, end_date)
else:
    Evaluate()
