import os
import re
import sys
import time
import traceback
import datetime
import warnings
import pandas as pd
import numpy as np
from tkinter import messagebox, simpledialog
from tkinter.filedialog import askdirectory, askopenfilename
from tkinter.messagebox import showerror, askyesno

from database.db_writer import DataExtractor, sort_table_on_time

import logging

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore")

if not os.path.exists("Trade Specifications"):
    os.mkdir("Trade Specifications")


def remove_temporary_files(output_dir):
    for file_name in os.listdir(output_dir):
        os.remove(os.path.join(output_dir, file_name))
    os.rmdir(output_dir)


def mainRunningCallBack():

    try:

        have_database = askyesno("Database Exist",
                                 "Is there an existing database?")
        if not have_database:
            ask_price_path = askopenfilename(
                title="Select Ask Price File".title(),
                filetypes=[("CSV", ["*.csv"])],
            )
            if not ask_price_path:
                raise FileNotFoundError(
                    "Error while opening ask prices, no path is given."
                )
            bid_price_path = askopenfilename(
                initialdir=ask_price_path,
                title="Select Bid Price File".title(),
                filetypes=[("CSV", ["*.csv"])],
            )
            if not bid_price_path:
                raise FileNotFoundError(
                    "Error while opening bid prices, no path is given."
                )
            csvPrices = askdirectory(
                initialdir=bid_price_path,
                title="Select price data for symbol folder".title(),
            )

            if not csvPrices:
                raise FileNotFoundError(
                    "Error while opening price data for all symbols, no path is given."
                )

        drop_not_unique_releases = False
        if drop_not_unique_releases:
            print("Dropping Non Unique Releases!")
            logger.info("Dropping Non Unique Releases!")

        else:
            print("Keep Non Unique Releases!")
            logger.info("Keep Non Unique Releases!")

    except Exception as e:
        showerror("Error in getting inputs".title(), traceback.format_exc())
        sys.exit()

    if not have_database:
        files = [
            filename
            for filename in os.listdir(csvPrices)
            if os.path.isfile(os.path.join(csvPrices, filename)) and filename.endswith(".csv")
        ]
        file = files[0]
        symbol = ".".join(str(file).split("/")[-1].split("\\")[-1].split(".")[:-1]).split("-")[0]

        extractor = DataExtractor()
        extractor.run()
        database_path = f'./{symbol}.db'
        sort_table_on_time(database_path, 'ticks', 'time')
        sort_table_on_time(database_path, 'askPrices', 'Gmt time')
        sort_table_on_time(database_path, 'bidPrices', 'Gmt time')
        sort_table_on_time(database_path, 'baseInterests', 'RELEASE_TIME')
        sort_table_on_time(database_path, 'quoteInterests', 'RELEASE_TIME')


if __name__ == '__main__':
    mainRunningCallBack()

    sys.exit()
