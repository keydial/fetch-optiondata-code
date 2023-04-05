import mysql.connector
import requests
import time
import json
import os.path
import gzip
import random
import string
import os
import datetime
import pytz
import pandas as pd
from sqlalchemy import create_engine


dbconn = mysql.connector.connect(
    host=os.environ['DB_HOST'],
    database=os.environ['DB_NAME'],
    user=os.environ['DB_USER'],
    passwd=os.environ['DB_PASS']
)


# # Define the table schema
table_schema = '''
CREATE TABLE IF NOT EXISTS option_chain (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(55) NOT NULL,
    instrument_type VARCHAR(10) NOT NULL,
    underlying VARCHAR(55) NOT NULL,
    identifier VARCHAR(255) NOT NULL,
    expiryDate VARCHAR(20) NOT NULL,
    strikePrice INT NOT NULL,
    openInterest INT NOT NULL,
    changeinOpenInterest INT NOT NULL,
    pchangeinOpenInterest FLOAT NOT NULL,
    totalTradedVolume INT NOT NULL,
    impliedVolatility FLOAT NOT NULL,
    lastPrice FLOAT NOT NULL,
    `change` FLOAT NOT NULL,
    pchange FLOAT NOT NULL,
    totalBuyQuantity INT NOT NULL,
    totalSellQuantity INT NOT NULL,
    bidQty INT NOT NULL,
    bidprice FLOAT NOT NULL,
    askQty INT NOT NULL,
    askPrice FLOAT NOT NULL,
    underlyingValue FLOAT NOT NULL,
    timestamp timestamp NOT NULL
)
'''

# Create the table if it doesn't exist
pycur = dbconn.cursor()
pycur.execute(table_schema)

# Define the start and end times
scrip = ["NIFTY", "BANKNIFTY"]
# timestamp = int(time.time())
timezone = pytz.timezone('Asia/Kolkata')
# Get the current date and time in India timezone
india_date = datetime.datetime.now(timezone)

def fetchoc(symbols):
    for symbol in symbols:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=" + symbol
        headers = {
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1402.2"
        }
        session = requests.Session()
        # Get the option chain data from the API
        optdata = session.get(url, headers=headers).json()["records"]["data"]
        ocdata = []
        # Extract the relevant information from the option chain data
        for i in optdata:
            for j, k in i.items():
                if j == "CE" or j == "PE":
                    info = k
                    info["instrument_type"] = j
                    info["symbol"] = symbol
                    # info["timestamp"] = timestamp
                    ocdata.append(info)
        # Insert the option chain data into the database
        if ocdata:
            df = pd.DataFrame(ocdata)
            engine = create_engine('mysql+mysqlconnector://u448360173_niftyoption:eDOzfAc8Y1@89.117.188.204/u448360173_niftyoption')
            df.to_sql("option_chain", con=engine, if_exists="append", index=False)

# Fetch the option chain data every 10 seconds
while True:
    try:
        fetchoc(scrip)
        time.sleep(179)
    except Exception as e:
        print(e)
        time.sleep(179)