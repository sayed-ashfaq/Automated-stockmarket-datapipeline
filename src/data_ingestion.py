# ingest the data from yfinance to google cloud bucket
import datetime, time
import pandas as pd
from google.cloud import storage
import yfinance as yf
from logger import GLOBAL_LOGGER as log

#step 1: load the data into dataframe

def load_data(ticker_symbol, start_date, end_date):
    df= yf.download(ticker_symbol, start=start_date, end=end_date)
    return df