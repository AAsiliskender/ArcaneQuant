###### -- By Ahmed Asiliskender, initial write date 25 June 2024

### Here we initialise important libraries and variables.

# Import pandas (python data analysis lib) and data analysis packages
def initialise():
        
    #%matplotlib inline
    import copy
    import json

    #import yfinance as yf # TODO: INSTALL THESE LATER
    #import tradingview_scraper as tvs
    #from selenium import webdriver
    #from selenium.webdriver.chrome.options import Options
    #from tradingview_scraper.symbols.ideas import Ideas
    # Other libs (system, graphical or time-compute analysis)
    import os
    import time
    from ast import literal_eval
    from datetime import datetime
    from io import StringIO
    from pathlib import Path

    #import pandas_ta as ta # TODO: FIX
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    import psycopg2

    #import unittest
    import pytest
    import requests
    import sqlalchemy.exc as sqlexc
    import statsmodels.api as sm

    #user_header = {'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
    #                                AppleWebKit/537.36 (KHTML, like Gecko) \
    #                                Chrome/122.0.0.0 Safari/537.36'}
    # Webscrape libs
    from bs4 import BeautifulSoup  #Use this for now
    from colorama import Back, Fore, Style
    from dotenv import find_dotenv, load_dotenv
    from dotenv.main import set_key
    from scipy import stats
    from sqlalchemy import create_engine, text
    from sqlalchemy import types as sqltype

    #import warnings

initialise()

# My packages
# For when 'from arcanequant import *' is used
__all__ = ["DataManifest",
    "DownloadIntraday", "ExtractData",
    "ModelEngine", "Process", "Pipeline",
    "RegisteredFunction", "SourceInfo",
    "Deferred", "Reference",
    "SQLSetup", "SQLEstablish", "SQLRepair", "SQLSave", "SQLSync", "SQLClear", "SQLNuke", "SetKeysQuery", "DropKeysQuery", "ExecuteSQL", "DFtoSQLFormat", "SQLtoDFFormat"
    ]

# Relative imports (child) (i.e. from arcanequant import DataManifest (relies on quantlib folder))
from .quantlib import *


# Paid APIs, (not used, left here)

# Bloomberg (not free)
#!pip install blpapi --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple/ blpapi
#!pip install xbbg
#from xbbg import blp
#blp.bdh( tickers='SPX Index', flds=['High', 'Low', 'Last_Price'], start_date='2018-10-10', end_date='2018-10-20')
#blp.bdp('AAPL US Equity', 'Eqy_Weighted_Avg_Px', VWAP_Dt='20181224')
#blp.bdp(tickers='NVDA US Equity', flds=['Security_Name', 'GICS_Sector_Name'])


### RAPID API FOR INTRADAYS (not free)
#import http.client
