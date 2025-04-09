###### -- Python Script in Jupyter to access, create and test financial data and models
###### -- By Ahmed Asiliskender, initial write date 25 June 2024
###### -- May also access MATLAB scripts through here and .py files.

### Here we initialise important libraries and variables.

## To download packages using pip
import sys #! allows to use command terminal code in here
#!{sys.executable} --version
#!pip install html5lib
#!pip install bs4
#!pip install yfinance
#!pip install tradingview-scraper
#!pip install --upgrade --no-cache tradingview-scraper
#!pip install selenium
#!pip install sqlalchemy
#!pip install python-dotenv
#!pip install pandas-ta
#!pip install pytest
#!pip install python-on-whales
# Security testing
#!pip install bandit

# Cmd terminal environment install (psycopg2)
#!pip install psycopg2-binary 
# Conda environment install
#!conda install -c anaconda psycopg2 



# Import pandas (python data analysis lib) and data analysis packages
#%matplotlib inline
import numpy as np
from scipy import stats
import pandas as pd
import pandas_ta as ta
import matplotlib.pyplot as plt
import statsmodels.api as sm


#user_header = {'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
#                                AppleWebKit/537.36 (KHTML, like Gecko) \
#                                Chrome/122.0.0.0 Safari/537.36'}

# Webscrape libs
from bs4 import BeautifulSoup
import yfinance as yf
import tradingview_scraper as tvs
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from tradingview_scraper.symbols.ideas import Ideas

# Other libs (system, graphical or time-compute analysis)
import os
from io import StringIO
import psycopg2
from dotenv import load_dotenv, find_dotenv
from dotenv.main import set_key
from sqlalchemy import create_engine, text
from sqlalchemy import types as sqltype
import sqlalchemy.exc as sqlexc
from colorama import Fore, Back, Style
import copy
from pathlib import Path
import time
from datetime import datetime
import requests
from ast import literal_eval
import json
import unittest
import pytest

#import warnings

# My packages
from arcanequant.quantlib.DataManifestManager import DataManifest
from arcanequant.quantlib.SQLManager import SetKeysQuery, DropKeysQuery, ExecuteSQL




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
