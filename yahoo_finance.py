#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script provides a basic access to yahoo finance data.

Copyright (c) 2018, Jan-Christopher Magel
License: MIT (see LICENSE for details)
"""
from __future__ import division

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

import re
import time
import threading
import datetime
import requests
import warnings
import functools

import pandas


__author__ = 'Jan-Christopher Magel'
__version__ = '0.13dev'
__license__ = 'MIT'

# Define coverage treshold for 
# synchronize_price_data()
COVERAGE_TRESHOLD = 0.9

# should we take adjusted prices?
ADJUST_PRICES = True

# Define time intervals
WEEKLY, DAILY, MONTHLY = "1wk", "1d", "1mo"

# Some other global vars
USE_THREADING = True
USE_CACHE = True
STORE_DATA_OFFLINE = True
CACHE = dict(cookie=None, crumb=None)

# We start with some utility functions:


def _get_www_raw(symbol):
    ''' Downloads the cookies and the raw content of a yahoo finance page. '''

    url = "https://finance.yahoo.com/quote/%s/?p=%s" % (symbol, symbol)
    r = requests.get(url)
    return r.cookies, r.content.decode("unicode-escape")


def _to_unix_epoch(dt=None):
    ''' Converts a datetime object into a 1970 unix epoch number. '''

    # @see https://www.linuxquestions.org/questions/programming-9/
    # python-datetime-to-epoch-4175520007/#post5244109
    
    if dt is None:
        dt = datetime.datetime.now()

    return int(time.mktime(dt.timetuple()))


def _process_raw_csv(csv):
    ''' Converts a raw csv string into pandas dataframe. '''

    csv = StringIO(csv)

    df = pandas.read_csv(csv, na_values=["null"])
    df["Date"] = pandas.to_datetime(df["Date"], format="%Y-%m-%d")
    
    return df.set_index("Date")


# Now we continue with the main functions:


def get_cookie_and_crumb(symbol):
    ''' Extracts the specific cookie and crumb value. '''

    if not USE_CACHE or not all(CACHE.values()):

        cookies, raw = _get_www_raw(symbol)
        
        # we try to find the crumb value using re
        # we know that the crumb value is always 11
        # chars long. Usually it is [a-zA-Z] but
        # sometimes there are some confusing chars
        # (see below)
        try:
            pattern = r'"CrumbStore":{"crumb":"[\w\/\.]{11}"}'
            crumb = re.findall(pattern, raw)[0].split('"')[-2]
        except IndexError:
            with open("debug.html", "wb") as fp: fp.write(raw.encode("utf8", "?"))
            raise RuntimeError, "Could not find crumb store. Does the symbol exist?" \
            " Please retry or consider updating crumb pattern."


        # knwon unusal crumb values
        # {"crumb":"hGjK6pd8E0\u002F"}
        # {"crumb":"FWP\u002F5EFll3U"}

        # we update the cache
        CACHE["crumb"] = crumb
        # since we are just interest in the B value
        # of the cookie we make a slimmer copy
        CACHE["cookie"] = dict(B=cookies["B"])
        

    return CACHE["cookie"], CACHE["crumb"]


def get_raw_csv_data(symbol, start_date, end_date, event="history", interval="1d"):
    ''' Low level function for downloading the desired csv data (raw!).
        I would rather recommend using the shortcut functions instead of 
        calling get_raw_csv_data() directly. 
    '''

    cookie, crumb = get_cookie_and_crumb(symbol)

    # known events: 
    # history => historic price data
    # div => historic dividend data
    # split => historic split data

    url = "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%s&period2=%s&interval=%s&events=%s&crumb=%s" % (
        symbol, start_date, end_date, interval, event, crumb)

    response = requests.get(url, cookies=cookie)

    return response.text


def download(symbols, start_date=None, end_date=None, event="history", interval="1d"):
    ''' Medium level function for downloading and converting the csv data.'''

    if start_date is None:
        # If no starting date is defined
        # we start at the beginning of the unix time epoch
        # By definition this is 0
        start_date = 0
    else:
        # we transform the datetime.date object
        # into the unix time epoch
        start_date = _to_unix_epoch(start_date)

    # we also transform the end_date (if end_date is None
    # we'll get the unix time epoch of today)
    end_date = _to_unix_epoch(end_date)

    # some backup checks...
    assert start_date >= 0
    assert start_date <= end_date

    # lets check if we have multiple symbols or just one.
    # if symbols is a string we simply make a list out of it
    if isinstance(symbols, str):
        symbols = [symbols]

    # get the raw csv
    def collector(dataframes, index, args):
        raw_csv = get_raw_csv_data(*args)
        df = _process_raw_csv(raw_csv)
        dataframes[index] = df

    dataframes = [None] * len(symbols)
    threads = []

    for index, symbol in enumerate(symbols):
        args = (symbol, start_date, end_date, event, interval)

        thread = threading.Thread(target=collector, args=(dataframes, index, args))
        threads.append(thread)
        thread.start()
        
        if not USE_THREADING:
            thread.join()

        if threading.activeCount() > 15:
            warnings.warn("More than 15 threads! Waiting 0.5 secs...")
            time.sleep(0.5)

    # wait until all threads finished
    for thread in threads: thread.join()

    if event != "history":
        return dataframes
    else:
        dataframes = [df["Adj Close"] for df in dataframes]
        master = pandas.concat(dataframes, axis=1)

        coverage = master.dropna().size / master.size
        if coverage <= COVERAGE_TRESHOLD:
            warnings.warn("Coverage treshold hit: resulting coverage is %.2f%%." % (coverage * 100))

        master.columns = symbols
        return master.dropna()


# Create high level shortcuts
download_quotes = functools.partial(download, event="history")
download_dividends = functools.partial(download, event="div")
download_splits = functools.partial(download, event="split")



if __name__ == "__main__":
    dax = [
    "ALV.DE", "ADS.DE", "BAS.DE", "LIN.DE", "CON.DE",
    "SAP.DE", "DB1.DE", "DPW.DE", "FRE.DE", "IFX.DE", "DTE.DE",
    "BEI.DE", "BAYN.DE", "DAI.DE", "DBK.DE", "HEI.DE", "VOW3.DE",
    "MUV2.DE", "BMW.DE", "TKA.DE", "SIE.DE", "MRK.DE",
    "EOAN.F", "LHA.DE", "CBK.DE", "HEN3.DE", "PSM.DE", "RWE.DE"
    ]
    print download_quotes(dax)
    #print download_dividends(dax) # bug!!
    #print download_splits(dax) # bug!!
