"""
Script to get current DB info and load it into the database.
"""

import logging
import os
import pandas as pd
from datetime import datetime
from lib import my_env
from piecash import open_book, Price

def add_quote(cdty, cdate):
    """
    This function gets the latest price and date for a commodity.

    :param cdty: Commodity
    :param cdate: Current date
    :return: True - latest quote can be updated, False - latest quote in Database
    """
    quotes = cdty.prices
    quotes_list = []
    for pr in quotes:
        quotes_dict = {'Date': pr.date, 'Value': pr.value}
        quotes_list.append(quotes_dict)
    if len(quotes_list) == 0:
        # No prices found, so add this one.
        return True
    sorted_list = sorted(quotes_list, key=lambda i: i['Date'], reverse=True)
    last_date = sorted_list[0]['Date']
    if cdate > last_date:
        return True
    else:
        logging.info(f"Quote for {cdty.mnemonic} is up to date.")
        return False

my_env.init_env("gnucash", __file__)
logging.info("Start application")
gnudb = os.path.join(os.getenv('GNUDIR'), os.getenv('GNUNAME'))
exportfn = '/home/dirk/development/python/gnu/data/export.csv'
exportpd = pd.read_csv(exportfn)
with open_book(gnudb, readonly=False, do_backup=True) as book:
    groei = exportpd['Pot'] == 'GROEI'
    assets = exportpd['Assets'] != 'Obligaties'
    aandelen = exportpd[groei & assets]
    for index, row in aandelen.iterrows():
        date = datetime.strptime(row['Pricing Date'][:10], '%Y-%m-%d').date()
        cusip = row['ISIN']
        currency = row['Pos Cur']
        price = row['Price']
        try:
            comm = book.commodities(cusip=cusip)
        except KeyError:
            logging.error(f"Cannot find entry for {row['Instrument Name']} ISIN: {cusip}")
        else:
            if add_quote(comm, date):
                curr = book.commodities(mnemonic=currency)
                logging.info(f"Add Price {price} {currency} at {date:%d/%m/%Y} from {cusip}")
                Price(comm, curr, date, str(price), type='last')
    bescherming = exportpd['Pot'] == 'BESCHERMING'
    obligaties = (exportpd['Pot'] == 'GROEI') & (exportpd['Assets'] == 'Obligaties')
    funds = exportpd[bescherming | obligaties]
    for index, row in funds.iterrows():
        date = datetime.strptime(row['Pricing Date'][:10], '%Y-%m-%d').date()
        cusip = row['ISIN']
        currency = row['Pos Cur']
        price = row['Quantity'] * row['Price'] / 100
        try:
            comm = book.commodities(cusip=cusip)
        except KeyError:
            logging.error(f"Cannot find entry for {row['Instrument Name']} ISIN: {cusip}")
        else:
            if add_quote(comm, date):
                curr = book.commodities(mnemonic=currency)
                logging.info(f"Add Price {price} {currency} at {date:%d/%m/%Y} from {cusip}")
                Price(comm, curr, date, str(price), type='last')
    book.save()
