#!/opt/envs/gnu/bin/python
"""
Script to get current DB info and load it into the database.
"""

import logging
import os
import pandas as pd
from datetime import datetime
from lib import my_env
from piecash import open_book, Price, Commodity

def add_quote(cdty, cdate, f_curr=None):
    """
    This function gets the latest price and date for a commodity.

    :param cdty: Commodity
    :param cdate: Current date
    :param f_curr: Foreign Currency mnemonic - required for Currency exchange rates, None for Fund
    :return: True - latest quote can be updated, False - latest quote in Database
    """
    quotes = cdty.prices
    quotes_list = []
    for pr in quotes:
        if f_curr:
            if pr.currency.mnemonic == f_curr:
                quotes_dict = {'Date': pr.date, 'Value': pr.value}
                quotes_list.append(quotes_dict)
        else:
            quotes_dict = {'Date': pr.date, 'Value': pr.value}
            quotes_list.append(quotes_dict)
    # No prices found, so add this one.
    if len(quotes_list) == 0:
        return True
    sorted_list = sorted(quotes_list, key=lambda i: i['Date'], reverse=True)
    last_date = sorted_list[0]['Date']
    if cdate > last_date:
        return True
    elif f_curr:
        logging.info(f"Exchange rate for {cdty.mnemonic} to {f_curr} is up to date.")
    else:
        logging.info(f"Quote for {cdty.mnemonic} is up to date.")
    return False

my_env.init_env("gnucash", __file__)
logging.info("Start application")
gnudb = os.path.join(os.getenv('GNUDIR'), os.getenv('GNUNAME'))
exportfn = '/home/dirk/Downloads/export.csv'
exportpd = pd.read_csv(exportfn)
with open_book(gnudb, readonly=False, do_backup=True) as book:
    # Currencies
    fcurr = exportpd['Pos Cur'] != 'EUR'
    assets = exportpd['Pot'] != 'LIQUIDITEITEN'
    xrates_all = exportpd[fcurr & assets]
    xrates = xrates_all[['Pos Cur', 'X Rate', 'Pricing Date']].copy()
    xrates.drop_duplicates(inplace=True)
    for index, row in xrates.iterrows():
        date = datetime.strptime(row['Pricing Date'][:10], '%Y-%m-%d').date()
        comm = book.get(Commodity, namespace="CURRENCY", mnemonic='EUR')
        currency = row['Pos Cur']
        price = row['X Rate']
        if add_quote(comm, date, currency):
            curr = book.commodities(mnemonic=currency)
            logging.info(f"Add Exchange Rate to {currency} {price} at {date:%d/%m/%Y}")
            Price(comm, curr, date, str(price), type='last')
    # Shares
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
    # Funds
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
