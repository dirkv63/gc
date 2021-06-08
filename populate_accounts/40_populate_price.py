"""
This script collects latest price per fund.
"""

import logging
import os
from lib.db_model import *
from lib import my_env
from lib import info_layer


# Initialize Environment
projectname = "gnucash"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")

gnudb = info_layer.DirectConn(os.getenv('GNUDIR'), os.getenv('GNUNAME'))
session = info_layer.init_session(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))

# Get latest price per fund from prices database
query = """
SELECT DISTINCT commodity_guid, mnemonic, date, value_denom, value_num
FROM
    (SELECT commodity_guid AS max_commodity_guid, max(date) AS max_date
    FROM prices
    LEFT JOIN commodities
    ON commodities.guid = prices.commodity_guid
    WHERE namespace='FUND' 
    GROUP BY commodity_guid) 
    AS latest_time
INNER JOIN prices ON commodity_guid=max_commodity_guid AND date=max_date
INNER JOIN commodities on commodities.guid=prices.currency_guid
"""
# AND length(cusip) > 3
res = gnudb.get_query(query)
for row in res:
    price_rec = Price(
        commodity_guid=row['commodity_guid'],
        currency=row['mnemonic'],
        date=row['date'],
        value_num=row['value_num'],
        value_denom=row['value_denom']
    )
    session.add(price_rec)

# Get latest currency exchange rates from prices database
query = """
SELECT date, local_curr, foreign_curr, value_num, value_denom
FROM
(SELECT commodity_guid as max_commodity_guid, currency_guid as max_currency_guid, 
		commodities.mnemonic as local_curr, curr.mnemonic as foreign_curr,
       max(date) as max_date
FROM prices
LEFT JOIN commodities ON commodities.guid = prices.commodity_guid
LEFT JOIN commodities as curr ON curr.guid = prices.currency_guid
WHERE commodities.namespace='CURRENCY' 
  AND commodities.mnemonic < curr.mnemonic
GROUP BY commodity_guid, currency_guid) as latest_time
INNER JOIN prices ON commodity_guid=max_commodity_guid
			 	 AND currency_guid=max_currency_guid
				 AND date=max_date
"""
res = gnudb.get_query(query)
for row in res:
    xrate_rec = XRate(
        local_curr=row['local_curr'],
        foreign_curr=row['foreign_curr'],
        date=row['date'],
        value_num=row['value_num'],
        value_denom=row['value_denom']
    )
    session.add(xrate_rec)
session.commit()
logging.info("End Application")
