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
SELECT commodity_guid, mnemonic, date, value_denom, value_num
FROM
    (SELECT commodity_guid AS max_commodity_guid, max(date) AS max_date
    FROM prices
    LEFT JOIN commodities
    ON commodities.guid = prices.commodity_guid
    WHERE namespace='FUND' AND length(cusip) > 3
    GROUP BY commodity_guid) 
    AS latest_time
INNER JOIN prices ON commodity_guid=max_commodity_guid AND date=max_date
INNER JOIN commodities on commodities.guid=prices.currency_guid
"""
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
session.commit()
logging.info("End Application")

"""
Query for exchange rate:
SELECT commodity_guid AS max_commodity_guid, commodities.mnemonic, curr.mnemonic, 
       currency_guid AS max_currency_guid, max(date) AS max_date
FROM prices
LEFT JOIN commodities ON commodities.guid = prices.commodity_guid
LEFT JOIN commodities as curr ON curr.guid = prices.currency_guid
WHERE commodities.namespace='CURRENCY' 
GROUP BY commodity_guid, currency_guid, commodities.mnemonic, curr.mnemonic
"""