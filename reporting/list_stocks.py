"""
This script collects summary information for the stock accounts.
"""

import logging
import os
from lib import my_env
from lib import info_layer


# Initialize Environment
projectname = "gnucash"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")

accountdb = info_layer.DirectConn(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))

# Get transactions for STOCK or MUTUAL accounts
query = """
select accounts.name as name, isin, max(date) as last_date, price, shares, cost, 
       sum(value_num) as value_dec, max(value_denom) as value_denom,
       sum(quantity_num) as quantity_dec, max(quantity_denom) as quantity_denom 
from transactions
join accounts on accounts.nid=transactions.account_id
join shares on transaction_id=transactions.nid
where length(isin) > 5
group by transactions.account_id
order by last_date desc
"""
res = accountdb.get_query(query)
for row in res:
    name = row['name']
    isin = row['isin']
    last_date = row['last_date']
    price = row['price']
    shares = row['shares']
    cost = row['cost']
    value_dec = row['value_dec']
    value_denom = row['value_denom']
    quantity_dec = row['quantity_dec']
    quantity_denom = row['quantity_denom']
    cost_calc = value_dec / value_denom
    quantity_calc = quantity_dec / quantity_denom
    print(f"{last_date}\t{isin}\t{name}\tPrice: {price:.2f}\tShares: {shares:.2f}\t"
          f"SharesCalc: {quantity_calc:.4f}\tCost: {cost:.2f}\tCostCalc: {cost_calc:.2f}")
logging.info("End Application")
