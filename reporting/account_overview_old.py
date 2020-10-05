"""
This script collects summary information for the accounts.
"""

import datetime
import logging
import os
from lib import my_env
from lib import info_layer
from lib import write2excel

# Initialize Environment
projectname = "gnucash"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")

accountdb = info_layer.DirectConn(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))
today = datetime.datetime.now().strftime("%Y-%m-%d")
wbdir = os.getenv('WBDIR')
wbname = os.getenv('WBNAME')
wbfile = f"{wbname}_summ_{today}.xlsx"
wbffn = os.path.join(wbdir, wbfile)
wb = write2excel.Write2Excel()

# Get transactions for accounts
query = f"""
SELECT groups.description as bank, categories.name as category, accounts.name as name, max(date) as last_date,
       sum(value_num) as value_dec, max(value_denom) as value_denom,
       sum(quantity_num) as quantity_dec, max(quantity_denom) as quantity_denom 
FROM transactions
JOIN accounts ON accounts.nid=transactions.account_id
JOIN groups ON groups.nid=accounts.group_id
JOIN categories ON categories.nid=accounts.category_id
WHERE groups.category='BANK'
AND transactions.date <= '{today}'
AND length(groups.description) > 0
AND accounts.placeholder=0
GROUP BY transactions.account_id
ORDER BY groups.description, categories.name, accounts.name
"""
res = accountdb.get_query(query)
sheet = []
for row in res:
    value_dec = row['value_dec']
    value_denom = row['value_denom']
    cost = value_dec / value_denom
    if row['category'] == 'BANK':
        quantity = ''
    else:
        quantity_dec = row['quantity_dec']
        quantity_denom = row['quantity_denom']
        quantity = quantity_dec / quantity_denom
    line = dict(
        bank=row['bank'],
        category=row['category'],
        name=row['name'],
        last_date=row['last_date'],
        quantity =quantity,
        cost=cost
    )
    sheet.append(line)
wb.init_sheet('Overzicht')
wb.write_content(sheet)
wb.close_workbook(wbffn)
logging.info("End Application")
