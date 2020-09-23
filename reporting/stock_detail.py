"""
This script collects detail information for a stock having multiple entries.
https://tryolabs.com/blog/2017/03/16/pandas-seaborn-a-guide-to-handle-visualize-data-elegantly/
"""

import argparse
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
# Configure command line arguments
parser = argparse.ArgumentParser(
    description="Provide nid for stock account."
)
parser.add_argument('-n', '--nid', type=int, default=165,
                    help='Provide the nid for the stock account')
args = parser.parse_args()
nid = args.nid
logging.info(f"Find detail information for Account {nid}")

accountdb = info_layer.DirectConn(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))

now = datetime.datetime.now().strftime("%Y%m%d")
wbdir = os.getenv('WBDIR')
wbname = os.getenv('WBNAME')
wbfile = f"{wbname}_{now}.xlsx"
wbffn = os.path.join(wbdir, wbfile)
wb = write2excel.Write2Excel()

# Get stock accounts
query = """
SELECT accounts.nid as nid
FROM accounts
JOIN categories ON categories.nid=accounts.category_id
JOIN groups ON groups.nid=accounts.group_id
WHERE categories.name='STOCK' OR categories.name='MUTUAL'
ORDER BY groups.name, accounts.name
"""
accounts = accountdb.get_query(query)
for account in accounts:
    account_id = account['nid']
    # Get transactions for STOCK or MUTUAL accounts
    query = f"""
    select accounts.name as name, isin, date, value_num, value_denom, quantity_num, quantity_denom
    from transactions
    join accounts on accounts.nid=transactions.account_id
    where account_id={account_id}
    order by date asc
    """
    res = accountdb.get_query(query)
    if len(res) > 5:
        lr = res[-1]
        try:
            current_price = (lr['value_num']/lr['value_denom']) / (lr['quantity_num']/lr['quantity_denom'])
        except ZeroDivisionError:
            current_price = 0
        acc_name = lr['name']
        sheet = []
        for row in res:
            line = {
                'name': row['name'],
                'isin': row['isin'],
                'date': row['date']}
            value_num = row['value_num']
            value_denom = row['value_denom']
            try:
                line['value'] = value_num/value_denom
            except ZeroDivisionError:
                line['value'] = 0
            quantity_num = row['quantity_num']
            quantity_denom = row['quantity_denom']
            try:
                line['quantity'] = quantity_num/quantity_denom
            except ZeroDivisionError:
                line['quantity'] = 0
            line['value_now'] = current_price*line['quantity']
            line['delta'] = line['value_now'] - line['value']
            # print(f"{name}\t{isin}\t{date}\t{price:.4f}\t{quantity:.4f}\t{value:.4f}\t{value_now:.4f}\t{delta:.4f}")
            sheet.append(line)
        wb.init_sheet(acc_name[:30])
        wb.write_content(sheet)
wb.close_workbook(wbffn)
logging.info("End Application")
