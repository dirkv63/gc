"""
This script collects detail information for an account.
It will collect all transactions for the account.
"""

import argparse
import datetime
import logging
import os
import pandas as pd
from lib import my_env
from lib import info_layer


# Initialize Environment
projectname = "gnucash"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")
# Configure command line arguments
parser = argparse.ArgumentParser(
    description="Provide nid for the account."
)
parser.add_argument('-n', '--nid', type=int, default=31,
                    help='Provide the nid for the account')
args = parser.parse_args()
nid = args.nid
logging.info(f"Find detail information for Account {nid}")

accountdb = info_layer.DirectConn(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))
cnx = info_layer.connect4pandas()

now = datetime.datetime.now().strftime("%Y%m%d")
wbdir = os.getenv('WBDIR')
wbname = os.getenv('WBNAME')
wbfile = f"{wbname}_{nid}_{now}.xlsx"
wbffn = os.path.join(wbdir, wbfile)
pdc = info_layer.PandasConn()

writer = pdc.writer(wbffn)
df = pdc.get_verzekering(30)
df.to_excel(writer, sheet_name='summary', index=False)
# Format the output
wb = writer.book
ws = writer.sheets['summary']
format1 = wb.add_format({'num_format': '#,##0.00'})
format2 = wb.add_format({'num_format': '0.00%'})
ws.set_column('B:F', None, format1)
ws.set_column('G:G', None, format2)
writer.save()
logging.info("End Application")
