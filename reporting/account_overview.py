"""
This script collects summary information for the accounts.
"""

import datetime
import logging
import os
from lib import my_env
from lib import info_layer

# Initialize Environment
projectname = "gnucash"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")
pdc = info_layer.PandasConn()

now = datetime.datetime.now().strftime("%Y%m%d")
wbdir = os.getenv('WBDIR')
wbfile = f"account_overview_{now}.xlsx"
wbffn = os.path.join(wbdir, wbfile)

name = 'overview'
writer = pdc.writer(wbffn)
fmt_dict = info_layer.format_book(writer.book)
# Collect info
df = pdc.get_account_summary()
df.to_excel(writer, sheet_name=name, index=False)
# Format the output
info_layer.format_summary(writer.sheets[name], fmt_dict)
writer.save()
logging.info("End Application")
