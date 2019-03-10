"""
This script will find and consolidate all transactions in IN account during a specific period of time.
"""

import logging
import os
import piecash
from lib import gc
from lib import my_env
from lib import write2excel

cfg = my_env.init_env("gnu", __file__)
logging.info("Start Application")
book_file = cfg['Main']['book']

with piecash.open_book(book_file, readonly=True, open_if_lock=True) as book:
    ra = book.root_account
    xl_arr = []
    res = {}
    for accn in ["in", "Vorticc"]:
        try:
            acc_name = ra.children(name=accn)
            gc.handle_account("2018-06-01", "2018-06-30", res, acc_name)
        except KeyError:
            logging.error("Categorie *{c}* niet gevonden.".format(c=accn))
    gc.add_result_to_xl(xl_arr, res)
    gs = gc.summary(xl_arr, "Categories", res)

    res = {}
    for accn in ["Uit", "vzw"]:
        try:
            acc_name = ra.children(name=accn)
            gc.handle_account("2018-06-01", "2018-06-30", res, acc_name)
        except KeyError:
            logging.error("Categorie *{c}* niet gevonden.".format(c=accn))
    gc.add_result_to_xl(xl_arr, res)
    totsum = gs + gc.summary(xl_arr, "Categories", res)
    gc.summary(xl_arr, "Report", total=totsum)

xl = write2excel.Write2Excel()
xl.init_sheet("MonthlyCashFlow")
xl.write_content(xl_arr)

fn = os.path.join(cfg["Main"]["reportdir"], "MCF201806.xlsx")
xl.close_workbook(fn)
