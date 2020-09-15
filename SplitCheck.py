#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# This program is Copyright (C) 2017, Paul Lutus
# and is released under the GPL:
# https://www.gnu.org/licenses/gpl-3.0.en.html
# This program is described on https://arachnoid.com/gnucash_utilities/

import logging
from lib import my_env
from piecash import open_book


cfg = my_env.init_env("gnu", __file__)
logging.info("Start Application")
book_file = cfg['Main']['book']
balance = 0


def rec_function(node, tab=''):
    global balance
    print("Trying to find balance for {node}".format(node=node))
    balance += node.get_balance(recurse=False)
    print("%-80s : %12.2f : %12.2f" % ("%s%s" % (tab, str(node)), node.get_balance(), balance))
    for child in node.children:
        rec_function(child, tab + '  ')


with open_book(book_file) as book:
    rec_function(book.root_account)
