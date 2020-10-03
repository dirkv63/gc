import logging
import numpy as np
import os
import pandas as pd
import sqlite3
from lib.db_model import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

spaarlist = ['Spaargeld', 'sparen', 'Opening Balance']


class DirectConn:
    """
    This class will set up a direct connection to the database. It allows to reset the database,
    in which case the database will be dropped and recreated, including all tables.
    """

    def __init__(self, dbdir, dbname):
        """
        To drop a database in sqlite3, you need to delete the file.
        """
        self.db = os.path.join(dbdir, dbname)
        self.dbConn, self.cur = self._connect2db()

    def _connect2db(self):
        """
        Internal method to create a database connection and a cursor. This method is called during object
        initialization.
        Note that sqlite connection object does not test the Database connection. If database does not exist, this
        method will not fail. This is expected behaviour, since it will be called to create databases as well.
        :return: Database handle and cursor for the database.
        """
        logging.debug("Creating Datastore object and cursor")
        db_conn = sqlite3.connect(self.db)
        db_conn.row_factory = sqlite3.Row
        logging.debug("Datastore object and cursor are created")
        return db_conn, db_conn.cursor()

    def rebuild(self):
        # A drop for sqlite is a remove of the file
        try:
            os.remove(self.db)
        except FileNotFoundError:
            # If the file is not there, then do not delete it.
            pass
        except PermissionError:
            self.dbConn.close()
            os.remove(self.db)
        # Reconnect to the Database
        self.dbConn, self.cur = self._connect2db()
        # Use SQLAlchemy connection to build the database
        conn_string = "sqlite:///{db}".format(db=self.db)
        engine = set_engine(conn_string=conn_string)
        Base.metadata.create_all(engine)

    def close(self):
        self.dbConn.close()

    def get_query(self, query):
        """
        This method will get a query and return the result of the query.

        :param query:
        :return:
        """
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def get_table(self, tablename):
        """
        This method will return the table as a list of named rows. This means that each row in the list will return
        the table column values as an attribute. E.g. row.name will return the value for column name in each row.

        :param tablename:
        :return:
        """
        query = "SELECT * FROM {t}".format(t=tablename)
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def insert_row(self, tablename, rowdict):
        """
        This method will insert a dictionary row into a table.

        :param tablename: Table Name to insert data into
        :param rowdict: Row Dictionary
        :return: Row ID of the last row inserted.
        """
        columns = ", ".join("`" + k + "`" for k in rowdict.keys())
        values_template = ", ".join(["?"] * len(rowdict.keys()))
        query = "insert into {tn} ({cols}) values ({vt})".format(tn=tablename, cols=columns, vt=values_template)
        values = tuple(rowdict[key] for key in rowdict.keys())
        logging.debug("Insert query: {q}".format(q=query))
        self.cur.execute(query, values)
        self.dbConn.commit()
        return self.cur.lastrowid

    def insert_rows(self, tablename, rowdict):
        """
        This method will insert a list of dictionary rows into a table.

        :param tablename: Table Name to insert data into
        :param rowdict: List of Dictionary Rows
        :return:
        """
        if len(rowdict) > 0:
            columns = ", ".join("`" + k + "`" for k in rowdict[0].keys())
            values_template = ", ".join(["?"] * len(rowdict[0].keys()))
            query = "insert into {tn} ({cols}) values ({vt})".format(tn=tablename, cols=columns, vt=values_template)
            logging.debug("Insert query: {q}".format(q=query))
            # cnt = my_env.LoopInfo(tablename, 50)
            for line in rowdict:
                # cnt.info_loop()
                logging.debug(line)
                values = tuple(line[key] for key in line.keys())
                try:
                    self.dbConn.execute(query, values)
                except sqlite3.IntegrityError:
                    logging.error("Integrity Error on query {q} with values {v}".format(q=query, v=values))
                except sqlite3.InterfaceError:
                    logging.error("Interface error on query {q} with values {v}".format(q=query, v=values))
            # cnt.end_loop()
            self.dbConn.commit()
        return

    def run_query(self, query):
        """
        Run a query that does not return results, such as create a table or remove a table. Execute the query in a
        cursor, then commit the change.

        :return:
        """
        self.cur.execute(query)
        self.dbConn.commit()
        return


class PandasConn:

    def __init__(self):
        self.cnx = connect4pandas()

    def get_verzekering(self, nid):
        """
        This method collects the dataframe for the verzekering with ID nid.

        :param nid:
        :return:
        """
        query = f"""
        SELECT accounts.name as name, isin, date, transactions.description as description, 
               value_num, value_denom, quantity_num, quantity_denom
        FROM transactions
        JOIN accounts ON accounts.nid=transactions.account_id
        WHERE account_id={nid}
        ORDER BY date asc
        """
        res = pd.read_sql_query(query, self.cnx)
        res['value'] = np.where(res['value_denom'] == 0, 0, res['value_num'] / res['value_denom'])
        res['quantity'] = np.where(res['quantity_denom'] == 0, 0, res['quantity_num'] / res['quantity_denom'])
        res['month'] = pd.to_datetime(res.date).dt.to_period('M')
        res.drop(['value_num', 'value_denom', 'quantity_num', 'quantity_denom'], axis=1, inplace=True)
        res['cat'] = np.where(res['description'].isin(spaarlist), 'savings', 'delta')
        per_month = pd.pivot_table(res, values='value', index=['month'], columns=['cat'], aggfunc=np.sum)
        per_month.reset_index(inplace=True)
        per_month['change'] = per_month['savings'].fillna(0) + per_month['delta'].fillna(0)
        per_month['in'] = per_month['savings'].fillna(0).cumsum()
        per_month['total'] = per_month['change'].cumsum()
        per_month['pct'] =  ((per_month['total'] - per_month['in']) / per_month ['in'])
        cols = ['month', 'savings', 'delta', 'change', 'in', 'total', 'pct']
        per_month = per_month[cols]
        return per_month

    @staticmethod
    def writer(ffn):
        return pd.ExcelWriter(ffn, engine='xlsxwriter')

def init_session(dbdir, dbname, echo=False):
    """
    This function configures the connection to the database and returns the session object.

    :param dbdir: Directory of the database
    :param dbname: Name of the database
    :param echo: True / False, depending if echo is required. Default: False
    :return: session object.
    """
    db = os.path.join(dbdir, dbname)
    conn_string = "sqlite:///{db}".format(db=db)
    engine = set_engine(conn_string, echo)
    session = set_session4engine(engine)
    return session


def set_engine(conn_string, echo=False):
    engine = create_engine(conn_string, echo=echo)
    return engine


def set_session4engine(engine):
    session_class = sessionmaker(bind=engine)
    session = session_class()
    return session


def connect4pandas():
    db = os.path.join(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))
    return sqlite3.connect(db)
