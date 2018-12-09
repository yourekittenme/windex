import os
import pandas as pd
import sqlite3


class SqlQuery:

    directory_db = '\\'.join(os.getcwd().split('\\')[0:-1])
    filename_db = 'db.sqlite3'
    path_db = directory_db + '\\' + filename_db

    def __init__(self):
        self.conn = sqlite3.connect(self.path_db)

    def read(self, sql):
        return pd.read_sql(sql, self.conn)

    def write(self, df, db_table):
        pd.DataFrame.to_sql(df, db_table, self.conn, if_exists='append', index=False)
