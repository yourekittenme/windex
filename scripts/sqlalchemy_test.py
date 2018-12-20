import pandas as pd
import os
from sqlalchemy import create_engine, MetaData, Table, select
import warnings
from sqlalchemy import exc as sa_exc


class SqlConnection:
    type_db = 'sqlite'
    directory_db = '\\'.join(os.getcwd().split('\\')[0:-1])
    filename_db = 'db.sqlite3'
    path_db = type_db + ':///' + directory_db + '\\' + filename_db

    def __init__(self, db_table):
        self.engine = create_engine(self.path_db)
        self.connection = self.engine.connect()
        self.metadata = MetaData()
        self.query = None
        self.results = None
        self.table = Table(db_table, self.metadata, autoload=True, autoload_with=self.engine)

    def select_query(self, sqlalchemy_query, output='records'):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            self.query = sqlalchemy_query
            self.results = self.connection.execute(self.query).fetchall()
            if output == 'df':
                return pd.DataFrame.from_records(self.results, columns=self.query.columns.keys())
            if output == 'records':
                return self.results

    def insert_query(self, sqlalchemy_query, insert_values):
        values_list = [x for x in insert_values.T.to_dict().values()]
        results = self.connection.execute(sqlalchemy_query, values_list)
        # print(results.rowcount) turn this into logging later


if __name__ == '__main__':
    s = SqlConnection('stockindex_app_stock')
    q = select([s.table])
    print(s.execute_query(q, output='df').to_string())

    test_records_update = [(1, 'BUI', 'Buhler Industries',   3.69,  0, 0, 9.22500000e+07, 'TSX', 'TSX:BUI', '',
                            3.69,  3.69,  3.69, 3.94, 3.53, 25000000),
                         (2, 'BYD-UN', 'The Boyd Group', 111.1,  7.68, 0, 2.33681536e+09, 'TSX', 'TSX:BYD-UN', '',
                          118.78, 115.97, 121.21, 133, 102.59,  19673475),
                         (3, 'GWO', 'Great West Life Company',  27.87,  2.13, 0, 2.96515036e+10, 'TSX', 'TSX:GWO', '',
                          30,  30.55,  30.59, 33.1,  28.37, 988383452),
                         (4, 'IGM', 'IGM Financial',  22.37, 12.47, 0, 8.39154568e+09, 'TSX', 'TSX:IGM', '',
                          34.84,  34.26,  34.84, 39.95, 31.54, 240859520),
                         (5, 'PBL', 'Pollard Banknote',  19.53,  2.84, 0, 5.73245969e+08, 'TSX', 'TSX:PBL', '',
                          22.37,  22.1,  22.37, 27.75,  19.91,  25625658),
                         (6, 'NFI', 'New Flyer Industries',  34.31,  3.05, 0, 2.32951738e+09, 'TSX', 'TSX:NFI', '',
                          37.36,  38.21,  38.42, 52.48,  32.95,  62353249),
                         (7, 'NWC', 'Northwest Company',  28,  1.3, 0, 1.42685108e+09, 'TSX', 'TSX:NWC', '',
                          29.3 ,  29.29,  29.44, 30.6 ,  27.03,  48697989),
                         (8, 'EIF', 'Exchange Income Corp',  30.23,  0.76, 0, 9.72296840e+08, 'TSX', 'TSX:EIF', '',
                          30.99,  31.3,  31.48, 35.34,  26.87,  31374535),
                         (9, 'AFN', 'Ag Growth International',  50,  3.63, 0, 9.84849521e+08, 'TSX', 'TSX:AFN', '',
                          53.63,  54.44,  54.44, 64.72,  47.84,  18363780)]

