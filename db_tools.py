import sqlite3 as sl
import pandas as pd

class Data:
    """ Класс для работы с базой данных """

    DB_NAME = 'data.db'
    TABLES = {
        'markets': [
            'id_market INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT'
            , 'market_name TEXT UNIQUE'
        ]
        , 'stocks': [
            'id_stock INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT'
            , 'trade_code TEXT'
            , 'trade_name TEXT'
            , 'isin TEXT UNIQUE'
            , 'id_market INTEGER'
        ]
        , 'prices': [
            'id_stock INTEGER NOT NULL'
            , 'tradedate TEXT NOT NULL'
            , 'open_price REAL NOT NULL'
            , 'high_price REAL NOT NULL'
            , 'low_price REAL NOT NULL'
            , 'close_price REAL NOT NULL'
            , 'PRIMARY KEY(id_stock, tradedate)'
        ]

    }

    def __init__(self):
        self.con = sl.connect(self.DB_NAME)

        for table in self.TABLES:
            self.__checking_table(table, self.TABLES[table])

    def __checking_table(self, table_name, attributes):
        # Дописать проверку на изменение структуры
        sql = f"create table if not exists {table_name} ({','.join(attributes)})"
        self.con.execute(sql)

    def dataframe_to_table(self, table_name, df):
        df.to_sql(name=table_name, con=self.con, if_exists='append', index=False)

    def get_table(self, table_name) -> pd.DataFrame:
        return pd.read_sql(sql=f'select * from {table_name}', con=self.con)



