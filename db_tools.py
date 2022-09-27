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
        self.con = sl.connect(self.DB_NAME, check_same_thread=False)

        for table in self.TABLES:
            self.__checking_table(table, self.TABLES[table])

    def __checking_table(self, table_name, attributes):
        # Дописать проверку на изменение структуры
        sql = f"create table if not exists {table_name} ({','.join(attributes)})"
        self.con.execute(sql)

    def dataframe_to_table(self, table_name, df):
        try:
            df.to_sql(name=table_name, con=self.con, if_exists='append', index=False)
            result = True
        except sl.IntegrityError as e:
            print(f'Нарушена реляционная целостность базы данных. {e}')
            result = False
        finally:
            return result

    def get_table(self, table_name) -> pd.DataFrame:
        return pd.read_sql(sql=f'select * from {table_name}', con=self.con)

    def get_stocks(self) -> pd.DataFrame:
        sql = """
            select m.market_name, s.trade_code, s.id_stock, max(p.tradedate) as max_tradedate
            from markets as m 
                join stocks as s on m.id_market = s.id_market
                left join prices as p on s.id_stock = p.id_stock
            group by m.market_name, s.trade_code, s.id_stock
            """
        return pd.read_sql(sql=sql, con=self.con, parse_dates={"max_tradedate":  {"errors": "ignore"}})

