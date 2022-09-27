import pandas as pd
import pandas_datareader as pdr
import datetime
from db_tools import Data
import threading

class Trading:
    """ Класс по торговли акциями  """

    START_DT = datetime.datetime(2015, 1, 1)
    END_DT = datetime.datetime.today()
    CNT_THREADING = 20

    def __init__(self):
        """ Создание главное объекта для управления """
        self.__db = Data()

    def load_stocks_from_file(self, file: str):
        """ Загрузка данных в справочник с акциями """

        df = pd.read_csv(file, sep=';')
        if self.__db.dataframe_to_table('stocks', df):
            print(f'Загрузка файла {file} в таблицу stocks завершена.')

    def insert_market(self, markets: list):
        """ Загрузка списка бирж """

        df_markets = pd.DataFrame({'market_name': markets})
        self.__db.dataframe_to_table('markets', df_markets)

    def get_table(self, table_name: str) -> pd.DataFrame:
        """ Вывести все данные из таблицы"""

        return self.__db.get_table(table_name)

    def __up_one_stock(self, trade_code, market, start_dt, end_dt):
        """ Скачивает и сохраняет историю цен одной акции """
        try:
            df_prices_tmp = pdr.DataReader(trade_code['name'], market, start=start_dt, end=end_dt).reset_index()
        except Exception as e:
            print(f'!!! ОШИБКА: {e}')
        else:
            if df_prices_tmp.shape[0] > 0:
                df_prices_tmp['id_stock'] = trade_code['id']
                df_prices_tmp = df_prices_tmp[['id_stock', 'TRADEDATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE']]
                df_prices_tmp.columns = ['id_stock', 'tradedate', 'open_price', 'high_price', 'low_price','close_price']
                df_prices_tmp = df_prices_tmp.dropna()
                self.__db.dataframe_to_table('prices', df_prices_tmp)

    def update_stocks(self):
        """ Дозагрузка истории цен акций """

        threads = []
        df_stocks = self.__db.get_stocks()
        for index, row in df_stocks.iterrows():
            market = row['market_name']
            trade_code = {'name': row['trade_code'], 'id': row['id_stock']}
            start_dt = self.START_DT if pd.isna(row['max_tradedate']) else row['max_tradedate'] + datetime.timedelta(days=1)
            t = threading.Thread(target=self.__up_one_stock, args=(trade_code, market, start_dt, self.END_DT))
            threads.append(t)
            t.start()
            if len(threads) == self.CNT_THREADING-1:
                for t in threads:
                    t.join()
                del threads[:]




trading = Trading()

#trading.update_stocks()

trading.insert_market(['moex'])
trading.load_stocks_from_file('./stocks.csv')

print('--------------------------')
print(trading.get_table('markets'))

print('--------------------------')
print(trading.get_table('stocks'))

print('--------------------------')
print(trading.get_table('prices'))





