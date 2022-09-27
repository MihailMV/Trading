import pandas as pd
import pandas_datareader as pdr
import datetime
from db_tools import Data

class Trading:
    """ Класс по торговли акциями  """

    START_DT = datetime.datetime(2015, 1, 1)
    END_DT = datetime.datetime.today()
    COLUMNS = ['MARKET', 'TRADEDATE', 'SECID', 'SHORTNAME', 'OPEN', 'HIGH', 'LOW', 'CLOSE']
    MARKETS = ['moex']
    PATH_FILE_HIST = './history.csv'

    def __init__(self):
        """ Создание главное объекта для управления """
        self.__db = Data()

    def load_stocks_from_file(self, file):
        """ Загрузка данных в справочник с акциями """

        df = pd.read_csv(file, sep=';')
        self.__db.dataframe_to_table('stocks', df)
        print(f'Загрузка файла {file} в таблицу stocks завершена.')

    def insert_market(self, markets):
        """ Загрузка списка бирж """
        df_markets = pd.DataFrame({'market_name': markets})
        self.__db.dataframe_to_table('markets', df_markets)

    def get_table(self, table_name) -> pd.DataFrame:
        """ Вывести все данные из таблицы"""
        return self.__db.get_table(table_name)
    def update_stocks(self):
        """ Дозагрузка истории цен акций """
        for market in self.stocks:
            for stock in self.stocks[market]:
                max_dt = self.df_hist[(self.df_hist['MARKET'] == market) & (self.df_hist['SECID'] == stock)]['TRADEDATE'].max()
                if pd.isnull(max_dt):
                    max_dt = self.START_DT
                else:
                    max_dt += datetime.timedelta(days=1)
                self.__load_stock(market, stock, max_dt)

    def __load_stock(self, market, stock, start_dt):
        """ Загрузка истории конкретной акции """
        print(f'Update: {market} - {stock}')
        df_tmp = pdr.DataReader(stock, market, start=start_dt, end=self.END_DT).reset_index()
        if df_tmp.shape[0] > 0:
            df_tmp['MARKET'] = market
            df_tmp = df_tmp[self.COLUMNS]
            self.df_hist = pd.concat([self.df_hist, df_tmp])
            df_tmp.to_csv(index=False, path_or_buf=self.PATH_FILE_HIST, sep='\t', decimal=',', encoding='cp1251', mode='a')


trading = Trading()

#trading.insert_market(['moex'])
#trading.load_stocks_from_file('./stocks.csv')

print('--------------------------')
print(trading.get_table('markets'))

print('--------------------------')
print(trading.get_table('stocks'))




