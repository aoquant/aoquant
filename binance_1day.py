"""
    1. 爬取交易所的数据， 并存入存入 CSV 里面.
    2. 对交易所的数据进行清洗.
"""

import pandas as pd
import time
import os
import datetime
import ccxt


pd.set_option('expand_frame_repr', False)  #

TIMEOUT = 6  # 6 second
BITFINEX_LIMIT = 5000
BITMEX_LIMIT = 500
BINANCE_LIMIT = 500


def crawl_exchanges_datas(exchange_name, symbol, start_time, end_time):
    """
    爬取交易所数据的方法.
    :param exchange_name:  交易所名称.
    :param symbol: 请求的symbol: like BTC/USDT, ETH/USD等。
    :param start_time: like 2018-1-1
    :param end_time: like 2019-1-1
    :return:
    """

    exchange_class = getattr(ccxt, exchange_name)   # 获取交易所的名称 ccxt.binance
    exchange = exchange_class()  # 交易所的类. 类似 ccxt.bitfinex()
    print(exchange)
    # exit()

    current_path = os.getcwd()
    file_dir = os.path.join(current_path, exchange_name, symbol.replace('/', ''))

    if not os.path.exists(file_dir):
        os.makedirs(file_dir)


    start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d')

    start_time_stamp = int(time.mktime(start_time.timetuple())) * 1000
    end_time_stamp = int(time.mktime(end_time.timetuple())) * 1000

    print(start_time)  # 1529233920000
    print(end_time)

    limit_count = 100000000
    if exchange_name == 'bitfinex':
        limit_count = BITFINEX_LIMIT
    elif exchange_name == 'bitmex':
        limit_count = BITMEX_LIMIT
    elif exchange_name == 'binance':
        limit_count = BINANCE_LIMIT

    while True:
        try:

            print('从' + str(start_time) +'开始,抓取中...')
            data = exchange.fetch_ohlcv(symbol, timeframe='1d', since=start_time_stamp, limit=limit_count)
            df = pd.DataFrame(data)

            df.rename(columns={0: 'open_time', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close', 5: 'Volume'}, inplace=True)

            start_time_stamp = int(df.iloc[-1]['open_time'])  # 获取下一个次请求的时间.

            filename = str(start_time_stamp) + '.csv'
            save_file_path = os.path.join(file_dir, filename)

            # exit()
            df.set_index('open_time', drop=True, inplace=True)
            df.to_csv(save_file_path)
            if start_time_stamp > end_time_stamp:
                print("BOSS,行情抓取完成.")
                break

            time.sleep(3)

        except Exception as error:
            print(error)
            time.sleep(10)


def sample_datas(exchange_name, symbol):
    """
    :param exchange_name:
    :param symbol:
    :return:
    """
    path = os.path.join(os.getcwd(), exchange_name, symbol.replace('/', ''))
    print(path)

    # exit()

    file_paths = []
    for root, dirs, files in os.walk(path):
        if files:
            for file in files:
                if file.endswith('.csv'):
                    file_paths.append(os.path.join(path, file))

    file_paths = sorted(file_paths)
    all_df = pd.DataFrame()

    for file in file_paths:
        df = pd.read_csv(file)
        all_df = all_df.append(df, ignore_index=True)

    all_df = all_df.sort_values(by='open_time', ascending=True)

    print(all_df)

    return all_df

    # for index, item in all_df.iterrows():
    #     try:
    #         dt = (pd.to_datetime(item['open_time'], unit='s'))
    #         print(dt)
    #         dt = datetime.datetime.strptime(str(dt), '%Y-%m-%d')  # 2018-01-01 17:36:00:42
    #         print(dt)
    #     except:
    #         dt = (pd.to_datetime(item['open_time'], unit='s'))
    #         print(dt)

def clear_datas(exchange_name, symbol):
    df = sample_datas(exchange_name, symbol)
    # print(df)
    # exit()
    # df['open_time'] = df['open_time'].apply(lambda x: time.mktime(x.timetuple()))
    # # 日期.timetuple() 这个用法 通过它将日期转换成时间元组
    # # print(df)
    # df['open_time'] = df['open_time'].apply(lambda x: (x // 60) * 60 * 1000)
    df['open_time'] = df['open_time'].apply(lambda x: (x//60)*60)
    print(df)
    df['Datetime'] = pd.to_datetime(df['open_time'], unit='ms') + pd.Timedelta(hours=8)
    df.drop_duplicates(subset=['open_time'], inplace=True)
    df.set_index('Datetime', inplace=True)
    print("*" * 20)
    print(df)
    symbol_path = symbol.replace('/', '')
    df.to_csv(f'{exchange_name}_{symbol_path}_1day_clean.csv')


if __name__ == '__main__':

    crawl_exchanges_datas('binance', 'BTC/USDT', '2017-9-1', '2020-2-28')
    # crawl_exchanges_datas('bitmex', 'BTC/USD', '2018-1-1', '2018-3-1')
    # crawl_exchanges_datas('bitfinex', 'BTC/USDT', '2018-1-1', '2019-7-22')

    #sample_datas('binance', 'BTC/USDT')
    clear_datas('binance', 'BTC/USDT') 

    pass
