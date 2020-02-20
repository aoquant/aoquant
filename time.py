import datetime
import pandas as pd

df = pd.read_csv('E:/centuryquant/NavinLab/binance/BTCUSDT/binance/BTCUSDT/Binance_btcusdt_1day_data.csv') #读取

# 如时间戳为open_time，将时间戳转化为时间
df['open_time'] = pd.to_datetime(df['open_time'], unit='ms') + pd.Timedelta(hours=8)  

#导出到csv 
df.to_csv('E:/centuryquant/NavinLab/binance/BTCUSDT/binance/BTCUSDT/btcnew.csv', index=False)

