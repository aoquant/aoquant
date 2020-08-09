# -*- coding:utf-8 -*-
import pandas as pd
from aioquant import const
from aioquant.configure import config
from aioquant.market import MarketSubscribe, Kline, Orderbook, Trade
from aioquant.order import Order
from aioquant.position import Position
from aioquant.trade import Trade
from aioquant.utils import logger
from aioquant.utils.tools import DingTalk


class VolStrategy:

    def __init__(self):
        # 保存最新的trade数据
        self._trades = []
        self.platform = config.ACCOUNTS[0]["platform"]

        # 订阅盘口数据
        MarketSubscribe(const.MARKET_TYPE_ORDERBOOK, self.platform, config.symbol, self.on_event_orderbook_update_callback)

        # 订阅Trade数据
        MarketSubscribe(const.MARKET_TYPE_TRADE, self.platform, config.symbol, self.on_event_trade_update_callback)

        # 持仓信息
        self.position = Position()

        # 当前盘口价格
        self.cur_price = None

        # 初始化Trade交易对象
        params = {
            "strategy": config.strategy,
            "platform": self.platform,
            "symbol": config.symbol,
            "account": config.ACCOUNTS[0]["account"],
            "access_key": config.ACCOUNTS[0]["access_key"],
            "secret_key": config.ACCOUNTS[0]["secret_key"],
            "order_update_callback": self.on_event_order_update_callback,
            "position_update_callback": self.on_event_position_update_callback,
            "init_callback": self.on_event_init_callback,
            "error_callback": self.on_event_error_callback
        }
        self.trade = Trade(**params)

    async def on_event_init_callback(self, success, **kwargs):
        """初始化回调"""
        logger.info("initialize status:", success, caller=self)
        await DingTalk.send_text_msg(access_token=config.ding_token, content='V策略*初始化成功')

    async def on_event_error_callback(self, error, **kwargs):
        logger.error("Error:", error, caller=self)
        # 推送钉钉报警

    async def on_event_position_update_callback(self, position: Position):
        """持仓更新回调"""
        self.position = position
        logger.info("position:", position, caller=self)

    async def on_event_order_update_callback(self, order: Order):
        """订单更新回调"""
        logger.info("order:", order, caller=self)

    async def on_event_orderbook_update_callback(self, orderbook: Orderbook):
        """盘口订单薄数据更新回调"""
        # 取盘口买一卖一最新价格的平均值，作为当前盘口价格
        self.ask_price = round(float(orderbook.asks[0][0]), 1)
        self.bid_price = round(float(orderbook.asks[0][0]), 1)
        self.cur_price = round((float(orderbook.asks[0][0]) + float(orderbook.bids[0][0])) / 2, 1)

    async def reformat_data(self, data, vol):

        data['total_vol'] = (data['quantity'].cumsum() / vol)  # 截止至当前时间总成交量所占份额

        data = data.reset_index()

        largest = int(data.iloc[-1]['total_vol'])

        new_data = pd.DataFrame()
        open_time, openp, close, high, low = [], [], [], [], []

        last_mid = 0

        for i in range(1, largest + 1):
            # print(i, largest)  # 打印当前数据转换进度
            mid_index = list(data[(data.total_vol > (i - 1)) & (data.total_vol <= i)].index)
            if len(mid_index) == 0:
                mid_index = [last_mid + 1]
            else:
                if float(data.iloc[mid_index[-1]]['quantity']) == i:
                    last_mid = mid_index[-1]
                else:
                    last_mid = mid_index[-1] + 1
                    mid_index.append(last_mid)
            mid = data.iloc[mid_index[0]:mid_index[-1] + 1]
            open_time.append(mid.iloc[0]['timestamp'])

            close.append(list(mid['price'])[-1])

        new_data['open_time'] = open_time
        new_data['total_vol'] = list(range(1, largest + 1))

        new_data['close'] = close

        return new_data


    async def on_event_trade_update_callback(self, trade: Trade):
        """trade数据更新回调"""

        d = {
            "platform": trade.platform,
            "symbol": trade.symbol,
            "action": trade.action,
            "price": trade.price,
            "quantity": trade.quantity,
            "timestamp": trade.timestamp
        }
        self._trades.append(d)

        if len(self._trades) <= 5000:
            print('len(self._trades) == {} , < {}'.format(len(self._trades), 5000))
            if len(self._trades) == 5000:
                await DingTalk.send_text_msg(access_token=config.ding_token, content='数据加载完成')
            return

        # 把多余的数据删除掉，只保留最新的N条K线
        if len(self._trades) > config.len_trades:
            self._trades = self._trades[1:]

        df = pd.DataFrame(self._trades)
        df.drop(['platform', 'symbol'], axis=1,inplace=True)
        df['price'] = df['price'].apply(float)
        df['quantity'] = df['quantity'].apply(int)

        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)
        df['qty_sum'] = df['quantity'].cumsum()

        if df['qty_sum'].iloc[-1] < (config.slow + 2) * config.vol_num:
            print("df['qty_sum']:{},不足{}！".format(df['qty_sum'].iloc[-1], (config.slow + 2) * config.vol_num))
            return


        try:
            df_volume = self.reformat_data(df, 200000)

            # 策略开始
            if df_volume:
                print('V策略开多')
                await DingTalk.send_text_msg(access_token=config.ding_token, content='V策略{}开多'.format(self.ask_price))

        except Exception as e:
            print("错误:", str(e))