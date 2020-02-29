# -*- coding: utf-8 -*-
###############################################################################
# Copyright (C) 2020 CenturyQuant.Inc
###############################################################################

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects
import backtrader as bt

# Create a Stratey
class TestStrategy(bt.Strategy):
    params = (
        # ('maperiod', 21),
        ('printlog', True),
    )

    def log(self, txt, dt=None, doprint=False):
        ''' Logging function fot this strategy'''
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close
        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None

        # Add a MovingAverageSimple indicator
        self.ema_fast = bt.indicators.ExponentialMovingAverage(self.datas[0], period= 5)
        self.ema_slow = bt.indicators.ExponentialMovingAverage(self.datas[0], period= 10)
        bt.indicators.MACDHisto(self.datas[0])
        rsi = bt.indicators.RSI(self.datas[0])
        #bt.indicators.ATR(self.datas[0])


    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)


        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS, %.2f, NET, %.2f' %
                 (trade.pnl, trade.pnlcomm))


    def next(self):
        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.dataclose[0])

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Check if we are in the market
        if not self.position:

            # Not yet ... we MIGHT BUY if ...
            if self.ema_fast > self.ema_slow:
                # BUY, BUY, BUY!!! (with all possible default parameters)
                self.log('BUY CREATE, %.2f' % self.dataclose[0])

                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()

        else:

            if self.ema_fast < self.ema_slow:
                # SELL, SELL, SELL!!! (with all possible default parameters)
                self.log('SELL CREATE, %.2f' % self.dataclose[0])

                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell()

    def stop(self):
        self.log('YOU WIN !' , doprint=True)


if __name__ == '__main__':
    cerebro = bt.Cerebro(maxcpus = 1 )
    cerebro.broker.setcash(1000000.0)

    # Add a strategy
    cerebro.addstrategy(TestStrategy)


    # Datas are in a subfolder of the samples. Need to find where the script is
    # because it could have been called from anywhere
    # modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
    #datapath = "E:/centuryquant/NavinLab/ccxtbk/backtrader-master/datas/orcl-1995-2014.txt"
    datapath = "E:/centuryquant/NavinLab/ccxtbk/binance/b.csv"


    # Create a Data Feed
    data = bt.feeds.GenericCSVData(
        dataname=datapath,
        # Do not pass values before this date
        fromdate=datetime.datetime(2017, 12, 1),
        # Do not pass values after this date
        todate=datetime.datetime(2020, 2, 27),
        datetime=0,
        open=1,
        high=2,
        low=3,
        close=4,
        volume=5,
        openinterest=6,
        code=-1,
        reverse=False,
        # decimals = 6
    )


    # Add the Data Feed to Cerebro
    cerebro.adddata(data)

    # Add a FixedSize sizer according to the stake
    cerebro.addsizer(bt.sizers.FixedSize, stake=100)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # Set the commission
    cerebro.broker.setcommission(commission=0.002)

    cerebro.run()

    earn = '%.2f' % (cerebro.broker.getvalue() - 1000000)
    earnrate = float(earn)/1000000
    print('***Final Portfolio Value: $%.2f' % cerebro.broker.getvalue())
    print('***You earned: $' + earn)
    print('***Earning rate:' + str('%.2f' % (earnrate*100)) + '%')
    cerebro.plot()
