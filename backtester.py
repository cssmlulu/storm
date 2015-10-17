"""
analyze the blotter result
"""

# #########################################################################
# Input filepath_blotter ='./blotter/Future/IF/IF_50_30.bl'
# datestimes,           direction,  lot,    product,    price,      refprice,   PNL,        maxPNL,     minPNL
# 2014-11-17 09:38:15,  B,          1,      IF,         2587.0000,  2586.1100
# 2014-11-17 09:40:15,  S,          1,      IF,         2581.8000,  2582.5770,  -5.2000,    1.4000,     -5.2000
# #########################################################################
# Output filepath_blotter ='./backtester/IF.bt' (single record)
# arg1, arg2,   TotalDays,  WinDays,    LossDays,   TotalPnL,   AvgDailyPnl,    MaxMarkdown,    MaxProfit,  MaxLoss,    SharpeRatio
# 50,   30,     22,         8,          9,          0.6000,     0.0273,         80.8000,        81.4000,    -43.0000,   0.0007
import numpy as np
import pandas as pd
import math,os,sys
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

import log
logger=log.setlog("backtester.py")

class Backtest(object):
    def __init__ ( self,s_prod, s_prodtype):
        if not (s_prodtype in ["Future","Stock"]):
            logger.error(r'prodtype not in ["Future","Stock"]. incorrect product type. ')
        self.prod = s_prod
        self.prodtype = s_prodtype
        self.filepath_blotter = None
        self.filepath_backtester =None

    def run( self):
        logger.info("backtest start.prod=%s;arg=%s,%s",self.prod,self.arg1,self.arg2)
        df_blotter = pd.read_csv( self.filepath_blotter, parse_dates=[0], index_col=0,skiprows=3)
        info = open(self.filepath_blotter,'r')
        self.startday = info.readline().split()[-1]
        self.endday = info.readline().split()[-1]
        self.totaldays = int(info.readline().split()[-1])

        '''blotter data group by each day'''
        df_dailydata = df_blotter.groupby(df_blotter.index.date).sum()

        self.winDays = len(df_dailydata[df_dailydata["PNL"] > 0])
        self.lossDays = len(df_dailydata[df_dailydata["PNL"] < 0])
        self.totalPnl = df_dailydata["PNL"].sum()
        self.avgDailyPnl = df_dailydata["PNL"].sum() / self.totaldays

        '''cumulative sum series of PnL'''
        df_PnlSum = df_dailydata["PNL"].cumsum().dropna()
        self.maxProfit = max(0, df_PnlSum.max())
        self.maxLoss = min(0, df_PnlSum.min())
        self.maxDrawdown = max(np.maximum.accumulate(df_PnlSum) - df_PnlSum)

        df_PnL = df_blotter["PNL"].dropna()
        i_tradingDaysPerYear = 252
        self.sharpeRatio = df_PnL.mean() * df_PnL.std() * math.sqrt(i_tradingDaysPerYear)

        df_rst = pd.DataFrame(
            [(self.totaldays, self.winDays, self.lossDays, self.totalPnl, self.avgDailyPnl, self.maxDrawdown, self.maxProfit, self.maxLoss, self.sharpeRatio)],
            columns=['TotalDays', 'WinDays', 'LossDays', 'TotalPnL', 'AvgDailyPnl', 'MaxMarkdown', 'MaxProfit', 'MaxLoss', 'SharpeRatio'])
        df_arg = pd.DataFrame([(self.arg1, self.arg2)], columns=['arg1', 'arg2'])
        df_rst = pd.concat([df_arg, df_rst], axis=1)
        df_rst.to_csv(self.filepath_backtester, mode='a', header=False, float_format='%.4f', index=False)
        logger.info("backtest done.prod=%s;arg=%s,%s",self.prod,self.arg1,self.arg2)

class MyBacktest(Backtest):
    def __init__(self, s_prod, s_prodtype, arg1, arg2):
        Backtest.__init__(self, s_prod, s_prodtype)
        self.arg1 = arg1
        self.arg2 = arg2
        self.filepath_blotter =BASE_DIR+ '/blotter/' + self.prodtype + '/' + self.prod + '/' + self.prod +  '_' + str(arg1) + '_' + str(arg2) + '.bl'
        if not os.path.exists(self.filepath_blotter):
            logger.error( "blotter file of {prod} not exist".format(prod=self.prod) )
            raise Exception("File not extists")
        self.filepath_backtester =BASE_DIR+ '/backtester/' + self.prod + '.bt'

if __name__ == '__main__':
    s_prod = "SH600000"
    s_prodtype = "Stock"
    s = MyBacktest(s_prod, s_prodtype,50,30)
    s.run()
