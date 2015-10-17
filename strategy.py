"""
execute trading according to strategy
output blotters
"""

# #########################################################################
# Input filepath_ts = './ts/Future/IF/IF_15s.ts'
# datestimes,          volrmb,         open,       high,       low,         close,      avgprice,   bid,        ask
# 2014-12-09 09:15:00, 880059660.000,  3306.800,   3307.200,   3306.800,    3307.200,   3307.331,   3306.900,   3308.100
#
# #########################################################################
# Input filepath_ics ='./ics/Future/IF/IF_15s.ics'
# datestimes,           typ,        ema50,      ema27,      rsi14,      cci10,  tr,     kpi
# 2014-11-12 09:15:00,  2557.9333,  2557.4000,  2557.4000,  0.5000,     0.0000, 2.8000, 2557.4000
#
# #########################################################################
# Output filepath_blotter ='./blotter/Future/IF/IF_50_30.bl'
# datestimes,           direction,  lot,    product,    price,      refprice,   PNL,        maxPNL,     minPNL
# 2014-11-17 09:38:15,  B,          1,      IF,         2587.0000,  2586.1100
# 2014-11-17 09:40:15,  S,          1,      IF,         2581.8000,  2582.5770,  -5.2000,    1.4000,     -5.2000
import os
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

import operation as op
import pandas as pd
import sys
import log
logger=log.setlog("strategy.py")

i_lot=1

class Strategy(object):
    def __init__(self, s_prod, s_prodtype):
        if not (s_prodtype in ["Future","Stock"]):
            logger.error(r'prodtype not in ["Future","Stock"]. incorrect product type. ')
        self.prod = s_prod
        self.prodtype = s_prodtype
        self.freq = None
        self.freq2 =None
        self.freq3 =None


    def preparedata(self):
        def getdata(freq):
            try:
                filepath_ics =BASE_DIR+ '/ics/' + self.prodtype + '/' + self.prod + '/'+self.prod+'_' + freq + '.ics'
                filepath_ts =BASE_DIR+ '/ts/' + self.prodtype + '/' + self.prod + '/'+self.prod+'_' + freq + '.ts'
                df_ics = pd.read_csv(filepath_ics, parse_dates=[0], index_col=0)
                df_ts = pd.read_csv(filepath_ts, parse_dates=[0], index_col=0)
            except IOError:
                logger.error( "time series or indicator files of {prod} (freq = {f}) not exist".format(prod=self.prod,f=freq)  )
                raise Exception("File not extists")

            def get_ics(indicator):
                typ = indicator["typ"]
                ema50 = indicator["ema50"]
                ema27 = indicator["ema27"]
                rsi14 = indicator["rsi14"]
                cci10 = indicator["cci10"]
                tr = indicator["tr"]
                kpi = indicator["kpi"]
                return typ, ema50, ema27, rsi14, cci10, tr, kpi
            def get_ts(ts):
                volrmb = ts["volrmb"]
                o = ts["open"]
                h = ts["high"]
                l = ts["low"]
                c = ts["close"]
                avgprice = ts["avgprice"]
                bid = ts["bid"]
                ask = ts["ask"]
                return volrmb, o, h, l, c, avgprice, bid, ask

            volrmb, o, h, l, c, avgprice, bid, ask = get_ts(df_ts)
            typ, ema50, ema27, rsi14, cci10, tr, kpi = get_ics(df_ics)
            return volrmb, o, h, l, c, avgprice, bid, ask, typ, ema50, ema27, rsi14, cci10, tr, kpi
        if self.freq is None:
            raise Exception("freq not set!")
        self.volrmb, self.o, self.h, self.l, self.c, self.avgprice, self.bid, self.ask, self.typ, self.ema50, self.ema27, self.rsi14, self.cci10, self.tr, self.kpi =  getdata(self.freq)
        self.len = len(self.c)
        if self.freq2 is not None:
            self.volrmb_2, self.o_2, self.h_2, self.l_2, self.c_2, self.avgprice_2, self.bid_2, self.ask_2, self.typ_2, self.ema50_2, self.ema27_2, self.rsi14_2, self.cci10_2, self.tr_2, self.kpi_2 = getdata(self.freq2)
        if self.freq3 is not None:
            self.volrmb_3, self.o_3, self.h_3, self.l_3, self.c_3, self.avgprice_3, self.bid_3, self.ask_3, self.typ_3, self.ema50_3, self.ema27_3, self.rsi14_3, self.cci10_3, self.tr_3, self.kpi_3 =  getdata(self.freq3)
    def entry(self):
        """
        :return: whether in_indic = True or False
        """
        raise Exception("Not implemented")


    def exit(self):
        """
        :return: whether out_indic = True or False
        """
        raise Exception("Not implemented")


    def TradePrice(self):
        """
        :return: trade price
        """
        return self.c[self.time]


    def RefPrice(self):
        """
        :return: reference price
        """
        return (self.ask[self.time] + self.bid[self.time]) / 2


    def PNL(self):
        """
        :return: profit and loss
        """
        return self.c[self.time] - self.tradePrice


    def tradeBuy(self):
        """
        add a buy trade to Blotter
        """
        self.refprice = self.RefPrice()
        self.tradePrice = self.TradePrice()
        df_blotter = pd.DataFrame([(self.time, 'B', i_lot, self.prod, self.tradePrice, self.refprice)],
                              columns=['datestimes', 'direction', 'lot', 'product', 'price', 'refprice'])
        df_blotter.to_csv(self.filepath_blotter, mode='a', header=False, float_format='%.4f', index=False)


    def tradeSell(self):
        """
        add a sell trade to Blotter
        """
        self.refprice = self.RefPrice()
        self.pnl = self.PNL()
        self.tradePrice = self.TradePrice()
        df_blotter = pd.DataFrame([(self.time, 'S', i_lot, self.prod, self.tradePrice, self.refprice, self.pnl, self.maxPNL, self.minPNL)],
                              columns=['datestimes', 'direction', 'lot', 'product', 'price', 'refprice', 'PNL',
                                       'maxPNL', 'minPNL'])
        df_blotter.to_csv(self.filepath_blotter, mode='a', header=False, float_format='%.4f', index=False)
        self.in_indic = False
        self.out_indic = False
        self.maxPNL = 0.0
        self.minPNL = 0.0

    def run(self,start=3240):
        logger.info("strategy start.prod=%s",self.prod)
        self.preparedata()
        ## store strategy information
        s_startDay = self.c.index.date[0].strftime('%Y%m%d')
        s_endDay = self.c.index.date[-1].strftime('%Y%m%d')
        i_totalDays = len(set(self.c.index.date))
        info={}
        info['startDay']=s_startDay
        info['endDay']=s_endDay
        info['totalDays']=i_totalDays



        ## create blotter file
        ## save information to head of blotter file
        ## print column names
        with open(self.filepath_blotter, 'w') as fout:
            fout.writelines("%s %s\n"%(k,v) for (k,v) in info.items())
            fout.write("\ndatestimes,direction,lot,product,price,refprice,PNL,maxPNL,minPNL\n")


        """
        execute trading
        """
        self.in_indic = False
        self.out_indic = False

        self.maxPNL = 0.0
        self.minPNL = 0.0

        '''from start to the end, change the time period'''
        for i in xrange(start, self.len):
            self.time = self.c.index[i]
            op.time = self.time

            if self.in_indic:
                ''' update maxPNL and minPNL'''
                self.maxPNL = max(self.maxPNL, self.PNL())
                self.minPNL = min(self.minPNL, self.PNL())

            if not self.in_indic:
                ''' detect in_indic '''
                self.in_indic = self.entry()
                if self.in_indic:
                    self.tradeBuy()
                continue

            if self.in_indic and not self.out_indic:
                ''' detect out_indic '''
                self.out_indic = self.exit()
                if self.out_indic:
                    self.tradeSell()

        logger.info("strategy done.prod=%s",self.prod)

class MyStrategy(Strategy):
    def __init__(self, s_prod, s_prodtype, arg1, arg2):
        Strategy.__init__(self, s_prod, s_prodtype)
        self.arg1 = arg1
        self.arg2 = arg2
        self.freq = '1H'
        self.filepath_blotter =BASE_DIR+ '/blotter/' +self.prodtype + '/' + self.prod + '/'+self.prod+  '_' + str(self.arg1) + '_' + str(self.arg2) + '.bl'
        if not os.path.exists(BASE_DIR + '/blotter/' +self.prodtype + '/' + self.prod):
            os.makedirs(BASE_DIR + '/blotter/' +self.prodtype + '/' + self.prod)

    def entry(self):
        cond1 = op.countif(self.ema27 > self.ema50, self.arg1) == self.arg2
        cond2 = op.mro(self.ema27> self.ema50,20,1)>=5
        cond = cond1 and cond2
        if cond:
            return True
        else:
            return False

    def exit(self):
        cond = abs(self.PNL()) > 0.03*self.TradePrice()
        if cond:
            return True
        else:
            return False


if __name__ == '__main__':
    s_prod = "SH600008"
    s_prodtype = "Stock"
    s = MyStrategy(s_prod, s_prodtype,50,30)
    s.run()
