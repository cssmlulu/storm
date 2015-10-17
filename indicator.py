#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
calculate indicators
"""
import numpy as np
import pandas as pd
import log

logger = log.setlog("indicator.py")


def typ(df_prices_high, df_prices_low, df_prices_close):
    """
    Typical Price = average( high + low + close )
    """
    df_typ = (df_prices_high + df_prices_low + df_prices_close) / 3
    df_typ.name = 'typ'
    return df_typ


def ema(df_prices, i_period, i_start=1):
    """
    Exponential Moving Average
    S(t) = w*P(t) + (1-w) * S(t-1)
    w = 2/(N+1) 
    """

    i_len = len(df_prices)
    assert i_len >= i_period

    df_ema = pd.ewma(df_prices, span=i_period, adjust=False, min_periods = i_start)
    ## copy value for datas before i_start
    df_ema[:i_start-1] = df_prices[:i_start -1 ]
    df_ema.name = 'ema' + str(i_period)

    return df_ema


def sma(df_prices, i_period):
    """
    Simple Moving Average
    SMA[i] = (P[i-k+1] +  ... + P[i]) / K
    where K = i_period and P[i] is the most recent price
    """

    i_len = len(df_prices)
    assert i_len >= i_period

    df_sma = pd.rolling_mean(df_prices,i_period)
    ## copy value for datas before i_period ( wait for enough data )
    df_sma[:i_period -1] = df_prices[:i_period - 1]
    df_sma.name = 'sma'+str(i_period)
    return df_sma


def rsi(df_prices, i_period=14):
    """
    Relative Strength Index
    Up periods:
	U = close(i) - close(i-1)
	D = 0
    Down period:
	U = 0
	D = close(i) - close(i-1)
    RS = EMA(U) / EMA(D)
    RSI = 100 - 100/(1+RS)
        = EMA(U) / (EMA(U) + EMA(D)) *100
    
    """
    i_len = len(df_prices)
    assert i_len >= i_period

    df_prices_U = df_prices - df_prices.shift(1)
    df_prices_U[df_prices_U < 0] = 0
    df_prices_U[0] = 0

    df_prices_D = df_prices.shift(1) - df_prices
    df_prices_D[df_prices_D < 0] = 0
    df_prices_D[0] = 0

    df_ema_U = ema(pd.Series(df_prices_U), i_period)
    df_ema_D = ema(pd.Series(df_prices_D), i_period)

    df_rsi = (df_ema_U / (df_ema_U + df_ema_D)) * 100

    ## set the first value
    df_rsi[0] = 0.5
    df_rsi = pd.Series(df_rsi, index=df_prices.index, name='rsi' + str(i_period))

    return df_rsi


def cci(df_typ, df_c, i_period):
    """
    http://en.wikipedia.org/wiki/Commodity_channel_index

    CCI = (p - SMA(p)) / (σ(p) * 0.015)

    p = typical price
    SMA = simple moving average
    σ = mean absolute deviation
    """
    i_len = len(df_typ)
    assert i_len >= i_period

    df_mad = pd.rolling_apply(df_c,10,lambda x : np.fabs(x-x.mean()).mean())

    df_sma = sma(df_c, i_period)

    df_cci = ( df_typ - df_sma) / (df_mad * 0.015)

    ## set values before i_period ( wait for enough data )
    df_cci[:i_period-1] = 0.

    df_cci.name = 'cci' + str(i_period)

    return df_cci


def tr(df_prices_high, df_prices_low, df_prices_close):
    """
    True Range
    TR=Max（︱high(i)-low(i) ︳，︳high(i)-close(i-1) ︳，︳low(i) - close(i-1) ︳）
    """

    df_tr = pd.concat([df_prices_high - df_prices_low, abs(df_prices_high - df_prices_close.shift(1)),
                       abs(df_prices_low - df_prices_close.shift(1))], axis=1).max(axis=1)

    df_tr[0] = df_prices_high[0] - df_prices_low[0]
    df_tr = pd.Series(df_tr, index=df_prices_close.index, name='tr')

    return df_tr


def kpi(df_prices):
    """
    KPI = 3*EMA27 - 2* EMA50
    """
    df_ema27 = ema(df_prices, 27, 27)
    df_ema50 = ema(df_prices, 50, 50)
    df_kpi = 3 * df_ema27 - 2 * df_ema50
    df_kpi[:50] = df_prices[:50]
    df_kpi.name = 'kpi'

    return df_kpi

"""
def test(filepath):
    df_prices = pd.read_csv(filepath, parse_dates=[0], index_col=0)
    df_c = df_prices['close']
    df_h = df_prices['high']
    df_l = df_prices['low']

    Typ = typ(df_h , df_l , df_c)
    X50 = ema(df_c, 50)
    X27 = ema(df_c, 27)
    Rsi = rsi(df_c)
    Cci10 = cci(Typ ,df_c, 10)
    Tr = tr(df_h,df_l,df_c)
    Kpi = kpi(df_c)
"""

if __name__ == "__main__":
    try:
        test('./ts/Stock/SH600000/SH600000_1D.ts')
    except Exception,ex:  
        logger.error( "{0}:	{1}".format(Exception,ex)  )
