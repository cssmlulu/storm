'''
define operation functions
'''

import pandas as pd

import log
logger=log.setlog("operation.py")

global time

def countif(df_data, i_period):
    """
    count the number of True data in the last i_period time
    Example: countif([True,False,True,True,False],3)=2
    :param df_data: a Series of boolean value, result from conditions such as 'ema27 > ema50 and cci10>100 or typ>ema27'
    :param i_period: time length
    :return: a number
    """
    assert i_period<=len(df_data)
    index = df_data.index.get_loc(time)
    i_count = len(filter(bool,df_data[index - i_period + 1:index + 1]))
    return i_count


def mro(df_data, i_period, i_index):
    """
    Most Recent occur
    Example: mro([True,False,True,True,False],3,1)=1
    :param df_data: df_data: a Series of boolean value, result from conditions such as 'ema27 > ema50 and cci10>100 or typ>ema27'
    :param i_period: time length
    :param i_index: find the index of the i_index_th value (backwards)
    :return: time gap ( index difference)
    """
    assert i_index<=i_period <=len(df_data)
    index = df_data.index.get_loc(time)
    df_subdata = df_data[index:index-i_period:-1]
    try:    
        return pd.expanding_apply(df_subdata,lambda x:x.tolist().count(True)).tolist().index(i_index)
    except ValueError:
        return -1    


def lro(df_data, i_period, i_index):
    """
    Last Recent occur
    Example: lro([True,False,True,True,False],3,1)=2
    :param df_data: df_data: a Series of boolean value, result from conditions such as 'ema27 > ema50 and cci10>100 or typ>ema27'
    :param i_period: time length
    :param i_index: find the index of the i_index_th value (forwards)
    :return: time gap ( index difference)
    """
    assert i_index<=i_period <=len(df_data)
    index = df_data.index.get_loc(time)
    df_subdata = df_data[index-i_period+1:index+1]
    try:    
        return i_peiod-1-pd.expanding_apply(df_subdata,lambda x:x.tolist().count(True)).tolist().index(i_index)
    except ValueError:
        return -1 


def maxlist(df_data, i_period):
    """
    return the max value in the last i_period time
    Example: maxlist([1,5,4,2,3],2) --> 3
    :param df_data: indicator such as ema, cci...
    :param i_len: time length
    :return: the max value
    """
    assert i_period<=len(df_data)
    index = df_data.index.get_loc(time)
    maxValue = max(df_data[index:index-i_period:-1])
    return maxValue


if __name__ == '__main__':
    try:
        filepath = './ics/IF_15s.ics'
        df_indicator = pd.read_csv(filepath, parse_dates=[0], index_col=0)
        typ = df_indicator["typ"]
        ema50 = df_indicator["ema50"]
        ema27 = df_indicator["ema27"]
        rsi14 = df_indicator["rsi14"]
        cci10 = df_indicator["cci10"]
        tr = df_indicator["tr"]
        kpi = df_indicator["kpi"]
        # print ema50.shift(1)
        # i1 = mro(ema27 > ema50, 50, 2)
        # i2 = mro(ema27 > ema50, 50, 4)
        # print  i1 - i2
        # print df_indicator.index.get_loc(i1) - df_indicator.index.get_loc(i2)
        time = pd.Timestamp('2014-11-12 09:17:15')
        print time
        print maxlist(ema50,6)
    except Exception,ex:  
        logger.error( "{0}:	{1}".format(Exception,ex)  )



