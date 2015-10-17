import pandas as pd
import multiprocessing as mp
import os
import glob
import dateutil
import fcntl

import log
logger = log.setlog("ts_futures.py")

"""
Input: individual file containing daily tick data
=================== 20140303_IF1403_CFFEX_L1.txt =======================
close	high	low	    tlots	tvols	    openint	    zero1	zero2	time	    zero3	bid	    bidvol	ask	    askvol

2165.6	2165.6	2165.6	587	    381362160	94019	    0	    0	    09:14:00 	0	    2165.4	7	    2165.6	8
2165.6	2166	2164.4	809	    525567540	94045	    0	    0	    09:15:00 	0	    2165.6	142	    2166	7
2165    2166.6  2164.4  1166    757512840   94027       0       0       09:15:00    0       2165    12      2165.4  9
...
========================================================================

OutPut: combine all daily files into one big tick file
"""

##############  global configuration    ############

prod = 'IF'
s_starttime = '09:15:00'
s_breakmor = '11:30:00'
s_breakrep = '11:29:59'
s_endtime = '15:15:00'
s_endrep = '15:14:59'
tickdollar = 300.0
ls_freq = ['1s','4s','15s', '1min']
s_dirpath = './datas_raw/'
s_stdpath = './datas/'
s_tspath = './ts/'
zipon = False


def combine_newfiles():
    try:
        filepath = s_dirpath + prod + '/'

        for freq in ls_freq:
        # open file and prepare for writing
            new_ts = s_tspath + prod + '/' + prod + '_' + freq + '.ts'
            with open(new_ts,'w') as f:
                f.write('datestimes,volrmb,open,high,low,close,avgprice,bid,ask\n')

        # glob daily tick data
        ls_files = glob.glob(filepath + '*.txt')
        ls_files.sort()

        # transform each daily tick data to timeseries and print to file
        po = mp.Pool (mp.cpu_count())
        po.map(convert_onefile, ls_files)
        po.close()
        po.join()

        #sort the records in timeseries files
        po2 = mp.Pool (mp.cpu_count())
        po2.map(sort_single_file,ls_freq)
        po2.close()
        po2.join()
        logger.debug(prod,ls_freq,'timeserieses produced')

    except Exception,ex:  
        logger.error( "{0}: {1}".format(Exception,ex)  )


def ts_one_freq(freqarg):
    ######################################################
    ############  produce one timeseriese  ###############
    ######################################################
    #df_result=pd.read_csv(s_outfile, parse_dates=[0], index_col=0)
    df_new,freq = freqarg

    s_ts = s_tspath + prod + '/' + prod + '_' + freq + '.ts'
    v = df_new[[0]]
    c = df_new[[4]]
    avgp = df_new[[5, 6, 7]]
    rv = v.resample(freq, how='sum').dropna()
    ro = c.resample(freq, how='first').dropna()
    rh = c.resample(freq, how='max').dropna()
    rl = c.resample(freq, how='min').dropna()
    rc = c.resample(freq, how='last').dropna()
    ravgp = avgp.resample(freq, how='mean').dropna()
    ro.columns = ['open']
    rh.columns = ['high']
    rl.columns = ['low']
    result = pd.concat([rv, ro, rh, rl, rc, ravgp], axis=1)

    ## print to files
    with open(s_ts,'a') as f:
        fcntl.flock(f,fcntl.LOCK_EX)
    	result.to_csv(f, float_format='%.3f',header=False)
    if zipon:
        os.system('gzip -c ' + s_newts + ' > ' + s_newts)


def convert_onefile(s_file):

    s_names = ['close', 'high', 'low', 'tlots', 'tvols', 'openint', 'zero1',
               'zero2', 'time', 'zero3', 'bid', 'bidvol', 'ask', 'askvol']
    s_date = s_file.split('/')[-1].split('_')[0]
    df_file = pd.read_csv(s_file, sep=' ', names=s_names)
    s_tempfile = s_stdpath + prod + '/' + prod + '_' + s_date + '.temp'

    ##############  process datetime    ##################################
    # 1. to avoid the market closing bar has only 1 tick, we need to:
    #    s_breakmor  = '11:30:00' toBeReplacedAs s_breakrep  = '11:29:59'
    #    s_endtime   = '15:15:00' toBeReplacedAs s_endrep    = '15:14:59'
    l_timetemp = df_file['time'].tolist()
    l_time = []
    for s in l_timetemp:
        if s == s_endtime:
            l_time.append(s_endrep)
        elif s == s_breakmor:
            l_time.append(s_breakrep)
        else:
            l_time.append(s)

    # 2. this df_file['time'] might contain repetive values
    #    '09:15:01' --> '09:15:01'
    #    '09:15:01' --> '09:15:01.001'
    #    '09:15:02'
    prei = 0
    ld_adj = [dateutil.parser.parse(s_date + ' ' + l_time[0])]
    for i in xrange(1, len(l_time)):
        if l_time[i] == l_time[prei]:
            ld_adj.append(dateutil.parser.parse(s_date + ' ' + l_time[i] + '.' + str((i-prei)/1000.0)[2:]))
        else:
            ld_adj.append(dateutil.parser.parse(s_date + ' ' + l_time[i]))
            prei = i
    df_time = pd.DataFrame(ld_adj)

    ##############  process ohlc    ############
    df_o = df_file['close']
    df_h = df_file['close']
    df_l = df_file['close']
    df_c = df_file['close']

    ##############  process lots   ############
    ## calc per timeslot traded lots
    df_tlots = df_file['tlots']
    df_nlots = df_tlots - df_tlots.shift(1).fillna(0)
    df_tvols = df_file['tvols']
    df_nvols = df_tvols - df_tvols.shift(1).fillna(0)

    ##############  process trade related prices   ############
    df_avgp = df_nvols / df_nlots / tickdollar
    df_avgp = df_avgp.fillna(method='pad')
    df_bid = df_file['bid']
    df_ask = df_file['ask']

    ##############  combin all  ############
    l_columns = ['datestimes', 'volrmb', 'open', 'high', 'low', 'close',
                         'avgprice', 'bid', 'ask']
    df_result = pd.concat([df_time, df_nvols, df_o, df_h, df_l, df_c,
                           df_avgp, df_bid, df_ask], axis=1)
    df_result.columns = l_columns
    df_result = pd.DataFrame(df_result, columns=l_columns)
    df_result = df_result.set_index('datestimes')

    ##############  filter by time session & save  ############
    dt_start = dateutil.parser.parse(s_date + ' ' + s_starttime)
    dt_end = dateutil.parser.parse(s_date + ' ' + s_endtime)

    df_result = df_result[(df_result.index >= dt_start) & (df_result.index <= dt_end)]
    #df_result.to_csv(s_tempfile, float_format='%.3f')
    #print df_result
    for freq in ls_freq:
        ts_one_freq((df_result, freq))
    
def sort_single_file(freq):
    s_ts = s_tspath + prod + '/' + prod + '_' + freq + '.ts'
    df_ts = pd.read_csv(s_ts,index_col=0,parse_dates=[0])
    df_ts_sorted = df_ts.sort_index(ascending =True)
    df_ts_sorted.to_csv(s_ts,float_format='%.3f')

if __name__ == "__main__":
    # ls_freq = raw_input('convert into: (5min/1H/M/D)')
    combine_newfiles()
    #convert_onefile('./datas_IF/20140303_IF1403_CFFEX_L1.txt')
