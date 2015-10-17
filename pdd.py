import datetime as dt
import dateutil
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os, glob
import multiprocessing as mp


import log
logger=log.setlog("pdd.py")

#rights = pd.DataFrame()
def process_pd():
    ## three types of profit distribution: cash, stock, increase
    ##Prod      ProductCode PlanDate    DecisionDate    PDD         Cash    Stock   Increase    StockSum
    ##SH        SH600000    20000328    20000509        20000706    0.15    0       0           0
    ##SH        SH600000    20020321    20020629        20020822    0.2     0       0.5         0.5
    try:
        confpath = './configuration/PDData.txt'
        global rights
        rights = pd.read_csv(confpath, sep ='\t')
        # process security simultanously using multiprocess
        filepath = './datas/Stock'
        seclist = glob.glob(filepath+'/*_nopd')
        logger.info("nopdlist:%s",seclist)

        po = mp.Pool (mp.cpu_count())
        teams = po.map(work, seclist)
        po.close()
        po.join()
    except Exception,ex:  
        logger.error( "{0}:	{1}".format(Exception,ex)  )

def work (s):
    ## ====== After processing ========================================================
    ##   0           1            2     3       4       5       6       7           8       9
    ## date        time        volrmb  open    high    low     close   avgprice    bid     ask
    ## 20050104    09:31:00    156234  6.98    6.98    6.95    6.95    6.97        6.94    7.01
    secname = s.split('/')[-1].split('_')[0]
    # print '==== apply profit rights to: ' + s
    secright = rights[rights.ProductCode ==secname]
    filepath = s 
    data = pd.read_csv(filepath, sep =' ')
    data = data.fillna(method ='pad')
    result = data[data.date<0]
    dates = data['date']
    secright = secright [ secright.PDD >= dates[0]]

    ## for each d in secright.PDD, i.e. 20050512,
    ##      apply entire secright[>=20050512 ] to data [ :20050511 ,:]
    ## start    = 20050104
    ## end0     = 20050512
    ## end1     = 20060525
    start = data.date.iloc[0]
    for end in secright.PDD:
        dataseg     = data[data.date>=start]
        dataseg     = dataseg[dataseg.date<int(end)]
        rightsseg   = secright [ secright.PDD >= end]
        #print (secname , dataseg.date.iloc[0] , dataseg.date.iloc[-1] )
        newseg      = rights_pd ( dataseg, rightsseg )
        result      = pd.concat((result,newseg))
        start=end

        ## apply no secrights to the remaining data
    #if end == secright.PDD.iloc[-1]:
    newseg  = data[data.date>=start]
    result  = pd.concat((result,newseg))

    ## combine days and times as key index
    dates = result['date']
    times = result['time']
    d = dates.astype('str')
    # print len(d)    # print times
    dandt =[]
    for i in range(len(d)):
        # print dateutil.parser.parse(d[i]+' '+times[i])
        dandt.append(dateutil.parser.parse(d[i]+' '+times[i]))
    result['datestimes']=dandt
    df = pd.DataFrame (result, columns =['datestimes','volrmb', 'open','high', 'low','close','avgprice','bid','ask'])
    df =df.set_index('datestimes')
    df.to_csv('./datas/Stock/'+secname+'_pd', float_format='%.3f')
    logger.info("work(%s) done",s)



## === perform prices calc for given dataseg on given rights table
def rights_pd (dataseg, r):
    meta    = dataseg.iloc[:,0:3]
    prices  = dataseg.iloc[:,3: ]
    #print dataseg.iloc[0]
    #print r.PDD.iloc[0]
    for i in range(len(r)) :
        prices = prices - r.Cash.iloc[i]
        prices = prices / (1+ r.Stock.iloc[i])
        prices = prices / (1+ r.Increase.iloc[i])

    return pd.concat ((meta, prices), axis=1)



if __name__ == "__main__":
    process_pd()
    # confpath = './configuration/PDData.txt'
    # global rights
    # rights = pd.read_csv(confpath, sep ='\t')
    # work('./datas_nopd/SH600000')
