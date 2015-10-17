import datetime as dt
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os, glob
import multiprocessing as mp

import log
logger=log.setlog("processdata.py")

def list_securities ():
    try:
        ## get all the securities to be processed 
        ## loop through to process
        filepath_rawdata='../share/stocks_data/'
        #filepath_rawdata='./datas_raw/Stock/'

        seclist = glob.glob(filepath_rawdata+'*')
        logger.info("rawdata list:%s",seclist)
        po = mp.Pool (mp.cpu_count())
        po.map(work, seclist)
        po.close()
        po.join()
    except Exception,ex:  
        logger.error( "{0}: {1}".format(Exception,ex)  )

def work (s):
    logger.info("work(%s) start",s)
    secname, result =process_one_stock(s)
    ## formated print
    '''
    outfiletime  = './datas/Stock/'+secname+ '_time'
    outfileprice = './datas/Stock/'+secname+ '_price'
    outfile = './datas/Stock/'+secname+ '_nopd'
    np.savetxt(outfiletime,  result[:, 0:3], fmt ='%s')
    np.savetxt(outfileprice, (result[:, 3: ]).astype('float') , fmt ='%1.3f')
    a= np.loadtxt(outfiletime, dtype='str')
    b= np.loadtxt(outfileprice, dtype='str')
    np.savetxt ( outfile, np.hstack( (a,b)), fmt = '%s', header=h, comments='')
    os.system('rm -f '+outfiletime)
    os.system('rm -f '+outfileprice)
    '''
    time = np.array(result[:,0:3])
    price = np.array(result[:,3:],dtype='float')
    price_float = np.array(["%.3f" % x for x in price.reshape(price.size)])
    price_float = price_float.reshape(price.shape)
    out = np.concatenate((time,price_float),axis=1)

    outfile = './datas/Stock/'+secname+ '_nopd'
    h = 'date time volrmb open high low close avgprice bid ask'
    np.savetxt(outfile,out,fmt='%s',header=h,comments='')
    logger.info("work(%s) done",s)

def process_one_stock (stockname):
    ## loop through all the day files 
    
    secname = stockname.split('/')[-1]
    dayfiles = glob.glob(stockname+'/*.txt')
    dayfiles.sort()
    # print dayfiles
    for f in dayfiles[0:1]:
        result = process_one_day(f)

    for f in dayfiles[1:]:
        result = np.vstack((result, process_one_day(f)))

    return secname, result

def process_one_day (onedayfile):

    ## go through one file (day) to process
    ## return list 
    
    ## ====== original format ===
    ##[ 0- 9] date      pcode  ex  pcode   close   c1  c2  c3  open    high    
    ##[10-19] low total_vol_lots  total_vol_dollar    zero1   zero2   zero3   zero4   zero5   zero6   zero7  
    ##[20-29] time    timemilli   bid1    bid1vol bid2    bid2vol bid3    bid3vol bid4    bid4vol 
    ##[30-39] bid5    bid5vol offer1  offer1vol   offer2  offer2vol   offer3  offer3vol   offer4  offer4vol   
    ##[40-42] offer4  offer5vol   l

    ## ====== After processing =====================================================================
    ##   0           1            2     3       4       5       6       7           8       9  
    ## date        time        volrmb  open    high    low     close   avgprice    bid     ask
    ## 20050104    09:31:00    156234  6.98    6.98    6.95    6.95    6.97        6.94    7.01
    import decimal as dm
    dm.getcontext().prec =3
    dm.getcontext().rounding = dm.ROUND_05UP

    data  = np.loadtxt(onedayfile, dtype = 'str')    

    vdate = data[:,0:1]
    vtime = data[:,20:21]       
    vvolrmb = net_value(data[:,12:13])
    vopen       = data[:,8 :9 ]   
    vhigh       = data[:,9 :10]   
    vlow        = data[:,10:11]    
    vclose      = data[:,4 :5 ]  
    vlot        = net_value(data[:,11:12]) 
    vavgprice   = vvolrmb / (vlot)

    slippage    = get_slippage ()
    vbid        = vavgprice* (1 - slippage/100)
    vask        = vavgprice* (1 + slippage/100) 
    result = np.hstack((vdate, vtime, vvolrmb, vopen, vhigh, vlow, vclose,vavgprice,vbid,vask))
    ##print result
    return result


def net_value (darray):
    rows = (darray.shape)[0]
    result = np.zeros(darray.shape)
    result[0][0] =darray[0][0]
    for i in range(1, rows):
        net =  round(float(darray[-1*i][0]) - float(darray[-1*i - 1][0]),1) 
        result[-1*i][0] = net
    return result

def get_slippage ():
    return 0.5

if __name__ == "__main__":
    list_securities()
    #work('../share/stocks_data/SZ000778')
