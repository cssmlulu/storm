import datetime as dt
import dateutil
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os, glob
import multiprocessing as mp

import log
logger = log.setlog("ts.py")



freqlist = ['5min','15min','1H','1D']


rights = pd.DataFrame()
def convert_ts():
    try:
        # process security simultanously using multiprocess
        filepath = './datas/Stock'
        seclist = glob.glob(filepath+'/*_pd')
        logger.info("pd file lists:%s",seclist)
        arglist=[]
        for f in seclist:
            for freq in freqlist:
                arglist.append((f,freq))
        po = mp.Pool (mp.cpu_count())
        teams = po.map(work, arglist)
        po.close()
        po.join()
    except Exception,ex:  
        logger.error( "{0}: {1}".format(Exception,ex)  )

def work (f_freq):
    ## 0                    1       2       3       4       5       6           7       8
    ## date                 volrmb  open    high    low     close   avgprice    bid     ask
    ## 20050104 09:31:00    156234  6.98    6.98    6.95    6.95    6.97        6.94    7.01
    filepath,freq=f_freq
    secname     = filepath.split('/')[-1].split('_')[0]
    outpath     = './ts/Stock/'+secname +'/'+secname+'_'+freq+'.ts'
    logger.info("work(%s,%s) start",secname,freq)
    #outpathzip  = './ts/'+secname +'_'+freq +'.gz'
    data =  pd.read_csv(filepath, parse_dates=[0], index_col=0)
    v = data [[0]]
    c = data [[4]]
    avgp = data [[5,6,7]]
    rv      = v.resample(freq, how = 'sum').dropna()
    ro      = c.resample(freq, how = 'first').dropna()
    rh      = c.resample(freq, how = 'max').dropna()
    rl      = c.resample(freq, how = 'min').dropna()
    rc      = c.resample(freq, how = 'last').dropna()
    ravgp   = avgp.resample(freq, how = 'mean').dropna()
    ro.columns=['open']
    rh.columns=['high']
    rl.columns=['low']
    result = pd.concat ([rv, ro, rh, rl, rc, ravgp], axis=1)

    ## print to files
    if not os.path.exists("./ts/Stock/"+secname):
        os.mkdir("./ts/Stock/"+secname)
    result.to_csv(outpath, float_format ='%.3f', compression='gzip')
    #os.system('gzip -c '+ outpath+ ' > '+ outpathzip)
    #os.system('rm -f '+outpath)
    logger.info("work(%s,%s) done",secname,freq)

if __name__ == "__main__":
    convert_ts()


