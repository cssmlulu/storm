
import pandas as pd
import glob,os,sys
import multiprocessing as mp
import indicator as ic

import log
logger = log.setlog("gencator.py")

stock_freq = ['5min','15min','1H','1D']
future_freq = ['4s','15s','1min']

def generate_all():
    try:
        if s_prodtype not in ["Future","Stock"]:
            logger.error("incorrect product type")
            return

        ## grab all prods under ts directory
        prodlist = glob.glob('./ts/'+s_prodtype+'/*')
        prodnamelist = []
        for prod in prodlist:
            prodname = prod.split('/')[-1]
            prodnamelist.append(prodname)

        if len(prodnamelist)==0:
            logger.warning("no prod to generate")
            return
        else:
            logger.info("{} prods to generate indicator (.ics)".format(len(prodnamelist)))

        po = mp.Pool(mp.cpu_count())
        po.map(generate_onesecurity, prodnamelist)
        po.close()
        po.join()
        logger.info("generate indicator (.ics) done")
    except Exception,ex:
        logger.error( "{0}:	{1}".format(Exception,ex)  )


def generate_onesecurity(s_prod):
    ## Input:  give one prodeuct IF, given the specific contract IF.hot
    ## Output: a list of time series for this contract
    s_dirpath = './ts/'+s_prodtype
    s_filepath = s_dirpath +'/' + s_prod + '/'

    ## glob timeseries files
    ## ['./ts/IF/IF_4s.ts',
    ##  './ts/IF/IF_15s.ts'
    ## ]

    if s_prodtype == "Stock":
        ls_freq = stock_freq
    elif s_prodtype == "Future":
        ls_freq = future_freq

    ls_filename =[]
    for freq in ls_freq:
        ls_filename += glob.glob(''.join([s_filepath, s_prod, '_', freq, '.ts']))

    ## print info
    logger.info("{0} timeserie file lists to convert into indicator collections: {1}".format(len(ls_filename),ls_filename))

    #mkdir
    s_outdir = './ics/'+s_prodtype + '/' +s_prod+'/'
    if not os.path.exists(s_outdir):
        os.mkdir(s_outdir)

    for filename in ls_filename:
        generate_onesecurity_onefreq(filename)



def generate_onesecurity_onefreq(filepath):
    ##########################################################################
    # Input filepath = './ts/Future/IF/IF_15s.ts'
    ##########################################################################
    #  datestimes,          volrmb,         open,       high,       low,
    #  2014-12-09 09:15:00, 880059660.000,  3306.800,   3307.200,   3306.800,
    #           close,      avgprice,   bid,        ask
    #           3307.200,   3307.331,   3306.900,   3308.100
    #
    ##########################################################################
    # Output s_outpath ='./ics/Future/IF/IF_15s.ics'
    # datestimes,	typ,	ema50,	ema27,	rsi14,	cci10,	tr,	kpi
    # 2005-01-04,	0.2367,	0.2360,	0.2360,	0.5000,	0.0000,	0.0420,	0.2360
    #
    s_prod = filepath.split('/')[-1].split('_')[0]
    s_freq = filepath.split('_')[-1].split('.')[0]

    s_outdir = './ics/'+s_prodtype + '/' +s_prod+'/'
    s_outpath = s_outdir + s_prod + '_' + s_freq + '.ics'
    logger.info("generate_onesecurity_onefreq(%s,%s) start",s_prod,s_freq)

    df_prices = pd.read_csv(filepath, parse_dates=[0], index_col=0)
    df_c = df_prices['close']
    df_h = df_prices['high']
    df_l = df_prices['low']

    typ = ic.typ(df_h , df_l , df_c)
    ema50 = ic.ema(df_c, 50)
    ema27 = ic.ema(df_c, 27)
    rsi = ic.rsi(df_c)
    cci10 = ic.cci(typ ,df_c, 10)
    tr = ic.tr(df_h,df_l,df_c)
    kpi = ic.kpi(df_c)

    ics = pd.concat([typ, ema50, ema27 ,rsi, cci10, tr, kpi], axis=1)
    ics.to_csv(s_outpath, float_format='%.4f')

    logger.info("generate_onesecurity_onefreq(%s,%s) done",s_prod,s_freq)


if __name__ == "__main__":
    ## set product type: "Future" or "Stock"
    s_prodtype="Stock"
    try:
        #generate_onesecurity_onefreq('./ts/Stock/SH600000/SH600000_1D.ts')
        generate_all()
    except KeyboardInterrupt:
        logger.warning('process killed by user')
        sys.exit(-1)
