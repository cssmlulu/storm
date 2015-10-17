"""
run several arguments and sort results
"""
import local
import strategy as st
import backtester as bt

import sys,os,itertools
import pandas as pd
import multiprocessing as mp

import log
logger=log.setlog("optimizer.py")


BASE_DIR = os.path.abspath(os.path.dirname(__file__))

def parameters_generator():
    arg1 = range(48,51)
    arg2 = range(28,31)
    return itertools.product(arg1,arg2)



def tryArgList():
    """
    run several arguments according to arglist file
    """
    logger.info("backtest start.prod=%s",s_prod)
    ### write backtester file header ###
    filepath_backtester =BASE_DIR+ '/backtester/' + s_prod +  '.bt'
    with open(filepath_backtester, 'w') as fout:
        fout.writelines(
            "arg1,arg2,TotalDays,WinDays,LossDays,TotalPnL,AvgDailyPnl,MaxMarkdown,MaxProfit,MaxLoss,SharpeRatio\n")
    
    arglist = parameters_generator()
    local.run(st.MyStrategy, bt.MyBacktest, s_prod, s_prodtype, arglist)


def sortResult():
    """
    sort results
    """
    logger.info("sort backtest result;prod=%s",s_prod)
    filepath_backtester =BASE_DIR+ '/backtester/' + s_prod + '.bt'
    df_backtest = pd.read_csv(filepath_backtester)

    ### sort rules ###
    df_backtest_sorted = df_backtest.sort_index(by=['TotalPnL', 'MaxMarkdown', 'SharpeRatio'],
                                                ascending=[False, True, False])

    ### print sorted result to .rpt(report) file ###
    filepath_report = BASE_DIR+'/backtester/' + s_prod +  '.rpt'
    df_backtest_sorted.to_csv(filepath_report, float_format='%.4f', index=False)
    logger.info("sort done. save to backtest/*.rpt")

    ### delete unsorted .bt file ###
    logger.info("delete unsorted .bt file in backtest/")
    os.remove(filepath_backtester)

def optimizer():
    try:
         if not (s_prod and s_prodtype in ["Future","Stock"]):
              logger.error("incorrect product name or product type")
              return
         tryArgList()
         sortResult()
    except Exception,ex:  
        logger.error( "{0}:	{1}".format(Exception,ex)  )

if __name__ == '__main__':
    s_prod = "SH600008"
    s_prodtype = "Stock"
    optimizer()
