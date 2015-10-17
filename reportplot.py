import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.style.use('ggplot')
df_ts = pd.read_csv('./ts/Stock/SH600000/SH600000_1H.ts',index_col=0,parse_dates=True)
close =df_ts.close

close.plot(figsize=(20,10),legend=True)

df_bl=pd.read_csv('./blotter/Stock/SH600000/SH600000_50_30.bl',index_col=0,parse_dates=True,skiprows=3)
buy = df_bl[df_bl['direction']=='B']['price']
sell= df_bl[df_bl['direction']=='S']['price']
buy.name='buy'
sell.name='sell'
buy.plot(style='g^',legend=True)
sell.plot(style='rv',legend=True)

pnl = df_bl['PNL']
pnl.cumsum().dropna().plot(kind='area',stacked=False,legend=True)

for i in range(len(sell)):
    time = [buy.index[i],sell.index[i]]
    value = [buy[i],sell[i]]
    plt.plot(time,value,'b')


plt.show()
