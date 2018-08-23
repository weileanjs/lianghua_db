# -*- coding: utf-8 -*-


import numpy as np
import pandas as pd
import talib


def moveff(bars, timeperiod, col='Close'):
    mov = np.diff(bars[col].values)  # 执行的是后一个元素减去前一个元素
    mov = np.abs(mov)
    mov = np.insert(mov, 0, np.NaN)
    # print(mov[:10])
    movpath = talib.SUM(mov, timeperiod=timeperiod)
    # print(movpath[:10])
    movrange = talib.MAX(bars[col].values, timeperiod=timeperiod) - \
               talib.MIN(bars[col].values, timeperiod=timeperiod)
    # print(movrange[:10])
    moveff_ = movrange / movpath
    moveff_ = pd.Series(moveff_, index=bars.index)
    moveff_.name = bars['code'].iloc[0]
    # print(moveff_)

    return moveff_


def trp(bars):
    tr = talib.TRANGE(bars['High'].values, bars['Low'].values, bars['Close'].values)
    tr = pd.Series(tr, index=bars.index)
    trp_ = tr / bars['Close'].shift(1)
    trp_.name = bars['code'].iloc[0]
    return trp_


def atrp(bars, timeperiod):
    trp_ = trp(bars)
    atrp_ = talib.MA(trp_.values, timeperiod=timeperiod)
    atrp_ = pd.Series(atrp_, index=trp_.index)
    atrp_.name = bars['code'].iloc[0]
    return atrp_





def strthindi(bars, timeperiod):
    rr = bars['Close'].pct_change()
    # tmp0 = rr[rr > 0]
    tmp0 = rr.apply(lambda x: x if x > 0 else 0.0)
    tmp1 = talib.SUM(tmp0.values, timeperiod=timeperiod)
    tmp1 = pd.Series(tmp1, index=tmp0.index)
    tmp2 = talib.SUM(rr.abs().values, timeperiod=timeperiod)
    tmp2 = pd.Series(tmp2, index=rr.index)
    rrsi = tmp1 / tmp2
    rrsi = pd.Series(rrsi, index=rr.index)
    rrsi.name = bars['code'].iloc[0]
    rmean = talib.SUM(rr.values, timeperiod=timeperiod)
    rmean = pd.Series(rmean, index=rr.index)
    rmean.name = bars['code'].iloc[0]
    # print(rrsi)
    # print(type(rrsi))
    return rrsi


def volumestat(bars, timeperiod):
    timeperiod_tmp = int(timeperiod * 0.8)
    tmp0 = talib.MA(bars['Volume'].values, timeperiod=timeperiod_tmp)
    tmp0 = pd.Series(tmp0, index=bars.index)
    tmp0 = tmp0.shift(timeperiod - timeperiod_tmp)
    tmp1 = talib.MA(bars['Volume'].values, timeperiod=timeperiod)
    tmp1 = pd.Series(tmp1, index=bars.index)
    voldiv = tmp0 / tmp1 - 1
    voldiv.name = bars['code'].iloc[0]
    volchg = bars['Volume'].pct_change()
    volchg_mean = talib.MA(volchg.values, timeperiod=timeperiod)
    volchg_mean = pd.Series(volchg_mean, index=volchg.index)
    volchg_mean.name = bars['code'].iloc[0]
    volchg_std = talib.STDDEV(volchg.values, timeperiod=timeperiod)
    volchg_std = pd.Series(volchg_std, index=volchg.index)
    volchg_std.name = bars['code'].iloc[0]
    return voldiv, volchg_mean, volchg_std


# def simatch(bars1, bars2):
#     r1 = bars1['close'] - bars1['open']
#     r2 = bars2['close'] - bars2['open']
#     corr_price = r1.corr(r2)
#     corr_volume = bars1['volume'].corr(bars2['volume'])
#     return corr_price, corr_volume
#
#
# def strthindi(bars):
#     rr = bars['close'].pct_change()
#     rrsi = rr[rr > 0].sum() / rr.abs().sum()
#     rmean = sum(rr.dropna())
#     return rrsi, rmean
#
#
# def volumestat(bars):
#     tmp = int(len(bars['volume']) * 0.8)
#     voldiv = bars['volume'][tmp:].mean() / bars['volume'].mean() - 1
#     volchg = bars['volume'].pct_change()
#     return voldiv, volchg.mean(), volchg.std()

##################################################################################################
def year_relative(data):
    data['last_year'] = data['index'].shift(-4)
    data['pct'] = (data['index'] - data['last_year'])/ abs(data['last_year'])
    data = data[:-4]
    data_r =pd.Series(data['pct'].values, index=data['reportdate'])
    data_r.name = data['code'].iloc[0]
    return data_r
