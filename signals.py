# -*- coding: utf-8 -*-

import multiprocessing as multi
import pandas as pd
import _signals
from dbdate import get_bars2


def get_moveff(begt, endt, timeperiod):
    moveff_list = []
    bars_all = get_bars2(begt, endt)
    pool = multi.Pool(processes=3)
    for ehcode in bars_all['code'].unique():
        print(ehcode)
        bars = bars_all[bars_all['code'] == ehcode]
        if bars.shape[0] > timeperiod:
            pool.apply_async(_signals.moveff,args=(bars, timeperiod),callback=moveff_list.append, error_callback=print)
    pool.close()
    pool.join()
    moveff = pd.concat(moveff_list, axis=1)
    moveff = moveff[begt:endt]
    return moveff

def get_atrp(begt, endt, timeperiod):
    atrp_list = []
    bars_all = get_bars2(begt, endt)
    pool = multi.Pool(processes=3)
    for ehcode in bars_all['code'].unique():
        bars = bars_all[bars_all['code'] == ehcode]
        if bars.shape[0] > timeperiod:
            pool.apply_async(_signals.atrp,args=(bars, timeperiod),callback=atrp_list.append,error_callback=print)
    pool.close()
    pool.join()
    atrp = pd.concat(atrp_list, axis=1)
    atrp = atrp[begt:endt]
    return atrp

#

def get_rrsi(begt, endt, timeperiod):
    atrp_list = []
    bars_all = get_bars2(begt, endt)
    pool = multi.Pool(processes=3)
    for ehcode in bars_all['code'].unique():
        bars = bars_all[bars_all['code'] == ehcode]
        if bars.shape[0] > timeperiod:
            pool.apply_async(_signals.strthindi,args=(bars, timeperiod),callback=atrp_list.append,error_callback=print)
    pool.close()
    pool.join()
    atrp = pd.concat(atrp_list, axis=1)
    atrp = atrp[begt:endt]
    return atrp



if __name__ == '__main__':
    begt = '2018-01-01'
    endt = '2018-06-01'
    print(get_rrsi(begt, endt, 20))



# def get_moveff(codes, bbegt, begt, endt, timeperiod, db):
#     """
#     计算moveff
#     bbegt至begt之间的数据为缓冲区, 建议大于timeperiod
#     :param codes:
#     :param bbegt: str,
#     :param begt: str,
#     :param endt: str,
#     :param timeperiod: int,
#     :param db: str,
#     :return moveff: pandas.DataFrame,
#         index = date
#         columns = codes
#     """
#     moveff_list = []
#     for ehcode in codes:
#         bars = readdata.get_bars(ehcode, bbegt, endt, db=db)
#         if bars.shape[0] > timeperiod:
#             moveff_tmp = _signals.moveff(bars, timeperiod)
#             moveff_tmp.name = ehcode
#             moveff_list.append(moveff_tmp)
#     moveff = pd.concat(moveff_list, axis=1)
#     moveff = moveff[begt:endt]
#     return moveff
#
#
# def get_atrp(codes, bbegt, begt, endt, timeperiod, db):
#     """
#     计算atrp
#     bbegt至begt之间的数据为缓冲区, 建议大于timeperiod
#
#     :param codes:
#     :param bbegt: str,
#     :param begt: str,
#     :param endt: str,
#     :param timeperiod: int,
#     :param db: str,
#     :return atrp: pandas.DataFrame,
#         index = date
#         columns = codes
#     """
#     moveff_list = []
#     for ehcode in codes:
#         bars = readdata.get_bars(ehcode, bbegt, endt, db=db)
#         if bars.shape[0] > timeperiod:
#             moveff_tmp = _signals.atrp(bars, timeperiod)
#             moveff_tmp.name = ehcode
#             moveff_list.append(moveff_tmp)
#     atrp = pd.concat(moveff_list, axis=1)
#     atrp = atrp[begt:endt]
#     return atrp
#
#
# def get_strthindi(codes, bbegt, begt, endt, timeperiod, db):
#     """
#     计算strthindi
#     bbegt至begt之间的数据为缓冲区, 建议大于timeperiod
#
#     :param codes:
#     :param bbegt: str,
#     :param begt: str,
#     :param endt: str,
#     :param timeperiod: int,
#     :param db: str,
#     :return atrp: pandas.DataFrame,
#         index = date
#         columns = codes
#     """
#     rrsi_list = []
#     rmean_list = []
#     for ehcode in codes:
#         bars = readdata.get_bars(ehcode, bbegt, endt, db=db)
#         if bars.shape[0] > timeperiod:
#             rrsi_tmp, rmean_tmp = _signals.strthindi(bars, timeperiod)
#             rrsi_tmp.name = ehcode
#             rmean_tmp.name = ehcode
#             rrsi_list.append(rrsi_tmp)
#             rmean_list.append(rmean_tmp)
#     rrsi = pd.concat(rrsi_list, axis=1)
#     rrsi = rrsi[begt:endt]
#     rmean = pd.concat(rmean_list, axis=1)
#     rmean = rmean[begt:endt]
#     return rrsi, rmean
