# -*- coding: utf-8 -*-
from dbdate import get_finance_element2
import numpy as np
import pandas as pd
import multiprocessing as multi
from _signals import year_relative

#  经营现金流净额同比
def mananetr_relative(begt, endt ):
    col = 'mananetr'
    tbn = 'cash_flow_sheet'
    bars_all = get_finance_element2(begt, endt, tbn ,col)
    bars_all = bars_all.rename(columns={col :'index'}, inplace = True)
    relative_list = []
    pool = multi.Pool(processes=3)
    for ehcode in bars_all['code'].unique():
        bars = bars_all[bars_all['code'] == ehcode].sort_values(by='reportdate',ascending=False)
        pool.apply_async(year_relative,args=(bars,), callback=relative_list.append, error_callback=print)
    pool.close()
    pool.join()
    relative = pd.concat(relative_list, axis=1)
    relative.replace([-np.nan, np.nan, np.inf], 0, inplace=True)   # 替换nan和inf为0   np.inf 无穷大正数
    # relative.to_csv('xjl.csv')
    # print(relative)
    return relative

# 投资现金流量净额同比
def invnetcashflow_relative(begt, endt ):
    col = 'invnetcashflow'
    tbn = 'cash_flow_sheet'
    bars_all = get_finance_element2(begt, endt, tbn ,col)
    bars_all = bars_all.rename(columns={col:'index'}, inplace = True)
    relative_list = []
    pool = multi.Pool(processes=3)
    for ehcode in bars_all['code'].unique():
        bars = bars_all[bars_all['code'] == ehcode].sort_values(by='reportdate',ascending=False)
        pool.apply_async(year_relative,args=(bars,), callback=relative_list.append, error_callback=print)
    pool.close()
    pool.join()
    relative = pd.concat(relative_list, axis=1)
    relative.replace([-np.nan, np.nan, np.inf], 0, inplace=True)   # 替换nan和inf为0   np.inf 无穷大正数
    # relative.to_csv('xjl.csv')
    # print(relative)
    return relative

# 筹资现金流量净额同比
def finnetcflow_relative(begt, endt ):
    col = 'finnetcflow'
    tbn = 'cash_flow_sheet'
    bars_all = get_finance_element2(begt, endt, tbn ,col)
    bars_all = bars_all.rename(columns={col:'index'}, inplace = True)
    relative_list = []
    pool = multi.Pool(processes=3)
    for ehcode in bars_all['code'].unique():
        bars = bars_all[bars_all['code'] == ehcode].sort_values(by='reportdate',ascending=False)
        pool.apply_async(year_relative,args=(bars,), callback=relative_list.append, error_callback=print)
    pool.close()
    pool.join()
    relative = pd.concat(relative_list, axis=1)
    relative.replace([-np.nan, np.nan, np.inf], 0, inplace=True)   # 替换nan和inf为0   np.inf 无穷大正数
    # relative.to_csv('xjl.csv')
    # print(relative)
    return relative

# 负债率
def debt_ratio(begt, endt):
    col = 'debt_ratio'
    col_1 = 'totalliab'
    col_2 = 'totalassets'
    tbn = 'finmain_sheet'
    bars_all = None
    bars_all_1 = get_finance_element2(begt, endt, tbn ,col_1)
    bars_all_2 = get_finance_element2(begt, endt, tbn ,col_2)
    bars_all_2.pop('code')
    bars_all_2.pop('reportdate')
    if bars_all_1.shape[0] == bars_all_2.shape[0]:
        bars_all = pd.concat([bars_all_1, bars_all_2], axis=1, sort=False)
        bars_all[col] = bars_all['totalliab']/bars_all['totalassets']
        bars_all.pop('totalliab')
        bars_all.pop('totalassets')
        if bars_all.shape[0]:
            bars_all['reportdate'] = pd.to_datetime(bars_all['reportdate'])
            bars_all = bars_all.pivot(index='reportdate', columns='code', values=col)
            print(bars_all)
    return bars_all

    # bars_all = bars_all.rename(columns={col:'index'}, inplace = True)
    # relative_list = []
    # pool = multi.Pool(processes=3)
    # for ehcode in bars_all['code'].unique():
    #     bars = bars_all[bars_all['code'] == ehcode].sort_values(by='reportdate',ascending=False)
    #     pool.apply_async(year_relative,args=(bars,), callback=relative_list.append, error_callback=print)
    # pool.close()
    # pool.join()
    # relative = pd.concat(relative_list, axis=1)
    # relative.replace([-np.nan, np.nan, np.inf], 0, inplace=True)   # 替换nan和inf为0   np.inf 无穷大正数
    # # relative.to_csv('xjl.csv')
    # # print(relative)
    # return relative

if __name__ == '__main__':
    begt = '20120101'
    endt = '20180819'
    debt_ratio(begt, endt)
