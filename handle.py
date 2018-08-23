# -*- coding: utf-8 -*-
from dbdate import get_finance_element2
import numpy as np
import pandas as pd
import multiprocessing as multi
from _signals import year_relative

def get_chg(close, uplimit=0.11, dnlimt=-0.11, uplimit_val=0.0, dnlimit_val=0.0):
    """
    获取涨幅
    :param close: pandas.DataFrame,
    :param uplimit: int/float,
    :param dnlimit: int/float,
    :param uplimit_val: int/float,
    :param dnlimit_val: int/float,
    :return chg: pandas.DataFrame,
        index = date
        columns = code
    """
    chg = close.pct_change()   # percent 百分比变化
    chg = chg.shift(-1)      # 默认是axis = 0轴的设定，向上移动
    chg.replace([-np.nan, np.nan, np.inf], 0, inplace=True)  # 替换nan和inf为0   np.inf 无穷大正数
    # 替换异常值
    chg[chg > uplimit] = uplimit_val
    chg[chg < dnlimt] = dnlimit_val
    # print(chg['601360'].values)
    return chg


def filt_factor(series, chunksnum=5, dropna=True, dropneg=True):
    """
    因子筛选, 返回筛选结果
    :param series: pandas.Series,
    :param chunksnum: int,
    :param dropna: bool,
    :param dropneg: bool, 是否过滤负数因子
    :return head: pandas.Series,
    :return tail: pandas.Series,
    """
    series_tmp = series.copy()
    series_tmp.sort_values(ascending=True, inplace=True)  # 升序排列
    if dropna:
        series_tmp.dropna(inplace=True)
    if dropneg:
        series_tmp = series_tmp[series_tmp > 0]
    head = series_tmp[:int(len(series_tmp) / chunksnum)]
    tail = series_tmp[-int(len(series_tmp) / chunksnum):]
    return head, tail


def get_relative(bars_all):
    """
    获取同比
    """
    relative_list = []
    pool = multi.Pool(processes=3)
    for ehcode in bars_all['code'].unique():
        bars = bars_all[bars_all['code'] == ehcode].sort_values(by='reportdate',ascending=False)
        pool.apply_async(year_relative,args=(bars,), callback=relative_list.append, error_callback=print)
    pool.close()
    pool.join()
    relative = pd.concat(relative_list, axis=1)
    relative.replace([-np.nan, np.nan, np.inf], 0, inplace=True)   # 替换nan和inf为0   np.inf 无穷大正数
    relative.to_csv('xjl.csv')
    return relative


# if __name__ == '__main__':
#     begt = '20120101'
#     endt = '20180819'
#     col = 'mananetr'
#     code = '600023'
#     tbn = 'cash_flow_sheet'
#     mananetr = get_finance_element2(begt, endt, tbn ,col)
#     # print(mananetr)
#     get_relative(mananetr)



# def get_chg_returns(chg_list):
#     chg_df = pd.DataFrame(chg_list, columns=['date', 'chg'])
#     chg_df.fillna(0, inplace=True)
#     chg_df['returns'] = chg_df['chg'] + 1
#     chg_df['returns'] = chg_df['returns'].cumprod() - 1  # 累乘
#     chg_df['returns'] = chg_df['returns'].shift(1)
#     chg_df.fillna(0, inplace=True)
#     return chg_df
#
#
# def __get_chg_returns2(chg_series):
#     chg_tmp = chg_series.copy()
#     chg_tmp.dropna(inplace=True)
#     return chg_tmp.mean()
#
#
# def get_chg_returns2(chg_list):
#     chg_codes = pd.DataFrame(chg_list)
#     chg_res = chg_codes.apply(__get_chg_returns2, axis=1)
#     # chg_codes.fillna(0, inplace=True)
#     chg_res.fillna(0, inplace=True)
#
#     returns_res = chg_res.copy()
#     returns_res = returns_res + 1
#     returns_res = returns_res.cumprod() - 1  # 累乘
#     returns_res = returns_res.shift(1)
#     returns_res.fillna(0, inplace=True)
#     return chg_codes, chg_res, returns_res
#
#
# def get_winratio_plratio(chg_codes):
#     win_times = 0
#     loss_times = 0
#     win = 0
#     loss = 0
#     for _, ehcolumn in chg_codes.iteritems():
#         index_tmp = []
#         for ehindex, ehele in ehcolumn.iteritems():
#             if pd.notna(ehele):
#                 index_tmp.append(ehindex)
#             else:
#                 if len(index_tmp) > 0:
#                     chg_tmp = ehcolumn[index_tmp]
#                     chg_tmp = chg_tmp.prod()
#                     if chg_tmp > 0:
#                         win_times = win_times + 1
#                         win = win + chg_tmp
#                     else:
#                         loss_times = loss_times + 1
#                         loss = loss + chg_tmp
#     winratio = win_times / loss_times
#     plratio = abs((win / win_times) / (loss / loss_times))
#     return winratio, plratio
