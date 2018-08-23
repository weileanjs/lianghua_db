# -*- coding: utf-8 -*-
from dbdate import get_table_element ,get_finance_element

# (begt, endt, tbn ,col)


def get_grow_factors(begt, endt):
    '''
    mbgr    mainbusincgrowrate   主营业务收入增长率(%)
    ngr     netincgrowrate       净利润增长率(%)
    nagr    netassgrowrate       净资产增长率(%)
    '''
    mbgr = get_finance_element(begt, endt, 'finmain_sheet', 'mainbusincgrowrate')
    ngr = get_finance_element(begt, endt, 'finmain_sheet', 'netincgrowrate')
    nagr = get_finance_element(begt, endt, 'finmain_sheet', 'netassgrowrate')
    return mbgr, ngr, nagr





def get_value_factors(begt, endt):
    pe = get_table_element(begt, endt, 'Pe')
    pb = get_table_element(begt, endt, 'Pb')
    return pe, pb



def get_profit_factors(begt, endt):
    '''
    row   weightedroe             加权净资产收益率 %
    gpr   salegrossprofitrto      毛利率%
    :param begt:
    :param endt:
    :return:
    '''
    roe = get_finance_element(begt, endt, 'finmain_sheet', 'weightedroe')
    gpr = get_finance_element(begt, endt, 'finmain_sheet', 'salegrossprofitrto')
    # npr = get_finance_element(begt, endt, 'npr', 'stock_basics')
    return roe, gpr




# begt = '20100101'
# endt = '20180819'
# a , b = get_profit_factors(begt, endt)
# print(a.head(10))
# print(b.head(10))

def get_lever_factors(begt, endt):
    '''
    股东权益比率
    :param begt:
    :param endt:
    :return:
    '''

    totsharequi = get_finance_element(begt, endt, 'finmain_sheet', 'totsharequi')  #股东权益
    totalassets = get_finance_element(begt, endt, 'finmain_sheet', 'totalassets')  #资产总额
    sheqratio = totsharequi/totalassets
    return (sheqratio,)

# begt = '20100101'
# endt = '20180819'
# a = get_profit_factors(begt, endt)
# print(a)


# def get_fund_factors(begt, endt, db):
#     nums = get_table_element(begt, endt, 'nums', 'fund_holdings', db)
#     ratio = get_table_element(begt, endt, 'ratio', 'fund_holdings', db)
#     return nums, ratio


def get_ffactors(begt, endt):
    grow = get_grow_factors(begt, endt)
    value = get_value_factors(begt, endt)
    profit = get_profit_factors(begt, endt)
    lever = get_lever_factors(begt, endt)
    print(grow, value, profit, lever)
    return grow, value, profit, lever


# begt = '20170801'
# endt = '20180819'
# get_ffactors(begt, endt)


def get_ifactors(begt, endt, db):
    moveff = signals.get_moveff(begt, endt, 20, db)
    print('get moveff complete')
    atrp = signals.get_atrp(begt, endt, 20, db)
    print('get atrp complete')
    rrsi = signals.get_rrsi(begt, endt, 20, db)
    print('get rrsi complete')
    return moveff, atrp, rrsi


# if __name__ == '__main__':
#     begt = '2018-01-01'
#     endt = '2018-06-01'
#     db = r'..\data\data.db'
#     # print(get_ffactors(begt, endt, db))
#     print(get_ifactors(begt, endt, db))
#     # codes = dbdata.get_codes(begt, endt, 'stock_basics', db)
#     # print(type(codes))
#     # print(signals.get_moveff(codes, begt, begt, endt, 20, db))
