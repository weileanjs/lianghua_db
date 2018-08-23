# -*- coding: utf-8 -*-
import pymysql
import pandas as pd
from config import MYSQL_CONFIG

conn = pymysql.connect(host=MYSQL_CONFIG['host'], port=MYSQL_CONFIG['port'], user=MYSQL_CONFIG['user'],
                             password=MYSQL_CONFIG['password'],db=MYSQL_CONFIG['db'],charset=MYSQL_CONFIG['charset'],
                             cursorclass=pymysql.cursors.DictCursor)

def get_codes(tbn='stock_basics'):
    """
    获取所有股票代码
    :param begt: str,
    :param endt: str,
    :param tbn: str,
    :param db: str,
    :return codes: pandas.DataFrame,    """

    sql = 'SELECT code,timeToMarket FROM %s' % (tbn)
    timetomarket = pd.read_sql(sql, conn)
    if timetomarket.shape[0]:
        # timetomarket['Date'] = pd.to_datetime(timetomarket['Date'])
        codes = timetomarket['code'].tolist()
        return codes

#获取单个日线数据 ， col !=None,单项数据
def get_bars(begt, endt, code ,col = None):
    if code :
        sql = "SELECT `Date`, `Open`, `High`, `Low`, `Close`, `Volume` , `code` , B.adj_factor ,Pb ,Pe FROM  (Date_K A " \
              "LEFT JOIN Date_Adj_Factor B  ON A.id = B.datek_id) LEFT JOIN Date_Pe_Pb C ON A.id = C.Datek_id WHERE A.`code`='{}' AND  A.date>='{}' AND A.date<='{}';".format(code,begt, endt)
        # print(sql)
        bars = pd.read_sql(sql, conn)
        last_adj_factor = float(bars.iloc[-1]['adj_factor'])
        bars['Aadj_factor'] = bars['adj_factor']/last_adj_factor
        bars['Adj_Close'] = bars['Aadj_factor']*bars['Close']
        bars.pop('adj_factor')
        bars.pop('Aadj_factor')
        if bars.shape[0]:
            bars['Date'] = pd.to_datetime(bars['Date'])
            bars.set_index('Date', inplace=True)
    else:
        sql = "SELECT `Date`, `code` , {0}  FROM  (Date_K A LEFT JOIN Date_Adj_Factor B  ON A.id = B.datek_id) " \
              "LEFT JOIN Date_Pe_Pb C ON A.id = C.Datek_id WHERE  A.date>='{1}' AND A.date<='{2}';".format(col ,begt, endt)
        bars = pd.read_sql(sql, conn)
    return bars

#获取所有股票日线数据
def get_bars2(begt, endt):
    sql = "SELECT `Date`, `Open`, `High`, `Low`, `Close`, `Volume` , `code` , B.adj_factor ,Pb ,Pe FROM  (Date_K A " \
          "LEFT JOIN Date_Adj_Factor B  ON A.id = B.datek_id) LEFT JOIN Date_Pe_Pb C ON A.id = C.Datek_id WHERE  A.date>='{}' AND A.date<='{}';".format(begt, endt)
    print(sql)
    bars = pd.read_sql(sql, conn)
    last_adj_factor = float(bars.iloc[-1]['adj_factor'])
    bars['Aadj_factor'] = bars['adj_factor']/last_adj_factor
    bars['Adj_Close'] = bars['Aadj_factor']*bars['Close']
    bars.pop('adj_factor')
    bars.pop('Aadj_factor')
    bars['Close'] = bars['Adj_Close']
    bars.pop('Adj_Close')
    if bars.shape[0]:
        bars['Date'] = pd.to_datetime(bars['Date'])
        bars.set_index('Date', inplace=True)
    return bars

#获取所有股票Adj_Close的值
def get_adjclose_bars(begt, endt):
    for code in get_codes()[:100]:
        df1 = get_bars(begt, endt ,code)
        try:
            frames = [data, df1]
            data = pd.concat(frames)
        except:
            data = df1
    return data

# 获取所有股票某一元素的值
def get_table_element(begt, endt, col):
    if col == 'Adj_Close':
        data = get_adjclose_bars(begt, endt)
    else:
        data = get_bars(begt, endt, col = col ,code = 0)
    data.reset_index(inplace=True)
    data = data[['code','Date',col]]
    if data.shape[0]:
        data['Date'] = pd.to_datetime(data['Date'])
        data = data.pivot(index='Date', columns='code', values=col)
    return data

#获取每个交易日距离上市日的天数
def get_days2market(begt, endt):
    # 获取所有个股
    sql = "SELECT `Date`, `code`  FROM  Date_K  WHERE  date>='{0}' AND date<='{1}'AND `code` NOT LIKE '15%' AND `code` NOT LIKE '5%' ;".format(begt, endt)
    bars = pd.read_sql(sql, conn)
    df_copy = bars[['code']]
    df_copy = df_copy.rename(columns={'code':'timeToMarket'})

    # 获取个股上市日期
    sql = "SELECT code,listing_date FROM stock_init_info"
    code2market = pd.read_sql(sql,conn)
    code2market_series = pd.Series(code2market['listing_date'].values, index=code2market['code'])
    t = code2market_series.apply(str).str.replace(' 00:00:00','', regex=False)
    d = t.to_dict()
    new_df = df_copy.replace(d)
    result = pd.concat([bars, new_df], axis=1, sort=False)

    #修改date timetomarket 数据为 datetime，转换为透视表，计算上市日期
    if result.shape[0]:
        result['timeToMarket'] = pd.to_datetime(result['timeToMarket'],format="%Y-%m-%d")
        result['Date'] = pd.to_datetime(result['Date'])
        days2market = result.pivot(index='Date', columns='code', values='timeToMarket')
        days2market.fillna(method='ffill', inplace=True)
        days2market.fillna(method='bfill', inplace=True)
        days2market.to_csv('r2.csv')

        # days2market = pd.to_datetime(days2market.stack()).unstack()
        for ehcol in days2market.columns:
            tmp = days2market.index - days2market[ehcol]
            # print(tmp)
            tmp = tmp.apply(lambda x: x.days if pd.notnull(x) else -1)
            days2market[ehcol] = tmp
    return days2market

# 所有股票单项财务指标透视表
def get_finance_element(begt, endt, tbn ,col):
    sql = "SELECT `code` ,`reportdate` ,`{0}` FROM {1}  WHERE   reportdate>='{2}' AND reportdate<='{3}';".format(col , tbn , begt ,endt)
    data = pd.read_sql(sql, conn)
    if data.shape[0]:
        data['reportdate'] = pd.to_datetime(data['reportdate'])
        data = data.pivot(index='reportdate', columns='code', values=col)
    return data

# 所有股票单项财务指标表，同比基数
def get_finance_element2(begt, endt, tbn ,col):
    bbegt = str(int(begt[:4]) -1) + begt[4:]
    sql = "SELECT `code` ,`reportdate` ,`{0}` FROM {1}  WHERE   reportdate>='{2}' AND reportdate<='{3}';".format(col , tbn , bbegt ,endt)
    data = pd.read_sql(sql, conn)
    return data



# begt = '20180101'
# endt = '20180819'
# t = get_days2market(begt, endt)
# print(t)
# t.to_csv('time2market.csv')


