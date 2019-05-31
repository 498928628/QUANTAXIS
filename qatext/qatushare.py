from QUANTAXIS.QAFetch.QAQuery import QA_fetch_stock_list
#from QUANTAXIS.QASU import crawl_eastmoney as crawl_eastmoney_file
from QUANTAXIS.QASU import save_tdx as stdx
from QUANTAXIS.QASU import save_tdx_parallelism as stdx_parallelism
from QUANTAXIS.QASU import save_tdx_file as tdx_file
from QUANTAXIS.QASU import save_gm as sgm
from QUANTAXIS.QASU import save_jq as sjq
from QUANTAXIS.QASU import save_tushare as sts
from QUANTAXIS.QASU import save_financialfiles
from QUANTAXIS.QAUtil import DATABASE
# from QUANTAXIS.QASU import crawl_jrj_financial_reportdate as save_financial_calendar
# from QUANTAXIS.QASU import crawl_jrj_stock_divyield as save_stock_divyield
import datetime
import json
import re
import time
import pymongo
import datetime
import tushare as ts
import copy
import pandas as pd
from QUANTAXIS.QAFetch.QATushare import (
    QA_fetch_get_stock_day,
    QA_fetch_get_stock_info,
    QA_fetch_get_stock_list,
    QA_fetch_get_trade_date,
    QA_fetch_get_lhb
)
from QUANTAXIS.QAUtil import (
    QA_util_date_stamp,
    QA_util_log_info,
    QA_util_time_stamp,
    QA_util_to_json_from_pandas,
    trade_date_sse,
    QA_util_get_real_date,
    QA_util_get_next_day
)
from QUANTAXIS.QAUtil.QASetting import DATABASE
import collections

import tushare as QATs

token='86175c6d96e732be3ef4c0d25ed6e6ba436cabee3fd2f4ce9b69d6ad'  #你的tusharepro token
pro=ts.pro_api(token)

def date2str(date):
    return str(date).replace('-','')

def open_trade_date(date):
    pass

#无初始时间,接口的次数单次限制的连续时间
def time_split(n,stime):
    '''
    :param n: 接口限制次数
    :param stime: 初始的str时间 '2015-01-01'
    :return:
    '''
    delta = datetime.timedelta(n)
    end_step = datetime.datetime.strptime(stime,'%Y-%m-%d').date()
    time_dict = {}
    while end_step < datetime.datetime.now().date():
        start_step = copy.deepcopy(end_step)
        end_step += delta
        time_dict.update({date2str(start_step):date2str(end_step)})
    time_dict.update({date2str(end_step):date2str(datetime.datetime.today().date())})
    for k,v in time_dict.items():
        yield k,v

# print(parse_ymd('2018-12-20'))

def QA_SU_save_stock_info_tushare(engine="tushare", client=DATABASE):
    '''

    :param engine: tushare
    :param client:
    :return: None
    '''

    # only support the tushare
    engine = select_save_engine("tushare")
    engine.QA_SU_save_stock_info_tushare()

def QA_SU_save_north_money_tushare(client=DATABASE):
    '''

    :param engine: tushare
    :param client:
    :return: None
    '''

    # only support the tushare
    engine = select_save_engine("tushare")
    QA_SU_save_north_money_tushare_action()

#区别save_day和save_all
def QA_SU_save_north_money_tushare_action(client=DATABASE):
    '''
        获取 北向资金，包含如下信息

        trade_date,交易日期
        ggt_ss,港股通（上海）
        ggt_sz,港股通（深圳）
        hgt,沪股通（百万元）
        sgt,深股通（百万元）


    在命令行工具 quantaxis 中输入 save north_money_tushare 中的命令
    :param client:
    :return:
    '''

    #数量是300条,insert是重新插入
    #开头末尾通过交易日历的方式比较麻烦
    i = 0
    for s,e in time_split(250, '2015-01-01'):
        print(s,e)
        i += 1
        if i == 1:
            df = pro.moneyflow_hsgt(start_date=s, end_date=e)
        else:
            df = df.append(pro.moneyflow_hsgt(start_date=s, end_date=e))
            print(len(df))
    #排除trade_date去重,这样去重的问题是可能出现有两天完全一样的
    #最好还是用交易日历,排序后取交易日历里的天数
    df = df.drop_duplicates(subset = ['ggt_ss','ggt_sz','hgt','sgt','north_money','south_money'],keep='first')
    coll = client.north_money_tushare
    client.drop_collection(coll)
    json_data = json.loads(df.reset_index().to_json(orient='records'))
    coll.insert_many(json_data)
    print(" Save data to north_money_tushare collection， OK")


def select_save_engine(engine, paralleled=False):
    '''
    select save_engine , tushare ts Tushare 使用 Tushare 免费数据接口， tdx 使用通达信数据接口
    :param engine: 字符串Str
    :param paralleled: 是否并行处理；默认为False
    :return: sts means save_tushare_py  or stdx means save_tdx_py
    '''
    if engine in ['tushare', 'ts', 'Tushare']:
        return sts
    elif engine in ['tdx']:
        if paralleled:
            return stdx_parallelism
        else:
            return stdx
    elif engine in ['gm', 'goldenminer']:
        return sgm
    elif engine in ['jq', 'joinquant']:
        return sjq
    else:
        print('QA Error QASU.main.py call select_save_engine with parameter %s is None of  thshare, ts, Thshare, or tdx', engine)
#QA_util_get_real_date
#将无效时间的QADate_trade.py改为从mongodb中获取

if __name__ == '__main__':
    QA_SU_save_north_money_tushare( client=DATABASE)
    # time_split(250,'2015-01-01')
