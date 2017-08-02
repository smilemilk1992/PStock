# _*_ coding: utf-8 _*_

"""
测试代码
"""

import os
import pandas
import logging
import datetime
import tushare as ts

# 全局变量设置
data_path = "./data/"
data_path_day = data_path + "day_data/"
data_path_week = data_path + "week_data/"
data_path_month = data_path + "month_data/"


# 全局函数设置
def code_to_6len(x):
    return "%06d" % int(x)


def get_data(_file_name, _func_name):
    """
    获取一般数据
    """
    if os.path.exists(_file_name):
        # 索引为0, 1, 2, ......
        temp_stocks = pandas.read_csv(_file_name, index_col=0, encoding="gbk")
        temp_stocks["code"] = temp_stocks["code"].astype("object").map(code_to_6len)
        logging.warning("get %s from disk done" % _file_name[len(data_path):-4])
    else:
        temp_stocks = _func_name()
        temp_stocks.to_csv(_file_name, encoding="gbk")
        logging.warning("get %s from web done" % _file_name[len(data_path):-4])
    return temp_stocks


# 获取全部股票列表：'code', ['name', 'industry', 'area', 'pe', 'outstanding', 'totals', 'totalAssets', 'liquidAssets', 'fixedAssets',
# 'reserved', 'reservedPerShare', 'esp', 'bvps', 'pb', 'timeToMarket', 'undp', 'perundp', 'rev', 'profit', 'gpr', 'npr', 'holders']
file_name = data_path + "all_stocks.csv"
if os.path.exists(file_name):
    all_stocks = pandas.read_csv(file_name, index_col="code", encoding="gbk")
    all_stocks.index = all_stocks.index.astype("object").map(code_to_6len)
    logging.warning("get all_stocks from disk done")
else:
    all_stocks = ts.get_stock_basics()
    all_stocks.to_csv(file_name, encoding="gbk")
    logging.warning("get all_stocks from web done")

# 获取股票分类数据 -- 行业分类：'index', ['code', 'name', 'c_name']
industry_stocks = get_data(data_path + "industry_stocks.csv", ts.get_industry_classified)

# 获取股票分类数据 -- 地区分类：'index', ['code', 'name', 'area']
area_stocks = get_data(data_path + "area_stocks.csv", ts.get_area_classified)

# 获取股票分类数据 -- 概念分类：'index', ['code', 'name', 'c_name']
concept_stocks = get_data(data_path + "concept_stocks.csv", ts.get_concept_classified)

# 获取股票分类数据 -- 中小板分类（002开头）：'index', ['code', 'name']
sme_stocks = get_data(data_path + "sme_stocks.csv", ts.get_sme_classified)

# 获取股票分类数据 -- 创业板分类（300开头）：'index', ['code', 'name']
gem_stocks = get_data(data_path + "gem_stocks.csv", ts.get_gem_classified)

# 获取股票分类数据 -- 风险提示类（st开头）：'index', ['code', 'name']
st_stocks = get_data(data_path + "st_stocks.csv", ts.get_st_classified)

# 数据groupby合并
industry_group = industry_stocks["c_name"].groupby(industry_stocks["code"]).apply(",".join)
area_group = area_stocks["area"].groupby(area_stocks["code"]).apply(",".join)
concept_group = concept_stocks["c_name"].groupby(concept_stocks["code"]).apply(",".join)

sme_stocks["is_sme"] = "1"
sme_group = sme_stocks["is_sme"].groupby(sme_stocks["code"]).apply(",".join)
gem_stocks["is_gem"] = "1"
gem_group = gem_stocks["is_gem"].groupby(gem_stocks["code"]).apply(",".join)
st_stocks["is_st"] = "1"
st_group = st_stocks["is_st"].groupby(st_stocks["code"]).apply(",".join)
logging.warning("stocks groupby done")

# 组成大宽表
big_table = all_stocks.join(industry_group, rsuffix="_g").join(area_group, rsuffix="_g").join(concept_group, rsuffix="_g").join(sme_group).join(gem_group).join(st_group)
big_table.to_csv(data_path + "big_table.csv", encoding="gbk")
logging.warning("stocks join done")


def get_stock_data(_code, _end_date=datetime.datetime.now(), _ktype="D", _data_path=data_path_day):
    """
    获取个股数据
    """
    temp_file_name = _data_path + _code + ".csv"
    temp_time_market = all_stocks.ix[_code]["timeToMarket"]

    temp_data = None
    if os.path.exists(temp_file_name):
        temp_data = pandas.read_csv(temp_file_name, index_col=0, encoding="gbk")
        temp_data["code"] = temp_data["code"].astype("object").map(code_to_6len)
    elif temp_time_market > 10000000:
        temp_time_market = datetime.datetime.strptime(str(temp_time_market), "%Y%m%d")
        if temp_time_market < (_end_date - datetime.timedelta(days=90)):
            # 过滤最近上市的股票
            _start_day = (_end_date - datetime.timedelta(days=1080)) if temp_time_market < (_end_date - datetime.timedelta(days=1080)) else temp_time_market
            temp_data = ts.get_k_data(_code, start=_start_day.strftime("%Y-%m-%d"), ktype=_ktype)
            temp_data.to_csv(temp_file_name, encoding="gbk")
    logging.warning("get stock data: %s", temp_file_name)
    return temp_data


stock_data = {}
for code in all_stocks.index:
    stock_data[code] = get_stock_data(code)
    print(len(stock_data))
