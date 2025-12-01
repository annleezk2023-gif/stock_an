import pandas as pd
import numpy as np
import backtrader as bt
from dateutil.relativedelta import relativedelta

import os
import sys
from sqlalchemy import text
# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
sys.path.append(root_path + '/bao/season')
import stock_common
sys.path.append(root_path + '/bao')

# 配置logger
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 获取全部股票
def get_all_stocks(conn):
    """获取全部股票"""
    sql = """SELECT * FROM bao_stock_basic order by code asc """
    stock_info_list = conn.execute(sql).fetchall()
    #stock_code_list = [r['code'] for r in results]
    return stock_info_list

def get_all_trade_dates(conn, start_date_str, end_date_str):
    """获取全部交易日"""
    sql = f"""SELECT calendar_date FROM bao_trade_date WHERE is_trading_day=1 and calendar_date >= '{start_date_str}' and calendar_date <= '{end_date_str}' order by calendar_date asc"""
    results = conn.execute(sql).fetchall()
    trade_date_list = [r['calendar_date'] for r in results]
    return trade_date_list

def get_all_trade_strategy_records(conn, strategy_code):
    """获取交易策略存档"""
    sql = f"""SELECT * FROM trade_strategy_record WHERE strategy_code = '{strategy_code}' order by hold_date asc"""
    results = conn.execute(sql).fetchall()
    trade_strategy_list = []
    for r in results:
        trade_strategy_record = StockDailyRecord(r['hold_date'], r['total_cost'], r['total_price'], r['total_commission'], r['stock_hold_list'], r['stock_trade_list'])
        # 解析stock_hold_list和stock_trade_list
        stock_trade_list = json.loads(r['stock_trade_list'])
        stock_hold_list = json.loads(r['stock_hold_list'])
        # 解析stock_hold_list中的stock_trade_list
        for stock_hold in stock_hold_list:
            stock_hold['stock_trade_list'] = json.loads(stock_hold['stock_trade_list'])
        
        trade_strategy_record.stock_trade_list = stock_trade_list
        trade_strategy_record.stock_hold_list = stock_hold_list
        trade_strategy_list.append(trade_strategy_record)
    return trade_strategy_list

# 获取分红标签，tags_type=1季度，2分红
def get_stock_tags_by_trade_date(conn, stock_code, trade_date, tags_type=2):
    """获取最近一条的分红标签，当天之前，最近的一条，不包含当天，以公告日期为准"""
    trade_date_str = trade_date.strftime('%Y-%m-%d')
    month_ago = None
    if tags_type == 1: #季度标签，数据要在6个月之内
        month_ago = trade_date - relativedelta(months=6)
    elif tags_type == 2: #分红标签，数据要在18个月之内
        month_ago = trade_date - relativedelta(months=18)
    month_ago_str = month_ago.strftime('%Y-%m-%d')

    # 不包含当天
    sql = f"""SELECT * FROM stock_auto_tags WHERE code = '{stock_code}' and pubDate < '{trade_date_str}' and pubDate >= '{month_ago_str}' 
            and tags_type = {tags_type} order by pubDate desc limit 1"""
    result = conn.execute(sql).fetchone()
    if result is None:
        return None
    return result

#获取当天全部的股票交易数据
def get_k_line_by_date(conn, trade_date_str):
    sql = ""
    for divide_table_num in range(0, 10, 1):
        sql_sub = f""" SELECT * from bao_stock_trade_{divide_table_num} where date = '{trade_date_str}' """
        if divide_table_num == 9:
            sql = sql + sql_sub
        else:
            sql = sql + sql_sub + " UNION "

    results = conn.execute(sql).fetchall()
    #转map
    trade_info_map = {}
    for r in results:
        trade_info_map[r['code']] = r
    return trade_info_map