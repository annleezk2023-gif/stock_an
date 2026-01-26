import tushare as ts
import sys
import os
from sqlalchemy import text
import pandas as pd
from datetime import date, datetime, timedelta

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
sys.path.append(root_path + '/tushare/day')
import stock_common
import tu_common
import zhishu

# 配置logger
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def _insert_index_k(conn, df):
    if df.empty:
        return
        
    for index, row in df.iterrows():
        ts_code = tu_common.convert_stock_code_2bao(row['ts_code'])
        trade_date_formatted = None
        if pd.notna(row['trade_date']) and row['trade_date'] != '':
            trade_date_formatted = f"{row['trade_date'][:4]}-{row['trade_date'][4:6]}-{row['trade_date'][6:8]}"
        
        #增加查询，如果数据库数据己存在就跳过
        query = """
            SELECT COUNT(*) FROM tu_index_daily
            WHERE ts_code = :ts_code AND trade_date = :trade_date
        """
        result = conn.execute(text(query), {
            'ts_code': ts_code,
            'trade_date': trade_date_formatted
        }).fetchone()
        if result[0] > 0:
            logger.info(f"数据己存在: {ts_code} {trade_date_formatted}")
            continue

        sql = """
            INSERT INTO tu_index_daily (ts_code, trade_date, open, high, low, close, pre_close, `change`, pct_chg, vol, amount)
            VALUES (:ts_code, :trade_date, :open, :high, :low, :close, :pre_close, :change, :pct_chg, :vol, :amount)
        """
        
        conn.execute(text(sql), {
            'ts_code': ts_code,
            'trade_date': trade_date_formatted,
            'open': row.get('open'),
            'high': row.get('high'),
            'low': row.get('low'),
            'close': row.get('close'),
            'pre_close': row.get('pre_close'),
            'change': row.get('change'),
            'pct_chg': row.get('pct_chg'),
            'vol': row.get('vol'),
            'amount': row.get('amount')
        })
    conn.commit()

def index_k_all(conn, pro):
    zhishu_list = zhishu.get_index_codes()
    zhishu_codes = list(zhishu_list.keys())
    
    if not zhishu_codes:
        logger.info("没有获取到指数数据")
        return
    
    start_date = datetime(2005, 1, 1)
    end_date = datetime.now()
    current_date = start_date
    
    while current_date <= end_date:
        next_date = current_date + timedelta(days=20*365)
        if next_date > end_date:
            next_date = end_date
        
        start_date_str = current_date.strftime('%Y%m%d')
        end_date_str = next_date.strftime('%Y%m%d')
        
        logger.info(f"开始处理日期段: {start_date_str} 到 {end_date_str}")
        
        for code in zhishu_codes:
            logger.info(f"开始处理指数: {code} {zhishu_list[code]}, 日期段: {start_date_str} 到 {end_date_str}")

            df_daily = pro.index_daily(ts_code=tu_common.convert_stock_code_2tu(code), start_date=start_date_str, end_date=end_date_str)
            if not df_daily.empty:
                _insert_index_k(conn, df_daily)
                logger.info(f"index_k 写入成功: {code} {zhishu_list[code]}, 记录数: {len(df_daily)}")
            else:
                logger.info(f"index_k 无数据: {code} {zhishu_list[code]}, 日期段: {start_date_str} 到 {end_date_str}")
            
            conn.commit()
        
        current_date = next_date + timedelta(days=1)

def index_k_increase(conn, pro):
    zhishu_list = zhishu.get_index_codes()
    zhishu_codes = list(zhishu_list.keys())
    
    if not zhishu_codes:
        logger.info("没有获取到指数数据")
        return

    end_date_str = datetime.now().strftime('%Y%m%d')
    
    for code in zhishu_codes:
        #取己保存数据的日期最大值
        query_max_date = "SELECT MAX(trade_date) FROM tu_index_daily WHERE ts_code = :ts_code"
        max_date_result = conn.execute(text(query_max_date), {'ts_code': code}).fetchone()
        max_date = max_date_result[0] if max_date_result[0] else datetime(2007, 1, 1)
        start_date = max_date + timedelta(days=1)
        start_date_str = start_date.strftime('%Y%m%d')

        #end_date必须大于start_date
        if start_date_str >= end_date_str:
            logger.info(f"指数 {code} {zhishu_list[code]} 已更新到最新, 无需增量更新")
            continue

        logger.info(f"开始处理指数: {code}, 日期段: {start_date_str} 到 {end_date_str}")
        
        df_daily = pro.index_daily(ts_code=tu_common.convert_stock_code_2tu(code), start_date=start_date_str, end_date=end_date_str)
        if not df_daily.empty:
            _insert_index_k(conn, df_daily)
            logger.info(f"index_daily 写入成功: {code}, 记录数: {len(df_daily)}")
        
        conn.commit()

if __name__ == "__main__":
    pro = tu_common.get_tushare_pro()

    conn = stock_common.get_db_conn(sql_echo=False)

    #再获取日K线数据
    #index_k_all(conn, pro)
    
    #增量更新
    index_k_increase(conn, pro)

    conn.close()
