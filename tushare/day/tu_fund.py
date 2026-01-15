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

# 配置logger
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def etf_basic_all(conn, pro):
    df = pro.etf_basic(fields='ts_code,csname,extname,index_code,index_name,setup_date,list_date,list_status,exchange,mgr_name,mgt_fee,etf_type')
    
    if df.empty:
        logger.info("没有获取到ETF数据")
        return

    for index, row in df.iterrows():
        ts_code = row['ts_code']
        csname = row['csname']
        extname = row['extname']
        index_code = row['index_code']
        index_name = row['index_name']
        setup_date = row['setup_date']
        list_date = row['list_date']
        list_status = row['list_status']
        exchange = row['exchange']
        mgr_name = row['mgr_name']
        mgt_fee = row['mgt_fee']
        etf_type = row['etf_type']
        
        setup_date_formatted = None
        if pd.notna(setup_date) and setup_date != '':
            setup_date_formatted = f"{setup_date[:4]}-{setup_date[4:6]}-{setup_date[6:8]}"
        
        list_date_formatted = None
        if pd.notna(list_date) and list_date != '':
            list_date_formatted = f"{list_date[:4]}-{list_date[4:6]}-{list_date[6:8]}"
        
        mgt_fee_value = float(mgt_fee) if pd.notna(mgt_fee) else None
        
        sql = """
            INSERT INTO tu_etf_basic (ts_code, csname, extname, index_code, index_name, setup_date, list_date, list_status, exchange, mgr_name, mgt_fee, etf_type)
            VALUES (:ts_code, :csname, :extname, :index_code, :index_name, :setup_date, :list_date, :list_status, :exchange, :mgr_name, :mgt_fee, :etf_type)
            ON DUPLICATE KEY UPDATE
                csname = VALUES(csname),
                extname = VALUES(extname),
                index_code = VALUES(index_code),
                index_name = VALUES(index_name),
                setup_date = VALUES(setup_date),
                list_date = VALUES(list_date),
                list_status = VALUES(list_status),
                exchange = VALUES(exchange),
                mgr_name = VALUES(mgr_name),
                mgt_fee = VALUES(mgt_fee),
                etf_type = VALUES(etf_type),
                updated_at = CURRENT_TIMESTAMP
        """
        
        conn.execute(text(sql), {
            'ts_code': ts_code,
            'csname': csname,
            'extname': extname,
            'index_code': index_code,
            'index_name': index_name,
            'setup_date': setup_date_formatted,
            'list_date': list_date_formatted,
            'list_status': list_status,
            'exchange': exchange,
            'mgr_name': mgr_name,
            'mgt_fee': mgt_fee_value,
            'etf_type': etf_type
        })
    
    conn.commit()

def _insert_fund_daily(conn, df):
    if df.empty:
        return
    
    for index, row in df.iterrows():
        trade_date_formatted = None
        if pd.notna(row['trade_date']) and row['trade_date'] != '':
            trade_date_formatted = f"{row['trade_date'][:4]}-{row['trade_date'][4:6]}-{row['trade_date'][6:8]}"
        
        #增加查询，如果数据库数据己存在就跳过
        query = """
            SELECT COUNT(*) FROM tu_fund_daily
            WHERE ts_code = :ts_code AND trade_date = :trade_date
        """
        result = conn.execute(text(query), {
            'ts_code': row.get('ts_code'),
            'trade_date': trade_date_formatted
        }).fetchone()
        if result[0] > 0:
            logger.info(f"数据己存在: {row.get('ts_code')} {trade_date_formatted}")
            continue

        sql = """
            INSERT INTO tu_fund_daily (ts_code, trade_date, open, high, low, close, pre_close, `change`, pct_chg, vol, amount)
            VALUES (:ts_code, :trade_date, :open, :high, :low, :close, :pre_close, :change, :pct_chg, :vol, :amount)
        """
        
        conn.execute(text(sql), {
            'ts_code': row.get('ts_code'),
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

def _update_fund_daily_share_size(conn, df):
    if df.empty:
        return
    
    for index, row in df.iterrows():
        trade_date_formatted = None
        if pd.notna(row['trade_date']) and row['trade_date'] != '':
            trade_date_formatted = f"{row['trade_date'][:4]}-{row['trade_date'][4:6]}-{row['trade_date'][6:8]}"
        
        sql = """
            UPDATE tu_fund_daily
            SET total_share = :total_share,
                total_size = :total_size,
                nav = :nav,
                exchange = :exchange,
                updated_at = CURRENT_TIMESTAMP
            WHERE ts_code = :ts_code AND trade_date = :trade_date
        """
        
        conn.execute(text(sql), {
            'ts_code': row.get('ts_code'),
            'trade_date': trade_date_formatted,
            'total_share': row.get('total_share'),
            'total_size': row.get('total_size'),
            'nav': row.get('nav'),
            'exchange': row.get('exchange')
        })
    

def _update_fund_daily_adj_factor(conn, df):
    if df.empty:
        return
    
    for index, row in df.iterrows():
        trade_date_formatted = None
        if pd.notna(row['trade_date']) and row['trade_date'] != '':
            trade_date_formatted = f"{row['trade_date'][:4]}-{row['trade_date'][4:6]}-{row['trade_date'][6:8]}"
        
        sql = """
            UPDATE tu_fund_daily
            SET adj_factor = :adj_factor,
                updated_at = CURRENT_TIMESTAMP
            WHERE ts_code = :ts_code AND trade_date = :trade_date
        """
        
        conn.execute(text(sql), {
            'ts_code': row.get('ts_code'),
            'trade_date': trade_date_formatted,
            'adj_factor': row.get('adj_factor')
        })
    

def fund_data_all(conn, pro):
    #待上市的数据不取
    query = "SELECT ts_code, list_date FROM tu_etf_basic where list_status <> 'P'"
    results = conn.execute(text(query)).fetchall()
    
    if not results:
        logger.info("没有获取到ETF数据")
        return
    
    start_date = datetime(2005, 1, 1)
    end_date = datetime.now()
    current_date = start_date
    
    while current_date <= end_date:
        next_date = current_date + timedelta(days=5*365)
        if next_date > end_date:
            next_date = end_date
        
        start_date_str = current_date.strftime('%Y%m%d')
        end_date_str = next_date.strftime('%Y%m%d')
        
        logger.info(f"开始处理日期段: {start_date_str} 到 {end_date_str}")
        
        for row in results:
            ts_code = row[0]
            #如果end_date小于list_date，跳过
            if next_date.date() < row[1]:
                continue

            logger.info(f"开始处理ETF: {ts_code} {row[1]}, 日期段: {start_date_str} 到 {end_date_str}")

            df_daily = pro.fund_daily(ts_code=ts_code, start_date=start_date_str, end_date=end_date_str)
            if not df_daily.empty:
                _insert_fund_daily(conn, df_daily)
                logger.info(f"fund_daily 写入成功: {ts_code} {row[1]}, 记录数: {len(df_daily)}")
            else:
                logger.info(f"fund_daily 无数据: {ts_code} {row[1]}, 日期段: {start_date_str} 到 {end_date_str}")
            
            df_share = pro.etf_share_size(ts_code=ts_code, start_date=start_date_str, end_date=end_date_str)
            if not df_share.empty:
                _update_fund_daily_share_size(conn, df_share)
                logger.info(f"etf_share_size 更新成功: {ts_code} {row[1]}, 记录数: {len(df_share)}")
            else:
                logger.info(f"etf_share_size 无数据: {ts_code} {row[1]}, 日期段: {start_date_str} 到 {end_date_str}")
            
            df_adj = pro.fund_adj(ts_code=ts_code, start_date=start_date_str, end_date=end_date_str)
            if not df_adj.empty:
                _update_fund_daily_adj_factor(conn, df_adj)
                logger.info(f"fund_adj 更新成功: {ts_code} {row[1]}, 记录数: {len(df_adj)}")
            else:
                logger.info(f"fund_adj 无数据: {ts_code} {row[1]}, 日期段: {start_date_str} 到 {end_date_str}")
                
            conn.commit()
        
        current_date = next_date + timedelta(days=1)

def fund_data_increase(conn, pro):
    query = "SELECT ts_code FROM tu_etf_basic"
    results = conn.execute(text(query)).fetchall()
    
    if not results:
        logger.info("没有获取到ETF数据")
        return

    end_date_str = datetime.now().strftime('%Y%m%d')
    
    for row in results:
        ts_code = row[0]

        #取己保存数据的日期最大值
        query_max_date = f"SELECT MAX(trade_date) FROM tu_fund_daily WHERE ts_code = '{ts_code}'"
        max_date_result = conn.execute(text(query_max_date)).fetchone()
        max_date = max_date_result[0] if max_date_result[0] else datetime(2007, 1, 1)
        start_date = max_date + timedelta(days=1)
        start_date_str = start_date.strftime('%Y%m%d')

        logger.info(f"开始处理ETF: {ts_code}, 日期段: {start_date_str} 到 {end_date_str}")

        df_daily = pro.fund_daily(ts_code=ts_code, start_date=start_date_str, end_date=end_date_str)
        if not df_daily.empty:
            _insert_fund_daily(conn, df_daily)
            logger.info(f"fund_daily 写入成功: {ts_code}, 记录数: {len(df_daily)}")
        
        df_share = pro.etf_share_size(ts_code=ts_code, start_date=start_date_str, end_date=end_date_str)
        if not df_share.empty:
            _update_fund_daily_share_size(conn, df_share)
            logger.info(f"etf_share_size 更新成功: {ts_code}, 记录数: {len(df_share)}")
        
        df_adj = pro.fund_adj(ts_code=ts_code, start_date=start_date_str, end_date=end_date_str)
        if not df_adj.empty:
            _update_fund_daily_adj_factor(conn, df_adj)
            logger.info(f"fund_adj 更新成功: {ts_code}, 记录数: {len(df_adj)}")
            
        conn.commit()

if __name__ == "__main__":
    pro = tu_common.get_tushare_pro()

    conn = stock_common.get_db_conn(sql_echo=False)
    
    # 调用函数获取ETF基本信息
    etf_basic(conn, pro)
    #再获取日K线数据
    fund_data_all(conn, pro)

    #增量更新
    #fund_data_increase(conn, pro)

    conn.close()
