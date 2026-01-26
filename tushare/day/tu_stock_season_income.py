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

def _insert_stock_season_income(conn, df):
    if df.empty:
        logger.info("数据为空，不写入tu_stock_season_income")
        return
    
    # 插入数据到表tu_stock_season_income，插入前判断，如果己存在，则不插入。判断条件ts_code, period_date

    # 准备插入数据，单条插入
    insert_count = 0
    for _, row in df.iterrows():
        ts_code = tu_common.convert_stock_code_2bao(row.get('ts_code'))
        period_date = pd.to_datetime(row.get('end_date'), format='%Y%m%d').strftime('%Y-%m-%d') if pd.notna(row.get('end_date')) else None
        f_ann_date = pd.to_datetime(row.get('f_ann_date'), format='%Y%m%d').strftime('%Y-%m-%d') if pd.notna(row.get('f_ann_date')) else None
        update_flag = row.get('update_flag')

        # ts_code和period_date不能为空
        if not ts_code or not period_date:
            logger.warning(f"股票代码或周期日期为空，跳过: {ts_code}, {period_date}")
            continue

        # 检查是否已存在
        check_query = f"""
        SELECT count(*) FROM tu_stock_season_income WHERE ts_code = :ts_code AND period_date = :period_date
        """
        existing_results = conn.execute(text(check_query), {'ts_code': ts_code, 'period_date': period_date}).fetchone()
        if existing_results[0] > 0:
            # 如果update_flag为1，则更新，否则跳过
            if update_flag == '1':
                logger.info(f"tu_stock_season_income股票代码{ts_code} 周期{period_date} 已存在，update_flag为1，更新")
                update_query = """
                UPDATE tu_stock_season_income 
                SET f_ann_date = :f_ann_date, basic_eps = :basic_eps, total_revenue = :total_revenue, total_cogs = :total_cogs, total_profit = :total_profit, updated_at = NOW()
                WHERE ts_code = :ts_code AND period_date = :period_date
                """
                conn.execute(text(update_query), {
                    'ts_code': ts_code,
                    'period_date': period_date,
                    'f_ann_date': f_ann_date,
                    'basic_eps': row.get('basic_eps'),
                    'total_revenue': row.get('total_revenue'),
                    'total_cogs': row.get('total_cogs'),
                    'total_profit': row.get('total_profit')
                })
            continue
        
        # 单条插入
        insert_query = """
        INSERT INTO tu_stock_season_income 
        (ts_code, period_date, f_ann_date, basic_eps, total_revenue, total_cogs, total_profit, created_at, updated_at)
        VALUES (:ts_code, :period_date, :f_ann_date, :basic_eps, :total_revenue, :total_cogs, :total_profit, NOW(), NOW())
        """
        conn.execute(text(insert_query), {
            'ts_code': ts_code,
            'period_date': period_date,
            'f_ann_date': f_ann_date,
            'basic_eps': row.get('basic_eps'),
            'total_revenue': row.get('total_revenue'),
            'total_cogs': row.get('total_cogs'),
            'total_profit': row.get('total_profit')
        })
        insert_count += 1
    
    conn.commit()
    logger.info(f"tu_stock_season_income写入成功: {insert_count} 条记录")
    return insert_count

#全量查询
def stock_season_income_all(conn, pro):
    #从bao_stock_basic取全部的股票数据
    query = "SELECT code FROM bao_stock_basic"
    results = conn.execute(text(query)).fetchall()
    
    if not results:
        logger.info("没有获取到股票数据")
        return
    
    #循环接口查询，查询参数ts_code=bao_stock_basic.code
    total_count = 0
    for row in results:
        code = row[0]

        if not code:
            logger.error(f"股票代码格式错误: {row[0]}")
            continue

        logger.info(f"开始处理股票: {code} ")
        #查询income接口，分页查询
        offset = 0
        limit = 100
        while True:
            df = pro.income(ts_code=tu_common.convert_stock_code_2tu(code), limit=limit, offset=offset, fields='ts_code,end_date,f_ann_date,update_flag,basic_eps,total_revenue,total_cogs,total_profit')
            if not df.empty:
                insert_count = _insert_stock_season_income(conn, df)
                total_count += insert_count
                logger.info(f"income 写入成功: {code}, offset: {offset}, 记录数: {insert_count}")
                if len(df) < limit:
                    break
                offset += limit
            else:
                logger.info(f"income 无数据: {code}, offset: {offset}")
                break
    logger.info(f"income 总计写入: {total_count}")


#增量查询
def stock_season_income_increase(conn, pro):
    #从bao_stock_basic取全部的股票数据
    query = "SELECT code FROM bao_stock_basic"
    results = conn.execute(text(query)).fetchall()
    
    if not results:
        logger.info("没有获取到股票数据")
        return
    
    #循环从income接口查询，查询参数ts_code=bao_stock_basic.code
    total_count = 0
    for row in results:
        code = row[0]

        if not code:
            logger.error(f"股票代码格式错误: {row[0]}")
            continue

        #从己有数据中查询最大的日期，后续从此日期往后查询
        start_date = '20070101'
        query = """
        SELECT MAX(period_date) FROM tu_stock_season_income WHERE ts_code = :ts_code
        """
        max_date_result = conn.execute(text(query), {'ts_code': code}).fetchone()
        if max_date_result[0]:
            start_date = (max_date_result[0] - timedelta(days=1)).strftime('%Y%m%d')

        logger.info(f"开始处理股票: {code} {start_date}")
        #查询income接口，分页查询
        offset = 0
        limit = 100
        while True:
            df = pro.income(ts_code=tu_common.convert_stock_code_2tu(code), start_date=start_date, limit=limit, offset=offset, fields='ts_code,end_date,f_ann_date,update_flag,basic_eps,total_revenue,total_cogs,total_profit')
            if not df.empty:
                insert_count = _insert_stock_season_income(conn, df)
                total_count += insert_count
                logger.info(f"income 写入成功: {code}, offset: {offset}, 记录数: {insert_count}")
                if len(df) < limit:
                    break
                offset += limit
            else:
                logger.info(f"income 无数据: {code}, offset: {offset}")
                break
    logger.info(f"income 总计写入: {total_count}")

if __name__ == "__main__":
    pro = tu_common.get_tushare_pro()

    conn = stock_common.get_db_conn(sql_echo=False)
    
    # 股票的季报income
    stock_season_income_all(conn, pro)

    #增量更新
    #stock_season_income_increase(conn, pro)

    conn.close()
