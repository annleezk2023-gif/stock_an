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

def _insert_stock_season_mainbz(conn, df):
    if df.empty:
        logger.info("数据为空，不写入数据库")
        return
    
    # bz_item为空的数据丢弃
    df = df[df['bz_item'].notna()]
    
    # 插入数据到表tu_stock_season_mainbz，插入前判断，如果己存在，则不插入。判断条件ts_code, period_date, type, bz_item
    # 数据库表结构：id, ts_code, period_date, type, bz_item, bz_sales, bz_profit, bz_cost, curr_type, created_at, updated_at
    
    # 准备插入数据，单条插入，插入前先查询，存在则不插入
    insert_count = 0
    for _, row in df.iterrows():
        ts_code = tu_common.convert_stock_code_2bao(row.get('ts_code'))

        period_date = pd.to_datetime(row.get('end_date'), format='%Y%m%d').strftime('%Y-%m-%d') if pd.notna(row.get('end_date')) else None
        bz_item = row.get('bz_item')
        bz_code = row.get('bz_code')
        update_flag = row.get('update_flag')

        # ts_code和period_date不能为空
        if not ts_code or not period_date:
            logger.warning(f"股票代码或周期日期为空，跳过: {ts_code}, {period_date}")
            continue

        #如果bz_code有值，则必须在P按产品 D按地区 I按行业
        if bz_code not in ['P', 'D', 'I']:
            logger.warning(f"股票代码{ts_code} 周期{period_date} 行业分类{bz_code} 不是P按产品 D按地区 I按行业，跳过")
            continue
    
        # 检查是否已存在
        if bz_code:
            check_query = """
            SELECT COUNT(*) FROM tu_stock_season_mainbz 
            WHERE ts_code = :ts_code AND period_date = :period_date AND bz_code = :bz_code AND bz_item = :bz_item
            """
            check_result = conn.execute(text(check_query), {'ts_code': ts_code, 'period_date': period_date, 'bz_code': bz_code, 'bz_item': bz_item}).fetchone()
        else:
            check_query = """
            SELECT COUNT(*) FROM tu_stock_season_mainbz 
            WHERE ts_code = :ts_code AND period_date = :period_date AND bz_item = :bz_item
            """
            check_result = conn.execute(text(check_query), {'ts_code': ts_code, 'period_date': period_date, 'bz_item': bz_item}).fetchone()
        if check_result[0] > 0:
            # 如果update_flag为1，则更新，否则跳过
            if update_flag == '1':
                logger.info(f"股票代码{ts_code} 周期{period_date} 行业分类{bz_code} 项目{bz_item} 已存在，update_flag为1，更新")
                update_query = """
                UPDATE tu_stock_season_mainbz 
                SET bz_sales = :bz_sales, bz_profit = :bz_profit, bz_cost = :bz_cost, curr_type = :curr_type, updated_at = NOW()
                WHERE ts_code = :ts_code AND period_date = :period_date AND bz_code = :bz_code AND bz_item = :bz_item
                """
                conn.execute(text(update_query), {
                    'ts_code': ts_code,
                    'period_date': period_date,
                    'bz_code': bz_code,
                    'bz_item': bz_item,
                    'bz_sales': row.get('bz_sales'),
                    'bz_profit': row.get('bz_profit'),
                    'bz_cost': row.get('bz_cost'),
                    'curr_type': row.get('curr_type')
                })
            continue
            
        # 单条插入
        insert_query = """
        INSERT INTO tu_stock_season_mainbz 
        (ts_code, period_date, bz_code, bz_item, bz_sales, bz_profit, bz_cost, curr_type, created_at, updated_at)
        VALUES (:ts_code, :period_date, :bz_code, :bz_item, :bz_sales, :bz_profit, :bz_cost, :curr_type, NOW(), NOW())
        """
        conn.execute(text(insert_query), {
            'ts_code': ts_code,
            'period_date': period_date,
            'bz_code': bz_code,
            'bz_item': bz_item,
            'bz_sales': row.get('bz_sales'),
            'bz_profit': row.get('bz_profit'),
            'bz_cost': row.get('bz_cost'),
            'curr_type': row.get('curr_type')
        })
        insert_count += 1
        
        logger.info(f"股票代码{ts_code} 周期{period_date} 行业分类{bz_code} 项目{bz_item} 已存在，更新成功")

    conn.commit()
    
    logger.info(f"写入数据库成功: {insert_count} 条记录")
    return insert_count

#全量查询
def stock_season_mainbz_all(conn, pro):
    #从bao_stock_basic取全部的股票数据
    query = "SELECT code FROM bao_stock_basic"
    results = conn.execute(text(query)).fetchall()
    
    if not results:
        logger.info("没有获取到股票数据")
        return
    
    #循环从fina_mainbz接口查询，查询参数ts_code=bao_stock_basic.code
    total_count = 0
    for row in results:
        code = row[0]

        logger.info(f"开始处理股票: {code} ")
        #查询fina_mainbz接口，分页查询
        offset = 0
        limit = 100
        while True:
            df = pro.fina_mainbz(ts_code=tu_common.convert_stock_code_2tu(code), limit=limit, offset=offset, fields='ts_code,bz_code,end_date,bz_item,bz_sales,bz_profit,bz_cost,curr_type,update_flag')
            if not df.empty:
                insert_count = _insert_stock_season_mainbz(conn, df)
                total_count += insert_count
                logger.info(f"fina_mainbz 写入成功: {code}, offset: {offset}, 记录数: {insert_count}")
                if len(df) < limit:
                    break
                offset += limit
            else:
                logger.info(f"fina_mainbz 无数据: {code}, offset: {offset}")
                break
    logger.info(f"fina_mainzb 总计写入: {total_count}")

#增量查询
def stock_season_mainbz_increase(conn, pro):
    #从bao_stock_basic取全部的股票数据
    query = "SELECT code FROM bao_stock_basic"
    results = conn.execute(text(query)).fetchall()
    
    if not results:
        logger.info("没有获取到股票数据")
        return
    
    #循环从fina_mainbz接口查询，查询参数ts_code=bao_stock_basic.code
    total_count = 0
    for row in results:
        code = row[0]

        if not code:
            logger.error(f"股票代码格式错误: {row[0]}")
            continue

        #从己有数据中查询最大的日期，后续从此日期往后查询
        start_date = '20070101'
        query = """
        SELECT MAX(period_date) FROM tu_stock_season_mainbz WHERE ts_code = :ts_code
        """
        max_date_result = conn.execute(text(query), {'ts_code': tu_common.convert_stock_code_2tu(code)}).fetchone()
        if max_date_result[0]:
            start_date = (max_date_result[0] - timedelta(days=1)).strftime('%Y%m%d')

        logger.info(f"开始处理股票: {code} {start_date}")
        #查询fina_mainbz接口，分页查询
        offset = 0
        limit = 100
        while True:
            df = pro.fina_mainbz(ts_code=tu_common.convert_stock_code_2tu(code), start_date=start_date, limit=limit, offset=offset, fields='ts_code,bz_code,end_date,bz_item,bz_sales,bz_profit,bz_cost,curr_type,update_flag')
            if not df.empty:
                insert_count = _insert_stock_season_mainbz(conn, df)
                total_count += insert_count
                logger.info(f"fina_mainbz 写入成功: {code}, offset: {offset}, 记录数: {insert_count}")
                if len(df) < limit:
                    break
                offset += limit
            else:
                logger.info(f"fina_mainbz 无数据: {code}, offset: {offset}")
                break
    logger.info(f"fina_mainzb 总计写入: {total_count}")


if __name__ == "__main__":
    pro = tu_common.get_tushare_pro()

    conn = stock_common.get_db_conn(sql_echo=False)
    
    # 股票的季报 业务构成
    stock_season_mainbz_all(conn, pro)

    #增量更新
    #stock_season_mainbz_increase(conn, pro)


    conn.close()
