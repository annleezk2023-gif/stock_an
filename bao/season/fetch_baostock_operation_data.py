#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载季频营运能力数据并保存到数据库
"""
import baostock as bs
import pandas as pd
from datetime import datetime
import time
import os
from sqlalchemy import text
import sys
# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common
sys.path.append(root_path + '/bao')
import baostock_common

# 配置logger
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 数据库连接信息
DATABASE_URI = os.getenv('DATABASE_URL', 'mysql+mysqlconnector://root:123456@localhost/stock?charset=utf8mb4')

# 初始化Flask应用上下文（用于独立运行此脚本）
def init_app_context():
    from flask import Flask
    from app import db
    
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)
    return app.app_context()


# 查询季频营运能力数据
def query_operation_data(code, year, quarter):
    """
    查询单只股票的季频营运能力数据
    :param code: 股票代码，如sh.600000
    :param year: 统计年份，如2017
    :param quarter: 统计季度，取值1-4
    :return: DataFrame，包含季频营运能力数据
    """

    # 查询季频营运能力数据
    rs_operation = bs.query_operation_data(code=code, year=year, quarter=quarter)
    if rs_operation.error_code != '0':
        raise Exception(f"查询失败: {rs_operation.error_msg}")
    
    # 获取数据
    data_list = []
    while (rs_operation.error_code == '0') & rs_operation.next():
        data_list.append(rs_operation.get_row_data())
    
    if not data_list:
        return None
    
    # 动态处理列名
    columns = rs_operation.fields
    
    # 转换为DataFrame
    df = pd.DataFrame(data_list, columns=columns)
    
    # 确保code列存在
    if 'code' not in df.columns:
        df['code'] = code
    
    # 添加年份和季度信息
    df['year'] = year
    df['quarter'] = quarter
    
    return df


# 保存营运能力数据到数据库
def save_operation_data(df, conn):
    """
    保存季频营运能力数据到数据库
    :param df: 包含季频营运能力数据的DataFrame
    :return: 成功保存的记录数
    """
    if df is None or df.empty:
        return 0
    
    saved_count = 0

    for _, row in df.iterrows():
        # 准备插入数据
        insert_data = {
            'code': row.get('code'),
            'pubDate': row.get('pubDate'),
            'statDate': row.get('statDate'),
            'NRTurnRatio': float(row.get('NRTurnRatio')) if row.get('NRTurnRatio') else None,
            'NRTurnDays': float(row.get('NRTurnDays')) if row.get('NRTurnDays') else None,
            'INVTurnRatio': float(row.get('INVTurnRatio')) if row.get('INVTurnRatio') else None,
            'INVTurnDays': float(row.get('INVTurnDays')) if row.get('INVTurnDays') else None,
            'CATurnRatio': float(row.get('CATurnRatio')) if row.get('CATurnRatio') else None,
            'AssetTurnRatio': float(row.get('AssetTurnRatio')) if row.get('AssetTurnRatio') else None,
            'year': int(row.get('year')),
            'quarter': int(row.get('quarter')),
            'data_exist': 1
        }
        
        # 使用INSERT ... ON DUPLICATE KEY UPDATE语句
        insert_sql = text("""
        INSERT INTO bao_stock_operation (
            code, pubDate, statDate, NRTurnRatio, NRTurnDays, 
            INVTurnRatio, INVTurnDays, CATurnRatio, AssetTurnRatio,
            year, quarter, data_exist
        ) VALUES (
            :code, :pubDate, :statDate, :NRTurnRatio, :NRTurnDays, 
            :INVTurnRatio, :INVTurnDays, :CATurnRatio, :AssetTurnRatio,
            :year, :quarter, :data_exist
        )""")
        
        conn.execute(insert_sql, insert_data)
        saved_count += 1
    
    conn.commit()
    return saved_count



# 保存空数据
def save_empty_operation_data(code, year, quarter, conn):
    # 准备插入数据
    insert_data = {
        'code': code,
        'year': year,
        'quarter': quarter
    }
    
    # 使用INSERT
    insert_sql = text("""
    INSERT INTO bao_stock_operation (code, year, quarter, data_exist) VALUES (:code, :year, :quarter, 0)""")
    
    # 执行插入
    conn.execute(insert_sql, insert_data)

    # 提交事务
    conn.commit()


# 批量获取并保存所有股票的季频营运能力数据
def batch_fetch_and_save_operation_data(start_year=2007, end_year=None, end_quarter=4, conn=None):
    """
    批量获取并保存所有股票的季频营运能力数据
    :param start_year: 开始年份，默认2007年
    :param end_year: 结束年份，默认当前年份
    """
    # 设置结束年份
    if end_year is None:
        end_year = datetime.now().year

    # 获取所有上市股票代码
    stocks = stock_common.get_stock_info_all(conn)
    
    if not stocks:
        logger.info("未获取到上市股票数据")
        return
    
    total_stocks = len(stocks)
    logger.info(f"开始处理{total_stocks}只股票的季频营运能力数据")

    # 处理每只股票
    for index, stock in enumerate(stocks):
        stock_code = stock.code
        stock_name = stock.code_name
        logger.info(f"处理 {index+1}/{total_stocks}: {stock_code} - {stock_name}")

        #查询数据是否己有数据
        check_sql = text("""
        SELECT COUNT(*) FROM bao_stock_operation
        WHERE code = :code AND year = :year AND quarter = :quarter
        """)
        result = conn.execute(check_sql, {'code': stock_code, 'year': end_year, 'quarter': end_quarter}).fetchone()
        if result[0] > 0:
            logger.debug(f"{index+1}/{total_stocks}  股票 {stock_code} {stock_name} {end_year}年Q{end_quarter} 营运能力数据已存在，跳过")
            continue
        
        # 处理该股票的所有年份和季度数据
        for year in range(start_year, end_year + 1):
            for quarter in range(1, 5):
                if(year == end_year and quarter > end_quarter):
                    continue

                # 己退市，那退市时间之后的数据不用再查了
                target_date = datetime.strptime(f"{year}-{quarter*3}-01", "%Y-%m-%d").date()
                
                # 将stock.out_date转换为日期对象进行比较
                out_date = datetime.strptime(stock.out_date, "%Y-%m-%d").date() if isinstance(stock.out_date, str) else stock.out_date
                
                if stock.out_date and out_date < target_date:
                    break


                #查询数据是否己有数据
                check_sql = text("""
                SELECT COUNT(*) FROM bao_stock_operation
                WHERE code = :code AND year = :year AND quarter = :quarter
                """)
                result = conn.execute(check_sql, {'code': stock_code, 'year': year, 'quarter': quarter}).fetchone()
                if result[0] > 0:
                    logger.debug(f"{index+1}/{total_stocks}  股票 {stock_code} {stock_name} {year}年Q{quarter} 营运能力数据已存在，跳过")
                    continue


                # 查询季频营运能力数据
                df = query_operation_data(stock_code, year, quarter)
                time.sleep(0.1)
                
                if df is not None and not df.empty:
                    # 保存数据到数据库
                    saved_count = save_operation_data(df, conn)
                    logger.debug(f"{index+1}/{total_stocks}  成功保存 {stock_code} {stock_name} {year}年Q{quarter} {saved_count}条季频营运能力数据")
                else:
                    save_empty_operation_data(stock_code, year, quarter, conn)
                    logger.debug(f"{index+1}/{total_stocks} 股票 {stock_code} {stock_name} {year}年Q{quarter} 营运能力数据为空，保存空数据成功")


if __name__ == '__main__':
    logger.info("开始获取季频营运能力数据")

    # 登录baostock
    if not baostock_common.login_baostock():
        raise Exception("登录baostock失败")
    
    try:
        # 批量获取并保存所有股票的季频营运能力数据
        conn = stock_common.get_db_conn()
        batch_fetch_and_save_operation_data(start_year=2007, end_year=2025, end_quarter=2, conn=conn)
        conn.close()
        logger.info("季频营运能力数据获取完成")
    except Exception as e:
        logger.error(f"批量处理失败: {str(e)}")
    finally:
        # 确保登出
        baostock_common.logout_baostock()