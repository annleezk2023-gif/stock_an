#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
import baostock_common

# 配置logger
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    # 登录baostock
    if not baostock_common.login_baostock():
        raise Exception("登录baostock失败")
    
    try:
        start_year = 2025
        end_year = 2025
        end_quarter = 3

        conn = stock_common.get_db_conn(sql_echo=False)
        # 0 清除旧数据
        logger.info("清除旧数据开始")
        conn.execute(text(f"delete from bao_stock_balance where year = {end_year} and quarter >= {end_quarter-1} and data_exist = 0"))
        conn.execute(text(f"delete from bao_stock_cash_flow where year = {end_year} and quarter > {end_quarter-1} and data_exist = 0"))
        conn.execute(text(f"delete from bao_stock_growth where year = {end_year} and quarter > {end_quarter-1} and data_exist = 0"))
        conn.execute(text(f"delete from bao_stock_operation where year = {end_year} and quarter > {end_quarter-1} and data_exist = 0"))
        conn.execute(text(f"delete from bao_stock_profit where year = {end_year} and quarter > {end_quarter-1} and data_exist = 0"))
        conn.execute(text(f"delete from bao_stock_dividend where year = {end_year} and data_exist = 0"))
        conn.commit()
        logger.info("清除旧数据完成")
        
        # 1
        logger.info("季频偿债能力数据获取开始")
        import fetch_baostock_balance_data
        fetch_baostock_balance_data.batch_fetch_and_save_balance_data(start_year=start_year, end_year=end_year, end_quarter=end_quarter, conn=conn)
        logger.info("季频偿债能力数据获取完成")
        
        #2
        logger.info("季频现金流量数据获取开始")
        import fetch_baostock_cash_flow_data
        fetch_baostock_cash_flow_data.batch_fetch_and_save_cash_flow_data(start_year=start_year, end_year=end_year, end_quarter=end_quarter, conn=conn)
        logger.info("季频现金流量数据获取完成")

        #3 
        logger.info("季频成长能力数据获取开始")
        import fetch_baostock_growth_data
        fetch_baostock_growth_data.batch_fetch_and_save_growth_data(start_year=start_year, end_year=end_year, end_quarter=end_quarter, conn=conn)
        logger.info("季频成长能力数据获取完成")

        #4
        logger.info("季频营运能力数据获取开始")
        import fetch_baostock_operation_data
        fetch_baostock_operation_data.batch_fetch_and_save_operation_data(start_year=start_year, end_year=end_year, end_quarter=end_quarter, conn=conn)
        logger.info("季频营运能力数据获取完成")

        #5
        logger.info("季频盈利能力数据获取开始")
        import fetch_baostock_profit_data
        fetch_baostock_profit_data.batch_fetch_and_save_profit_data(start_year=start_year, end_year=end_year, end_quarter=end_quarter, conn=conn)
        logger.info("季频盈利能力数据获取完成")

        #6
        logger.info("分红数据获取开始")
        import fetch_baostock_dividend_data
        fetch_baostock_dividend_data.batch_fetch_and_save_dividend_data(start_year=end_year, conn=conn, check_exist=False)
        logger.info("分红数据获取完成")

        conn.close()
        
    except Exception as e:
        logger.error(f"批量处理失败: {str(e)}")
    finally:
        # 确保登出
        baostock_common.logout_baostock()