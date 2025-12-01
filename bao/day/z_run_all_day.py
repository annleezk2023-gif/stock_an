#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys

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
        conn = stock_common.get_db_conn(sql_echo=False)

        # 取当前的日期的天
        import time
        day_num = int(time.strftime("%Y%m%d", time.localtime()))

        # 每10天执行一次
        if day_num % 10 == 0:
            # 1
            logger.info("基本信息开始")
            import fetch_baostock_stock_basic
            fetch_baostock_stock_basic.fetch_and_save_stock_basic(conn=conn)
            logger.info("基本信息完成")

            #2
            logger.info("分类信息开始")
            import fetch_baostock_stock_basic_industry
            fetch_baostock_stock_basic_industry.update_stock_industry_info(conn=conn)
            logger.info("分类信息完成")

        #3 
        logger.info("非股票交易记录开始")
        import fetch_baostock_nostock_trade
        fetch_baostock_nostock_trade.batch_fetch_and_save_kline_data(conn=conn)
        logger.info("非股票交易记录完成")

        #4
        logger.info("股票交易记录开始")
        import fetch_baostock_stock_trade
        fetch_baostock_stock_trade.batch_fetch_and_save_kline_data(conn=conn)
        logger.info("股票交易记录完成")

        conn.close()
        
    except Exception as e:
        logger.error(f"批量处理失败: {str(e)}")
    finally:
        # 确保登出
        baostock_common.logout_baostock()