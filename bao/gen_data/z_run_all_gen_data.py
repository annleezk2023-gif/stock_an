#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

import sys
# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
sys.path.append(root_path + '/bao/gen_data')
import stock_common
sys.path.append(root_path + '/bao')
import baostock_common

# 配置logger
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    # 登录baostock
    if not baostock_common.login_baostock():
        raise Exception("登录baostock失败")
    
    try:
        conn = stock_common.get_db_conn(sql_echo=False)
        # 1
        logger.info("预处理空数据开始")
        import a_dividend_data
        a_dividend_data.bao_stock_dividend_zhuansong_num(conn=conn)

        import a_profit_data_cash
        a_profit_data_cash.bao_stock_profit_data(conn=conn)

        import a_trade_pe_data
        a_trade_pe_data.bu_trade_data(conn=conn)
        a_trade_pe_data.bu_bao_stock_basic(conn=conn)

        logger.info("预处理空数据完成")

        #2
        logger.info("计算pe分位值等开始")
        import b_trade_pe1year_dividend
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # 使用多线程执行任务
        with ThreadPoolExecutor(max_workers=10) as executor:
            # 提交任务 - 不传递conn参数，让每个线程自己创建
            futures = {executor.submit(b_trade_pe1year_dividend.gen_pe_data, divide_table_num): divide_table_num for divide_table_num in range(0, 10, 1)}
            
            # 等待所有任务完成
            for future in as_completed(futures):
                divide_table_num = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"处理分表 {divide_table_num} 时出错: {str(e)}")
        logger.info("计算pe分位值等完成")

        #3 
        logger.info("生成ana报告开始")
        import d_stock_ana_data
        d_stock_ana_data.stock_basic_ana_data_gen()
        logger.info("生成ana报告完成")

        conn.close()
        
    except Exception as e:
        logger.error(f"批量处理失败: {str(e)}")
    finally:
        # 确保登出
        baostock_common.logout_baostock()