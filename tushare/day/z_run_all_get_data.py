#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os

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

if __name__ == '__main__':
    conn = stock_common.get_db_conn(sql_echo=False)
    pro = tu_common.get_tushare_pro()

    # 1
    logger.info("ETF基金信息，日K")
    import tu_fund
    tu_fund.etf_basic_all(conn=conn, pro=pro)
    tu_fund.etf_k_increase(conn=conn, pro=pro)
    logger.info("ETF基金信息，日K完成")

    #2
    logger.info("股票的季报开始 营收")
    import tu_stock_season_income
    tu_stock_season_income.stock_season_income_increase(conn, pro)

    logger.info("股票的季报开始 财务指标")
    import tu_stock_season_fina_indicator
    tu_stock_season_fina_indicator.stock_season_fina_indicator_increase(conn, pro)

    logger.info("股票的季报开始 主营业务构成")
    import tu_stock_season_mainbz
    tu_stock_season_mainbz.stock_season_mainbz_increase(conn, pro)

    logger.info("股票的季报完成")

    #3 
    logger.info("index_k 日K线")
    import tu_index_k
    tu_index_k.index_k_increase(conn, pro)
    logger.info("index_k 日K线完成")

    conn.close()
    