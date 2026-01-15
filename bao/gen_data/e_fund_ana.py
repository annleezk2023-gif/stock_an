import sys
import os
from sqlalchemy import text

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)
sys.path.append(root_path + '/api')
sys.path.append(root_path + '/bao')

import stock_common

# 配置logger
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 获取行业所有股票代码
def get_industry_stocks(conn, industry):
    sql = f"""SELECT code, code_name FROM bao_stock_basic WHERE industry = :industry AND status = '1' ORDER BY code ASC"""
    results = conn.execute(text(sql), {'industry': industry}).fetchall()
    return [(row.code, row.code_name) for row in results]

# 获取股票最新交易数据（根据code最后一位直接查询对应表）
def get_latest_trade_data(conn, code):
    table_name = f"bao_stock_trade_{code[-1]}"
    sql = f"""SELECT code, date, total_market_value, pe_year_1_percent, pe_year_3_percent, pe_year_5_percent, pe_year_10_percent,
               ps_year_1_percent, ps_year_3_percent, ps_year_5_percent, ps_year_10_percent
               FROM {table_name} WHERE code = :code ORDER BY date DESC LIMIT 1"""
    result = conn.execute(text(sql), {'code': code}).fetchone()
    return result

# 生成行业分析数据
def fund_ana_data_gen():
    conn = stock_common.get_db_conn(sql_echo=False)
    try:
        # 查询所有行业
        sql = """SELECT DISTINCT industry FROM bao_stock_basic WHERE industry IS NOT NULL AND industry <> '' ORDER BY industry"""
        industries = conn.execute(text(sql)).fetchall()
        
        logger.info(f"共找到 {len(industries)} 个行业")
        
        for industry_row in industries:
            industry = industry_row.industry
            logger.info(f"开始处理行业: {industry}")
            
            # 获取该行业的所有股票代码和名称
            codes_with_names = get_industry_stocks(conn, industry)
            
            # 过滤掉名称包含"ST"的股票
            codes_with_names = [(code, code_name) for code, code_name in codes_with_names if 'ST' not in code_name]
            
            if not codes_with_names:
                logger.warning(f"行业 {industry} 没有股票数据，跳过")
                continue
            
            codes = [code for code, code_name in codes_with_names]
            
            # 生成all_stock字段（逗号分隔的代码）
            all_stock = ','.join(codes)
            logger.info(f"行业 {industry} 共有 {len(codes)} 只股票（已排除ST股票）")
            
            # 查询所有股票的最新交易数据
            stock_data_list = []
            for code, code_name in codes_with_names:
                latest_data = get_latest_trade_data(conn, code)
                if latest_data and latest_data.total_market_value:
                    stock_data_list.append(latest_data)
            
            if not stock_data_list:
                logger.warning(f"行业 {industry} 没有交易数据，跳过")
                continue
            
            # 按总市值排序，取TOP 10
            stock_data_list.sort(key=lambda x: x.total_market_value, reverse=True)
            top_stocks = [data.code for data in stock_data_list[:10]]
            top_stock = ','.join(top_stocks)
            logger.info(f"行业 {industry} TOP 10股票: {top_stock}")
            
            # 计算PE分位值平均值（只取TOP 10股票，过滤NULL值）
            pe_1_values = [data.pe_year_1_percent for data in stock_data_list[:10] if data.pe_year_1_percent is not None]
            pe_3_values = [data.pe_year_3_percent for data in stock_data_list[:10] if data.pe_year_3_percent is not None]
            pe_5_values = [data.pe_year_5_percent for data in stock_data_list[:10] if data.pe_year_5_percent is not None]
            pe_10_values = [data.pe_year_10_percent for data in stock_data_list[:10] if data.pe_year_10_percent is not None]
            
            pe_year_1_percent = sum(pe_1_values) / len(pe_1_values) if pe_1_values else None
            pe_year_3_percent = sum(pe_3_values) / len(pe_3_values) if pe_3_values else None
            pe_year_5_percent = sum(pe_5_values) / len(pe_5_values) if pe_5_values else None
            pe_year_10_percent = sum(pe_10_values) / len(pe_10_values) if pe_10_values else None
            
            # 计算PS分位值平均值（只取TOP 10股票，过滤NULL值）
            ps_1_values = [data.ps_year_1_percent for data in stock_data_list[:10] if data.ps_year_1_percent is not None]
            ps_3_values = [data.ps_year_3_percent for data in stock_data_list[:10] if data.ps_year_3_percent is not None]
            ps_5_values = [data.ps_year_5_percent for data in stock_data_list[:10] if data.ps_year_5_percent is not None]
            ps_10_values = [data.ps_year_10_percent for data in stock_data_list[:10] if data.ps_year_10_percent is not None]
            
            ps_year_1_percent = sum(ps_1_values) / len(ps_1_values) if ps_1_values else None
            ps_year_3_percent = sum(ps_3_values) / len(ps_3_values) if ps_3_values else None
            ps_year_5_percent = sum(ps_5_values) / len(ps_5_values) if ps_5_values else None
            ps_year_10_percent = sum(ps_10_values) / len(ps_10_values) if ps_10_values else None
            
            logger.info(f"行业 {industry} PE分位值平均值 - 1年: {pe_year_1_percent}, 3年: {pe_year_3_percent}, 5年: {pe_year_5_percent}, 10年: {pe_year_10_percent}")
            logger.info(f"行业 {industry} PS分位值平均值 - 1年: {ps_year_1_percent}, 3年: {ps_year_3_percent}, 5年: {ps_year_5_percent}, 10年: {ps_year_10_percent}")
            
            # 使用INSERT ... ON DUPLICATE KEY UPDATE写入数据
            upsert_sql = """INSERT INTO fund_ana (industry, all_stock, top_stock, pe_year_1_percent, pe_year_3_percent, pe_year_5_percent, pe_year_10_percent,
                            ps_year_1_percent, ps_year_3_percent, ps_year_5_percent, ps_year_10_percent, created_at, updated_at)
                            VALUES (:industry, :all_stock, :top_stock, :pe_year_1_percent, :pe_year_3_percent, :pe_year_5_percent, :pe_year_10_percent,
                            :ps_year_1_percent, :ps_year_3_percent, :ps_year_5_percent, :ps_year_10_percent, NOW(), NOW())
                            ON DUPLICATE KEY UPDATE 
                            all_stock = VALUES(all_stock),
                            top_stock = VALUES(top_stock),
                            pe_year_1_percent = VALUES(pe_year_1_percent),
                            pe_year_3_percent = VALUES(pe_year_3_percent),
                            pe_year_5_percent = VALUES(pe_year_5_percent),
                            pe_year_10_percent = VALUES(pe_year_10_percent),
                            ps_year_1_percent = VALUES(ps_year_1_percent),
                            ps_year_3_percent = VALUES(ps_year_3_percent),
                            ps_year_5_percent = VALUES(ps_year_5_percent),
                            ps_year_10_percent = VALUES(ps_year_10_percent),
                            updated_at = NOW()"""
            
            conn.execute(text(upsert_sql), {
                'industry': industry,
                'all_stock': all_stock,
                'top_stock': top_stock,
                'pe_year_1_percent': pe_year_1_percent,
                'pe_year_3_percent': pe_year_3_percent,
                'pe_year_5_percent': pe_year_5_percent,
                'pe_year_10_percent': pe_year_10_percent,
                'ps_year_1_percent': ps_year_1_percent,
                'ps_year_3_percent': ps_year_3_percent,
                'ps_year_5_percent': ps_year_5_percent,
                'ps_year_10_percent': ps_year_10_percent
            })
            conn.commit()
            
            logger.info(f"行业 {industry} 数据更新完成")
        
        logger.info("所有行业数据生成完成！")
    finally:
        conn.close()

if __name__ == "__main__":
    logger.info("开始生成行业分析数据...")
    fund_ana_data_gen()
    logger.info("生成行业分析数据完成！")
