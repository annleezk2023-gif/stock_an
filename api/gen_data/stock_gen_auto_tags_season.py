import sys
import os
from sqlalchemy import text
import json
import datetime

# 配置logger
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common
import stock_tags
sys.path.append(root_path + '/api/gen_data')
import stock_gen_auto_tags_dividend

""" 自动计算标签 ，分2种标签。
基础条件：上市时间2年内，>2年。市值50亿，100亿，200亿，500亿，1000亿+。
分红股：2年连续分红，分红金额稳定+增长，销售额业绩稳定+增长。
成长股：转送次数，销售额稳定+增长，净利润额稳定+增长，净利率高，资产负债表资产稳定+增长，资产负债表负债稳定+增长。"""

# 检查这几天是否有最新数据
def is_fetch_data_update(tableType, stock_code, pre_trade_date_str, trade_date_str, conn=None):
    if tableType == "fenghong":
        # 分红，转送
        # 包含前一个交易日，不包含当前交易日
        sql = f"SELECT COUNT(*) FROM bao_stock_dividend WHERE code = '{stock_code}' AND dividOperateDate >= '{pre_trade_date_str}' AND dividOperateDate < '{trade_date_str}'"
        result = conn.execute(text(sql)).fetchone()
        if result and result[0] > 0:
            return True 
        else:
            return False
    elif tableType == "season":
        # 季度数据
        # 包含前一个交易日，不包含当前交易日
        sql = f"SELECT COUNT(*) FROM bao_stock_profit WHERE code = '{stock_code}' AND pubDate >= '{pre_trade_date_str}' AND pubDate < '{trade_date_str}'"
        result = conn.execute(text(sql)).fetchone()
        if result and result[0] > 0:
            return True 
        else:
            return False
    else:
        return False



# 季报后刷新
def gen_season_auto_tags_4_stock(stock_code, cur_year, cur_quarter, conn=None):
    """ 为股票生成自动标签 """
    positive_tags = []
    loss_tags = []
    pubDate = None

    statDateStr = f"{cur_year}-{cur_quarter}"
    cur_year_int = int(cur_year)
    preStatDateStr = f"{cur_year_int-1}-{cur_quarter}"

    #1，bao_stock_balance资产负债表。
    
    #2，bao_stock_cash_flow现金流表

    #3，bao_stock_growth成长表
    sql = f"""select YOYNI, code, year, quarter, statDate, pubDate from bao_stock_growth where code = '{stock_code}' and statDate = '{statDateStr}' and data_exist = 1
            and YOYNI is not null"""
    growth_result = conn.execute(text(sql)).fetchone()
    if growth_result:
        if pubDate is None:
            pubDate = growth_result.pubDate
        #净利润同比增长
        if growth_result.YOYNI:
            if growth_result.YOYNI > 1:
                positive_tags.append(stock_tags.StockTagsSeason.profit_netProfit_pct_100)
            elif growth_result.YOYNI > 0.5:
                positive_tags.append(stock_tags.StockTagsSeason.profit_netProfit_pct_50)
            elif growth_result.YOYNI > 0.2:
                positive_tags.append(stock_tags.StockTagsSeason.profit_netProfit_pct_20)
            elif growth_result.YOYNI < -0.1:
                loss_tags.append(stock_tags.StockTagsSeason.profit_netProfit_pct_10_loss)

    #4，bao_stock_operation 运营表，总资产周转率
  

    #5，bao_stock_profit 营收表，
    sql = f"""select roeAvg, npMargin, gpMargin, netProfit, epsTTM, MBRevenue, code, year, quarter, statDate, pubDate from bao_stock_profit 
            where code = '{stock_code}' and statDate in ('{statDateStr}', '{preStatDateStr}') and data_exist = 1 order by statDate desc"""
    profit_result = conn.execute(text(sql)).fetchall()
    if profit_result and len(profit_result) > 0:
        cur_profit = None
        pre_profit = None
        for item in profit_result:
            if statDateStr == item.statDate.strftime("%Y-%m-%d"):
                cur_profit = item
            elif preStatDateStr == item.statDate.strftime("%Y-%m-%d"):
                pre_profit = item
        
        if cur_profit is not None:
            if pubDate is None:
                pubDate = cur_profit.pubDate
            
            # roeAvg净资产收益率
            if cur_profit.roeAvg is not None:
                if cur_profit.roeAvg > 20:
                    positive_tags.append(stock_tags.StockTagsSeason.profit_roeAvg_pct_20)
                elif cur_profit.roeAvg > 10:
                    positive_tags.append(stock_tags.StockTagsSeason.profit_roeAvg_pct_10_20)
                elif cur_profit.roeAvg > 0:
                    positive_tags.append(stock_tags.StockTagsSeason.profit_roeAvg_pct_0_10)
                elif cur_profit.roeAvg < 0:
                    loss_tags.append(stock_tags.StockTagsSeason.profit_roeAvg_pct_0_loss)

            # npMargin净利率
            if cur_profit.npMargin is not None:
                if cur_profit.npMargin > 0.5:
                    positive_tags.append(stock_tags.StockTagsSeason.profit_npMargin_pct_50)
                elif cur_profit.npMargin > 0.3:
                    positive_tags.append(stock_tags.StockTagsSeason.profit_npMargin_pct_30_50)
                elif cur_profit.npMargin > 0.1:
                    positive_tags.append(stock_tags.StockTagsSeason.profit_npMargin_pct_10_30)
                elif cur_profit.npMargin < 0:
                    loss_tags.append(stock_tags.StockTagsSeason.profit_npMargin_pct_0_loss)

            if pre_profit is not None:
                # netProfit净利润
                if cur_profit.netProfit is not None and pre_profit.netProfit is not None:
                    if cur_profit.netProfit > 0 and pre_profit.netProfit < 0:
                        positive_tags.append(stock_tags.StockTagsSeason.profit_netProfit_1_season_add)
                    if cur_profit.netProfit < 0 and pre_profit.netProfit > 0:
                        loss_tags.append(stock_tags.StockTagsSeason.profit_netProfit_1_season_loss)

                # MBRevenue主营收
                if cur_profit.MBRevenue is not None and pre_profit.MBRevenue is not None:
                    if cur_profit.MBRevenue > pre_profit.MBRevenue * 2:
                        positive_tags.append(stock_tags.StockTagsSeason.profit_MBRevenue_pct_100)
                    elif cur_profit.MBRevenue > pre_profit.MBRevenue * 1.5:
                        positive_tags.append(stock_tags.StockTagsSeason.profit_MBRevenue_pct_50)  
                    elif cur_profit.MBRevenue > pre_profit.MBRevenue * 1.2:
                        positive_tags.append(stock_tags.StockTagsSeason.profit_MBRevenue_pct_20)
                    elif cur_profit.MBRevenue < pre_profit.MBRevenue * 0.9:
                        loss_tags.append(stock_tags.StockTagsSeason.profit_MBRevenue_pct_10_loss)

    return positive_tags, loss_tags, pubDate

# 按季度的业绩增长标签
def gen_season_tags_data(conn=None, start_year=2010, end_year=2025, end_quarter="09-30", is_run_all = False):
    date_param = ["03-31", "06-30", "09-30", "12-31"]
    for year_param in range(start_year, end_year+1, 1):
        for date_param_item in date_param:
            gen_season_tags_data_single(conn=conn, cur_year=year_param, cur_quarter=date_param_item, is_run_all = is_run_all)

# 按季度的业绩增长标签
def gen_season_tags_data_single(conn=None, cur_year=2010, cur_quarter="09-30", is_run_all = False): 
    # 初始化分红日期
    stock_gen_auto_tags_dividend.init_dividend_date(conn)

    # 获取所有上市股票代码
    query = f"SELECT * FROM bao_stock_basic where type='1' order by code asc"
    stocks = conn.execute(text(query)).fetchall()
    
    if not stocks:
        logger.info("未获取到上市股票数据")
        return
    
    total_stocks = len(stocks)
    logger.info(f"开始处理{total_stocks}只股票的自动标签数据")

    # 处理每只股票
    for index, stock in enumerate(stocks):
        stock_code = stock.code
        stock_name = stock.code_name

        cur_date_str = f"{cur_year}-{cur_quarter}"
        logger.info(f"处理 {index+1}/{total_stocks}: {stock_code} - {stock_name} - {cur_date_str}")

        if not is_run_all:
            sql = f"""SELECT * FROM stock_auto_tags WHERE code = '{stock_code}' and statDate = '{cur_date_str}'"""
            stock_auto_tags = conn.execute(text(sql)).fetchone()
            if stock_auto_tags is not None:
                logger.debug(f"股票{stock_code}自动标签中已存在{cur_date_str}，跳过添加")
                continue

        season_positive_tags = gen_season_auto_tags_4_stock(stock_code, cur_year, cur_quarter, conn)
        positive_tags = season_positive_tags[0]
        loss_tags = season_positive_tags[1]
        pubDate = season_positive_tags[2]

        # 补充到数据库
        logger.info(f"股票{stock_code}自动标签中{cur_date_str}标签 正面{positive_tags} 负面{loss_tags}")
        stock_gen_auto_tags_dividend.save_bao_tags_2_db(stock_code, cur_date_str, pubDate, positive_tags, loss_tags, 1, conn)

def count_gen_num(conn):
    sql = f"""select 'dividend_source', count(*) from bao_stock_dividend where data_exist = 1 union
        select 'dividend_num', count(*)  from stock_auto_tags where tags_type = 2 union
        (select 'season_source', count(distinct code, statDate) from (
        select code, statDate from bao_stock_balance where data_exist = 1 union 
        select code, statDate from bao_stock_cash_flow where data_exist = 1 union 
        select code, statDate from bao_stock_growth where data_exist = 1 union 
        select code, statDate from bao_stock_operation where data_exist = 1 union 
        select code, statDate from bao_stock_profit where data_exist = 1 
        ) as temp1) union
        select 'season_num', count(*) from stock_auto_tags where tags_type = 1"""
    results = conn.execute(text(sql)).fetchall()
    for item in results:
        logger.info(f"{item[0]}: {item[1]}")

if __name__ == "__main__":
    conn = stock_common.get_db_conn(sql_echo = False)
    logger.info("开始gen_season_tags_data...")
    
    count_gen_num(conn)

    gen_season_tags_data(conn=conn, start_year=2010, end_year=2025, end_quarter="09-30", is_run_all = True)
#    gen_season_tags_data_single(conn=conn, cur_year=2025, cur_quarter="09-30")
    logger.info("结束gen_season_tags_data...")

    # ["3股息", "20净利"]
    #测试标签字段的更新
"""     conn = stock_common.get_db_conn()
    positive_tags = []
    positive_tags.append("测试")
    loss_tags = []
    loss_tags.append("测试2")
    cur_date = datetime.datetime.strptime("2023-12-31", "%Y-%m-%d").date()
    save_bao_tags_2_db("sh.600000", cur_date, positive_tags, loss_tags, conn) """
