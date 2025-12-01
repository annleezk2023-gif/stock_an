import sys
import os
from sqlalchemy import create_engine, text

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common

from app import db, BaoStockBasic, StockBasicAna
from flask import Flask

# 配置logger
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 数据库连接信息
DATABASE_URI = os.getenv('DATABASE_URL', 'mysql+mysqlconnector://root:123456@localhost/stock?charset=utf8mb4')

# 初始化Flask应用上下文（用于独立运行此脚本）
def init_app_context():
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'mysql+mysqlconnector://root:123456@localhost/stock')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)
    return app.app_context()

# 补充分析数据
def stock_basic_ana_data_gen():
    with init_app_context():
        engine = create_engine(DATABASE_URI, echo=True)
        with engine.connect() as conn:
            stocks = stock_common.get_stock_info_all(conn)

            # 补充数据
            for cur_stock in stocks:
                trade_table_name = "bao_stock_trade_" + cur_stock.code[-1]

                stock_ana = StockBasicAna.query.filter_by(code=cur_stock.code).first()

                if not stock_ana:
                    # 创建新记录
                    stock_ana = StockBasicAna(
                        code=cur_stock.code
                    )
                    db.session.add(stock_ana)

                #开始查询和更新数据
                # 查询最后一个交易日的数据
                sql = f"""SELECT * FROM {trade_table_name} WHERE code = '{cur_stock.code}' order by date desc limit 1"""
                result_trade = conn.execute(text(sql)).fetchone()

                # 查询经营数据
                # 最近1季营收
                sql = f"""SELECT * FROM bao_stock_profit WHERE code = '{cur_stock.code}' and data_exist = 1 order by year desc, quarter desc limit 1"""
                result_profit = conn.execute(text(sql)).fetchone()

                if result_profit:
                    stock_ana.epsTTM = result_profit.epsTTM/result_profit.quarter * 4 # 每股收益TTM
                    stock_ana.npMargin = result_profit.npMargin*100 if result_profit.npMargin else None  # 最后一季度净利润率
                    stock_ana.gpMargin = result_profit.gpMargin*100 if result_profit.gpMargin else None  # 最后一季度毛利率
                    stock_ana.roeAvg = result_profit.roeAvg*100 if result_profit.roeAvg else None  # 最近1季度净资产收益率(平均)
                    if result_profit.totalShare and result_trade.close:
                        stock_ana.curTotalprice = result_profit.totalShare * result_trade.close  # 当前总市值     

                    # 再上一年当季总营收
                    sql = f"""SELECT * FROM bao_stock_profit WHERE code = '{cur_stock.code}' and year = {result_profit.year-1} and quarter = {result_profit.quarter} and data_exist = 1"""
                    result_profit_pre_season = conn.execute(text(sql)).fetchone()
                    if result_profit_pre_season and result_profit_pre_season.MBRevenue and result_profit.MBRevenue:
                        # 2 营收同比
                        stock_ana.MBRevenueRate = (result_profit.MBRevenue - result_profit_pre_season.MBRevenue)/result_profit_pre_season.MBRevenue

                # 最近1季净利润同比
                sql = f"""SELECT * FROM bao_stock_growth WHERE code = '{cur_stock.code}' and data_exist = 1 order by year desc, quarter desc limit 1"""
                result_growth = conn.execute(text(sql)).fetchone()

                if result_growth and result_growth.YOYNI:
                    stock_ana.netProfitRate = result_growth.YOYNI  # 最近1季度净利润同比增长率

                # 查询每股收益，股息率，最近1年
                sql = f"""select * from bao_stock_dividend where code = '{cur_stock.code}' and dividOperateDate > DATE_SUB(CURDATE(), INTERVAL 1 YEAR) and data_exist = 1"""
                result_dividend = conn.execute(text(sql)).fetchall()

                total_dividCashPsBeforeTax = 0 # 最近1年总股息
                if result_dividend:
                    for item in result_dividend:
                        total_dividCashPsBeforeTax += item.dividCashPsBeforeTax if item.dividCashPsBeforeTax else 0
                # 3 最近1年总股息
                stock_ana.dividCashPsBeforeTax = total_dividCashPsBeforeTax 
                # 4 最近1年股息率
                if result_trade and result_trade.close and result_trade.close > 0:
                    stock_ana.dividCashPsPercent = total_dividCashPsBeforeTax/result_trade.close*100  


                #  `jijinNum` int(11) DEFAULT NULL COMMENT '持仓基金数量',
                #  `jijinPercent` float DEFAULT NULL COMMENT '基金持仓百分比',

                # 标准PE10，以下数据加PE，销售额增长大于10%，每多10%加5PE，按增长率对比PE。
                stock_ana.tradeBuyPE = 10
                if stock_ana.MBRevenueRate:
                    if stock_ana.MBRevenueRate > 0.5:
                        stock_ana.tradeBuyPE += 25
                    elif stock_ana.MBRevenueRate > 0.4:
                        stock_ana.tradeBuyPE += 20
                    elif stock_ana.MBRevenueRate > 0.3:
                        stock_ana.tradeBuyPE += 15   
                    elif stock_ana.MBRevenueRate > 0.2:
                        stock_ana.tradeBuyPE += 10
                    elif stock_ana.MBRevenueRate > 0.1:
                        stock_ana.tradeBuyPE += 5
                
                # 净利率高于10%，每多10%加5PE。
                if stock_ana.netProfitRate:
                    if stock_ana.netProfitRate > 0.5:
                        stock_ana.tradeBuyPE += 25
                    elif stock_ana.netProfitRate > 0.4:
                        stock_ana.tradeBuyPE += 20
                    elif stock_ana.netProfitRate > 0.3:
                        stock_ana.tradeBuyPE += 15
                    elif stock_ana.netProfitRate > 0.2:
                        stock_ana.tradeBuyPE += 10
                    elif stock_ana.netProfitRate > 0.1:
                        stock_ana.tradeBuyPE += 5
                
                # roe 20以上，每多10%加5PE。
                if stock_ana.roeAvg:
                    if stock_ana.roeAvg > 50:
                        stock_ana.tradeBuyPE += 20
                    elif stock_ana.roeAvg > 40:
                        stock_ana.tradeBuyPE += 15
                    elif stock_ana.roeAvg > 30:
                        stock_ana.tradeBuyPE += 10
                    elif stock_ana.roeAvg > 20:
                        stock_ana.tradeBuyPE += 5
        
                stock_ana.tradeBuyAllPE = stock_ana.tradeBuyPE - 5
                stock_ana.tradeSalePE = stock_ana.tradeBuyPE * 2
                stock_ana.tradeSaleAllPE = stock_ana.tradeSalePE + 5

            db.session.commit()
            

if __name__ == "__main__":
    logger.info("开始补充stock_basic_ana表数据...")
    # 设置起始日期为2007年1月1日，结束日期为今天
    stock_basic_ana_data_gen()
    logger.info("补充stock_basic_ana表数据完成！")