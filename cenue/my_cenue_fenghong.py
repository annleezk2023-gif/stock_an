import pandas as pd
import numpy as np
import backtrader as bt
from dateutil.relativedelta import relativedelta

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

# 配置logger
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

"""
成长：净利率20%+。销售额增长，现金流增长。

分红策略
1，从开始日期和结束日期取交易的日期列表
2，按每个交易日取全部股票当天的K线数据
    2.1，对股票进行过滤，上市时间，ST等过滤条件（上市时间大于2年，非ST，流动市值>50亿，销售额2年正增长）
    2.2，对剩下的股票进行分析，打分，排序（加分原则：1/3/5股息率，3年2次/3年连续/5年3次/5年连续分红&拆股，销售额稳定，现金流稳定）
    2.3，满足交易条件进行交易
        止损：下跌5%停止买入，下跌10%全部卖出-保留100股
        止盈：PE&PS有一个涨到70%分位开始卖出，2个70%分位卖出50%；有一个到90%分位卖出100%
        交易：
            1，每个交易日，取所有股票中，打分最高的20%，不限数量，购买价格为当前价格
    2.4，结束后仓位、盈利分析
    2.4，结束后仓位、盈利分析
3，结束后，生成结果表格日期，持仓情况
"""


# 交易参数配置
class StrategyConfig:
    #筛选配置：
    listing_years_threshold = 2  # 上市时间阈值（年）
    market_cap_threshold = 50*10000*10000  # 筛选市值阈值（亿元）
    pe_threshold = 35  # PE阈值，高于此值时不买入
    fenghong_percent_threshold = 1   #股息率阈值，低于此值时不买入
    pe_pct_threshold = 0.2  # PE分位阈值，低于此值时开始买入
    ps_pct_threshold = 0.2  # PS分位阈值，低于此值时开始买入

    #交易配置：
    trading_interval = 1  # 每1个交易日交易一次
    max_holdings = 30  # 最大持仓股票数量
    stop_trade_day = 5	#止损止盈后暂停买入时长

    #交易配置：止盈
    profit_threshold_pe_start = 0.7  # 开始止盈阈值（PE分位），即此时开始卖出
    profit_threshold_pe_end = 0.9  # 清仓止盈阈值（PE分位），即此时全部卖出

    #交易配置：止损
    sell_threshold_initial = 0.03  # 开始止损阈值（3%），即此时不再买入
    sell_threshold_all = 0.10  # 清仓止损阈值（10%），此时全部卖出
    sell_market_cap_threshold = 30  # 清仓市值阈值（亿元），低于此值时全部卖出

    #订单配置：买入
    initial_buy_num = 100  # 首次买入股票数量
    add_buy_pct = 0.01  # 加仓比例
    max_position_pct = 0.2  # 单个股票最大仓位比例
    buy_num = 100  # 每次买入股票数量基数
    top_buy_num = 2  # 行业TOP公司2倍买入

    #订单配置：买出
    initial_sell_pct = 0.01  # 首次卖出比例
    add_sell_pct = 0.01  # 后续卖出比例
    min_position = 0  # 最低仓位（1%）
    sell_num = 100  # 每次卖出股票数量基数

class SortConfig:
    turn_weight = 0.5  # 成交量排序权重
    pe_1_year_weight = 0.3  # 1年PE排序权重
    ps_1_year_weight = 0.2  # 1年PS排序权重
    pe_5_year_weight = 0.2  # 5年PE排序权重
    ps_5_year_weight = 0.1  # 5年PS排序权重
    pe_10_year_weight = 0.1  # 10年PE排序权重
    ps_10_year_weight = 0.1  # 10年PS排序权重
    income_1_year_weight = 0.1  # 1年营收增长率排序权重
    profit_1_year_weight = 0.1  # 1年利润增长率排序权重
    cash_1_year_weight = 0.1  # 1年现金流增长率排序权重
    roe_1_year_weight = 0.1  # 1年ROE排序权重
    devidend_1_year_weight = 0.1  # 1年股息率排序权重
    gross_margin_1_year_weight = 0.1  # 1年毛利率排序权重
    net_profit_1_year_weight = 0.1  # 1年净利润增长率排序权重
    total_market_value_weight = 0.1  # 总市值排序权重
    hold_fund_percent_weight = 0.1  # 基金持仓占比排序权重


# 交易策略
class TradeStrategy():
    def __init__(self, cur_trade_date_str, stock_info_list, trade_strategy_record_list, k_line_list):
        self.cur_trade_date_str = cur_trade_date_str #交易日期
        self.stock_info_list = stock_info_list.copy() #股票信息列表，复制一份股票信息列表，避免修改原始数据
        self.trade_strategy_record_list = trade_strategy_record_list #交易策略记录列表
        self.k_line_list = k_line_list #k线数据列表

        # 股票信息列表转map
        self.stock_info_map = {}
        for stock_info in stock_info_list:
            self.stock_info_map[stock_info['stock_code']] = stock_info

        # 交易记录列表转map
        self.trade_strategy_record_map = {}
        for trade_record in trade_strategy_record_list:
            self.trade_strategy_record_map[trade_record['hold_date']] = trade_record
        # k线数据列表转map
        self.k_line_map = {}
        for k_line in k_line_list:
            self.k_line_map[k_line['code']] = k_line

        # 计数器，用于控制交易频率
        self.trading_counter = 0
        # 记录每个股票的交易次数
        self.stock_trade_count = {}
        # 记录每个股票的买入价格
        self.buy_prices = {}
        # 交易日期列表
        self.trade_dates = []
        # 沪深300指数数据
        self.hs300_index = None
        
        # 初始化指标
        self.setup_indicators()

    def run(self):
        # 1过滤股票
        filtered = self.filter_stocks()
        logger.info(f"在{self.cur_trade_date_str}筛选出{len(filtered)}只股票")
        # 2打分，排序，选择股票
        filtered = self.sort_stocks()
        logger.info(f"在{self.cur_trade_date_str}排序出{len(filtered)}只股票")

        #3执行交易
        self.execute_trade()
        #4计算盈利

        #5存档：投入资金，当前持仓，当前市值，盈利，交易历史

    
    def execute_trade(self):
        logger.info(f"在 {self.cur_trade_date_str} 执行交易")

        # 计算是否需要交易
        
        
        # 计算目标仓位（基于百分位）
        target_position = self.calculate_target_position()
        logger.info(f"目标仓位: {target_position:.2%}")
        
        # 获取股票基本面信息

        # 卖出操作
        self.sell_operations()
        
        # 买入操作
        self.buy_operations(target_position)
    
    def calculate_target_position(self):
        """计算目标仓位"""

        """ # 获取上证指数3年百分位
        percentile = StockFundamental.get_index_percentile(current_date)
        logger.info(f"上证指数3年百分位: {percentile:.2f}%")
        
        # 百分位越高，仓位越低，最低20%仓位
        # 0%百分位 -> 100%仓位
        # 100%百分位 -> 0%仓位
        target_position = max(0, 1.0 - 0.008 * percentile) """
        
        return 1
    
    def sell_operations(self, stock_basic_df, current_date):
        """执行卖出操作"""
        
    
    def buy_operations(self, stock_basic_df, current_date, target_position):
        """执行买入操作"""
        # 计算可用资金
        
    
    def filter_stocks(self):
        """根据条件过滤股票"""
        cur_date = datetime.datetime.strptime(self.cur_trade_date_str, '%Y-%m-%d')
        #计算2年前
        two_years_ago = cur_date.replace(year=cur_date.year - StrategyConfig.listing_years_threshold)
        #处理闰年 2 月 29 日（如 2024-02-29 减 2 年应转为 2022-02-28）
        if cur_date.month == 2 and cur_date.day == 29:
            # 检查新年份是否为闰年，不是则调整为 2 月 28 日
            if not (two_years_ago.year % 4 == 0 and (two_years_ago.year % 100 != 0 or two_years_ago.year % 400 == 0)):
                two_years_ago = two_years_ago.replace(day=28)
        
        # 使用切片 [:] 创建一个完整的副本，删除元素不会错误
        for stock_info in self.stock_info_list[:]:
            # 1过滤ST股票，在交易数据中过滤

            # 2过滤上市时间
            if pd.to_datetime(stock_info['ipo_date']) > two_years_ago: #上市时间在2年前的之后
                self.stock_info_list.remove(stock_info)
                continue
            #2.2过滤退市时间
            if pd.to_datetime(stock_info['out_date']) < cur_date: #退市时间在当前日期之前
                self.stock_info_list.remove(stock_info)
                continue

            # 3过滤分红标签
            has_fenghong_tag = False
            stock_auto_tags = get_stock_tags_by_trade_date(conn, stock_info.code, self.cur_trade_date_str, tag_type=2)
            if stock_auto_tags:
                if stock_auto_tags.bao_tags_loss:
                    bao_tags_loss = stock_auto_tags.bao_tags_loss
                    if len(bao_tags_loss) > 0: #有负面标签
                        self.stock_info_list.remove(stock_info)
                        continue
                if stock_auto_tags.bao_tags_positive:
                    bao_tags_positive = stock_auto_tags.bao_tags_positive
                    if len(bao_tags_positive) > 0: #有正面标签
                        has_fenghong_tag = True
                        break
            if not has_fenghong_tag:
                self.stock_info_list.remove(stock_info)
                continue

            # 4过滤市值
            k_line = self.k_line_map.get(stock_info.code)
            if not k_line: #没有k线数据
                self.stock_info_list.remove(stock_info)
                continue
            if not k_line.total_market_value: #k线数据没有市值
                self.stock_info_list.remove(stock_info)
                continue
            # 4.1过滤市值
            if k_line.total_market_value < StrategyConfig.market_cap_threshold: #市值小于配置市值阈值
                self.stock_info_list.remove(stock_info)
                continue
            
            # 5过滤pe
            if not k_line.peTTM: #k线数据没有peTTM
                self.stock_info_list.remove(stock_info)
                continue
            if k_line.peTTM > StrategyConfig.pe_threshold: #peTTM大于配置pe阈值
                self.stock_info_list.remove(stock_info)
                continue

            # 5.1过滤股息率
            if not k_line.stock_fenghong_percent: #k线数据没有股息率
                self.stock_info_list.remove(stock_info)
                continue
            if k_line.stock_fenghong_percent < StrategyConfig.fenghong_percent_threshold: #股息率小于配置股息率阈值
                self.stock_info_list.remove(stock_info)
                continue

            #5.2过滤ST
            if k_line.isST and k_line.isST == 1: #ST股票 
                self.stock_info_list.remove(stock_info)
                continue

            #5.3过滤pe,ps百分位
            if k_line.pe_year_1_percent and k_line.pe_year_1_percent > StrategyConfig.pe_pct_threshold: #pe百分位大于配置pe百分位阈值
                self.stock_info_list.remove(stock_info)
                continue
            if k_line.ps_year_1_percent and k_line.ps_year_1_percent > StrategyConfig.ps_pct_threshold: #ps百分位大于配置ps百分位阈值
                self.stock_info_list.remove(stock_info)
                continue
        return self.stock_info_list
    
    def sort_stocks(self):
        # 己选好的股票进行排序
        """ fenghong_tags = get_stock_tags_by_trade_date(conn, cur_trade_date_str, tags_type=2)
        # 个股的运营标签
        season_tags = get_stock_tags_by_trade_date(conn, cur_trade_date_str, tags_type=1) """
        return self.stock_info_list

    def notify_order(self, order):
       return order
    



    # 回测执行函数
def run_backtest(conn, start_date_str, end_date_str, strategy_code):
    # 1股票列表
    stock_info_list = get_all_stocks(conn)
    # 转map
    stock_info_map = {r['code']: r for r in stock_info_list}
    logger.info(f"筛选出{len(stock_info_list)}只股票")

    # 2交易日列表
    trade_date_list = get_all_trade_dates(conn, start_date_str, end_date_str)
    trade_date_length = len(trade_date_list)
    logger.info(f"筛选出{trade_date_length}个交易日")

    # 3己执行的存档交易记录
    trade_strategy_record_list = get_all_trade_strategy_records(conn, strategy_code)
    # 转map，key=hold_date
    trade_strategy_record_map = {r.hold_date: r for r in trade_strategy_record_list}
    logger.info(f"筛选出{len(trade_strategy_record_map)}条交易日期记录")
    # 持仓情况转map，key=stock_code
    stock_hold_list = trade_strategy_record_list[-1].stock_hold_list
    stock_hold_map = {r.stock_code: r for r in stock_hold_list}
    logger.info(f"筛选出{len(stock_hold_map)}只持仓股票")

    # 4开始按日执行交易
    for index, item in enumerate(trade_date_list):
        cur_trade_date_str = item.strftime('%Y-%m-%d')
        logger.debug(f"{index+1}/{trade_date_length} 开始执行 {cur_trade_date_str}")

        # 4.1直接定位到己执行的存档记录
        trade_strategy_record = trade_strategy_record_map.get(cur_trade_date_str, None)
        if trade_strategy_record:
            logger.debug(f"{index+1}/{trade_date_length} 在存档中己执行{cur_trade_date_str}")
            continue
        
        # 4.2获取当天全部的股票交易数据
        k_line_list = get_k_line_by_date(conn, cur_trade_date_str)

        # 4.3执行策略
        tradeStrategy = TradeStrategy(cur_trade_date_str, stock_info_list, trade_strategy_record_list, k_line_list)
        tradeStrategy.run()

    # 5，执行结束，打印结果
    print(f"回测结束资金: {final_value}")
    print(f"收益率: {(final_value - initial_capital)/initial_capital*100:.2f}%")
        
    # 添加股票数据
    count = 0
    max_stocks = 20  # 限制最大股票数量
    for stock_code in small_cap_stocks:
        if count >= max_stocks:
            break
            
        try:
            # 获取股票历史数据
            df = ts.pro_bar(ts_code=stock_code, adj='qfq', 
                           start_date=start_date, end_date=end_date)
            if df is None or len(df) < 60:
                continue
                
            # 转换数据格式
            df = df.sort_values('trade_date')
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df.set_index('trade_date', inplace=True)
            df = df[['open', 'high', 'low', 'close', 'vol']]
            df.columns = ['open', 'high', 'low', 'close', 'volume']
            
            # 添加到回测引擎
            data = bt.feeds.PandasData(dataname=df, name=stock_code)
            cerebro.adddata(data)
            count += 1
        except Exception as e:
            print(f"获取{stock_code}数据出错: {e}")
            continue
    
    print(f"成功添加{count}只股票数据进行回测")


if __name__ == '__main__':
    conn = stock_common.get_db_conn(sql_echo=True)
    # 运行回测
    run_backtest(conn, start_date_str='2010-01-01', end_date_str='2023-12-31', strategy_code='my_cenue_fenghong')

    conn.close()
