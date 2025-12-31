import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os
import sys

# 添加项目根目录到Python路径，确保能找到cenue模块
# 当前文件路径: stock_an/cenue/auto/auto_cenue1.py
# 需要添加到: stock_an
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from cenue.util.CommissionConfig import CommissionConfig
from cenue.auto.log_config import LogConfig
from cenue.auto.db_reader import DBReader
from cenue.auto.txt_writer import TXTWriter, INDEX_CODES

"""
写一个自动交易策略回测代码，代码全部写在auto_cenue1.py文件中。   超跌股

每个交易日进行一次选股，交易。
选股条件：上市时间大于2年，
    市值大于50亿，动态PE不高于35，动态股息率不低于1%，动态PE和动态PS的1年分位值不高于20%。
    最近连续2年有分红
    ---roe大于5%。
排序：ps1年分位值的位次 + pe1年分位值的位次，按它们的和从小到大排序
买入：买入排序TOP10股票，卖出不在TOP10股票。
    最大资金1000万。
    个股最大仓位10%，分批次买入，每次交易1%总仓位，同一股票的交易要间隔5个交易日。
    每次买入的股票数量必须是100的整数倍。
    股价低于1元不买入，ST的股票不买入
卖出：动态PE的1年分位值或者动态PS的1年分位值高于70%
    分批次卖出，每次交易1%总仓位，同一股票的交易要间隔5个交易日。
    每次卖出的股票数量必须是100的整数倍，如果剩余的持仓数量不足100股，则全部卖出
    股价低于1元清仓，清仓后不再买入
    ST的股票清仓，清仓后不再买入
止盈止损：个股上涨30%开始止盈，上涨100%清仓，清仓后90天不再买入。个股下跌3%停止买入，下跌10%清仓，清仓后30天不再买入。
回测时间从2023-01-01到当前时间，
执行回测后，要生成收益与沪深300对比的曲线图
要生成按交易时间顺序的清单到txt文件，按日的持仓金额和收益清单到txt文件中，整个执行日志也写到txt文件中。文件放入logs目录，文件名称加上前缀，前缀使用当前py文件名


判断日期是否是交易日查询表bao_trade_date，股票基本信息查询表bao_stock_basic。股票日K信息查询表bao_stock_trade_0到bao_stock_trade_9，以股票最后一位数字进行分表存储的。
roe查询表bao_stock_profit，字段为code, date, roe。
表结构查询数据库stock，帐号root，密码123456。
代码中不能改数据库的表结构和更新数据库的数据。
不能查询互联网上的股票数据，只能使用数据库的数据，如果缺少数据就提示，由我人工添加提示。
"""



class BacktestEngine:
    def __init__(self):
        # 资金相关
        self.initial_capital = 1000*10000  # 初始资金，单位：元
        self.capital = self.initial_capital  # 当前可用资金，单位：元
        
        # 持仓信息，结构：{code: {'shares': int, 'avg_price': float, 'buy_price': float}}
        self.holdings = {}
        # 交易记录列表，每个元素为交易详情字典
        self.trade_records = []
        # 每日结果记录列表，每个元素为每日资产详情字典
        self.daily_records = []
        # 股票最后交易日期，结构：{code: date}
        self.last_trade_date = {}
        # 禁止买入股票，结构：{code: forbidden_date}，表示在forbidden_date之前禁止买入
        self.forbidden_buy = {}
        # 停止买入股票，结构：{code: date}，表示从date开始停止买入
        self.stop_buy = {}
        
        # 手续费配置对象
        self.commission_config = CommissionConfig()  # 初始化手续费配置
        
        # 回测时间范围
        self.start_date = '2015-01-01'  # 回测开始日期
        self.end_date = '2025-03-01'  # 回测结束日期
        
        # 交易日历
        self.trading_days = []  # 交易日列表，格式：['2023-01-03', '2023-01-04', ...]
        
        # 使用当前文件名作为前缀
        self.prefix = os.path.splitext(os.path.basename(__file__))[0]  # 日志文件名前缀，使用当前文件名
        
        # 当日交易计数器，用于统计每个工作日的交易次数
        self.daily_trade_count = 0
        
        # 初始化外部模块
        self.log_config = LogConfig(self.prefix)  # 日志配置模块
        self.db_reader = DBReader()  # 数据库读取模块
        self.txt_writer = TXTWriter(self.prefix, self.db_reader)  # 文件写入模块
    
    def close(self):
        """关闭资源"""
        self.db_reader.close()
        self.log_config.close()
    
    def get_trading_days(self):
        """获取交易日历"""
        self.trading_days = self.db_reader.get_trading_days(self.start_date, self.end_date)
        return len(self.trading_days) > 0
    

    
    def sort_stocks(self, selected_stocks, date):
        """股票排序逻辑：按ps1年分位值的位次 + pe1年分位值的位次 + 股息率的位次的和从小到大排序"""
        if not selected_stocks:
            return []
        
        # 1. 分别获取ps、pe分位值、股息率的排序位次
        # 按ps_year_1_percent从小到大排序，获取每只股票的ps位次
        ps_sorted = sorted(selected_stocks, key=lambda x: x['ps_year_1_percent'])
        ps_rank_dict = {stock['code']: i+1 for i, stock in enumerate(ps_sorted)}
        
        # 按pe_year_1_percent从小到大排序，获取每只股票的pe位次
        pe_sorted = sorted(selected_stocks, key=lambda x: x['pe_year_1_percent'])
        pe_rank_dict = {stock['code']: i+1 for i, stock in enumerate(pe_sorted)}
        
        # 按dividend_yield从大到小排序，获取每只股票的股息率位次
        dividend_sorted = sorted(selected_stocks, key=lambda x: x['dividend_yield'], reverse=True)
        dividend_rank_dict = {stock['code']: i+1 for i, stock in enumerate(dividend_sorted)}
        
        # 2. 计算每只股票的综合位次（ps位次 + pe位次 + 股息率位次）
        # 给每个股票添加综合位次属性
        for stock in selected_stocks:
            ps_rank = ps_rank_dict[stock['code']]
            pe_rank = pe_rank_dict[stock['code']]
            dividend_rank = dividend_rank_dict[stock['code']]
            stock['composite_rank'] = ps_rank + pe_rank + dividend_rank
        
        # 3. 按综合位次从小到大排序
        sorted_stocks = sorted(selected_stocks, key=lambda x: x['composite_rank'])
        
        return sorted_stocks
    
    def select_stocks(self, date, stock_basic):
        """选股逻辑"""
        # 获取所有股票的日K数据
        daily_data_list = self.db_reader.get_all_stocks_daily(date)
        if not daily_data_list:
            print(f"{date} 没有日K数据")
            return []
        
        selected = []
        for daily_data in daily_data_list:
            code = daily_data['code']
            
            # 检查基本信息
            if code not in stock_basic:
                continue
            
            # 检查上市时间
            ipo_date = stock_basic[code]['ipo_date']
            current_date = datetime.strptime(date, '%Y-%m-%d').date()
            if (current_date - ipo_date).days < 2*365:
                continue
            
            # 检查总市值
            if not daily_data['total_market_value'] or daily_data['total_market_value'] <= 50:
                continue
            
            # 检查动态PE
            if not daily_data['peTTM'] or daily_data['peTTM'] > 35:
                continue
            
            # 检查动态股息率（使用数据库中已计算好的字段）
            dividend_yield = daily_data['stock_fenghong_percent'] if daily_data['stock_fenghong_percent'] else 0
            if dividend_yield < 1:
                continue
            
            # 检查PE 1年分位值
            if not daily_data['pe_year_1_percent'] or daily_data['pe_year_1_percent'] > 20:
                continue
            
            # 检查PS 1年分位值
            if not daily_data['ps_year_1_percent'] or daily_data['ps_year_1_percent'] > 20:
                continue
            
            # 计算动态PS
            if not daily_data['psTTM']:
                continue
            
            # 检查是否有分红记录：当前交易日的前2年每年都有分红
            if not self.db_reader.has_dividend(code, date):
                continue
            
            # 检查是否为ST股票，ST股票不买入
            if daily_data['isST'] == 1:
                continue
            
            # 检查股价是否低于1元，低于1元不买入
            if daily_data['close'] < 1:
                continue
            
            # 检查roe，roe<5%不买入
            """ roe_avg = self.get_roe_avg(code, date)
            if roe_avg < 0.05:
                continue """
            
            selected.append({
                'code': code,
                'code_name': stock_basic[code]['code_name'],
                'peTTM': daily_data['peTTM'],
                'psTTM': daily_data['psTTM'],
                'total_market_value': daily_data['total_market_value'],
                'pe_year_1_percent': daily_data['pe_year_1_percent'],
                'ps_year_1_percent': daily_data['ps_year_1_percent'],
                'close': daily_data['close'],
                'dividend_yield': dividend_yield
            })
        
        # 调用排序方法
        sorted_stocks = self.sort_stocks(selected, date)
        top10 = sorted_stocks[:10]
        
        # 打印选股信息到日志
        print(f"{date} 选股完成，共选出{len(selected)}只股票，排序后选择TOP10")
        
        # 打印排序后的股票列表到日志
        print(f"{date} 排序后的股票列表（共{len(sorted_stocks)}只）：")
        for i, stock in enumerate(sorted_stocks, 1):
            print(f"  {i}. {stock['code']} {stock['code_name']} - PE: {stock['peTTM']:.2f}  PS: {stock['psTTM']:.2f} PE 1年分位值: {stock['pe_year_1_percent']:.2f}% PS 1年分位值: {stock['ps_year_1_percent']:.2f}% 市值: {stock['total_market_value']:.2f}亿 股息率: {stock['dividend_yield']:.2f}% 收盘价: {stock['close']:.2f}元")

        return top10
    
    def calculate_pnl(self, code, buy_price, current_price):
        """计算收益率"""
        if buy_price <= 0:
            return 0
        return (current_price - buy_price) / buy_price * 100
    
    def check_stop_condition(self, code, buy_price, current_price, date):
        """检查止盈止损条件"""
        pnl = self.calculate_pnl(code, buy_price, current_price)
        
        # 止盈检查
        if pnl >= 100:
            # 清仓，90天内禁止买入
            forbidden_date = self.get_future_date(date, 90)
            self.forbidden_buy[code] = forbidden_date
            return 'sell_all'  # 清仓
        elif pnl >= 30:
            return 'sell_part'  # 开始止盈
        
        # 止损检查
        if pnl <= -10:
            # 清仓，30天内禁止买入
            forbidden_date = self.get_future_date(date, 30)
            self.forbidden_buy[code] = forbidden_date
            return 'sell_all'  # 清仓
        elif pnl <= -3:
            self.stop_buy[code] = date  # 停止买入
            return 'stop_buy'  # 停止买入
        
        return 'hold'  # 持有
    
    def get_future_date(self, current_date, days):
        """获取未来N个交易日的日期"""
        current_idx = self.trading_days.index(current_date)
        future_idx = min(current_idx + days, len(self.trading_days) - 1)
        return self.trading_days[future_idx]
    
    def check_trade_interval(self, code, date):
        """检查是否满足交易间隔要求"""
        if code not in self.last_trade_date:
            return True
        last_date = self.last_trade_date[code]
        last_idx = self.trading_days.index(last_date)
        current_idx = self.trading_days.index(date)
        return (current_idx - last_idx) >= 5
    
    def execute_trade(self, date, selected_stocks, stock_basic):
        """执行交易"""
        selected_codes = {stock['code'] for stock in selected_stocks}
        current_holdings = list(self.holdings.keys())
        
        # 计算当前持仓市值
        total_value = self.capital
        for code, holding in self.holdings.items():
            daily_data = self.db_reader.get_stock_daily(code, date)
            if daily_data:
                total_value += holding['shares'] * daily_data['close']
        
        # 1. 处理卖出：动态PE的1年分位值或者动态PS的1年分位值高于70%
        for code in current_holdings:
            holding = self.holdings[code]
            daily_data = self.db_reader.get_stock_daily(code, date)
            if not daily_data:
                continue
            
            # 检查卖出条件：动态PE的1年分位值或者动态PS的1年分位值高于70%
            pe_year_1_percent = daily_data.get('pe_year_1_percent', 0)
            ps_year_1_percent = daily_data.get('ps_year_1_percent', 0)
            
            if pe_year_1_percent > 70 or ps_year_1_percent > 70:
                # 需要卖出
                if not self.check_trade_interval(code, date):
                    continue
                
                # 计算可卖出数量（每次交易1%总仓位）
                trade_value = total_value * 0.01
                sell_shares = int(trade_value / daily_data['close'])
                
                # 卖出数量必须是100的整数倍，不足100股则全部卖出
                if sell_shares > 0 and sell_shares <= holding['shares']:
                    # 调整为100的整数倍
                    if holding['shares'] < 100:
                        # 不足100股，全部卖出
                        sell_shares = holding['shares']
                    else:
                        # 调整为100的整数倍
                        sell_shares = (sell_shares // 100) * 100
                        # 确保不超过持仓数量
                        sell_shares = min(sell_shares, holding['shares'])
                        # 确保卖出数量至少为100股，除非持仓不足100股
                        if sell_shares < 100:
                            continue
                    
                    # 执行卖出
                    sell_amount = sell_shares * daily_data['close']
                    fee = self.commission_config._getcommission(-sell_shares, daily_data['close'])
                    self.capital += (sell_amount - fee)
                    
                    # 更新持仓
                    self.holdings[code]['shares'] -= sell_shares
                    self.last_trade_date[code] = date
                    
                    # 记录交易
                    trade = {
                        'date': date,
                        'code': code,
                        'code_name': stock_basic[code]['code_name'],
                        'action': 'sell',
                        'reason': f'动态PE1年分位值({pe_year_1_percent}%)或动态PS1年分位值({ps_year_1_percent}%)高于70%',
                        'price': daily_data['close'],
                        'shares': sell_shares,
                        'amount': sell_amount,
                        'fee': fee,
                        'total_shares': self.holdings[code]['shares'],
                        'capital': self.capital
                    }
                    self.trade_records.append(trade)
                    self.daily_trade_count += 1
                    
                    # 打印交易记录到日志
                    print(f"交易记录：{date} {code} {stock_basic[code]['code_name']} {trade['action']} {sell_shares}股 @ {daily_data['close']:.2f}元，金额：{sell_amount:.2f}元，手续费：{fee:.2f}元，剩余资金：{self.capital:.2f}元，原因：{trade['reason']}")
                    
                    # 如果持仓为0，移除该股票
                    if self.holdings[code]['shares'] == 0:
                        del self.holdings[code]
        
        # 2. 处理买入
        for stock in selected_stocks:
            code = stock['code']
            
            # 检查是否在禁止买入列表
            if code in self.forbidden_buy and date <= self.forbidden_buy[code]:
                continue
            
            # 检查是否停止买入
            if code in self.stop_buy:
                continue
            
            # 检查交易间隔
            if not self.check_trade_interval(code, date):
                continue
            
            daily_data = self.db_reader.get_stock_daily(code, date)
            if not daily_data:
                continue
            
            # 计算个股持仓市值
            current_value = 0
            if code in self.holdings:
                current_value = self.holdings[code]['shares'] * daily_data['close']
            
            # 检查个股最大仓位（10%）
            if current_value >= total_value * 0.1:
                continue
            
            # 计算可买入数量（每次交易1%总仓位）
            trade_value = total_value * 0.01
            buy_shares = int(trade_value / daily_data['close'])
            
            # 买入数量必须是100的整数倍
            buy_shares = (buy_shares // 100) * 100
            
            if buy_shares > 0:
                # 检查资金是否足够
                buy_amount = buy_shares * daily_data['close']
                fee = self.commission_config._getcommission(buy_shares, daily_data['close'])
                total_cost = buy_amount + fee
                
                if self.capital >= total_cost:
                    # 执行买入
                    self.capital -= total_cost
                    
                    # 更新持仓
                    if code not in self.holdings:
                        self.holdings[code] = {
                            'shares': 0,
                            'avg_price': 0,
                            'buy_price': daily_data['close']
                        }
                    
                    # 更新平均成本
                    total_shares = self.holdings[code]['shares'] + buy_shares
                    total_cost_hold = self.holdings[code]['shares'] * self.holdings[code]['avg_price']
                    new_avg_price = (total_cost_hold + buy_amount) / total_shares
                    
                    self.holdings[code]['shares'] = total_shares
                    self.holdings[code]['avg_price'] = new_avg_price
                    self.last_trade_date[code] = date
                    
                    # 记录交易
                    trade = {
                        'date': date,
                        'code': code,
                        'code_name': stock['code_name'],
                        'action': 'buy',
                        'reason': '入选TOP10',
                        'price': daily_data['close'],
                        'shares': buy_shares,
                        'amount': buy_amount,
                        'fee': fee,
                        'total_shares': total_shares,
                        'capital': self.capital
                    }
                    self.trade_records.append(trade)
                    self.daily_trade_count += 1
                    
                    # 打印交易记录到日志
                    print(f"交易记录：{date} {code} {stock['code_name']} {trade['action']} {buy_shares}股 @ {daily_data['close']:.2f}元，金额：{buy_amount:.2f}元，手续费：{fee:.2f}元，剩余资金：{self.capital:.2f}元，原因：{trade['reason']}")
        
        # 3. 检查ST股票，ST股票立即清仓
        for code in list(self.holdings.keys()):
            holding = self.holdings[code]
            daily_data = self.db_reader.get_stock_daily(code, date)
            if daily_data:
                # 检查是否为ST股票，ST股票立即清仓
                if daily_data['isST'] == 1:
                    # 清仓ST股票
                    sell_amount = holding['shares'] * daily_data['close']
                    fee = self.commission_config._getcommission(-holding['shares'], daily_data['close'])
                    self.capital += (sell_amount - fee)
                    
                    # 记录交易
                    trade = {
                        'date': date,
                        'code': code,
                        'code_name': stock_basic[code]['code_name'],
                        'action': 'sell_all',
                        'reason': 'ST股票清仓',
                        'price': daily_data['close'],
                        'shares': holding['shares'],
                        'amount': sell_amount,
                        'fee': fee,
                        'total_shares': 0,
                        'capital': self.capital
                    }
                    self.trade_records.append(trade)
                    self.daily_trade_count += 1
                    # 仅写入日志文件，不在控制台打印详细交易记录
                    # print(f"交易记录：{date} {code} {stock_basic[code]['code_name']} {trade['action']} {holding['shares']}股 @ {daily_data['close']:.2f}元，金额：{sell_amount:.2f}元，手续费：{fee:.2f}元，剩余资金：{self.capital:.2f}元，原因：{trade['reason']}")
                    
                    # 移除持仓
                    del self.holdings[code]
                # 检查股价是否低于1元，低于1元清仓且不再买入
                elif daily_data['close'] < 1:
                    # 清仓，不再买入
                    sell_amount = holding['shares'] * daily_data['close']
                    fee = self.commission_config._getcommission(-holding['shares'], daily_data['close'])
                    self.capital += (sell_amount - fee)
                    
                    # 记录交易
                    trade = {
                        'date': date,
                        'code': code,
                        'code_name': stock_basic[code]['code_name'],
                        'action': 'sell_all',
                        'reason': '股价低于1元清仓',
                        'price': daily_data['close'],
                        'shares': holding['shares'],
                        'amount': sell_amount,
                        'fee': fee,
                        'total_shares': 0,
                        'capital': self.capital
                    }
                    self.trade_records.append(trade)
                    self.daily_trade_count += 1
                    # 仅写入日志文件，不在控制台打印详细交易记录
                    # print(f"交易记录：{date} {code} {stock_basic[code]['code_name']} {trade['action']} {holding['shares']}股 @ {daily_data['close']:.2f}元，金额：{sell_amount:.2f}元，手续费：{fee:.2f}元，剩余资金：{self.capital:.2f}元，原因：{trade['reason']}")
                    
                    # 禁止买入该股票
                    # 设置一个很远的禁止日期，相当于永久禁止
                    self.forbidden_buy[code] = self.trading_days[-1] if self.trading_days else date
                    
                    # 移除持仓
                    del self.holdings[code]
        
        # 4. 检查止盈止损
        for code in list(self.holdings.keys()):
            holding = self.holdings[code]
            daily_data = self.db_reader.get_stock_daily(code, date)
            if not daily_data:
                continue
            
            action = self.check_stop_condition(code, holding['buy_price'], daily_data['close'], date)
            if action == 'sell_all':
                # 清仓，全部卖出
                sell_shares = holding['shares']
                
                # 执行卖出
                sell_amount = sell_shares * daily_data['close']
                fee = self.commission_config._getcommission(-sell_shares, daily_data['close'])
                self.capital += (sell_amount - fee)
                
                # 记录交易
                trade = {
                    'date': date,
                    'code': code,
                    'code_name': stock_basic[code]['code_name'],
                    'action': 'sell_all',
                    'reason': '止盈/止损清仓',
                    'price': daily_data['close'],
                    'shares': holding['shares'],
                    'amount': sell_amount,
                    'fee': fee,
                    'total_shares': 0,
                    'capital': self.capital
                }
                self.trade_records.append(trade)
                self.daily_trade_count += 1
                # 仅写入日志文件，不在控制台打印详细交易记录
                # print(f"交易记录：{date} {code} {stock_basic[code]['code_name']} {trade['action']} {holding['shares']}股 @ {daily_data['close']:.2f}元，金额：{sell_amount:.2f}元，手续费：{fee:.2f}元，剩余资金：{self.capital:.2f}元，原因：{trade['reason']}")
                
                # 移除持仓
                del self.holdings[code]
            elif action == 'sell_part':
                # 止盈卖出部分（1%总仓位）
                if not self.check_trade_interval(code, date):
                    continue
                
                trade_value = (self.capital + sum(h['shares'] * self.db_reader.get_stock_daily(code, date)['close'] for code, h in self.holdings.items())) * 0.01
                sell_shares = int(trade_value / daily_data['close'])
                
                # 卖出数量必须是100的整数倍，不足100股则全部卖出
                if sell_shares > 0 and sell_shares <= holding['shares']:
                    # 调整为100的整数倍
                    if holding['shares'] < 100:
                        # 不足100股，全部卖出
                        sell_shares = holding['shares']
                    else:
                        # 调整为100的整数倍
                        sell_shares = (sell_shares // 100) * 100
                        # 确保不超过持仓数量
                        sell_shares = min(sell_shares, holding['shares'])
                        # 确保卖出数量至少为100股，除非持仓不足100股
                        if sell_shares < 100:
                            continue
                    
                    # 执行卖出
                    sell_amount = sell_shares * daily_data['close']
                    fee = self.commission_config._getcommission(-sell_shares, daily_data['close'])
                    self.capital += (sell_amount - fee)
                    
                    # 更新持仓
                    self.holdings[code]['shares'] -= sell_shares
                    self.last_trade_date[code] = date
                    
                    # 记录交易
                    trade = {
                        'date': date,
                        'code': code,
                        'code_name': stock_basic[code]['code_name'],
                        'action': 'sell_profit',
                        'reason': '止盈卖出部分',
                        'price': daily_data['close'],
                        'shares': sell_shares,
                        'amount': sell_amount,
                        'fee': fee,
                        'total_shares': self.holdings[code]['shares'],
                        'capital': self.capital
                    }
                    self.trade_records.append(trade)
                    self.daily_trade_count += 1
                    # 仅写入日志文件，不在控制台打印详细交易记录
                    # print(f"交易记录：{date} {code} {stock_basic[code]['code_name']} {trade['action']} {sell_shares}股 @ {daily_data['close']:.2f}元，金额：{sell_amount:.2f}元，手续费：{fee:.2f}元，剩余资金：{self.capital:.2f}元，原因：{trade['reason']}")
    
    def calculate_daily_result(self, date):
        """计算每日结果"""
        # 计算持仓市值
        position_value = 0
        holding_details = []
        
        for code, holding in self.holdings.items():
            daily_data = self.db_reader.get_stock_daily(code, date)
            if daily_data:
                current_price = daily_data['close']
                holding_value = holding['shares'] * current_price
                position_value += holding_value
                pnl = self.calculate_pnl(code, holding['buy_price'], current_price)
                holding_details.append({
                    'code': code,
                    'shares': holding['shares'],
                    'avg_price': holding['avg_price'],
                    'current_price': current_price,
                    'holding_value': holding_value,
                    'pnl': pnl
                })
        
        # 计算总资产
        total_asset = self.capital + position_value
        
        # 计算累计收益率
        total_return = (total_asset - self.initial_capital) / self.initial_capital * 100
        
        # 记录每日结果
        self.daily_records.append({
            'date': date,
            'capital': self.capital,
            'position_value': position_value,
            'total_asset': total_asset,
            'total_return': total_return
        })
        
        # 仅在控制台打印一行每日汇总信息
        # 格式：日期，可用资金，总市值，累计收益率，持仓股票数量，当日交易数量
        print(f"{date} | 可用资金：{self.capital:.2f} | 总市值：{total_asset:.2f} | 累计收益率：{total_return:.2f}% | 持仓股票数：{len(self.holdings)} | 当日交易数：{self.daily_trade_count}")
    

    
    def run_backtest(self):
        """运行回测"""
        print("开始回测...")
        
        # 连接数据库
        if not self.db_reader.connect_db():
            return False
        
        # 获取交易日历
        if not self.get_trading_days():
            return False
        
        if not self.trading_days:
            print("没有获取到交易日历，无法进行回测")
            return False
        
        # 一次性加载所有日K数据到内存
        if not self.db_reader.load_all_stock_daily_data(self.start_date, self.end_date):
            return False
        
        # 一次性加载所有roe数据到内存
        """ if not self.load_all_roe_data():
            return False """
        
        # 一次性加载所有分红数据到内存
        if not self.db_reader.load_dividend_data(self.start_date, self.end_date):
            return False
        
        # 获取股票基本信息
        stock_basic = self.db_reader.get_stock_basic()
        if not stock_basic:
            print("没有获取到股票基本信息，无法进行回测")
            return False
        
        # 遍历每个交易日
        for i, date in enumerate(self.trading_days):
            # 重置当日交易计数器
            self.daily_trade_count = 0
            
            # 选股
            selected_stocks = self.select_stocks(date, stock_basic)
            
            # 执行交易
            self.execute_trade(date, selected_stocks, stock_basic)
            
            # 计算每日结果
            self.calculate_daily_result(date)
        
        return True
    


    def run(self):
        """执行完整回测流程"""
        print("开始执行完整回测流程")
        # 运行回测
        try:
            if self.run_backtest():
                # 生成报告
                self.txt_writer.generate_trade_report(self.trade_records)
                self.txt_writer.generate_daily_report(self.daily_records)
                self.txt_writer.generate_equity_curve(self.daily_records, self.start_date, self.end_date)
                
                if self.daily_records:
                    print("回测完成！")
                    print(f"初始资金: {self.initial_capital:.2f}")
                    print(f"最终资金: {self.daily_records[-1]['total_asset']:.2f}")
                    print(f"总收益率: {self.daily_records[-1]['total_return']:.2f}%")
                else:
                    print("回测完成，但没有生成每日记录")
            else:
                print("回测失败")
        finally:
            # 关闭日志文件
            self.close()

if __name__ == '__main__':
    backtest = BacktestEngine()
    backtest.run()