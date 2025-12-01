import backtrader as bt
import pandas as pd
import pymysql
import numpy as np
import datetime
import logging
from backtrader import indicators as btind
from backtrader.utils import date2num

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MySQL连接配置
MYSQL_CONFIG = {
    'host': 'localhost',  # 假设数据库在本地
    'user': 'root',  # 请修改为实际用户名
    'password': '123456',  # 请修改为实际密码
    'database': 'stock',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# 交易参数配置
class StrategyConfig:
    trading_interval = 4  # 每4个交易日交易一次
    max_holdings = 30  # 最大持仓数量
    initial_buy_pct = 0.01  # 首次买入比例
    add_buy_pct = 0.02  # 加仓比例
    max_position_pct = 0.1  # 单个股票最大仓位比例
    initial_sell_pct = 0.1  # 首次卖出比例
    add_sell_pct = 0.2  # 后续卖出比例
    min_position = 0.01  # 最低仓位（1%）
    market_cap_threshold = 50  # 市值阈值（亿元）
    listing_years_threshold = 2  # 上市时间阈值（年）
    profit_threshold = 0.5  # 止盈阈值（50%）
    buy_num = 5  # 每次买入股票数量
    sell_num = 5  # 每次卖出股票数量

# 交易费用配置
class CommissionConfig:
    commission = 0.0002  # 佣金率，万分之2
    min_commission = 5  # 最低佣金
    stamp_duty = 0.001  # 印花税（卖出时），千分之1
    transfer_fee = 0.00002  # 过户费，十万分之2

# 自定义佣金类
class ChineseStockCommission(bt.CommInfoBase):
    params = (
        ('commission', CommissionConfig.commission),
        ('stocklike', True),
        ('commtype', bt.CommInfoBase.COMM_PERC),
        ('mincommission', CommissionConfig.min_commission),
        ('stamp_duty', CommissionConfig.stamp_duty),
        ('transfer_fee', CommissionConfig.transfer_fee),
    )

    def _getcommission(self, size, price, pseudoexec):
        # 计算佣金
        comm = abs(size) * price * self.p.commission
        # 最低佣金限制
        comm = max(comm, self.p.mincommission)
        # 卖出时收取印花税
        if size < 0:
            comm += abs(size) * price * self.p.stamp_duty
        # 过户费
        comm += abs(size) * price * self.p.transfer_fee
        return comm

# 自定义数据源 - 从MySQL获取数据
class MySQLData(bt.feeds.PandasData):
    params = (
        ('fromdate', datetime.datetime(2023, 1, 1)),
        ('todate', datetime.datetime.now()),
        ('symbol', None),
    )

    def __init__(self):
        # 创建一个空的DataFrame作为初始dataname
        import pandas as pd
        self.p.dataname = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
        self.p.dataname['date'] = pd.to_datetime(self.p.dataname['date'])
        self.p.dataname.set_index('date', inplace=True)
        
        # 调用父类初始化
        super().__init__()
        
        if not self.p.symbol:
            raise ValueError("必须指定股票代码")

    def start(self):
        # 先获取数据
        self.dataframe = self._fetch_data_from_mysql()
        if self.dataframe.empty:
            logger.warning(f"未获取到股票 {self.p.symbol} 的数据")
        else:
            # 将获取的数据设置为dataname
            self.p.dataname = self.dataframe
        
        # 调用父类的start方法
        super().start()

    def _get_table_name(self):
        """根据股票代码的最后一位确定表名"""
        # 获取股票代码的最后一位数字
        last_char = self.p.symbol[-1]
        # 如果最后一位不是数字，默认使用表0
        if not last_char.isdigit():
            table_index = 0
        else:
            table_index = int(last_char)
        return f"bao_stock_trade_{table_index}"

    def _fetch_data_from_mysql(self):
        try:
            conn = pymysql.connect(**MYSQL_CONFIG)
            try:
                with conn.cursor() as cursor:
                    # 根据股票代码获取对应的分表名
                    table_name = self._get_table_name()
                    logger.info(f"为股票 {self.p.symbol} 使用表 {table_name}")
                    
                    # 使用正确的表名和字段名
                    # 注意：表名不能作为参数传递，需要直接拼接到SQL语句中
                    sql = f"""
                    SELECT date, open, high, low, close, volume, amount 
                    FROM {table_name} 
                    WHERE code = %s AND date >= %s AND date <= %s 
                    ORDER BY date ASC
                    """
                    cursor.execute(sql, (self.p.symbol, self.p.fromdate, self.p.todate))
                    data = cursor.fetchall()
                    
                    if not data:
                        return pd.DataFrame()
                    
                    df = pd.DataFrame(data)
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                    
                    # 重命名列以符合backtrader要求
                    df.rename(columns={
                        'open': 'open',
                        'high': 'high',
                        'low': 'low',
                        'close': 'close',
                        'volume': 'volume',
                        'amount': 'openinterest'  # backtrader要求的列
                    }, inplace=True)
                    

                    
                    return df
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"获取股票 {self.p.symbol} 数据时出错: {e}")
            return pd.DataFrame()

# 获取股票基本面信息
class StockFundamental:
    @staticmethod
    def get_stock_basic_info(date):
        dateStr = date.strftime("%Y-%m-%d")
        """获取股票基本信息，包括市值、上市日期等"""
        try:
            conn = pymysql.connect(**MYSQL_CONFIG)
            try:
                with conn.cursor() as cursor:
                    sql = f""""""
                    for divide_table_num in range(0, 10, 1):
                        sql_sub = f"""SELECT a.code, a.code_name, a.ipo_date, 0 as is_st, 
                            (select total_market_value from bao_stock_trade_{divide_table_num} where code = a.code and date = {dateStr}) as market_cap
                            FROM bao_stock_basic a where RIGHT(a.code, 1) = '{divide_table_num}'
                        """
                        if divide_table_num == 9:
                            sql = sql + sql_sub
                        else:
                            sql = sql + sql_sub + " UNION "

                    cursor.execute(sql)
                    data = cursor.fetchall()
                    return pd.DataFrame(data)
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"获取股票基本信息时出错: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def get_index_level(date):
        """获取大盘指数（上证指数）"""
        try:
            conn = pymysql.connect(**MYSQL_CONFIG)
            try:
                with conn.cursor() as cursor:
                    sql = """
                    SELECT close 
                    FROM bao_nostock_trade 
                    WHERE code = 'sh.000001' AND date = %s
                    """
                    cursor.execute(sql, (date,))
                    result = cursor.fetchone()
                    return result['close'] if result else 0
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"获取大盘指数时出错: {e}")
            return 0
    
    @staticmethod
    def get_index_percentile(date, years=3):
        """获取上证指数的3年百分位"""
        try:
            conn = pymysql.connect(**MYSQL_CONFIG)
            try:
                with conn.cursor() as cursor:
                    # 计算3年前的日期
                    start_date = date - pd.Timedelta(days=365*years)
                    
                    # 获取3年历史数据
                    sql = """
                    SELECT close 
                    FROM bao_nostock_trade 
                    WHERE code = 'sh.000001' 
                    AND date >= %s AND date <= %s
                    ORDER BY date ASC
                    """
                    cursor.execute(sql, (start_date, date))
                    history_data = cursor.fetchall()
                    
                    if not history_data:
                        return 50.0  # 默认返回50%百分位
                    
                    # 获取当前值
                    sql_current = """
                    SELECT close 
                    FROM bao_nostock_trade 
                    WHERE code = 'sh.000001' AND date = %s
                    """
                    cursor.execute(sql_current, (date,))
                    current_result = cursor.fetchone()
                    
                    if not current_result:
                        return 50.0
                    
                    current_value = current_result['close']
                    
                    # 计算百分位
                    history_values = [d['close'] for d in history_data]
                    percentile = np.sum(np.array(history_values) <= current_value) / len(history_values) * 100
                    
                    return percentile
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"获取上证指数百分位时出错: {e}")
            return 50.0  # 出错时返回50%百分位

# 量化交易策略
class MarketCapStrategy(bt.Strategy):
    params = (
        ('trading_interval', StrategyConfig.trading_interval),
        ('max_holdings', StrategyConfig.max_holdings),
        ('initial_buy_pct', StrategyConfig.initial_buy_pct),
        ('add_buy_pct', StrategyConfig.add_buy_pct),
        ('max_position_pct', StrategyConfig.max_position_pct),
        ('initial_sell_pct', StrategyConfig.initial_sell_pct),
        ('add_sell_pct', StrategyConfig.add_sell_pct),
        ('min_position', StrategyConfig.min_position),
        ('market_cap_threshold', StrategyConfig.market_cap_threshold),
        ('listing_years_threshold', StrategyConfig.listing_years_threshold),
        ('profit_threshold', StrategyConfig.profit_threshold),
        ('buy_num', StrategyConfig.buy_num),
        ('sell_num', StrategyConfig.sell_num),
    )
    
    def __init__(self):
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
    
    def setup_indicators(self):
        # 可以在这里设置技术指标
        pass
    
    def start(self):
        # 初始化
        logger.info("策略开始运行")
    
    def next(self):
        # 记录交易日期
        current_date = bt.num2date(self.datas[0].datetime[0])
        if current_date not in self.trade_dates:
            self.trade_dates.append(current_date)
        
        # 检查是否需要交易
        self.trading_counter += 1
        if self.trading_counter % self.params.trading_interval != 0:
            return
        
        # 执行交易
        self.execute_trade(current_date)
    
    def execute_trade(self, current_date):
        logger.info(f"在 {current_date} 执行交易")
        
        # 获取当前大盘指数
        market_index = StockFundamental.get_index_level(current_date)
        logger.info(f"当前大盘指数: {market_index}")
        
        # 计算目标仓位（基于百分位）
        target_position = self.calculate_target_position(market_index)
        logger.info(f"目标仓位: {target_position:.2%}")
        
        # 获取股票基本面信息
        stock_basic_df = StockFundamental.get_stock_basic_info(current_date)
        if stock_basic_df.empty:
            logger.warning("未获取到股票基本面信息，跳过交易")
            return
        
        # 卖出操作
        self.sell_operations(stock_basic_df, current_date)
        
        # 买入操作
        self.buy_operations(stock_basic_df, current_date, target_position)
    
    def calculate_target_position(self, market_index):
        """根据大盘指数计算目标仓位"""
        # 获取当前日期
        current_date = bt.num2date(self.datas[0].datetime[0])
        
        # 获取上证指数3年百分位
        percentile = StockFundamental.get_index_percentile(current_date)
        logger.info(f"上证指数3年百分位: {percentile:.2f}%")
        
        # 百分位越高，仓位越低，最低20%仓位
        # 0%百分位 -> 100%仓位
        # 100%百分位 -> 20%仓位
        target_position = max(0.2, 1.0 - 0.008 * percentile)
        
        return target_position
    
    def sell_operations(self, stock_basic_df, current_date):
        """执行卖出操作"""
        positions = self.getpositions()
        holdings = [(d, pos.size, pos.price) for d, pos in positions.items() if pos.size > 0]
        
        if not holdings:
            logger.info("当前无持仓，跳过卖出操作")
            return
        
        # 获取持仓股票的市值
        holding_stocks = []
        for data, size, price in holdings:
            symbol = data._name
            # 查找股票基本信息
            stock_info = stock_basic_df[stock_basic_df['code'] == symbol]
            
            if not stock_info.empty:
                market_cap = stock_info.iloc[0]['market_cap']
                is_st = stock_info.iloc[0]['is_st']
                # 计算当前价格
                current_price = data.close[0]
                # 计算收益率
                profit_rate = (current_price - price) / price
                
                holding_stocks.append({
                    'symbol': symbol,
                    'size': size,
                    'buy_price': price,
                    'current_price': current_price,
                    'market_cap': market_cap,
                    'is_st': is_st,
                    'profit_rate': profit_rate,
                    'trade_count': self.stock_trade_count.get(symbol, 0)
                })
        
        if not holding_stocks:
            return
        
        # 转换为DataFrame便于排序
        holdings_df = pd.DataFrame(holding_stocks)
        
        # 优先级：1. ST股票 2. 收益率超过阈值 3. 市值最高
        sell_candidates = []
        
        # 1. 找出ST股票
        st_stocks = holdings_df[holdings_df['is_st'] == 1]
        sell_candidates.extend(st_stocks['symbol'].tolist())
        
        # 2. 找出收益率超过阈值的股票
        profit_stocks = holdings_df[
            (holdings_df['is_st'] == 0) & 
            (holdings_df['profit_rate'] >= self.params.profit_threshold)
        ].sort_values('profit_rate', ascending=False)
        sell_candidates.extend(profit_stocks['symbol'].tolist())
        
        # 3. 找出市值最高的股票，补足卖出数量
        remaining_stocks = holdings_df[
            ~holdings_df['symbol'].isin(sell_candidates)
        ].sort_values('market_cap', ascending=False)
        
        # 确保卖出不超过sell_num个
        needed = max(0, self.params.sell_num - len(sell_candidates))
        sell_candidates.extend(remaining_stocks['symbol'].tolist()[:needed])
        
        # 执行卖出
        for symbol in sell_candidates[:self.params.sell_num]:
            stock_data = next((d for d in self.datas if d._name == symbol), None)
            if stock_data:
                pos = self.getposition(stock_data)
                if pos.size > 0:
                    # 计算卖出比例
                    trade_count = self.stock_trade_count.get(symbol, 0)
                    if trade_count == 0:
                        sell_pct = self.params.initial_sell_pct
                    else:
                        sell_pct = self.params.add_sell_pct
                    
                    # 计算卖出数量
                    sell_size = int(pos.size * sell_pct)
                    if sell_size > 0:
                        # 更新交易次数
                        self.stock_trade_count[symbol] = trade_count + 1
                        # 执行卖出
                        self.sell(data=stock_data, size=sell_size)
                        logger.info(f"卖出 {symbol}: {sell_size} 股, 价格: {stock_data.close[0]}")
    
    def buy_operations(self, stock_basic_df, current_date, target_position):
        """执行买入操作"""
        # 计算可用资金
        available_cash = self.broker.getcash()
        total_value = self.broker.getvalue()
        
        if total_value == 0:
            logger.warning("总资产为0，跳过买入操作")
            return
        
        current_position = (total_value - available_cash) / total_value
        logger.info(f"当前仓位: {current_position:.2%}")
        
        # 如果当前仓位已经达到或超过目标仓位，减少买入
        if current_position >= target_position:
            logger.info("当前仓位已达到目标仓位，减少买入")
            return
        
        # 获取当前持仓股票
        positions = self.getpositions()
        holdings = [d._name for d, pos in positions.items() if pos.size > 0]
        
        # 过滤股票
        filtered_stocks = self.filter_stocks(stock_basic_df, current_date, holdings)
        
        if filtered_stocks.empty:
            logger.warning("没有符合条件的股票，跳过买入操作")
            return
        
        # 按市值排序，选择市值最低的股票
        buy_candidates = filtered_stocks.nsmallest(self.params.buy_num, 'market_cap')
        
        # 计算可用于买入的资金
        available_for_buy = (target_position - current_position) * total_value
        logger.info(f"可用于买入的资金: {available_for_buy:.2f}")
        
        # 执行买入
        for _, stock in buy_candidates.iterrows():
            symbol = stock['code']
            # 检查是否已有足够数据
            stock_data = next((d for d in self.datas if d._name == symbol), None)
            if stock_data and not np.isnan(stock_data.close[0]):
                # 计算买入比例
                trade_count = self.stock_trade_count.get(symbol, 0)
                if trade_count == 0:
                    buy_pct = self.params.initial_buy_pct
                else:
                    buy_pct = self.params.add_buy_pct
                
                # 检查单个股票最大仓位限制
                current_stock_value = 0
                pos = self.getposition(stock_data)
                if pos.size > 0:
                    current_stock_value = pos.size * stock_data.close[0]
                
                max_stock_value = total_value * self.params.max_position_pct
                if current_stock_value >= max_stock_value:
                    logger.info(f"股票 {symbol} 已达到最大仓位限制，跳过买入")
                    continue
                
                # 计算买入资金
                buy_amount = min(
                    available_for_buy * buy_pct,
                    max_stock_value - current_stock_value
                )
                
                if buy_amount > 0:
                    # 计算买入数量（取整）
                    buy_size = int(buy_amount / stock_data.close[0])
                    if buy_size > 0:
                        # 更新交易次数
                        self.stock_trade_count[symbol] = trade_count + 1
                        # 记录买入价格
                        if symbol not in self.buy_prices:
                            self.buy_prices[symbol] = stock_data.close[0]
                        # 执行买入
                        self.buy(data=stock_data, size=buy_size)
                        logger.info(f"买入 {symbol}: {buy_size} 股, 价格: {stock_data.close[0]}, 金额: {buy_amount:.2f}")
    
    def filter_stocks(self, stock_basic_df, current_date, holdings):
        """根据条件过滤股票"""
        # 过滤ST股票
        filtered = stock_basic_df[stock_basic_df['is_st'] == 0]
        
        # 过滤市值
        filtered = filtered[filtered['market_cap'] > self.params.market_cap_threshold]
        
        # 过滤上市时间
        filtered = filtered[pd.to_datetime(filtered['ipo_date']) < current_date - pd.Timedelta(days=365*self.params.listing_years_threshold)]
        
        # 排除已持仓的股票（如果持仓数量已达到上限）
        if len(holdings) >= self.params.max_holdings:
            filtered = filtered[filtered['code'].isin(holdings)]  # 只保留已持仓股票（用于加仓）
        else:
            # 优先选择未持仓的股票
            new_stocks = filtered[~filtered['code'].isin(holdings)]
            if not new_stocks.empty:
                filtered = new_stocks
        
        return filtered
    
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # 订单已提交或接受，无需操作
            return
        
        if order.status in [order.Completed]:
            if order.isbuy():  # 买入订单完成
                logger.info(f"买入完成: {order.data._name}, 价格: {order.executed.price}, 数量: {order.executed.size}")
            elif order.issell():  # 卖出订单完成
                logger.info(f"卖出完成: {order.data._name}, 价格: {order.executed.price}, 数量: {order.executed.size}")
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            logger.warning(f"订单失败: {order.data._name}, 状态: {order.getstatusname()}")
    
    def notify_trade(self, trade):
        if trade.isclosed:
            logger.info(f"交易完成: {trade.data._name}, 毛收益: {trade.pnl:.2f}, 净利润: {trade.pnlcomm:.2f}")

# 回测函数
def run_backtest(start_date, end_date, initial_cash=1000000):
    # 创建回测引擎
    cerebro = bt.Cerebro()
    
    # 设置初始资金
    cerebro.broker.setcash(initial_cash)
    
    # 设置佣金
    comminfo = ChineseStockCommission()
    cerebro.broker.addcommissioninfo(comminfo)
    
    # 添加策略
    cerebro.addstrategy(MarketCapStrategy)
    
    # 获取需要回测的股票列表
    stock_list = get_stock_list_for_backtest()
    logger.info(f"回测股票数量: {len(stock_list)}")
    
    # 添加数据
    for symbol in stock_list[:10000]:  # 限制股票数量，避免数据过多
        data = MySQLData(
            symbol=symbol,
            fromdate=start_date,
            todate=end_date
        )
        cerebro.adddata(data, name=symbol)
    
    # 添加沪深300指数作为对比基准
    add_hs300_index(cerebro, start_date, end_date)
    
    # 添加分析指标
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Transactions, _name='transactions')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    
    # 运行回测
    logger.info(f"开始回测，初始资金: {initial_cash:.2f}")
    results = cerebro.run()
    
    # 输出结果
    strategy = results[0]
    
    # 打印回测结果
    print("\n回测结果:")
    print(f"最终资金: {cerebro.broker.getvalue():.2f}")
    
    # 获取分析结果
    returns = strategy.analyzers.returns.get_analysis() if hasattr(strategy.analyzers, 'returns') else None
    sharpe = strategy.analyzers.sharpe.get_analysis() if hasattr(strategy.analyzers, 'sharpe') else None
    drawdown = strategy.analyzers.drawdown.get_analysis() if hasattr(strategy.analyzers, 'drawdown') else None
    
    # 打印分析结果，添加空值检查
    if returns and 'rtot' in returns:
        print(f"总收益率: {returns['rtot'] * 100:.2f}%")
    else:
        print("总收益率: 无法计算")
    
    if sharpe and 'sharperatio' in sharpe and sharpe['sharperatio'] is not None:
        print(f"夏普比率: {sharpe['sharperatio']:.2f}")
    else:
        print("夏普比率: 无法计算（可能数据不足或无交易）")
    
    if drawdown and 'max' in drawdown and 'drawdown' in drawdown['max']:
        print(f"最大回撤: {drawdown['max']['drawdown']:.2f}%")
    else:
        print("最大回撤: 无法计算")
    
    """ # 可视化结果 - 先检查matplotlib是否可用
    try:
        import matplotlib
        # 尝试绘制图表，并处理可能的绘制错误
        try:
            cerebro.plot()
        except ValueError as e:
            if "NaN or Inf" in str(e):
                logger.warning(f"可视化时遇到数据问题: {e}，可能是由于无交易或数据异常导致")
            else:
                logger.error(f"可视化过程中出错: {e}")
    except ImportError:
        logger.warning("matplotlib未安装，跳过可视化环节") """

# 获取回测股票列表
def get_stock_list_for_backtest():
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        try:
            with conn.cursor() as cursor:
                # 获取活跃股票列表
                # status = 1
                sql = """SELECT code FROM bao_stock_basic """
                cursor.execute(sql)
                results = cursor.fetchall()
                return [r['code'] for r in results]
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取股票列表时出错: {e}")
        return []

# 添加沪深300指数数据
def add_hs300_index(cerebro, start_date, end_date):
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        try:
            with conn.cursor() as cursor:
                sql = """
                SELECT date, open, high, low, close, volume 
                FROM bao_nostock_trade 
                WHERE code = 'sh.000300' AND date >= %s AND date <= %s 
                ORDER BY date ASC
                """
                cursor.execute(sql, (start_date, end_date))
                data = cursor.fetchall()
                
                if data:
                    df = pd.DataFrame(data)
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                    
                    # 创建指数数据源
                    index_data = bt.feeds.PandasData(
                        dataname=df,
                        fromdate=start_date,
                        todate=end_date,
                        open='open',
                        high='high',
                        low='low',
                        close='close',
                        volume='volume',
                        openinterest=None
                    )
                    
                    # 添加指数数据作为参考
                    cerebro.adddata(index_data, name='HS300')
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"添加沪深300指数数据时出错: {e}")

# 主函数
if __name__ == '__main__':
    try:
        # 设置回测时间范围
        start_date = datetime.datetime(2023, 1, 1)
        end_date = datetime.datetime.now()
        
        # 运行回测
        run_backtest(start_date, end_date, initial_cash=1000000)
    except Exception as e:
        logger.error(f"回测过程中出错: {e}")
        import traceback
        traceback.print_exc()