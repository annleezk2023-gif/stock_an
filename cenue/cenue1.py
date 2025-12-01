import pandas as pd
import numpy as np
import tushare as ts
import backtrader as bt
import datetime

# 设置tushare API（需要注册获取）
TS_TOKEN = 'dab46015dee241ef1a23b5a417c2ffdc37d4f5d8ce0ddeba8ae538c2'  # 替换为你的tushare token
ts.set_token(TS_TOKEN)
pro = ts.pro_api()

# 获取小盘股列表
def get_small_cap_stocks(percentile=30):
    """获取市值排名后30%的小盘股列表"""
    stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,circ_mv')
    stock_basic['circ_mv'] = stock_basic['circ_mv'].astype(float)
    stock_basic = stock_basic.sort_values('circ_mv')
    
    # 取市值最小的30%
    small_cap_threshold = np.percentile(stock_basic['circ_mv'], percentile)
    small_cap_stocks = stock_basic[stock_basic['circ_mv'] <= small_cap_threshold]
    
    return small_cap_stocks['ts_code'].tolist()

# 带止损止盈的策略类
class StopStrategy(bt.Strategy):
    params = (
        # 均线参数
        ('short_period', 5),      # 短期均线周期
        ('long_period', 20),      # 长期均线周期
        
        # 止损参数
        ('fixed_stop_loss', 0.05),# 固定比例止损 (5%)
        ('trailing_stop', 0.08),  # 追踪止损比例 (8%)
        
        # 止盈参数
        ('fixed_take_profit', 0.15),# 固定比例止盈 (15%)
        ('step_take_profit', 0.1), # 阶梯止盈比例 (10%)
        ('step_partial', 0.5),    # 阶梯止盈部分平仓比例 (50%)
        
        # 其他参数
        ('max_position', 5),      # 最大持仓数
        ('volume_factor', 1.5),   # 成交量放大倍数
    )
    
    def __init__(self):
        # 记录交易状态
        self.order = None
        
        # 存储持仓相关信息
        self.buy_details = {}  # 格式: {股票代码: {'price': 买入价, 'highest': 最高价, 'shares': 持仓数量}}
        
        # 技术指标
        self.sma_short = {}
        self.sma_long = {}
        self.volume_ma = {}
        
        for d in self.datas:
            self.sma_short[d._name] = bt.indicators.SimpleMovingAverage(
                d.close, period=self.params.short_period)
            self.sma_long[d._name] = bt.indicators.SimpleMovingAverage(
                d.close, period=self.params.long_period)
            self.volume_ma[d._name] = bt.indicators.SimpleMovingAverage(
                d.volume, period=20)
    
    def next(self):
        if self.order:
            return  # 有未完成订单时不操作
        
        current_holdings = len([d for d in self.datas if self.getposition(d).size > 0])
        
        for d in self.datas:
            code = d._name
            position = self.getposition(d)
            close_price = d.close[0]
            
            # 处理持仓股票的止损止盈
            if position.size > 0:
                # 更新最高价记录（用于追踪止损）
                if close_price > self.buy_details[code]['highest']:
                    self.buy_details[code]['highest'] = close_price
                
                # 计算各项止损止盈条件
                stop_conditions = self.check_stop_conditions(code, close_price)
                take_conditions = self.check_take_conditions(code, close_price)
                
                # 执行止损
                if any(stop_conditions.values()):
                    self.log(f"触发止损: {', '.join([k for k, v in stop_conditions.items() if v])}")
                    self.order = self.sell(data=d, size=position.size)
                    del self.buy_details[code]
                    continue
                
                # 执行止盈
                if any(take_conditions.values()):
                    # 处理阶梯止盈（部分平仓）
                    if take_conditions['step_take_profit']:
                        # 只平掉部分仓位
                        sell_size = int(position.size * self.params.step_partial)
                        self.log(f"触发阶梯止盈，平仓 {sell_size} 股")
                        self.order = self.sell(data=d, size=sell_size)
                        self.buy_details[code]['shares'] -= sell_size
                    else:
                        # 全部平仓
                        self.log(f"触发止盈: {', '.join([k for k, v in take_conditions.items() if v])}")
                        self.order = self.sell(data=d, size=position.size)
                        del self.buy_details[code]
                    continue
            
            # 买入逻辑（未持仓时）
            else:
                # 均线金叉且成交量放大
                ma_cross = self.sma_short[code][0] > self.sma_long[code][0] and \
                           self.sma_short[code][-1] <= self.sma_long[code][-1]
                volume_ok = d.volume[0] > self.volume_ma[code][0] * self.params.volume_factor
                
                if ma_cross and volume_ok and current_holdings < self.params.max_position:
                    # 计算买入数量
                    buy_value = self.broker.cash / (self.params.max_position - current_holdings)
                    size = int(buy_value / close_price / 100) * 100
                    
                    if size > 0:
                        self.order = self.buy(data=d, size=size)
                        # 记录买入详情
                        self.buy_details[code] = {
                            'price': close_price,
                            'highest': close_price,  # 初始最高价为买入价
                            'shares': size
                        }
                        current_holdings += 1
    
    def check_stop_conditions(self, code, current_price):
        """检查各种止损条件"""
        details = self.buy_details[code]
        return {
            # 固定比例止损：从买入价下跌一定比例
            'fixed_stop_loss': (details['price'] - current_price) / details['price'] > self.params.fixed_stop_loss,
            
            # 追踪止损：从最高价回落一定比例
            'trailing_stop': (details['highest'] - current_price) / details['highest'] > self.params.trailing_stop,
            
            # 均线止损：收盘价跌破长期均线
            'ma_stop_loss': current_price < self.sma_long[code][0] * 0.98
        }
    
    def check_take_conditions(self, code, current_price):
        """检查各种止盈条件"""
        details = self.buy_details[code]
        profit_ratio = (current_price - details['price']) / details['price']
        
        return {
            # 固定比例止盈：达到目标收益率
            'fixed_take_profit': profit_ratio > self.params.fixed_take_profit,
            
            # 阶梯止盈：达到部分止盈目标
            'step_take_profit': profit_ratio > self.params.step_take_profit and details['shares'] > 0,
            
            # 均线止盈：短期均线下穿长期均线
            'ma_take_profit': self.sma_short[code][0] < self.sma_long[code][0] and \
                             self.sma_short[code][-1] >= self.sma_long[code][-1]
        }
    
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f"买入 {order.data._name} 价格: {order.executed.price:.2f}, 数量: {order.executed.size}")
            else:
                self.log(f"卖出 {order.data._name} 价格: {order.executed.price:.2f}, 数量: {order.executed.size}")
        
        self.order = None
    
    def log(self, txt, dt=None):
        """日志函数"""
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} {txt}')

# 回测执行函数
def run_backtest(start_date, end_date, initial_capital=100000):
    # 获取小盘股列表
    small_cap_stocks = get_small_cap_stocks()
    print(f"筛选出{len(small_cap_stocks)}只小盘股")
    
    # 初始化回测引擎
    cerebro = bt.Cerebro()
    cerebro.addstrategy(StopStrategy)
    
    # 设置初始资金
    cerebro.broker.setcash(initial_capital)
    
    # 设置佣金（万分之二）
    cerebro.broker.setcommission(commission=0.0002)
    
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
    
    # 执行回测
    print(f"初始资金: {initial_capital}")
    cerebro.run()
    final_value = cerebro.broker.getvalue()
    print(f"回测结束资金: {final_value}")
    print(f"收益率: {(final_value - initial_capital)/initial_capital*100:.2f}%")
    
    # 绘制回测结果
    cerebro.plot(style='candlestick')

if __name__ == '__main__':
    # 设置回测时间段
    start_date = '20200101'
    end_date = '20231231'
    
    # 运行回测
    run_backtest(start_date, end_date)
