import pandas as pd
import numpy as np
import tushare as ts
import backtrader as bt
from datetime import datetime
import matplotlib.pyplot as plt

# 设置tushare API
TS_TOKEN = 'your_tushare_token'  # 替换为你的tushare token
ts.set_token(TS_TOKEN)
pro = ts.pro_api()

# 获取A股股票池（沪深300成分股，具有代表性）
def get_stock_pool():
    """获取沪深300成分股列表"""
    hs300 = pro.index_weight(index_code='000300.SH', start_date='20070101', end_date='20231231')
    return hs300['con_code'].unique().tolist()

# 获取股票财务和交易数据
def get_stock_data(stock_code, start_date, end_date):
    """获取股票的价格和财务数据"""
    try:
        # 价格数据
        price_df = ts.pro_bar(ts_code=stock_code, adj='qfq',
                             start_date=start_date, end_date=end_date)
        if price_df is None or len(price_df) < 10:
            return None
            
        price_df = price_df.sort_values('trade_date')
        price_df['trade_date'] = pd.to_datetime(price_df['trade_date'])
        price_df.set_index('trade_date', inplace=True)
        
        # 财务数据（PE、PB、ROE等）
        fin_df = pro.fina_indicator(ts_code=stock_code, start_date=start_date, end_date=end_date,
                                   fields='ann_date, pe, pb, roe, dv_ratio')
        if fin_df is not None and len(fin_df) > 0:
            fin_df['ann_date'] = pd.to_datetime(fin_df['ann_date'])
            fin_df = fin_df.sort_values('ann_date').set_index('ann_date')
            # 向前填充财务数据（财报发布后沿用至下一财报）
            price_df = price_df.join(fin_df, how='left').fillna(method='ffill')
        
        return price_df
    except Exception as e:
        print(f"获取{stock_code}数据出错: {e}")
        return None

# 经典策略基类
class ClassicStrategy(bt.Strategy):
    params = (
        ('rebalance_days', 20),  # 调仓周期（交易日）
        ('stock_num', 20),       # 持仓数量
        ('stop_loss', 0.10),     # 止损比例
    )
    
    def __init__(self):
        self.order = None
        self.buy_price = {}
        self.day_count = 0
        
    def next(self):
        self.day_count += 1
        
        # 定期调仓
        if self.day_count % self.params.rebalance_days == 0:
            self.rebalance()
        
        # 检查止损
        self.check_stop_loss()
    
    def rebalance(self):
        """调仓逻辑，由子类实现"""
        pass
    
    def check_stop_loss(self):
        """止损检查"""
        for d in self.datas:
            if self.getposition(d).size > 0:
                pct_change = (d.close[0] - self.buy_price[d._name]) / self.buy_price[d._name]
                if pct_change < -self.params.stop_loss:
                    self.order = self.sell(data=d, size=self.getposition(d).size)
                    self.log(f"止损卖出 {d._name}, 跌幅: {pct_change:.2%}")
    
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} {txt}')

# 1. 价值策略（低PE低PB）
class ValueStrategy(ClassicStrategy):
    def rebalance(self):
        # 收集所有股票的PE和PB
        candidates = []
        for d in self.datas:
            if hasattr(d, 'pe') and hasattr(d, 'pb') and not np.isnan(d.pe[0]) and not np.isnan(d.pb[0]):
                if d.pe[0] > 0 and d.pb[0] > 0:  # 排除负的PE/PB
                    candidates.append({
                        'data': d,
                        'pe': d.pe[0],
                        'pb': d.pb[0],
                        'score': d.pe[0] + d.pb[0]  # 综合评分，越低越好
                    })
        
        # 按评分排序，选择最低的股票
        if candidates:
            candidates.sort(key=lambda x: x['score'])
            selected = [c['data'] for c in candidates[:self.params.stock_num]]
            
            # 卖出不在选中列表的股票
            for d in self.datas:
                if self.getposition(d).size > 0 and d not in selected:
                    self.order = self.sell(data=d, size=self.getposition(d).size)
            
            # 买入选中的股票，平均分配资金
            if selected:
                target_value = self.broker.getvalue() / len(selected)
                for d in selected:
                    current_size = self.getposition(d).size
                    target_size = int(target_value / d.close[0] / 100) * 100
                    
                    if target_size > current_size:
                        self.order = self.buy(data=d, size=target_size - current_size)
                        self.buy_price[d._name] = d.close[0]
                    elif target_size < current_size:
                        self.order = self.sell(data=d, size=current_size - target_size)

# 2. 动量策略（过去6个月涨幅）
class MomentumStrategy(ClassicStrategy):
    params = (
        ('momentum_period', 120),  # 动量计算周期（交易日）
    )
    
    def rebalance(self):
        candidates = []
        for d in self.datas:
            if len(d) > self.params.momentum_period:  # 确保有足够数据
                # 计算过去N天的涨幅
                momentum = (d.close[0] - d.close[-self.params.momentum_period]) / d.close[-self.params.momentum_period]
                candidates.append({
                    'data': d,
                    'momentum': momentum
                })
        
        # 选择动量最高的股票
        if candidates:
            candidates.sort(key=lambda x: x['momentum'], reverse=True)
            selected = [c['data'] for c in candidates[:self.params.stock_num]]
            
            # 调仓逻辑与价值策略相同
            for d in self.datas:
                if self.getposition(d).size > 0 and d not in selected:
                    self.order = self.sell(data=d, size=self.getposition(d).size)
            
            if selected:
                target_value = self.broker.getvalue() / len(selected)
                for d in selected:
                    current_size = self.getposition(d).size
                    target_size = int(target_value / d.close[0] / 100) * 100
                    
                    if target_size > current_size:
                        self.order = self.buy(data=d, size=target_size - current_size)
                        self.buy_price[d._name] = d.close[0]
                    elif target_size < current_size:
                        self.order = self.sell(data=d, size=current_size - target_size)

# 3. 质量策略（高ROE）
class QualityStrategy(ClassicStrategy):
    def rebalance(self):
        candidates = []
        for d in self.datas:
            if hasattr(d, 'roe') and not np.isnan(d.roe[0]) and d.roe[0] > 0:
                candidates.append({
                    'data': d,
                    'roe': d.roe[0]
                })
        
        # 选择ROE最高的股票
        if candidates:
            candidates.sort(key=lambda x: x['roe'], reverse=True)
            selected = [c['data'] for c in candidates[:self.params.stock_num]]
            
            # 调仓逻辑
            for d in self.datas:
                if self.getposition(d).size > 0 and d not in selected:
                    self.order = self.sell(data=d, size=self.getposition(d).size)
            
            if selected:
                target_value = self.broker.getvalue() / len(selected)
                for d in selected:
                    current_size = self.getposition(d).size
                    target_size = int(target_value / d.close[0] / 100) * 100
                    
                    if target_size > current_size:
                        self.order = self.buy(data=d, size=target_size - current_size)
                        self.buy_price[d._name] = d.close[0]
                    elif target_size < current_size:
                        self.order = self.sell(data=d, size=current_size - target_size)

# 4. 红利策略（高股息率）
class DividendStrategy(ClassicStrategy):
    def rebalance(self):
        candidates = []
        for d in self.datas:
            if hasattr(d, 'dv_ratio') and not np.isnan(d.dv_ratio[0]) and d.dv_ratio[0] > 0:
                candidates.append({
                    'data': d,
                    'div_ratio': d.dv_ratio[0]
                })
        
        # 选择股息率最高的股票
        if candidates:
            candidates.sort(key=lambda x: x['div_ratio'], reverse=True)
            selected = [c['data'] for c in candidates[:self.params.stock_num]]
            
            # 调仓逻辑
            for d in self.datas:
                if self.getposition(d).size > 0 and d not in selected:
                    self.order = self.sell(data=d, size=self.getposition(d).size)
            
            if selected:
                target_value = self.broker.getvalue() / len(selected)
                for d in selected:
                    current_size = self.getposition(d).size
                    target_size = int(target_value / d.close[0] / 100) * 100
                    
                    if target_size > current_size:
                        self.order = self.buy(data=d, size=target_size - current_size)
                        self.buy_price[d._name] = d.close[0]
                    elif target_size < current_size:
                        self.order = self.sell(data=d, size=current_size - target_size)

# 运行回测
def run_strategy_test(strategy_class, start_date, end_date, initial_capital=1000000):
    cerebro = bt.Cerebro()
    cerebro.addstrategy(strategy_class)
    cerebro.broker.setcash(initial_capital)
    cerebro.broker.setcommission(commission=0.0003)  # 佣金+印花税
    
    # 添加沪深300指数作为基准
    index_df = ts.pro_bar(ts_code='000300.SH', asset='I', adj='qfq',
                         start_date=start_date, end_date=end_date)
    index_df = index_df.sort_values('trade_date')
    index_df['trade_date'] = pd.to_datetime(index_df['trade_date'])
    index_df.set_index('trade_date', inplace=True)
    index_data = bt.feeds.PandasData(dataname=index_df, name='HS300')
    cerebro.adddata(index_data)
    
    # 获取股票池并添加数据
    stock_pool = get_stock_pool()
    count = 0
    max_stocks = 100  # 限制股票数量，加快回测
    for code in stock_pool:
        if count >= max_stocks:
            break
        df = get_stock_data(code, start_date, end_date)
        if df is not None and len(df) > 365:  # 至少有一年数据
            data = bt.feeds.PandasData(
                dataname=df,
                name=code,
                openinterest=None  # 没有持仓兴趣数据
            )
            cerebro.adddata(data)
            count += 1
    
    print(f"共加载{count}只股票数据进行回测")
    
    # 添加分析指标
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    
    # 执行回测
    print(f"初始资金: {initial_capital}")
    results = cerebro.run()
    strat = results[0]
    
    # 输出结果
    final_value = cerebro.broker.getvalue()
    print(f"最终资金: {final_value:.2f}")
    print(f"总收益率: {(final_value - initial_capital)/initial_capital:.2%}")
    print(f"夏普比率: {strat.analyzers.sharpe.get_analysis()['sharperatio']:.2f}")
    print(f"最大回撤: {strat.analyzers.drawdown.get_analysis()['max']['drawdown']:.2%}")
    
    # 绘图
    cerebro.plot(style='candlestick', iplot=False)
    return final_value

if __name__ == '__main__':
    # 回测时间段：2007年1月1日至2023年12月31日
    start_date = '20070101'
    end_date = '20231231'
    
    # 测试各种经典策略
    print("====== 价值策略（低PE/PB）======")
    value_result = run_strategy_test(ValueStrategy, start_date, end_date)
    
    print("\n====== 动量策略（6个月涨幅）======")
    momentum_result = run_strategy_test(MomentumStrategy, start_date, end_date)
    
    print("\n====== 质量策略（高ROE）======")
    quality_result = run_strategy_test(QualityStrategy, start_date, end_date)
    
    print("\n====== 红利策略（高股息率）======")
    dividend_result = run_strategy_test(DividendStrategy, start_date, end_date)
