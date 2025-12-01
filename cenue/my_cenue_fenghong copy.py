
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
    
    
    def sort_stocks(self):
        # 个股的分红标签
        """ fenghong_tags = get_stock_tags_by_trade_date(conn, cur_trade_date_str, tags_type=2)
        # 个股的运营标签
        season_tags = get_stock_tags_by_trade_date(conn, cur_trade_date_str, tags_type=1) """
        return self.stock_info_list

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():  # 买入订单完成
                logger.info(f"买入完成: {order.data._name}, 价格: {order.executed.price}, 数量: {order.executed.size}")
            elif order.issell():  # 卖出订单完成
                logger.info(f"卖出完成: {order.data._name}, 价格: {order.executed.price}, 数量: {order.executed.size}")
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            logger.warning(f"订单失败: {order.data._name}, 状态: {order.getstatusname()}")
    

