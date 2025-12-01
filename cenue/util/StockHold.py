#持仓object
class StockHold:
    def __init__(self, stock_code, stock_name, avg_cost_price, cur_close_price, total_cost_price, total_price, total_commission, is_sell_all, stock_order_list):
        self.stock_code = stock_code #股票代码
        self.stock_name = stock_name #股票名称
        self.avg_cost_price = avg_cost_price #平均持仓成本价格
        self.cur_close_price = cur_close_price #持仓现价
        self.total_cost_price = total_cost_price #总持仓成本
        self.total_price = total_price #总持仓市值
        self.total_commission = total_commission #总持仓手续费
        self.is_sell_all = is_sell_all #是否全部卖出
        self.stock_order_list = stock_order_list #订单记录列表
        self.total_profit = total_price - total_cost_price - total_commission #持仓盈利
        self.total_profit_percent = self.total_profit / (total_cost_price + total_commission) * 100 #持仓收益率