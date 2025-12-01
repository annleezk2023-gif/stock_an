
#按日的存盘记录
class StockDailyRecord:
    def __init__(self, hold_date_str, total_cost, total_price, total_commission, stock_hold_list, stock_order_list):
        self.hold_date = hold_date_str #持仓日期
        self.total_cost = total_cost #持仓成本
        self.total_price = total_price #持仓市值
        self.total_commission = total_commission #持仓手续费
        self.stock_hold_list = stock_hold_list #持仓记录列表
        self.stock_order_list = stock_order_list #当天订单记录列表
        self.total_profit = total_price - total_cost - total_commission #持仓盈利
        self.total_profit_percent = self.total_profit / (total_cost + total_commission) * 100 #持仓收益率