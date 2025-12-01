
#订单object
class StockOrder:
    def __init__(self, order_date_str, order_type, stock_code, stock_name, stock_price, stock_shares, order_reason, total_cost, total_commission):
        self.order_date_str = order_date_str #订单日期
        self.order_type = order_type #订单类型，买或卖
        self.stock_code = stock_code #股票代码
        self.stock_name = stock_name #股票名称
        self.stock_price = stock_price #股票价格
        self.stock_shares = order_shares #股票数量
        self.order_reason = order_reason #订单原因
        self.total_cost = total_cost #总持仓成本
        self.total_commission = total_commission #总交易手续费