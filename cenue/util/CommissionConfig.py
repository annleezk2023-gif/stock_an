
# 交易费用配置
class CommissionConfig:
    commission = 0.0002  # 佣金率，万分之2
    min_commission = 5  # 最低佣金
    stamp_duty = 0.001  # 印花税（卖出时），千分之1
    transfer_fee = 0.00002  # 过户费，十万分之2

    #size交易数量（单位：股，卖出为负数），price交易价格
    def _getcommission(self, size, price):
        # 计算佣金
        comm = abs(size) * price * self.commission
        # 最低佣金限制
        comm = max(comm, self.min_commission)
        # 卖出时收取印花税
        if size < 0:
            comm += abs(size) * price * self.stamp_duty
        # 过户费
        comm += abs(size) * price * self.transfer_fee
        return comm