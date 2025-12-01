
#公司分析object
class CompanyAnalysis:
    def __init__(self, stock, stock_name, trade_date_str):
        self.stock = stock
        self.stock_name = stock_name
        self.trade_date_str = trade_date_str #分析日期

    #公司本身得分，维度行业，公司行业地位，行业进入门槛；品牌形象，研发投入，价格垄断；老板技术/管理/投资；经营稳定；
    def calculate_company_score(self):
        """ self.industry_score = industry_score #行业得分
        self.company_top_score = company_top_score #公司行业地位
        self.boss_score = boss_score #老板技术/管理/投资
        self.manager_score = manager_score #管理层稳定
        self.positive_score = positive_score #价值观对+对员工+对合作方好
        self.brand_score = brand_score #垄断得分，品牌垄断，位置垄断，经营垄断
        self.stability_score = stability_score #经营稳定性 """

        self.company_score = 0 #得分

        return self.company_score
    
    #计算最近1季经营得分
    def calculate_operating_score(self):
        """ self.net_profit_rate = net_profit_rate #净利率
        self.sales_growth_pct = sales_growth_pct #销售额增长率（同比）        
        self.profits_growth_pct = profits_growth_pct #利润额增长率（同比）
        self.cashflow_growth_pct = cashflow_growth_pct #现金流增长率（同比） """
        self.operating_score = 0 #得分

        return self.operating_score

    #计算股价得分
    def calculate_price_score(self):
        price_score = 0

        self.price_score = price_score #股价得分
        return price_score

    #计算得分
    def calculate_score(self):
        company_score = self.calculate_company_score() #公司得分
        operating_score = self.calculate_operating_score()#最近1季经营得分
        price_score = self.calculate_price_score()#股价得分

        self.total_score = company_score + operating_score + price_score #总得分
        return self.total_score
