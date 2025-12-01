# 标签
class StockTagsFile:
    def __init__(self):
        self.growth_pct_15 = "15成长"
        self.growth_pct_5_market_1000 = "5成长市值1000亿"
        self.dividend_pct_3 = "3股息"
        self.dividend_pct_1_growth_pct_5 = "1股息5成长"
        self.net_profit_pct_20 = "20净利"
        self.roe_pct_20 = "20roe"

    growth_pct_15 = "15成长"
    growth_pct_5_market_1000 = "5成长市值1000亿"
    dividend_pct_3 = "3股息"
    dividend_pct_1_growth_pct_5 = "1股息5成长"
    net_profit_pct_20 = "20净利"
    roe_pct_20 = "20roe"

class StockTagsDividend:
    def __init__(self):
        self.fenghong_2_year_all = "2年连续分红"
        self.fenghong_2_year = "近2年有分红"
        self.zhaunsong_2_year_all = "2年连续转送"
        self.zhaunsong_2_year = "近2年有转送"

        self.no_fenghong_2_year = "近2年无分红"
        self.no_zhaunsong_2_year = "近2年无转送"

    #分红转送
    fenghong_2_year_all = "2年连续分红"
    fenghong_2_year = "近2年有分红"
    zhaunsong_2_year_all = "2年连续转送"
    zhaunsong_2_year = "近2年有转送"

    #tags_by_dividend_del = [self.no_fenghong_2_year, self.no_zhaunsong_2_year]
    no_fenghong_2_year = "近2年无分红"
    no_zhaunsong_2_year = "近2年无转送"

class StockTagsSeason:
    def __init__(self):
        self.profit_MBRevenue_pct_100 = "同比主营收增长100%以上"
        self.profit_MBRevenue_pct_50 = "同比主营收增长50%以上"
        self.profit_MBRevenue_pct_20 = "同比主营收增长20%以上"
        self.profit_MBRevenue_pct_10_loss = "同比主营收减少10%以上"

        self.profit_netProfit_pct_100 = "净利润同比增长100%以上"
        self.profit_netProfit_pct_50 = "净利润同比增长50%以上"
        self.profit_netProfit_pct_20 = "净利润同比增长20%以上"
        self.profit_netProfit_pct_10_loss = "净利润同比减少10%以上"

        self.profit_netProfit_1_season_add = "最近1季度扭亏为盈"
        self.profit_netProfit_1_season_loss = "最近1季度盈利变亏损"

        self.profit_roeAvg_pct_20 = "净资产收益率20%以上"
        self.profit_roeAvg_pct_10_20 = "净资产收益率10-20%"
        self.profit_roeAvg_pct_0_10 = "净资产收益率0-10%"
        self.profit_roeAvg_pct_0_loss = "净资产收益率0%以下"

        #参考打分
        self.profit_npMargin_pct_50 = "净利率大于50%"
        self.profit_npMargin_pct_30_50 = "净利率30-50%"
        self.profit_npMargin_pct_10_30 = "净利率10-30%"
        self.profit_npMargin_pct_0_loss = "净利率0%以下"
    
    #以下用来打分
    profit_MBRevenue_pct_100 = "同比主营收增长100%以上"
    profit_MBRevenue_pct_50 = "同比主营收增长50%以上"
    profit_MBRevenue_pct_20 = "同比主营收增长20%以上"
    profit_MBRevenue_pct_10_loss = "同比主营收减少10%以上"

    profit_netProfit_pct_100 = "同比净利润增长100%以上"
    profit_netProfit_pct_50 = "同比净利润增长50%以上"
    profit_netProfit_pct_20 = "同比净利润增长20%以上"
    profit_netProfit_pct_10_loss = "同比净利润减少10%以上"
    profit_netProfit_1_season_add = "最近1季度扭亏为盈"
    profit_netProfit_1_season_loss = "最近1季度盈利变亏损"

    profit_roeAvg_pct_20 = "净资产收益率20%以上"
    profit_roeAvg_pct_10_20 = "净资产收益率10-20%"
    profit_roeAvg_pct_0_10 = "净资产收益率0-10%"
    profit_roeAvg_pct_0_loss = "净资产收益率0%以下"

    #参考打分
    profit_npMargin_pct_50 = "净利率大于50%"
    profit_npMargin_pct_30_50 = "净利率30-50%"
    profit_npMargin_pct_10_30 = "净利率10-30%"
    profit_npMargin_pct_0_loss = "净利率0%以下"






