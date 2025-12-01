import sys
import os
from sqlalchemy import text
import json
import datetime

# 配置logger
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common
import stock_tags

""" 自动计算标签 ，分2种标签。
基础条件：上市时间2年内，>2年。市值50亿，100亿，200亿，500亿，1000亿+。
分红股：2年连续分红，分红金额稳定+增长，销售额业绩稳定+增长。
成长股：转送次数，销售额稳定+增长，净利润额稳定+增长，净利率高，资产负债表资产稳定+增长，资产负债表负债稳定+增长。"""

# 检查这几天是否有最新数据
def is_fetch_data_update(tableType, stock_code, pre_trade_date_str, trade_date_str, conn=None):
    if tableType == "fenghong":
        # 分红，转送
        # 包含前一个交易日，不包含当前交易日
        sql = f"SELECT COUNT(*) FROM bao_stock_dividend WHERE code = '{stock_code}' AND dividOperateDate >= '{pre_trade_date_str}' AND dividOperateDate < '{trade_date_str}'"
        result = conn.execute(text(sql)).fetchone()
        if result and result[0] > 0:
            return True 
        else:
            return False
    elif tableType == "season":
        # 季度数据
        # 包含前一个交易日，不包含当前交易日
        sql = f"SELECT COUNT(*) FROM bao_stock_profit WHERE code = '{stock_code}' AND pubDate >= '{pre_trade_date_str}' AND pubDate < '{trade_date_str}'"
        result = conn.execute(text(sql)).fetchone()
        if result and result[0] > 0:
            return True 
        else:
            return False
    else:
        return False


# 分红和拆股数据
def gen_fenhong_auto_tags_4_stock(stock_code, dividOperateDate_str, conn=None):
    """ 为股票生成自动标签 """
    positive_tags = []
    loss_tags = []
    
    dividOperateDate = datetime.datetime.strptime(dividOperateDate_str, '%Y-%m-%d').date()
    # 开始时间取3年前，如23,24,25
    start_date = datetime.datetime(dividOperateDate.year - 2, 1, 1).date()
    start_date_str = start_date.strftime('%Y-%m-%d')
    #1  bao_stock_dividend表，2年连续分红，2年分红金额正增长，2年分红金额正增长50%，2年分红金额正增长100%。
    # 日期不包含当天，以最早时间为准
    sql = f"""select sum(dividCashPsBeforeTax) fenghong, code, year(dividOperateDate) divid_year from bao_stock_dividend where code = '{stock_code}' and data_exist = 1
            and dividOperateDate >= '{start_date_str}' and dividOperateDate < '{dividOperateDate_str}'
             group by code, divid_year order by divid_year desc;
            """
    result = conn.execute(text(sql)).fetchall()
    if result:
        result_len = len(result)
        #标签：连续分红
        if result_len >= 2:
            positive_tags.append(stock_tags.StockTagsDividend.fenghong_2_year_all)
        elif result_len == 1: #有1条数据
            positive_tags.append(stock_tags.StockTagsDividend.fenghong_2_year)
        else: #没有数据
            loss_tags.append(stock_tags.StockTagsDividend.no_fenghong_2_year)
    else: #没有数据
        loss_tags.append(stock_tags.StockTagsDividend.no_fenghong_2_year)




    #转送 # 日期不包含当天，以最早时间为准
    sql = f"""select sum(zhuan_num + song_num) zhaunsong_num, code, year(dividOperateDate) divid_year from bao_stock_dividend where code = '{stock_code}' and data_exist = 1
            and dividOperateDate >= '{start_date_str}' and dividOperateDate < '{dividOperateDate_str}'
            group by code, divid_year order by divid_year desc;
            """
    result = conn.execute(text(sql)).fetchall()
    if result:
        result_len = len(result)
        #标签：连续转送次数
        if result_len >= 2:
            positive_tags.append(stock_tags.StockTagsDividend.zhaunsong_2_year_all)
        elif result_len >= 1:
            positive_tags.append(stock_tags.StockTagsDividend.zhaunsong_2_year)
        else:
            loss_tags.append(stock_tags.StockTagsDividend.no_zhaunsong_2_year)
    else:
        loss_tags.append(stock_tags.StockTagsDividend.no_zhaunsong_2_year)
        
    return positive_tags, loss_tags


def save_bao_tags_2_db(stock_code, statDateStr, pubDate, positive_tags, loss_tags, tags_type=0, conn=None):
    pubDateStr = statDateStr
    if pubDate is not None:
        pubDateStr = pubDate.strftime("%Y-%m-%d")
    # 补充到数据库
    sql = f"""SELECT * FROM stock_auto_tags WHERE code = '{stock_code}' and statDate = '{statDateStr}' and tags_type = {tags_type}"""
    stock = conn.execute(text(sql)).fetchone()
    if stock is None:
        logger.debug(f"股票{stock_code}不存在，跳过更新自动标签")
        conn.execute(text(f"""INSERT INTO stock_auto_tags (code, statDate, pubDate, tags_type, bao_tags_positive, bao_tags_loss)
            VALUES ('{stock_code}', '{statDateStr}', '{pubDateStr}', {tags_type}, JSON_ARRAY({','.join([f"'{tag}'" for tag in positive_tags])}), JSON_ARRAY({','.join([f"'{tag}'" for tag in loss_tags])}))"""))
        conn.commit()
    else:
        # 解析JSON数组字符串为Python列表
        new_positive_tags = json.loads(stock.bao_tags_positive) if stock.bao_tags_positive else []
        new_loss_tags = json.loads(stock.bao_tags_loss) if stock.bao_tags_loss else []

        # 新的标签增加进去
        for cur_tag in positive_tags:
            if cur_tag in new_positive_tags:
                logger.debug(f"股票{stock_code}自动标签中已存在{cur_tag}，跳过添加")
            else:
                new_positive_tags.append(cur_tag)
        # 排序
        new_positive_tags.sort()

        # 新的标签删除进去
        for cur_tag in loss_tags:
            if cur_tag in new_loss_tags:
                logger.debug(f"股票{stock_code}自动标签中已存在{cur_tag}，跳过删除")
            else:
                new_loss_tags.append(cur_tag)
        # 排序
        new_loss_tags.sort()

        # 如果内容没改变，就不更新
        new_positive_tags_txt = json.dumps(new_positive_tags, ensure_ascii=False)
        new_loss_tags_txt = json.dumps(new_loss_tags, ensure_ascii=False)
        if stock.bao_tags_positive == new_positive_tags_txt and stock.bao_tags_loss == new_loss_tags_txt:
            logger.debug(f"股票{stock_code}自动标签内容未改变，跳过更新")
            return
        
        sql = f"""UPDATE stock_auto_tags 
            SET bao_tags_positive = JSON_ARRAY({','.join([f"'{tag}'" for tag in new_positive_tags])}),
                bao_tags_loss = JSON_ARRAY({','.join([f"'{tag}'" for tag in new_loss_tags])})
            WHERE code = '{stock_code}' and statDate = '{statDateStr}' and tags_type = {tags_type}"""
        conn.execute(text(sql))
        conn.commit()  # 提交连接层面的事务

    # 再查一次
    sql = f"""SELECT * FROM stock_auto_tags WHERE code = '{stock_code}' and statDate = '{statDateStr}' and tags_type = {tags_type}"""
    stock = conn.execute(text(sql)).fetchone()
    logger.debug(f"更新股票{stock_code}自动标签为{stock.bao_tags_positive}，{stock.bao_tags_loss}")

def gen_fenhong_tags_data(conn=None):
    # 初始化分红日期
    init_dividend_date(conn)
    
    # 获取所有上市股票代码
    query = f"SELECT * FROM bao_stock_basic where type='1' order by code asc"
    stocks = conn.execute(text(query)).fetchall()
    
    if not stocks:
        logger.info("未获取到上市股票数据")
        return
    
    total_stocks = len(stocks)
    logger.info(f"开始处理{total_stocks}只股票的自动标签数据")

    # 处理每只股票
    for index, stock in enumerate(stocks):
        #查询全部的分红日期，pubDate=statDate=dividOperateDate保持一致
        sql = f"""select dividOperateDate from bao_stock_dividend where code = '{stock.code}' and dividOperateDate >= '2010-01-01' and data_exist = 1
            and dividOperateDate not in (select statDate from stock_auto_tags where code = '{stock.code}' and tags_type = 2)
            order by dividOperateDate asc"""
        result = conn.execute(text(sql)).fetchall()
        for item in result:
            dividOperateDate = item[0]
            dividOperateDate_str = datetime.datetime.strftime(dividOperateDate, '%Y-%m-%d')

            stock_code = stock.code
            stock_name = stock.code_name
            logger.info(f"处理 {index+1}/{total_stocks}: {stock_code} - {stock_name}, {dividOperateDate_str}")
            
            fenhong_positive_tags = gen_fenhong_auto_tags_4_stock(stock_code, dividOperateDate_str, conn)

            positive_tags = fenhong_positive_tags[0]
            loss_tags = fenhong_positive_tags[1]

            # 补充到数据库
            if len(positive_tags) > 0 or len(loss_tags) > 0:
                save_bao_tags_2_db(stock_code, dividOperateDate_str, dividOperateDate, positive_tags, loss_tags, 2, conn)
            else:
                logger.error(f"股票{stock_code}自动标签中{dividOperateDate_str}无新增标签，跳过添加")


def init_dividend_date(conn):
    #更新earliest_date
    sql = f"""update bao_stock_dividend set earliest_date = LEAST(
                COALESCE(dividPreNoticeDate, '9999-12-31'),
                COALESCE(dividPlanAnnounceDate, '9999-12-31'),
                COALESCE(dividAgmPumDate, '9999-12-31'),
                COALESCE(dividPlanDate, '9999-12-31'),
                COALESCE(dividRegistDate, '9999-12-31'),
                COALESCE(dividOperateDate, '9999-12-31'),
                COALESCE(dividPayDate, '9999-12-31'),
                COALESCE(dividStockMarketDate, '9999-12-31')
            ) where earliest_date is null and data_exist = 1"""
    conn.execute(text(sql))
    conn.commit()  # 提交连接层面的事务
    logger.info(f"bao_stock_dividend表earliest_date字段更新")

if __name__ == "__main__":
    conn = stock_common.get_db_conn()
    logger.info("开始gen_season_tags_data...")
    gen_fenhong_tags_data(conn=conn)
    logger.info("结束gen_season_tags_data...")

    # ["3股息", "20净利"]
    #测试标签字段的更新
"""     conn = stock_common.get_db_conn()
    positive_tags = []
    positive_tags.append("测试")
    loss_tags = []
    loss_tags.append("测试2")
    cur_date = datetime.datetime.strptime("2023-12-31", "%Y-%m-%d").date()
    save_bao_tags_2_db("sh.600000", cur_date, positive_tags, loss_tags, conn) """
