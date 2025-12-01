import sys
import os
from sqlalchemy import text

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common

# 配置logger
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


""" profit表的经营数据，依赖bao_stock_cash_flow表。补充profit表的计算字段，用于后续自动标签 """

def bao_stock_profit_data(conn = None):
    # 更新MBRevenue_2
    sql = f"update bao_stock_profit set MBRevenue_2=netProfit/npMargin where (MBRevenue is null or MBRevenue = 0) AND npMargin > 0 and netProfit > 0 and data_exist = 1"
    conn.execute(text(sql))
    conn.commit()  # 提交连接层面的事务
    logger.info(f"bao_stock_profit表MBRevenue_2字段更新")

    # 更新bao_stock_profit表数据
    sql = f"SELECT count(*) FROM bao_stock_profit where data_exist = 1 and MBRevenue_percent is null"
    results = conn.execute(text(sql)).fetchone()
    total_count = results[0]
    if total_count == 0:
        logger.info(f"执行结束: bao_stock_profit表 无数据")
        return

    done_count = 0
    while True:
        sql = f"SELECT * FROM bao_stock_profit where data_exist = 1 and MBRevenue_percent is null order by code asc, year asc, quarter asc LIMIT 100"
        results = conn.execute(text(sql)).fetchall()
        if results is None or len(results) < 1:
            # 无数据，结束
            logger.info(f"执行结束: bao_stock_profit表 无数据")
            break
        # 开始循环更新
        for item in results:
            single_update_trade_data(cur_profit=item, conn=conn)
        # 每批次提交一次DB
        conn.commit()
        done_count += len(results)
        logger.info(f"执行结束: bao_stock_profit表，己处理 {done_count} / {total_count}, id:{results[0].id} {results[0].code} {results[0].statDate}")

def single_update_trade_data(cur_profit = None, conn = None):
    # 总营业额优先取下载的数据，其次取计算的数据
    cur_MBRevenue = 0
    if cur_profit.MBRevenue is not None and cur_profit.MBRevenue > 0:
        cur_MBRevenue = cur_profit.MBRevenue
    else:
        logger.debug(f"执行结束: bao_stock_profit表 MBRevenue为空或0, {cur_profit.code} {cur_profit.year} {cur_profit.quarter}")
        if cur_profit.MBRevenue_2 is not None and cur_profit.MBRevenue_2 > 0:
                cur_MBRevenue = cur_profit.MBRevenue_2
    
    if cur_MBRevenue is None or cur_MBRevenue == 0:
        logger.debug(f"执行结束: bao_stock_profit表 MBRevenue，MBRevenue_2为空或0, {cur_profit.code} {cur_profit.year} {cur_profit.quarter}")
        sql = f"UPDATE bao_stock_profit SET MBRevenue_percent = 0 WHERE id = {cur_profit.id}"
        conn.execute(text(sql))
        conn.commit()  # 提交连接层面的事务
        logger.debug(f"bao_stock_profit表更新数据, {cur_profit.id} {cur_profit.code} {cur_profit.year} {cur_profit.quarter}")
        return
    
    # cash_all = mb_revenue营收*cfo_to_gr总现金流占营收比例
    # cash_income = mb_revenue营收*cfo_to_or占比
    cash_all = 0
    cash_income = 0
    # 取当前季度的现金流数据
    sql = f"SELECT * FROM bao_stock_cash_flow where code = '{cur_profit.code}' and year = {cur_profit.year} and quarter = {cur_profit.quarter} and data_exist = 1"
    results = conn.execute(text(sql)).fetchone()
    if results is None or len(results) < 1:
        logger.debug(f"执行结束: bao_stock_cash_flow表 无数据, {cur_profit.code} {cur_profit.year} {cur_profit.quarter}")
    else:
        if results.CFOToGr is not None and results.CFOToGr > 0:
            cash_all = cur_MBRevenue * results.CFOToGr * 100
        else:
            logger.debug(f"执行结束: bao_stock_cash_flow表 CFOToGr为空或0, {cur_profit.code} {cur_profit.year} {cur_profit.quarter}")
        
        if results.CFOToOR is not None and results.CFOToOR > 0:
            cash_income = cur_MBRevenue * results.CFOToOR * 100
        else:
            logger.debug(f"执行结束: bao_stock_cash_flow表 CFOToOR为空或0, {cur_profit.code} {cur_profit.year} {cur_profit.quarter}")

    # MBRevenue_percent 营收同比
    MBRevenue_percent = 100
    cash_all_percent = 100
    cash_income_percent = 100
    pre_year = cur_profit.year - 1
    # 取去年相同季度的数据
    sql = f"SELECT * FROM bao_stock_profit where code = '{cur_profit.code}' and year = {pre_year} and quarter = {cur_profit.quarter} and data_exist = 1"
    results = conn.execute(text(sql)).fetchone()
    if results is None or len(results) < 1:
        logger.debug(f"执行结束: bao_stock_profit表 无数据, {cur_profit.code} {pre_year} {cur_profit.quarter}")
    else:
        # 当前值/去年值
        if results.cash_all is not None and results.cash_all > 0:
            cash_all_percent = cash_all / results.cash_all * 100
        else:
            logger.debug(f"执行结束: bao_stock_cash_flow表 cash_all为空或0, {cur_profit.code} {cur_profit.year} {cur_profit.quarter}")
        
        if results.cash_income is not None and results.cash_income > 0:
            cash_income_percent = cash_income / results.cash_income * 100
        else:
            logger.debug(f"执行结束: bao_stock_cash_flow表 cash_income为空或0, {cur_profit.code} {cur_profit.year} {cur_profit.quarter}")

        # 优先取下载的MBRevenue，其次取计算的MBRevenue_2
        if results.MBRevenue is not None and results.MBRevenue > 0:
            MBRevenue_percent = cur_MBRevenue / results.MBRevenue * 100
        else:
            if results.MBRevenue_2 is not None and results.MBRevenue_2 > 0:
                MBRevenue_percent = cur_MBRevenue / results.MBRevenue_2 * 100
            else:
                logger.debug(f"执行结束: bao_stock_profit表 MBRevenue，MBRevenue_2为空或0, {cur_profit.code} {cur_profit.year} {cur_profit.quarter}")

    # 仅在第4季度计算股利支付率
    dividend_pay_percent = 0
    if cur_profit.quarter == 4:
        # 查询当年的分红金额
        total_tax = 0
        # 取当年度的分红总金额
        sql = f"SELECT sum(dividCashPsBeforeTax) total_tax FROM bao_stock_dividend where code = '{cur_profit.code}' and year = {cur_profit.year} and data_exist = 1"
        results = conn.execute(text(sql)).fetchone()
        if results is None or len(results) < 1 or results.total_tax is None:
            logger.debug(f"执行结束: bao_stock_dividend表 无数据, {cur_profit.code} {cur_profit.year}")
        elif results.total_tax > 0:
            total_tax = results.total_tax
        else:
            logger.debug(f"执行结束: bao_stock_dividend表 分红为0, {cur_profit.code} {cur_profit.year}")
        
        if total_tax == 0:
            logger.debug(f"执行结束: bao_stock_dividend表 分红金额为0, {cur_profit.code} {cur_profit.year}")
            dividend_pay_percent = 0
        else:
            # 每股分红金额>0，查询当年的每股收益
            sql = f"SELECT sum(epsTTM) total_eps FROM bao_stock_profit where code = '{cur_profit.code}' and year = {cur_profit.year} and data_exist = 1"
            results = conn.execute(text(sql)).fetchone()
            if results is None or len(results) < 1 or results is None:
                logger.debug(f"执行结束: bao_stock_profit表 eps_ttm为空, {cur_profit.code} {cur_profit.year}")
            elif results.total_eps > 0:
                # 每股分红金额/每股收益 * 100
                dividend_pay_percent = total_tax / results.total_eps * 100
            else:
                logger.debug(f"执行结束: bao_stock_profit表 eps_ttm为0, {cur_profit.code} {cur_profit.year}")

    # 更新bao_stock_profit表数据
    sql = f"UPDATE bao_stock_profit SET MBRevenue_percent = {MBRevenue_percent}, cash_all = {cash_all}, cash_all_percent = {cash_all_percent}, cash_income = {cash_income}, cash_income_percent = {cash_income_percent}, dividend_pay_percent = {dividend_pay_percent} WHERE id = {cur_profit.id}"
    conn.execute(text(sql))
    conn.commit()  # 提交连接层面的事务
    logger.debug(f"bao_stock_profit表更新数据, {cur_profit.id} {cur_profit.code} {cur_profit.year} {cur_profit.quarter}")


if __name__ == "__main__":
    logger.info("开始补充数据...")
    conn = stock_common.get_db_conn(sql_echo=False)
    bao_stock_profit_data(conn)
    conn.close()
    logger.info("补充数据完成！")