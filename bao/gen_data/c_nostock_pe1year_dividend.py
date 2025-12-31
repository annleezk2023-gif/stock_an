import sys
import os
import threading
from sqlalchemy import text
import numpy as np

# 配置logger
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common

# bao_stock_trade_0表数据补充，依赖bao_stock_dividend

def get_stock_trade_page(table_name, page_num, page_size, conn):
    offset = (page_num - 1) * page_size
    sql = f"SELECT * FROM {table_name} where pe_year_1_percent is null order by code asc, date asc LIMIT {offset}, {page_size}"
    results = conn.execute(text(sql)).fetchall()
    return results

def get_stock_trade_total(table_name, conn):
    sql = f"SELECT count(*) FROM {table_name} where pe_year_1_percent is null"
    results = conn.execute(text(sql)).fetchone()
    return results[0]

def update_stock_total_market_value(table_name, conn):
    sql = f"""update {table_name} set total_market_value = amount/turn*100/10000/10000 where total_market_value is null"""
    conn.execute(text(sql))
    conn.commit()

def get_stock_trade_turn(cur_table_name, cur_code, cur_date_str, days, conn):
    # 包含当天的数据
    sql = f"""SELECT SUM(turn) AS total_turn FROM (
                select b.turn from {cur_table_name} b where b.code = '{cur_code}' and date <= '{cur_date_str}' order by date desc limit {days}) AS subquery"""
    results = conn.execute(text(sql), {'date': cur_date_str}).fetchone()
    if results is None or results.total_turn is None:
        # 无数据，换手率设置为0
        return 0
    else:
        return results.total_turn

def get_fenhong_percent(cur_code, cur_date_str, cur_open_price, conn):
    # 1年内，包含交易日当天的分红
    sql = f"""select sum(dividCashPsBeforeTax) total_cash from bao_stock_dividend where code = '{cur_code}'
                and dividOperateDate <= '{cur_date_str}' and dividOperateDate > DATE_SUB('{cur_date_str}', INTERVAL 1 YEAR)"""
    results = conn.execute(text(sql)).fetchone()
    if results is None or results.total_cash is None:
        # 最近一年分红为0
        logger.debug("最近一年没有分红: %s %s", cur_code, cur_date_str)
        return 0
    else:
        stock_fenghong_percent = results.total_cash/cur_open_price*100
        return round(stock_fenghong_percent, 2)

def single_update_trade_data(cur_table_name, cur_code, cur_date_str, cur_open_price, conn):
    insert_data = {'code': cur_code, 'date': cur_date_str}
    stock_fenghong_percent = None

    # 股息率 最近一年分红/开盘价*100
    if cur_open_price is None or cur_open_price == 0:
        stock_fenghong_percent = 0
    else:
        stock_fenghong_percent = get_fenhong_percent(cur_code, cur_date_str, cur_open_price, conn)
    insert_data['stock_fenghong_percent'] = stock_fenghong_percent

    # 近5天换手率
    insert_data['turn_percent_5'] = get_stock_trade_turn(cur_table_name, cur_code, cur_date_str, 5, conn)
    # 近15天换手率
    insert_data['turn_percent_15'] = get_stock_trade_turn(cur_table_name, cur_code, cur_date_str, 15, conn)
    # 近30天换手率
    insert_data['turn_percent_30'] = get_stock_trade_turn(cur_table_name, cur_code, cur_date_str, 30, conn)

    # 近1年pe 百分位
    param_year = [1,3,5,10]
    for year in param_year:
        pepbpspcf_percent = get_pepspbpcf_percent(cur_table_name, cur_code, cur_date_str, year, conn)
        if pepbpspcf_percent is None:
            logger.debug(f"最近{year}年没有pepbpspcf数据: %s %s", cur_code, cur_date_str)
            insert_data[f'pe_year_{year}_percent'] = 0
            insert_data[f'ps_year_{year}_percent'] = 0
            insert_data[f'pb_year_{year}_percent'] = 0
            insert_data[f'pcf_year_{year}_percent'] = 0
        else:
            insert_data[f'pe_year_{year}_percent'] = round(float(pepbpspcf_percent['peTTM']), 2)
            insert_data[f'ps_year_{year}_percent'] = round(float(pepbpspcf_percent['psTTM']), 2)
            insert_data[f'pb_year_{year}_percent'] = round(float(pepbpspcf_percent['pbMRQ']), 2)
            insert_data[f'pcf_year_{year}_percent'] = round(float(pepbpspcf_percent['pcfNcfTTM']), 2)

    # 更新DB 股票信息
    up_query = f"""update {cur_table_name} set stock_fenghong_percent = :stock_fenghong_percent, turn_percent_5 = :turn_percent_5, turn_percent_15 = :turn_percent_15, turn_percent_30 = :turn_percent_30, 
                    pe_year_1_percent = :pe_year_1_percent, pe_year_3_percent = :pe_year_3_percent, pe_year_5_percent = :pe_year_5_percent, pe_year_10_percent = :pe_year_10_percent,
                    pb_year_1_percent = :pb_year_1_percent, pb_year_3_percent = :pb_year_3_percent, pb_year_5_percent = :pb_year_5_percent, pb_year_10_percent = :pb_year_10_percent,
                    ps_year_1_percent = :ps_year_1_percent, ps_year_3_percent = :ps_year_3_percent, ps_year_5_percent = :ps_year_5_percent, ps_year_10_percent = :ps_year_10_percent,
                    pcf_year_1_percent = :pcf_year_1_percent, pcf_year_3_percent = :pcf_year_3_percent, pcf_year_5_percent = :pcf_year_5_percent, pcf_year_10_percent = :pcf_year_10_percent
                    where code = :code and date = :date"""
    conn.execute(text(up_query), insert_data)
    logger.debug("更新分析后的数据: %s %s", cur_code, cur_date_str)

def get_pepspbpcf_percent(cur_table_name, cur_code, cur_date_str, cur_year_num, conn):
    # 取日K数据，包含当天的交易数据
    query = f"""SELECT peTTM, pbMRQ, psTTM, pcfNcfTTM, code, date, turn_percent_5
               FROM {cur_table_name} where code = '{cur_code}' and date > DATE_SUB('{cur_date_str}', INTERVAL {cur_year_num} YEAR) and date <= '{cur_date_str}' order by date asc"""
    history_data = conn.execute(text(query)).fetchall()
    if history_data is None or len(history_data) == 0:
        # 无数据
        logger.debug("%s 最近 %s 年无交易数据: %s %s", cur_table_name, cur_year_num, cur_code, cur_date)
        return None
    
    # 获取当前值
    sql_current = f"""SELECT peTTM, pbMRQ, psTTM, pcfNcfTTM, code, date, turn_percent_5
            FROM {cur_table_name} 
            WHERE code = '{cur_code}' AND date = '{cur_date_str}'
            """
    current_result = conn.execute(text(sql_current)).fetchone()
    
    if not current_result:
        # 无数据
        logger.debug("无当天的交易数据 %s %s %s", cur_table_name, cur_code, cur_date_str)
        return None
    
    param_columns = ['peTTM','pbMRQ','psTTM','pcfNcfTTM','turn_percent_5']
    return_result = {}

    for index, cur_column in enumerate(param_columns):
        current_value = current_result[index]
        
        # 计算百分位
        history_values = [d[index] for d in history_data]
        percentile = np.sum(np.array(history_values) <= current_value) / len(history_values) * 100
        return_result[cur_column] = percentile
    
    return return_result
    

# 补充分析数据
def gen_pe_data(divide_table_num=0, conn=None):

    # 获取pe为空的数据
    cur_table_name = f"""bao_stock_trade_{divide_table_num}"""

    # 更新股票总市值
    update_stock_total_market_value(table_name=cur_table_name, conn=conn)
    logger.info(f"执行结束: {divide_table_num}表 更新股票总市值")

    total_count = get_stock_trade_total(table_name=cur_table_name, conn=conn)
    if total_count == 0:
        logger.info(f"执行结束: {divide_table_num}表 无数据")
        return

    done_count = 0
    while True:
        trade_data_list = get_stock_trade_page(table_name=cur_table_name, page_num=1, page_size=10, conn=conn)
        if trade_data_list is None or len(trade_data_list) < 1:
            # 无数据，结束
            logger.info(f"执行结束: {divide_table_num}表 无数据")
            break
        # 开始循环更新
        for item in trade_data_list:
            cur_date_str = item.date.strftime("%Y-%m-%d")
            single_update_trade_data(cur_table_name=cur_table_name, cur_code=item.code, cur_date_str=cur_date_str, cur_open_price=item.open, conn=conn)
        # 每批次提交一次DB
        conn.commit()
        done_count += len(trade_data_list)
        logger.info(f"执行结束: {divide_table_num}表，己处理 {done_count} / {total_count}, {trade_data_list[0].id} {trade_data_list[0].code} {trade_data_list[0].date}")


if __name__ == "__main__":
    logger.info("开始补充bao_stock_trade表pe数据...")
    #000001覆盖上交所全部 2300 余家 A 股
    #399106覆盖深市全部 500 余家 A 股
    #000680覆盖科创板全部 590 余家上市公司
    #399102覆盖创业板全部 500 余家 A 股
    zhishu_list = {"sh.000001": "上证综指", "sz.399106": "深证综指", "sh.000680": "科创综指", "sz.399102": "创业板综指"}

    #沪深市场规模最大、流动性最好的 300 只股票（大盘）
    zhishu_list.add("sh.000300", "沪深300")
    #剔除沪深 300 成分股后，规模和流动性排名前 500 的股票（中盘）
    zhishu_list.add("sh.000905", "中证500")
    #剔除沪深 300 + 中证 500 成分股后，规模和流动性排名前 1000 的股票（中小盘）
    zhishu_list.add("sh.000852", "中证1000")

    #选取 50 只市值大、流动性好的企业，集中了半导体、AI、生物医药等前沿技术领域的龙头，研发投入强度高
    zhishu_list.add("sh.000688", "科创50")
    #选取 100 家市值大、流动性好的企业，高新技术企业占比超 9 成，战略新兴产业占比超 8 成，涵盖新能源、生物医药、高端制造等领域
    zhishu_list.add("sz.399006", "创业板指")
    #选取 50 只市值大、流动性好的企业，集中了创业板中的新兴成长企业
    zhishu_list.add("sz.399673", "创业板50")



    etf_list = {"沪深300": ["510310","510330","159919"]}
    etf_list["创业板指"] = ["159915","159949"]
    etf_list["创业板指"] = ["159915","159949"]
    etf_list["科创板50"] = ["588000","588080"]
    
    for divide_table_num in range(0, 10, 1):
        conn = stock_common.get_db_conn(sql_echo=False)
        # 创建并启动线程
        t = threading.Thread(
            target=gen_pe_data,
            args=(divide_table_num, conn),
            name=f"pe数据生成线程-{divide_table_num}"
        )
        t.start()
        logger.info(f"启动线程执行gen_pe_data(divide_table_num={divide_table_num})...")
        #conn.close()
    #t.join()  # 等待所有线程完成
    #conn.close()

