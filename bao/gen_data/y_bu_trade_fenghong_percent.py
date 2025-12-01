import datetime
import sys
import os
from sqlalchemy import text

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common
sys.path.append(root_path + '/bao/gen_data')
import b_trade_pe1year_dividend

# 配置logger
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

""" 开始补分红数据
如果分红数据有错，则将分红数据置null。再在这里重新跑即可
pe_year_1_percent is not null and stock_fenghong_percent is null """

def get_fh_trade_page(table_name, page_num, page_size, conn):
    offset = (page_num - 1) * page_size
    sql = f"SELECT * FROM {table_name} where pe_year_1_percent is not null and stock_fenghong_percent is null order by code asc, date asc LIMIT {offset}, {page_size}"
    results = conn.execute(text(sql)).fetchall()
    return results

def get_fh_trade_total(table_name, conn):
    sql = f"SELECT count(*) FROM {table_name} where pe_year_1_percent is not null and stock_fenghong_percent is null"
    results = conn.execute(text(sql)).fetchone()
    return results[0]


def single_update_fh_trade_data(cur_table_name, cur_code, cur_date_str, cur_open_price, conn):
    stock_fenghong_percent = None
    # 股息率 最近一年分红/开盘价*100
    if cur_open_price is None or cur_open_price == 0:
        stock_fenghong_percent = 0
    else:
        stock_fenghong_percent = b_trade_pe1year_dividend.get_fenhong_percent(cur_code, cur_date_str, cur_open_price, conn)

    # 更新DB 股票信息，有则更新，无则新增
    sql = f"""update {cur_table_name} set stock_fenghong_percent = {stock_fenghong_percent}
                    where code = '{cur_code}' and date = '{cur_date_str}'"""
    conn.execute(text(sql))
    conn.commit()  # 提交连接层面的事务

    logger.debug("更新分析后的数据: %s %s %s", cur_table_name, cur_code, cur_date_str)

# 补充分红数据
def gen_fh_data(divide_table_num=0, conn=None):
    # 获取fenghong为空的数据
    cur_table_name = f"""bao_stock_trade_{divide_table_num}"""

    total_count = get_fh_trade_total(table_name=cur_table_name, conn=conn)
    if total_count == 0:
        logger.info(f"执行结束: {divide_table_num}表 无数据")
        return

    done_count = 0
    while True:
        trade_data_list = get_fh_trade_page(table_name=cur_table_name, page_num=1, page_size=10, conn=conn)
        if trade_data_list is None or len(trade_data_list) < 1:
            # 无数据，结束
            logger.info(f"执行结束: {divide_table_num}表 {done_count} / {total_count}")
            break
        # 开始循环更新
        for item in trade_data_list:
            cur_date_str = datetime.datetime.strptime(item.date, '%Y-%m-%d').strftime('%Y-%m-%d')
            single_update_fh_trade_data(cur_table_name=cur_table_name, cur_code=item.code, cur_date_str=cur_date_str, cur_open_price=item.open, conn=conn)
        # 每批次提交一次DB
        conn.commit()
        done_count += len(trade_data_list)
        logger.info(f"执行: {divide_table_num}表 {done_count} / {total_count} 提交DB")
     

if __name__ == "__main__":
    logger.info("开始补充bao_stock_trade表fenghong数据...")
    conn = stock_common.get_db_conn(sql_echo=False)
    # 补充fenghong百分比数据
    for divide_table_num in range(0, 10, 1):
       gen_fh_data(divide_table_num=divide_table_num, conn=conn)
    conn.close()
    logger.info("补充bao_stock_trade表fenghong数据完成！")
