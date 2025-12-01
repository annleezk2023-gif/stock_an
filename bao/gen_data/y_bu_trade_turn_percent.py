import sys
import os
import threading
from sqlalchemy import text

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common
sys.path.append(root_path + '/bao/gen_data')
import gen_pe1year_bao_stock_trade2

# 配置logger
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

""" 开始补turn换手率数据
如果换手率数据有错，则将换手率turn_percent_5置null。再在这里重新跑即可
pe_year_1_percent is not null and turn_percent_5 is null """

def get_turn_trade_page(table_name, page_num, page_size, conn):
    offset = (page_num - 1) * page_size
    query = f"SELECT * FROM {table_name} where pe_year_1_percent is not null and turn_percent_5 is null order by code asc, date asc LIMIT {offset}, {page_size}"
    results = conn.execute(text(query)).fetchall()
    return results

def get_turn_trade_total(table_name, conn):
    query = f"SELECT count(*) FROM {table_name} where pe_year_1_percent is not null and turn_percent_5 is null"
    results = conn.execute(text(query)).fetchone()
    return results[0]

def single_update_turn_data(cur_table_name, cur_code, cur_date_str, conn):
    # 近5天换手率
    turn_percent_5 = b_trade_pe1year.get_stock_trade_turn(cur_table_name, cur_code, cur_date_str, 5, conn)
    # 近15天换手率
    turn_percent_15 = b_trade_pe1year.get_stock_trade_turn(cur_table_name, cur_code, cur_date_str, 15, conn)
    # 近30天换手率
    turn_percent_30 = b_trade_pe1year.get_stock_trade_turn(cur_table_name, cur_code, cur_date_str, 30, conn)
    
    # 更新DB 股票信息，有则更新，无则新增
    sql = f"""update {cur_table_name} set turn_percent_5 = {turn_percent_5}, turn_percent_15 = {turn_percent_15}, turn_percent_30 = {turn_percent_30}
                    where code = '{cur_code}' and date = '{cur_date_str}'"""
    conn.execute(text(sql))
    conn.commit()

    logger.debug("更新分析后的数据: %s %s" % (cur_code, cur_date_str))


# 补充turn换手率数据
def gen_turn_data(divide_table_num=0, conn=None):
    # 获取turn为空的数据
    cur_table_name = f"""bao_stock_trade_{divide_table_num}"""

    total_count = get_turn_trade_total(table_name=cur_table_name, conn=conn)
    if total_count == 0:
        logger.info(f"执行结束: {divide_table_num}表 无数据")
        return
        
    done_count = 0
    while True:
        trade_data_list = get_turn_trade_page(table_name=cur_table_name, page_num=1, page_size=10, conn=conn)
        if trade_data_list is None or len(trade_data_list) < 1:
            # 无数据，结束
            logger.info(f"执行结束: {divide_table_num}表 {done_count} / {total_count}")
            break
        # 开始循环更新
        for item in trade_data_list:
            single_update_turn_data(cur_table_name=cur_table_name, cur_code=item.code, cur_date=item.date, conn=conn)
        # 每批次提交一次DB
        conn.commit()
        done_count += len(trade_data_list)
        logger.info(f"执行: {divide_table_num}表 {done_count} / {total_count}")
            

if __name__ == "__main__":
    logger.info("开始补充bao_stock_trade表turn换手率数据...")

    for divide_table_num in range(0, 1, 1):
        conn = stock_common.get_db_conn(sql_echo=False)
        # 创建并启动线程
        t = threading.Thread(
            target=gen_turn_data,
            args=(divide_table_num, conn),
            name=f"turn数据生成线程-{divide_table_num}"
        )
        t.start()
        logger.info(f"启动线程执行gen_turn_data(divide_table_num={divide_table_num})...")
        #conn.close()
    #t.join()  # 等待所有线程完成
    #conn.close()
