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
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

""" bao_stock_trade_0 单表数据补充, 填充空值为前一条记录的值 """


def bu_trade_data(conn = None):
    param_columns = ['peTTM','psTTM','pbMRQ','pcfNcfTTM']

    param_tables = []
    for divide_table_num in range(0, 10, 1):
        param_tables.append(f"bao_stock_trade_{divide_table_num}")

    for cur_table in param_tables:
        for cur_column in param_columns:
            sql = f"SELECT * FROM {cur_table} where {cur_column} is null"
            results = conn.execute(text(sql))
            if results:
                total_count = results.rowcount
                logger.info(f"table: {cur_table}, column: {cur_column}, count: {total_count}")
                for index, item in enumerate(results):
                    logger.debug(f"table: {cur_table}, column: {cur_column}, {index}/{total_count}")
                    # 查询前一条记录
                    sql = f"SELECT * FROM {cur_table} WHERE code = '{item.code}' AND date < '{item.date}' and {cur_column} is not null ORDER BY date DESC LIMIT 1"
                    prev_results = conn.execute(text(sql))
                    if prev_results and prev_results.rowcount > 0:
                        # 用前一条记录的值
                        prev_item = prev_results.fetchone()
                        sql = f"UPDATE {cur_table} SET {cur_column} = {prev_item.__getattribute__(cur_column)} WHERE code = '{item.code}' AND date = '{item.date}'"
                        conn.execute(text(sql))
                        conn.commit()
                    else:
                        # 用0填充
                        logger.debug(f"table: {cur_table}, column: {cur_column} {item.code} {item.date}, {index}/{total_count}, 没有前一条记录")
                        sql = f"UPDATE {cur_table} SET {cur_column} = 0 WHERE code = '{item.code}' AND date = '{item.date}'"
                        conn.execute(text(sql))
                        conn.commit()
            
if __name__ == "__main__":
    logger.info("开始补充数据...")
    conn = stock_common.get_db_conn()
    bu_trade_data(conn)
    conn.close()
    logger.info("补充数据完成！")