import sys
import os
from sqlalchemy import text
import re

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common

# 配置logger
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

""" bao_stock_dividend单表数据补充，提取转、送的股票数 """

def get_zhuan_song(zs_text):
    # 正则表达式：匹配“转”或“送”后面紧跟的数字（整数或小数）
    # 分组1：匹配“转”或“送”；分组2：匹配紧跟的数字
    pattern = r'(转|送)(\d+\.?\d*)'
    
    # 查找所有匹配
    matches = re.findall(pattern, zs_text)

    # 整理结果
    result = {}
    for match in matches:
        keyword = match[0]
        number = match[1]
        result[keyword] = float(number) if '.' in number else int(number)

    logger.debug(f"{zs_text} 提取结果：{result}")
    return result

# 转送
def bao_stock_dividend_zhuansong_num(conn = None):
    # bao_stock_dividend表提取转、送的股票数
    sql = f"SELECT count(*) FROM bao_stock_dividend where zhuan_num is null and dividCashStock is not null"
    results = conn.execute(text(sql)).fetchone()
    total_count = results[0]
    if total_count == 0:
        logger.info(f"执行结束: bao_stock_dividend表 无数据")
        return

    while True:
        sql = f"SELECT * FROM bao_stock_dividend where zhuan_num is null and dividCashStock is not null limit 100"
        results = conn.execute(text(sql)).fetchall()
        if results is None or len(results) < 1:
            # 无数据，结束
            logger.info(f"执行结束: bao_stock_dividend表 无数据")
            return

        for index, item in enumerate(results):
            if item.dividCashStock is None or item.dividCashStock == '':
                continue

            logger.debug(f"bao_stock_dividend表zhuan_num is null, {index}/{total_count}")
            zs_result = get_zhuan_song(item.dividCashStock)

            zhuan_num = 0
            song_num = 0
            if '转' in zs_result:
                zhuan_num = zs_result['转']
            if '送' in zs_result:
                song_num = zs_result['送']
            
            sql = f"UPDATE bao_stock_dividend SET zhuan_num = {zhuan_num}, song_num = {song_num} WHERE id = {item.id}"
            conn.execute(text(sql))
            conn.commit()

if __name__ == "__main__":
    logger.info("开始补充数据...")
    conn = stock_common.get_db_conn()
    bao_stock_dividend_zhuansong_num(conn)
    conn.close()
    logger.info("补充数据完成！")