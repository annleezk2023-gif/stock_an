import os
from sqlalchemy import create_engine, text

import sys
# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径

# 数据库连接信息
DATABASE_URI = os.getenv('DATABASE_URL', 'mysql+mysqlconnector://root:123456@localhost/stock?charset=utf8mb4')

# 补充分析数据
def get_db_conn(sql_echo = True):
    engine = create_engine(DATABASE_URI, echo=sql_echo)
    conn = engine.connect()
    return conn

# 获取全部股票基本信息, 包含tags标签
def get_stock_info_tagslist(conn):
    # 获取所有上市股票代码
    # SELECT * FROM bao_stock_basic WHERE type='1' and tags IS NOT NULL AND tags != '' order by id asc
    # SELECT * FROM bao_stock_basic where type='1' order by code asc
    query = f"SELECT * FROM bao_stock_basic WHERE type='1' and tags IS NOT NULL AND tags != '' order by id asc"
    results = conn.execute(text(query)).fetchall()
    return results

def get_stock_info_all(conn):
    # 获取所有上市股票代码
    query = f"SELECT * FROM bao_stock_basic where type='1' order by code asc"
    results = conn.execute(text(query)).fetchall()
    return results

# 获取全部非上市股票基本信息
def get_nostock_info_all(conn):
    # 获取所有上市股票代码
    query = f"SELECT * FROM bao_nostock_basic where status='1' order by code asc"
    results = conn.execute(text(query)).fetchall()
    return results

# 获取2个日期之间的交易日天数
def getTradeNum(start_date, end_date, conn):
    # 检查是否已存在该日期数据
    sql = f"SELECT COUNT(*) FROM bao_trade_date WHERE calendar_date >= '{start_date}' AND calendar_date < '{end_date}' and is_trading_day = 1"
    trade_num = conn.execute(text(sql)).fetchone()
    if trade_num:
        trade_num = trade_num[0]
    else:
        trade_num = 0
    return trade_num


# 获取所有上市基金代码
def get_ak_fund_all(conn):
    query = f"SELECT * FROM ak_fund_basic order by fd_code asc"
    results = conn.execute(text(query)).fetchall()
    return results

# 字符串分割辅助函数：将逗号分隔的字符串转换为列表
def split_tags_to_list(tags_str):
    """将逗号分隔的标签字符串转换为列表"""
    if not tags_str:
        return []
    return [tag.strip() for tag in tags_str.split(',') if tag.strip()]

# 字符串合并辅助函数：将列表转换为逗号分隔的字符串
def join_list_to_tags(tags_list):
    """将标签列表转换为逗号分隔的字符串"""
    if not tags_list:
        return None
    return ','.join(sorted(tags_list))




# 测试
if __name__ == "__main__":
    conn = get_db_conn()
    start_date = '2023-01-01'
    end_date = '2023-01-05'
    trade_num = getTradeNum(start_date, end_date, conn)
    print(f"{start_date} 到 {end_date} 之间的交易日天数: {trade_num}")
    conn.close()