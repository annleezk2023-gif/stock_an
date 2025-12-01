import baostock as bs
import pandas as pd
import sys
import os
from sqlalchemy import text

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common
sys.path.append(root_path + '/bao')
import baostock_common

# 配置logger
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 获取并保存证券基本信息数据
def fetch_and_save_stock_basic(conn=None):
    # 查询证券基本资料
    rs = bs.query_stock_basic()
    if rs.error_code != '0':
        logger.error(f"查询失败: {rs.error_msg}")
        return False
    
    # 获取数据
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    
    # 转换为DataFrame
    columns = ['code', 'code_name', 'ipo_date', 'out_date', 'type', 'status']
    df = pd.DataFrame(data_list, columns=columns)
    
    # 保存到数据库

    stock_count_add = 0
    stock_count_update = 0
    nostock_count_add = 0
    nostock_count_update = 0
    
    for _, row in df.iterrows():
        # 判断是股票还是非股票
        is_stock = row['type'] == '1'
        if row['out_date'] == '':
            row['out_date'] = None
        
        if is_stock:
            # 处理股票信息
            existing_stock = conn.execute(
                text("SELECT * FROM bao_stock_basic WHERE code = :code"),
                {'code': row['code']}
            ).fetchone()
            
            if existing_stock:
                if existing_stock.code_name != row['code_name'] or existing_stock.status != row['status'] or existing_stock.out_date != row['out_date']:
                    logger.debug(f"证券代码为{row['code']}的记录 名称、状态或上市日期发生改变")
                    
                    # 更新现有记录
                    conn.execute(
                        text("UPDATE bao_stock_basic SET code_name = :code_name, out_date = :out_date, status = :status WHERE code = :code"),
                        {'code': row['code'], 'code_name': row['code_name'], 'out_date': row['out_date'], 'status': row['status']}
                    )
                    stock_count_update += 1
            else:
                # 创建新记录
                conn.execute(
                    text("INSERT INTO bao_stock_basic (code, code_name, ipo_date, out_date, type, status) VALUES (:code, :code_name, :ipo_date, :out_date, :type, :status)"),
                    {'code': row['code'], 'code_name': row['code_name'], 'ipo_date': row['ipo_date'], 'out_date': row['out_date'], 'type': row['type'], 'status': row['status']}
                )
                
                stock_count_add += 1
        else:
            # 处理非股票信息
            existing_nostock = conn.execute(
                text("SELECT * FROM bao_nostock_basic WHERE code = :code"),
                {'code': row['code']}
            ).fetchone()
            
            if existing_nostock:
                if existing_nostock.code_name != row['code_name'] or existing_nostock.status != row['status'] or existing_nostock.out_date != row['out_date']:
                    logger.debug(f"证券代码为{row['code']}的记录 名称、状态或上市日期发生改变")
                    # 更新现有记录
                    conn.execute(
                        text("UPDATE bao_nostock_basic SET code_name = :code_name, out_date = :out_date, status = :status WHERE code = :code"),
                        {'code': row['code'], 'code_name': row['code_name'], 'out_date': row['out_date'], 'status': row['status']}
                    )
                    nostock_count_update += 1
            else:
                # 创建新记录
                conn.execute(
                    text("INSERT INTO bao_nostock_basic (code, code_name, ipo_date, out_date, type, status) VALUES (:code, :code_name, :ipo_date, :out_date, :type, :status)"),
                    {'code': row['code'], 'code_name': row['code_name'], 'ipo_date': row['ipo_date'], 'out_date': row['out_date'], 'type': row['type'], 'status': row['status']}
                )
                nostock_count_add += 1
    
    logger.info(f"成功获取并保存了{stock_count_add}条新增股票基本信息和{stock_count_update}条更新股票基本信息")
    logger.info(f"成功获取并保存了{nostock_count_add}条新增非股票基本信息和{nostock_count_update}条更新非股票基本信息")
    conn.commit()
    
    
if __name__ == "__main__":
    logger.info("开始更新证券基本信息...")
    if not baostock_common.login_baostock():
        raise Exception("登录baostock失败")
    try:
        conn = stock_common.get_db_conn()
        fetch_and_save_stock_basic(conn)
        conn.close()
        logger.info("更新证券基本信息完成！")
    except Exception as e:
        logger.error(f"批量处理失败: {str(e)}")
    finally:
        # 确保登出
        baostock_common.logout_baostock()
