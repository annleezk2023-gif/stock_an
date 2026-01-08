import baostock as bs
import pandas as pd
from datetime import timedelta, date
from sqlalchemy import text
import os
import baostock as bs
import sys
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

# 获取交易日数据并保存到数据库
def fetch_and_save_trade_dates(start_date, end_date, conn=None):
    # 查询交易日数据
    rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
    if rs.error_code != '0':
        logger.error(f"查询失败: {rs.error_msg}")
        return False
    
    # 获取数据
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    
    # 转换为DataFrame
    columns = ['calendar_date', 'is_trading_day']
    df = pd.DataFrame(data_list, columns=columns)
    
    # 转换数据类型
    df['is_trading_day'] = df['is_trading_day'].apply(lambda x: True if x == '1' else False)
    
    # 保存到数据库
    for _, row in df.iterrows():
        # 检查是否已存在该日期数据
        existing_date = conn.execute(text("SELECT * FROM bao_trade_date WHERE calendar_date = :calendar_date"), 
                                    {'calendar_date': row['calendar_date']}).fetchone()
        
        if existing_date:
            # 更新现有记录
            conn.execute(text("UPDATE bao_trade_date SET is_trading_day = :is_trading_day WHERE calendar_date = :calendar_date"),
                            {'is_trading_day': row['is_trading_day'], 'calendar_date': row['calendar_date']})
        else:
            # 创建新记录
            conn.execute(text("insert into bao_trade_date(calendar_date, is_trading_day) values(:calendar_date, :is_trading_day)"),
                            {'calendar_date': row['calendar_date'], 'is_trading_day': row['is_trading_day']})
    # 提交事务
    conn.commit()
    
    logger.info(f"成功获取并保存了{len(df)}条交易日数据")


# 增量更新交易日数据
def incremental_update_trade_dates(conn=None):
    # 获取数据库中最新的交易日
    latest_date = conn.execute(text("SELECT MAX(calendar_date) FROM bao_trade_date")).fetchone()
    
    if not latest_date or not latest_date[0]:
        # 如果数据库为空，从2007年开始获取
        start_date = date(2007, 1, 1)
    else:
        # 否则从最新日期的下一天开始获取
        start_date = (latest_date[0] + timedelta(days=1))
    
    end_date = date(2026, 12, 31)
    
    # 检查是否需要更新
    if start_date > end_date:
        logger.info("没有需要更新的交易日数据")
        return
    
    logger.info(f"正在增量更新从{start_date}到{end_date}的交易日数据")
    fetch_and_save_trade_dates(start_date.__str__(), end_date.__str__(), conn)

if __name__ == "__main__":
    logger.info("开始增量更新交易日数据...")
    # 登录baostock
    if not baostock_common.login_baostock():
        raise Exception("登录baostock失败")
    
    try:
        # 设置起始日期为2007年1月1日，结束日期为今天
        conn = stock_common.get_db_conn()
        incremental_update_trade_dates(conn)
        logger.info("增量更新交易日数据完成！")
    except Exception as e:
        logger.error(f"发生错误: {str(e)}")
        raise e
    finally:
        # 确保登出
        baostock_common.logout_baostock()