import baostock as bs
import pandas as pd
from datetime import datetime
import time
import os
from sqlalchemy import text
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

# 查询除权除息数据
def query_dividend_data(stock_code, stock_name, year):
    """
    查询单只股票的除权除息数据
    :param stock_code: 股票代码
    :param stock_name: 股票名称
    :param year: 年份
    :return: 除权除息数据的DataFrame
    """
    if year is None:
        return
    
    logger.info(f"查询 {stock_name}({stock_code}) 的除权除息数据: {year}")
    
    # 使用baostock的API查询除权除息数据
    rs = bs.query_dividend_data(
        code=stock_code,
        year=str(year),
        yearType='report'
    )

    if rs.error_code != '0':
        raise Exception(f"查询失败: {rs.error_msg}")
    
    # 获取数据
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    
    if not data_list:
        logger.info(f"没有找到 {stock_name}({stock_code}) 的除权除息数据")
        return None
    
    # 动态处理列名
    columns = rs.fields
    
    # 转换为DataFrame
    df = pd.DataFrame(data_list, columns=columns)
    
    # 确保code列存在
    if 'code' not in df.columns:
        df['code'] = code
    
    # 添加年份
    df['year'] = year

    return df

# 保存除权除息数据到数据库
def save_dividend_data(df, conn):
    """
    保存除权除息数据到数据库
    :param dividend_data: 除权除息数据的DataFrame
    :return: 保存成功的记录数
    """
    if df is None or df.empty:
        return 0

    saved_count = 0

    for _, row in df.iterrows():
        # 准备插入数据
        insert_data = {
            'code': row.get('code'),
            'dividPayDate': datetime.strptime(row.get('dividPayDate'), '%Y-%m-%d') if row.get('dividPayDate') else None,
            'dividPreNoticeDate': datetime.strptime(row.get('dividPreNoticeDate'), '%Y-%m-%d') if row.get('dividPreNoticeDate') else None,
            'dividAgmPumDate': datetime.strptime(row.get('dividAgmPumDate'), '%Y-%m-%d') if row.get('dividAgmPumDate') else None,
            'dividPlanAnnounceDate': datetime.strptime(row.get('dividPlanAnnounceDate'), '%Y-%m-%d') if row.get('dividPlanAnnounceDate') else None,
            'dividPlanDate': datetime.strptime(row.get('dividPlanDate'), '%Y-%m-%d') if row.get('dividPlanDate') else None,
            'dividRegistDate': datetime.strptime(row.get('dividRegistDate'), '%Y-%m-%d') if row.get('dividRegistDate') else None,
            'dividOperateDate': datetime.strptime(row.get('dividOperateDate'), '%Y-%m-%d') if row.get('dividOperateDate') else None,
            'dividStockMarketDate': datetime.strptime(row.get('dividStockMarketDate'), '%Y-%m-%d') if row.get('dividStockMarketDate') else None,
            'dividCashPsBeforeTax': float(row.get('dividCashPsBeforeTax')) if row.get('dividCashPsBeforeTax') else None,
            'dividCashPsAfterTax': row.get('dividCashPsAfterTax'),
            'dividStocksPs': float(row.get('dividStocksPs')) if row.get('dividStocksPs') else None,
            'dividCashStock': row.get('dividCashStock'),
            'dividReserveToStockPs': float(row.get('dividReserveToStockPs')) if row.get('dividReserveToStockPs') else None,
            'year': int(row.get('year')),
            'data_exist': 1
        }
        
        # 使用INSERT ... ON DUPLICATE KEY UPDATE语句
        insert_sql = text("""
        INSERT INTO bao_stock_dividend (
            code, dividPreNoticeDate, dividAgmPumDate, dividPlanAnnounceDate, dividPlanDate, dividRegistDate, 
            dividOperateDate, dividPayDate, dividStockMarketDate, dividCashPsBeforeTax, dividCashPsAfterTax, 
            dividStocksPs, dividCashStock, dividReserveToStockPs, year, data_exist
        ) VALUES (
            :code, :dividPreNoticeDate, :dividAgmPumDate, :dividPlanAnnounceDate, :dividPlanDate, :dividRegistDate, 
            :dividOperateDate, :dividPayDate, :dividStockMarketDate, :dividCashPsBeforeTax, :dividCashPsAfterTax, 
            :dividStocksPs, :dividCashStock, :dividReserveToStockPs, :year, :data_exist
        ) on duplicate key update
        data_exist = values(data_exist)
        """)
        
        conn.execute(insert_sql, insert_data)
        saved_count += 1
    
    conn.commit()

    return saved_count


# 保存空数据
def save_empty_dividend_data(code, year, conn):
    # 准备插入数据
    insert_data = {
        'code': code,
        'year': year
    }
    
    # 使用INSERT
    insert_sql = text("""
    INSERT INTO bao_stock_dividend (code, year, data_exist) VALUES (:code, :year, 0)""")
    
    # 执行插入
    conn.execute(insert_sql, insert_data)

    # 提交事务
    conn.commit()


# 批量获取并保存除权除息数据
def batch_fetch_and_save_dividend_data(start_year=2007, conn=None, check_exist=False):
    """
    批量获取并保存所有上市股票的除权除息数据
    :param start_date: 开始日期，默认为'2007-01-01'
    :param end_date: 结束日期，默认为当前日期
    :return: 任务是否成功
    """

     # 设置结束年份
    end_year = datetime.now().year

    # 获取所有上市股票代码
    stocks = stock_common.get_stock_info_all(conn)
        
    
    if not stocks:
        logger.info("没有找到股票数据")
        return False
    
    total_stocks = len(stocks)
    logger.info(f"开始处理{total_stocks}只股票的除权除息数据")

    success_count = 0
    error_count = 0

    # 处理每只股票
    for index, stock in enumerate(stocks):
        logger.info(f"处理进度: {index+1}/{total_stocks} - {stock.code_name}({stock.code})")

        # 处理该股票的所有年份数据
        for year in range(start_year, end_year + 1):

            # 己退市，那退市时间之后的数据不用再查了
            target_date = datetime.strptime(f"{year+1}-01-01", "%Y-%m-%d").date()
            
            # 将stock.out_date转换为日期对象进行比较
            out_date = datetime.strptime(stock.out_date, "%Y-%m-%d").date() if isinstance(stock.out_date, str) else stock.out_date
            
            if stock.out_date and out_date < target_date:
                break


            #查询数据是否己有数据
            if check_exist:
                check_sql = text("""
                SELECT COUNT(*) FROM bao_stock_dividend
                WHERE code = :code AND year = :year
                """)
                result = conn.execute(check_sql, {'code': stock.code, 'year': year}).fetchone()
                if result[0] > 0:
                    logger.debug(f"{index+1}/{total_stocks}  股票 {stock.code} {stock.code_name} {year}年 除权除息数据已存在，跳过")
                continue

            # 获取除权除息数据
            dividend_data = query_dividend_data(stock.code, stock.code_name, year)
            time.sleep(0.1)
            
            # 保存数据
            if dividend_data is not None and not dividend_data.empty:
                saved = save_dividend_data(dividend_data, conn)
                success_count += saved
                logger.debug(f"{index+1}/{total_stocks}  成功保存{saved}条除权除息数据")
            else:
                save_empty_dividend_data(stock.code, year, conn)
                logger.debug(f"{index+1}/{total_stocks} 股票 {stock.code} {stock.code_name} {year}年 除权除息数据为空，保存空数据成功")

    logger.info(f"除权除息数据获取完成！成功处理{success_count}条记录，{error_count}只股票获取失败")


# 主函数
if __name__ == "__main__":
    logger.info(f"开始获取除权除息数据")
    # 登录baostock
    if not baostock_common.login_baostock():
        raise Exception("登录baostock失败")
    
    try:
        # 执行批量获取和保存操作
        end_year = datetime.now().year
        conn = stock_common.get_db_conn()
        conn.execute(text(f"delete from bao_stock_dividend where year = {end_year} and data_exist = 0"))
        conn.commit()
        logger.info("清除旧数据完成")
        batch_fetch_and_save_dividend_data(start_year=end_year, conn=conn, check_exist=False)
        conn.close()
        logger.info("除权除息数据获取任务完成")
    except Exception as e:
        logger.error(f"获取除权除息数据失败: {str(e)}")
        raise e
    finally:
        # 确保登出
        baostock_common.logout_baostock()
