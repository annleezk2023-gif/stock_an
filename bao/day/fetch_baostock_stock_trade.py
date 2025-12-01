import os
import sys
import baostock as bs
import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import text
import time

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common
sys.path.append(root_path + '/bao')
import baostock_common

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 配置logger
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 获取单个股票的K线数据
def fetch_single_stock_kline(stock_code, stock_name, start_date, end_date, adjustflag=1):
    """获取单个股票的K线数据"""
    rs = bs.query_history_k_data_plus(
        stock_code,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag=str(adjustflag)  # 1:后复权
    )
    
    if rs.error_code != '0':
        raise Exception(f"查询失败: {stock_name}({stock_code}) 失败: {rs.error_msg}")
    
    # 获取数据
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())

    if not data_list:
        return None
    
    # 动态处理列名
    columns = rs.fields
    
    # 转换为DataFrame
    df = pd.DataFrame(data_list, columns=columns)
    
    # 确保code列存在
    if 'code' not in df.columns:
        df['code'] = code
    
    return df

# 保存K线数据到bao_stock_trade表
def save_kline_data(df, conn):
    """保存K线数据到bao_stock_trade表"""
    if df is None or df.empty:
        return 0

    saved_count = 0

    for _, row in df.iterrows():
        # 准备插入数据
        insert_data = {
            'code': row.get('code'),
            'date': row.get('date'),
            'open': float(row.get('open')) if row.get('open') else None,
            'high': float(row.get('high')) if row.get('high') else None,
            'low': float(row.get('low')) if row.get('low') else None,
            'close': float(row.get('close')) if row.get('close') else None,
            'preclose': float(row.get('preclose')) if row.get('preclose') else None,
            'volume': float(row.get('volume')) if row.get('volume') else None,
            'amount': float(row.get('amount')) if row.get('amount') else None,
            'adjustflag': int(row.get('adjustflag')) if row.get('adjustflag') else None,
            'turn': float(row.get('turn')) if row.get('turn') else None,
            'tradestatus': int(row.get('tradestatus')) if row.get('tradestatus') else None,
            'pctChg': float(row.get('pctChg')) if row.get('pctChg') else None,
            'peTTM': float(row.get('peTTM')) if row.get('peTTM') else None,
            'pbMRQ': float(row.get('pbMRQ')) if row.get('pbMRQ') else None,
            'psTTM': float(row.get('psTTM')) if row.get('psTTM') else None,
            'pcfNcfTTM': float(row.get('pcfNcfTTM')) if row.get('pcfNcfTTM') else None,
            'isST': int(row.get('isST')) if row.get('isST') else None
        }
        
        # 使用INSERT ... ON DUPLICATE KEY UPDATE语句
        insert_sql = text(f"""
        INSERT INTO bao_stock_trade_{insert_data['code'][-1]} (
            code, date, open, high, low, close, preclose, volume, amount, adjustflag, turn, tradestatus, pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM, isST
        ) VALUES (
            :code, :date, :open, :high, :low, :close, :preclose, :volume, :amount, :adjustflag, :turn, :tradestatus, :pctChg, :peTTM, :pbMRQ, :psTTM, :pcfNcfTTM, :isST
        ) on duplicate key update
        isST = values(isST)
        """)
        
        conn.execute(insert_sql, insert_data)
        saved_count += 1
    
    conn.commit()

    return saved_count

# 批量获取并保存股票K线数据
def batch_fetch_and_save_kline_data(conn):
    date_2007 = date(2007, 1, 1)
    end_date = datetime.now().strftime('%Y-%m-%d')
    yesterday = datetime.now().date() - timedelta(days=1)

    # 获取所有股票代码
    stocks = stock_common.get_stock_info_all(conn)
    
    if not stocks or len(stocks) == 0:
        logger.info("未获取到股票数据")
        return
    
    total_stocks = len(stocks)
    logger.info(f"开始处理{total_stocks}只股票的K线数据")

    # 处理每只股票
    for index, stock in enumerate(stocks):
        stock_code = stock.code
        stock_name = stock.code_name
        ipo_date = stock.ipo_date

        """计算开始日期
        1. 如果表中不存在数据，从start_date开始
        2. 如果表中存在数据，从最新日期的下一天开始
        """
        # 查询表中最新日期
        result = conn.execute(
            text(f"SELECT MAX(date) FROM bao_stock_trade_{stock.code[-1]} WHERE code = :code"), 
            {'code': stock_code}
        ).fetchone()
        # 检查result[0]的类型，如果是datetime则转换为date，如果已经是date则直接使用
        if result and result[0]:
            if hasattr(result[0], 'date'):  # 如果有date方法（datetime类型）
                latest_date = result[0].date()
            else:  # 已经是date类型
                latest_date = result[0]
        else:
            latest_date = date_2007
        
        # 计算开始日期，如果最近日期是2007-01-01，从2007-01-01开始。否则从最新数据的下一天开始
        if latest_date == date_2007:
            start_date = date_2007
        else:
            start_date = latest_date + timedelta(days=1)
        
        logger.info(f"{index+1}/{total_stocks} 处理股票: {ipo_date}{stock_name}({stock_code})  {start_date}  {end_date}")

        # 如果start_date大于昨天，就跳过当前股票。即最新数据己拿到
        if start_date > yesterday:
            logger.debug(f"{index+1}/{total_stocks}  股票 {stock_name}({stock_code}) 最新数据已获取，跳过")
            continue
        
        if "0" == stock.status and stock.out_date is not None and stock.out_date.strftime('%Y-%m-%d') == latest_date.__str__():
            logger.debug(f"{index+1}/{total_stocks}  股票 {stock_name}({stock_code}) 退市前全部数据已获取，跳过")
            continue
        

        # 开始时间和结束时间无开市日，跳过当前股票
        tradeday_num = stock_common.getTradeNum(start_date.__str__(), end_date.__str__(), conn)
        if tradeday_num == 0:
            logger.debug(f"{index+1}/{total_stocks}  股票 {stock_name}({stock_code}) {start_date} {end_date} 无交易日，跳过")
            continue

        # 获取K线数据
        df = fetch_single_stock_kline(stock_code, stock_name, start_date.__str__(), end_date.__str__())
        time.sleep(0.1)
        
        if df is not None and not df.empty:
            # 保存数据到表
            saved_count = save_kline_data(df, conn)
            logger.debug(f"{index+1}/{total_stocks} {stock_name}({stock_code})  成功保存{saved_count}条数据")
        else:
            logger.debug(f"{index+1}/{total_stocks}  没有获取到数据或数据为空")

        if index % 100 == 0:
            time.sleep(1)
        

if __name__ == "__main__":
    logger.info("开始获取股票历史K线数据...")
    # 设置起始日期为2007年1月1日，结束日期为今天
    

    if not baostock_common.login_baostock():
        raise Exception("登录baostock失败")
    try:
        conn = stock_common.get_db_conn(sql_echo=False)
        batch_fetch_and_save_kline_data(conn)
        conn.close()
        logger.info("股票历史K线数据获取完成！")
    except Exception as e:
        logger.error(f"批量处理失败: {str(e)}")
    finally:
        # 确保登出
        baostock_common.logout_baostock()