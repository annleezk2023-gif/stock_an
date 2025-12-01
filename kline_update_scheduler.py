import os
import sys
import time
from datetime import datetime, timedelta
from fetch_baostock_trade_date import fetch_and_save_stock_basic
from baostock_kline_fetch import incremental_update_kline_data

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 配置logger
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 定时任务：每天凌晨3点执行增量更新
def schedule_kline_incremental_update():
    """定期执行股票K线数据的增量更新"""
    while True:
        # 获取当前时间
        now = datetime.now()
        
        # 计算距离下次更新的时间（凌晨3点）
        if now.hour >= 3:
            # 如果当前时间已过今天的凌晨3点，则计算到明天凌晨3点的时间
            next_run = now.replace(hour=3, minute=0, second=0, microsecond=0) + timedelta(days=1)
        else:
            # 如果当前时间未到今天的凌晨3点，则计算到今天凌晨3点的时间
            next_run = now.replace(hour=3, minute=0, second=0, microsecond=0)
        
        # 计算需要等待的秒数
        wait_seconds = (next_run - now).total_seconds()
        logger.info(f"下次K线数据增量更新将在{next_run}进行，等待{wait_seconds:.0f}秒")
        
        # 等待到下次更新时间
        time.sleep(wait_seconds)
        
        # 执行增量更新
        logger.info("开始执行K线数据增量更新...")
        
        try:
            # 先更新证券基本信息，确保获取到新增股票
            logger.info("更新证券基本信息...")
            fetch_and_save_stock_basic()
            
            # 然后更新K线数据
            logger.info("更新股票K线数据...")
            incremental_update_kline_data()
            
            logger.info("K线数据增量更新完成！")
        except Exception as e:
            logger.error(f"K线数据增量更新过程中发生错误: {str(e)}")

if __name__ == "__main__":
    logger.info("启动K线数据增量更新定时任务...")
    schedule_kline_incremental_update()