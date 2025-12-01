import tushare as ts
import sys
import os
from sqlalchemy import text
import pandas as pd

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common

# 配置logger
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # 替换为你的Tushare Token
    ts.set_token('你的专属Token')
    pro = ts.pro_api()
    # 查询某只股票的ST相关时间（以002122.SZ为例）
    df = pro.namechange(ts_code='002122.SZ')
    # 筛选ST相关记录并去重处理
    st_df = df[(df['change_reason'].str.contains('ST', na=False))].sort_values(by='end_date', ascending=True).drop_duplicates(subset=('ts_code', 'start_date'), keep='first')
    print(st_df[['name', 'start_date', 'end_date', 'change_reason']])