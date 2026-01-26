import sys
import os
import threading
from sqlalchemy import text
import numpy as np

# 配置logger
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common

def get_index_codes():
    #000001覆盖上交所全部 2300 余家 A 股
    #399106覆盖深市全部 500 余家 A 股
    #000680覆盖科创板全部 590 余家上市公司
    #399102覆盖创业板全部 500 余家 A 股
    zhishu_list = {"sh.000001": "上证综指", "sz.399106": "深证综指", "sh.000680": "科创综指", "sz.399102": "创业板综指"}

    #沪深市场规模最大、流动性最好的 300 只股票（大盘）
    zhishu_list["sh.000300"] = "沪深300"
    #剔除沪深 300 成分股后，规模和流动性排名前 500 的股票（中盘）
    zhishu_list["sh.000905"] = "中证500"
    #剔除沪深 300 + 中证 500 成分股后，规模和流动性排名前 1000 的股票（中小盘）
    zhishu_list["sh.000852"] = "中证1000"

    #选取 50 只市值大、流动性好的企业，集中了半导体、AI、生物医药等前沿技术领域的龙头，研发投入强度高
    zhishu_list["sh.000688"] = "科创50"
    #选取 100 家市值大、流动性好的企业，高新技术企业占比超 9 成，战略新兴产业占比超 8 成，涵盖新能源、生物医药、高端制造等领域
    zhishu_list["sz.399006"] = "创业板指"
    #选取 50 只市值大、流动性好的企业，集中了创业板中的新兴成长企业
    zhishu_list["sz.399673"] = "创业板50"

    return zhishu_list

if __name__ == "__main__":
    logger.info("开始补充bao_stock_trade表pe数据...")

