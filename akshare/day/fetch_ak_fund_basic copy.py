import akshare as ak
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







# 公募基金列表
def ak_fetch_fund_list():
    df = ak.fund_name_em()
    return df


# 基金的基本信息
def ak_fetch_fund_info(fd_code):
    try:
        # 使用akshare的API查询持仓数据
        result = ak.fund_individual_basic_info_xq(symbol=fd_code)
        
        # 检查返回的数据类型和结构
        if isinstance(result, pd.DataFrame):
            logger.info(f"成功获取到数据，行数: {len(result)}")
            return result
        elif isinstance(result, dict):
            # 如果返回的是字典，尝试获取'data'字段
            if 'data' in result:
                logger.debug(f"从字典中获取data字段，类型: {type(result['data'])}")
                # 如果data是列表，转换为DataFrame
                if isinstance(result['data'], list):
                    return pd.DataFrame(result['data'])
                return result['data']
            else:
                logger.debug(f"返回的字典中没有'data'字段，键列表: {list(result.keys())}")
                return None
        else:
            logger.debug(f"返回数据类型: {type(result)}，不支持的格式")
            return None
    except Exception as e:
        logger.error(f"获取基金信息时出错: {str(e)}")
        return None

# 获取并保存公募基金基本信息数据
def fetch_and_save_fund_basic(conn=None):
    df = ak_fetch_fund_list()
    if df is None or len(df) == 0:
        return False
    
    total_fund = len(df)
    for index, row in df.iterrows():
        fd_code = row['基金代码']
        fd_name = row['基金简称'] 

        logger.info(f"查询 {index} / {total_fund} {fd_code} {fd_name} 的基本信息")

        insert_data = {
            'fd_code': fd_code,
            'fd_name': fd_name,
            'fd_full_name': None,
            'found_date': None,
            'totshare': None,
            'keeper_name': None,
            'manager_name': None,
            'trup_name': None,
            'type_desc': None,
            'rating_source': None,
            'rating_desc': None,
            'invest_orientation': None,
            'invest_target': None,
            'performance_bench_mark': None
        }

        info_df = ak_fetch_fund_info(fd_code)
        if info_df is None or len(info_df) == 0:
            logger.info(f"没有找到 {index} / {total_fund} {fd_code} {fd_name} 的基本信息")
        else:
            for _, row2 in info_df.iterrows():
                if row2['item'] == "基金名称":
                    insert_data['fd_name'] = row2['value'] if row2['value'] else None
                elif row2['item'] == "基金全称":
                    insert_data['fd_full_name'] = row2['value'] if row2['value'] else None
                elif row2['item'] == "成立时间":
                    insert_data['found_date'] = row2['value'] if row2['value'] else None
                elif row2['item'] == "最新规模":
                    try:
                        value = row2['value']
                        if not value:
                            insert_data['totshare'] = None
                        elif '万' in value:
                            # 处理单位为'万'的情况
                            insert_data['totshare'] = float(value.replace('万', '')) / 10000  # 转换为亿
                        elif '亿' in value:
                            # 处理单位为'亿'的情况
                            insert_data['totshare'] = float(value.replace('亿', ''))
                        else:
                            # 尝试直接转换
                            insert_data['totshare'] = float(value)
                    except Exception as e:
                        logger.error(f"处理规模数据时出错: {str(e)}")
                        insert_data['totshare'] = None
                elif row2['item'] == "基金公司":
                    insert_data['keeper_name'] = row2['value'] if row2['value'] else None
                elif row2['item'] == "基金经理":
                    insert_data['manager_name'] = row2['value'] if row2['value'] else None
                elif row2['item'] == "托管银行":
                    insert_data['trup_name'] = row2['value'] if row2['value'] else None
                elif row2['item'] == "基金类型":
                    insert_data['type_desc'] = row2['value'] if row2['value'] else None
                elif row2['item'] == "评级机构":
                    if pd.isna(row2['value']):
                        insert_data['rating_source'] = None
                    else:
                        insert_data['rating_source'] = row2['value'] if row2['value'] else None
                elif row2['item'] == "基金评级":
                    insert_data['rating_desc'] = row2['value'] if row2['value'] else None
                elif row2['item'] == "投资策略":
                    insert_data['invest_orientation'] = row2['value'] if row2['value'] else None
                elif row2['item'] == "投资目标":
                    insert_data['invest_target'] = row2['value'] if row2['value'] else None
                elif row2['item'] == "业绩比较基准":
                    insert_data['performance_bench_mark'] = row2['value'] if row2['value'] else None
                """ else:
                    logger.debug(f"未处理的字段: {row2['item']} {row2['value']}") """

        # 使用INSERT ... ON DUPLICATE KEY UPDATE语句
        insert_sql = text("""
        INSERT INTO ak_fund_basic (
            fd_code, fd_name, fd_full_name, found_date, totshare, keeper_name, manager_name, trup_name, type_desc, rating_source, rating_desc, invest_orientation, invest_target, performance_bench_mark
        ) VALUES (
            :fd_code, :fd_name, :fd_full_name, :found_date, :totshare, :keeper_name, :manager_name, :trup_name, :type_desc, :rating_source, :rating_desc, :invest_orientation, :invest_target, :performance_bench_mark
        ) on duplicate key update
        fd_name = values(fd_name),
        fd_full_name = values(fd_full_name),
        found_date = values(found_date),
        totshare = values(totshare),
        keeper_name = values(keeper_name),
        manager_name = values(manager_name),
        trup_name = values(trup_name),
        type_desc = values(type_desc),
        rating_source = values(rating_source),
        rating_desc = values(rating_desc),
        invest_orientation = values(invest_orientation),
        invest_target = values(invest_target),
        performance_bench_mark = values(performance_bench_mark)
        """)
        
        conn.execute(insert_sql, insert_data)
        
        if index > 0 and index % 100 == 0:
            conn.commit()
            logger.info(f"已处理 {index} / {total_fund} 条数据")

    conn.commit()
    logger.info(f"成功获取并保存了{total_fund}条公募基金基本信息") 
    
    
if __name__ == "__main__":
    conn = stock_common.get_db_conn()
    fetch_and_save_fund_basic(conn)
    conn.close()
    logger.info("更新公募基金基本信息完成！")

