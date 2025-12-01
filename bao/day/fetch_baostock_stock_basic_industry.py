import os
import sys
import baostock as bs
import pandas as pd
from sqlalchemy import text

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
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 查询并更新股票行业信息
def update_stock_industry_info(conn=None):
    # 初始化应用上下文

    logger.info("开始查询所有股票行业信息...")
    # 调用query_stock_industry()接口获取所有股票的行业信息
    rs = bs.query_stock_industry()
    
    if rs.error_code != '0':
        logger.error(f"查询行业信息失败: {rs.error_msg}")
        return False
    
    # 准备存储结果的数据列表
    industry_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录并添加到列表中
        industry_list.append(rs.get_row_data())
    
    # 将结果转换为DataFrame
    result_df = pd.DataFrame(industry_list, columns=rs.fields)
    
    logger.info(f"成功获取{len(result_df)}条股票行业信息")
    
    # 统计更新成功和失败的数量
    success_count = 0
    fail_count = 0
    
    # 遍历结果并更新到数据库
    for _, row in result_df.iterrows():
        code = row['code']
        industry = row['industry']
        industry_classification = row['industryClassification']
        
        # 查找对应的股票记录
        stock = conn.execute(
            text("SELECT * FROM bao_stock_basic WHERE code = :code"),
            {'code': code}
        ).fetchone()

        if not stock:
            logger.debug(f"未找到证券代码为{code}的记录")
            continue
        elif stock.industry == industry:
            logger.debug(f"证券代码为{code}的记录 行业未改变")
            continue
        else:
            logger.debug(f"证券代码为{code}的记录 行业发生改变")
            # 更新行业信息
            sql = "UPDATE bao_stock_basic SET industry = :industry, industryClassification = :industryClassification WHERE code = :code"
            conn.execute(text(sql),{'industry': industry, 'industryClassification': industry_classification, 'code': code})
            success_count += 1
    logger.info(f"股票行业信息更新完成。成功: {success_count}条，失败: {fail_count}条")
    conn.commit()
       

# 主函数
if __name__ == '__main__':
    logger.info("===== 开始更新股票行业信息 =====")
    # 登录baostock
    if not baostock_common.login_baostock():
        raise Exception("登录baostock失败")
    
    try:
        conn = stock_common.get_db_conn()
        update_stock_industry_info(conn)
        conn.close()
        logger.info("===== 股票行业信息更新成功 =====")
    except Exception as e:
        logger.error(f"批量处理失败: {str(e)}")
    finally:
        # 确保登出
        baostock_common.logout_baostock()
