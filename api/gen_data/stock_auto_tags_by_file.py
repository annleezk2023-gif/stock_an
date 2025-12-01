import pandas as pd
import sys
import os
from sqlalchemy import text

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
import stock_common
import stock_tags

# 配置logger
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

""" 从文件中导入标签 """

def read_stock_codes_from_txt(stock_type):
    file_path = r'C:\Users\User\Desktop\{}.txt'.format(stock_type)
    # 使用dtype=str确保所有内容以字符串形式读取，避免数字自动转换
    df = pd.read_csv(file_path, sep='\t', encoding='gbk', dtype=str)
    # 确保以字符串形式获取第一列数据
    # 去掉字符串前后的空格
    stock_codes = df.iloc[:, 0].astype(str).str.strip().tolist()
    return stock_codes

# 获取并保存证券基本信息数据
def update_stock_auto_tags(cur_auto_tags, conn = None):

    for cur_tag in cur_auto_tags:
        stock_codes = read_stock_codes_from_txt(cur_tag)
        if stock_codes is None or len(stock_codes) < 1:
            continue

        # 清空原来的自动标签内容
        sql = f"""UPDATE bao_stock_basic 
                    SET auto_tags = JSON_REMOVE(auto_tags, 
                        JSON_UNQUOTE(
                            JSON_SEARCH(auto_tags, 'one', '{cur_tag}')
                        )
                    )
                    WHERE JSON_SEARCH(auto_tags, 'one', '{cur_tag}') IS NOT NULL"""
        results = conn.execute(text(sql))
        conn.commit()  # 提交连接层面的事务
        logger.info(f"成功清空{results.rowcount}个股票自动标签中的{cur_tag}")

        stock_count = 0
        # 循环更新auto_tags自动标签
        for item in stock_codes:
            #item长度不足6位，则前面补0
            if len(item) < 6:
                item = '0' * (6 - len(item)) + item
            # 查询数据库中是否存在该股票，以code like %item
            #stock = BaoStockBasic.query.filter(BaoStockBasic.code.like(f'%{item}')).first()
            
            # 使用连接的execute方法执行SQL语句
            sql = f"SELECT * FROM bao_stock_basic WHERE code LIKE '%{item}'"
            stock = conn.execute(text(sql)).fetchone()
            
            # update语句在auto_tags中添加cur_tag
            if stock is None:
                logger.info(f"股票{item}不存在，跳过更新自动标签")
                continue

            # 使用连接的commit方法提交直接执行的SQL语句
            sql = f"""UPDATE bao_stock_basic 
                SET auto_tags = JSON_ARRAY_APPEND(COALESCE(auto_tags, '[]'), '$', '{cur_tag}')
                WHERE id = {stock.id}"""
            conn.execute(text(sql))
            conn.commit()  # 提交连接层面的事务

            # 再查一次
            sql = f"SELECT * FROM bao_stock_basic WHERE id = {stock.id}"
            stock = conn.execute(text(sql)).fetchone()

            logger.info(f"更新股票{stock.code}自动标签为{stock.auto_tags}")
            stock_count += 1
            
    
        logger.info(f"成功更新{stock_count}个股票自动标签中的{cur_tag}")


if __name__ == "__main__":
    logger.info("开始更新自动标签...")
    cur_auto_tags = [stock_tags.StockTagsFile.growth_pct_15, stock_tags.StockTagsFile.growth_pct_5_market_1000, stock_tags.StockTagsFile.dividend_pct_3, stock_tags.StockTagsFile.dividend_pct_1_growth_pct_5, stock_tags.StockTagsFile.net_profit_pct_20, stock_tags.StockTagsFile.roe_pct_20]
    conn = stock_common.get_db_conn()
    update_stock_auto_tags(cur_auto_tags, conn)
    conn.close()
    logger.info("更新自动标签完成！")