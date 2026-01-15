import tushare as ts
import sys
import os
from sqlalchemy import text
import pandas as pd
from datetime import date, datetime, timedelta

# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径
sys.path.append(root_path + '/api')
sys.path.append(root_path + '/tushare/day')
import stock_common
import tu_common

# 配置logger
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def _insert_stock_season_fina_indicator(conn, df):
    if df.empty:
        logger.info("数据为空，不写入tu_stock_season_fina_indicator")
        return
    
    # 插入数据到表tu_stock_season_fina_indicator，插入前判断，如果己存在，则不插入。判断条件ts_code, period_date

    # 准备插入数据，单条插入
    insert_count = 0
    for _, row in df.iterrows():
        ts_code = row.get('ts_code')
        period_date = pd.to_datetime(row.get('end_date'), format='%Y%m%d').strftime('%Y-%m-%d') if pd.notna(row.get('end_date')) else None
        f_ann_date = pd.to_datetime(row.get('f_ann_date'), format='%Y%m%d').strftime('%Y-%m-%d') if pd.notna(row.get('f_ann_date')) else None
        update_flag = row.get('update_flag')

        # ts_code和period_date不能为空
        if not ts_code or not period_date:
            logger.warning(f"股票代码或周期日期为空，跳过: {ts_code}, {period_date}")
            continue

        # 检查是否已存在
        check_query = f"""
        SELECT count(*) FROM tu_stock_season_fina_indicator WHERE ts_code = :ts_code AND period_date = :period_date
        """
        existing_results = conn.execute(text(check_query), {'ts_code': ts_code, 'period_date': period_date}).fetchone()
        if existing_results[0] > 0:
            # 如果update_flag为1，则更新，否则跳过
            if update_flag == '1':
                logger.info(f"tu_stock_season_fina_indicator股票代码{ts_code} 周期{period_date} 已存在，update_flag为1，更新")
                update_query = """
                UPDATE tu_stock_season_fina_indicator 
                SET f_ann_date = :f_ann_date, eps = :eps, total_revenue_ps = :total_revenue_ps, bps = :bps, cfps = :cfps, netprofit_margin = :netprofit_margin, grossprofit_margin = :grossprofit_margin, cogs_of_sales = :cogs_of_sales, roe = :roe, roe_yearly = :roe_yearly, roe_avg = :roe_avg, q_eps = :q_eps, q_netprofit_margin = :q_netprofit_margin, q_gsprofit_margin = :q_gsprofit_margin, q_roe = :q_roe, basic_eps_yoy = :basic_eps_yoy, cfps_yoy = :cfps_yoy, op_yoy = :op_yoy, ebts_yoy = :ebt_yoy, netprofit_yoy = :netprofit_yoy, ocf_yoy = :ocf_yoy, tr_yoy = :tr_yoy, q_gr_yoy = :q_gr_yoy, q_op_yoy = :q_op_yoy, q_profit_yoy = :q_profit_yoy, q_netprofit_yoy = :q_netprofit_yoy, rd_exp = :rd_exp, updated_at = NOW()
                WHERE ts_code = :ts_code AND period_date = :period_date
                """
                conn.execute(text(update_query), {
                    'ts_code': ts_code,
                    'period_date': period_date,
                    'f_ann_date': f_ann_date,
                    'eps': row.get('eps'),
                    'total_revenue_ps': row.get('total_revenue_ps'),
                    'bps': row.get('bps'),
                    'cfps': row.get('cfps'),
                    'netprofit_margin': row.get('netprofit_margin'),
                    'grossprofit_margin': row.get('grossprofit_margin'),
                    'cogs_of_sales': row.get('cogs_of_sales'),
                    'roe': row.get('roe'),
                    'roe_yearly': row.get('roe_yearly'),
                    'roe_avg': row.get('roe_avg'),
                    'q_eps': row.get('q_eps'),
                    'q_netprofit_margin': row.get('q_netprofit_margin'),
                    'q_gsprofit_margin': row.get('q_gsprofit_margin'),
                    'q_roe': row.get('q_roe'),
                    'basic_eps_yoy': row.get('basic_eps_yoy'),
                    'cfps_yoy': row.get('cfps_yoy'),
                    'op_yoy': row.get('op_yoy'),
                    'ebt_yoy': row.get('ebt_yoy'),
                    'netprofit_yoy': row.get('netprofit_yoy'),
                    'ocf_yoy': row.get('ocf_yoy'),
                    'tr_yoy': row.get('tr_yoy'),
                    'q_gr_yoy': row.get('q_gr_yoy'),
                    'q_op_yoy': row.get('q_op_yoy'),
                    'q_profit_yoy': row.get('q_profit_yoy'),
                    'q_netprofit_yoy': row.get('q_netprofit_yoy'),
                    'rd_exp': row.get('rd_exp')
                })
            continue
        
        # 单条插入
        insert_query = """
        INSERT INTO tu_stock_season_fina_indicator 
        (ts_code, period_date, f_ann_date, eps, total_revenue_ps, bps, cfps, netprofit_margin, grossprofit_margin, cogs_of_sales, roe, roe_yearly, roe_avg, q_eps, q_netprofit_margin, q_gsprofit_margin, q_roe, basic_eps_yoy, cfps_yoy, op_yoy, ebts_yoy, netprofit_yoy, ocf_yoy, tr_yoy, q_gr_yoy, q_op_yoy, q_profit_yoy, q_netprofit_yoy, rd_exp, created_at, updated_at)
        VALUES (:ts_code, :period_date, :f_ann_date, :eps, :total_revenue_ps, :bps, :cfps, :netprofit_margin, :grossprofit_margin, :cogs_of_sales, :roe, :roe_yearly, :roe_avg, :q_eps, :q_netprofit_margin, :q_gsprofit_margin, :q_roe, :basic_eps_yoy, :cfps_yoy, :op_yoy, :ebts_yoy, :netprofit_yoy, :ocf_yoy, :tr_yoy, :q_gr_yoy, :q_op_yoy, :q_profit_yoy, :q_netprofit_yoy, :rd_exp, NOW(), NOW())
        """
        conn.execute(text(insert_query), {
            'ts_code': ts_code,
            'period_date': period_date,
            'f_ann_date': f_ann_date,
            'eps': row.get('eps'),
            'total_revenue_ps': row.get('total_revenue_ps'),
            'bps': row.get('bps'),
            'cfps': row.get('cfps'),
            'netprofit_margin': row.get('netprofit_margin'),
            'grossprofit_margin': row.get('grossprofit_margin'),
            'cogs_of_sales': row.get('cogs_of_sales'),
            'roe': row.get('roe'),
            'roe_yearly': row.get('roe_yearly'),
            'roe_avg': row.get('roe_avg'),
            'q_eps': row.get('q_eps'),
            'q_netprofit_margin': row.get('q_netprofit_margin'),
            'q_gsprofit_margin': row.get('q_gsprofit_margin'),
            'q_roe': row.get('q_roe'),
            'basic_eps_yoy': row.get('basic_eps_yoy'),
            'cfps_yoy': row.get('cfps_yoy'),
            'op_yoy': row.get('op_yoy'),
            'ebt_yoy': row.get('ebt_yoy'),
            'netprofit_yoy': row.get('netprofit_yoy'),
            'ocf_yoy': row.get('ocf_yoy'),
            'tr_yoy': row.get('tr_yoy'),
            'q_gr_yoy': row.get('q_gr_yoy'),
            'q_op_yoy': row.get('q_op_yoy'),
            'q_profit_yoy': row.get('q_profit_yoy'),
            'q_netprofit_yoy': row.get('q_netprofit_yoy'),
            'rd_exp': row.get('rd_exp')
        })
        insert_count += 1
    
    conn.commit()
    logger.info(f"tu_stock_season_fina_indicator写入成功: {insert_count} 条记录")

#全量查询
def stock_season_fina_indicator_all(conn, pro):
    #从bao_stock_basic取全部的股票数据
    query = "SELECT code FROM bao_stock_basic"
    results = conn.execute(text(query)).fetchall()
    
    if not results:
        logger.info("没有获取到股票数据")
        return
    
    #循环接口查询，查询参数ts_code=bao_stock_basic.code
    for row in results:
        code = row[0]

        #数据库格式为sh.600000，tushare格式为600000.SH，需要转换，考虑sh和sz
        code = tu_common.convert_stock_code(code)
        if not code:
            logger.error(f"股票代码格式错误: {row[0]}")
            continue

        logger.info(f"开始处理股票: {code} ")
        #查询fina_indicator接口，分页查询
        offset = 0
        limit = 100
        total_count = 0
        while True:
            df = pro.fina_indicator(ts_code=code, limit=limit, offset=offset, fields='ts_code,end_date,f_ann_date,update_flag,eps,total_revenue_ps,bps,cfps,netprofit_margin,grossprofit_margin,cogs_of_sales,roe,roe_yearly,roe_avg,q_eps,q_netprofit_margin,q_gsprofit_margin,q_roe,basic_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,ocf_yoy,tr_yoy,q_gr_yoy,q_op_yoy,q_profit_yoy,q_netprofit_yoy,rd_exp')
            if not df.empty:
                _insert_stock_season_fina_indicator(conn, df)
                count = len(df)
                total_count += count
                logger.info(f"fina_indicator 写入成功: {code}, offset: {offset}, 记录数: {count}")
                if count < limit:
                    break
                offset += limit
            else:
                logger.info(f"fina_indicator 无数据: {code}, offset: {offset}")
                break
        logger.info(f"fina_indicator 总计写入: {code}, 总记录数: {total_count}")

#增量查询
def stock_season_fina_indicator_increase(conn, pro):
    #从bao_stock_basic取全部的股票数据
    query = "SELECT code FROM bao_stock_basic"
    results = conn.execute(text(query)).fetchall()
    
    if not results:
        logger.info("没有获取到股票数据")
        return
    
    #循环从fina_indicator接口查询，查询参数ts_code=bao_stock_basic.code
    for row in results:
        code = row[0]

        #数据库格式为sh.600000，tushare格式为600000.SH，需要转换，考虑sh和sz
        code = tu_common.convert_stock_code(code)
        if not code:
            logger.error(f"股票代码格式错误: {row[0]}")
            continue
        
        #从己有数据中查询最大的日期，后续从此日期往后查询
        start_date = '20070101'
        query = """
        SELECT MAX(period_date) FROM tu_stock_season_fina_indicator WHERE ts_code = :ts_code
        """
        max_date_result = conn.execute(text(query), {'ts_code': code}).fetchone()
        if max_date_result[0]:
            start_date = (max_date_result[0] - timedelta(days=1)).strftime('%Y%m%d')

        logger.info(f"开始处理股票: {code} {start_date}")
        #查询fina_mainbz接口，分页查询
        offset = 0
        limit = 100
        total_count = 0
        while True:
            df = pro.fina_indicator(ts_code=code, start_date=start_date, limit=limit, offset=offset, fields='ts_code,end_date,f_ann_date,update_flag,eps,total_revenue_ps,bps,cfps,netprofit_margin,grossprofit_margin,cogs_of_sales,roe,roe_yearly,roe_avg,q_eps,q_netprofit_margin,q_gsprofit_margin,q_roe,basic_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,ocf_yoy,tr_yoy,q_gr_yoy,q_op_yoy,q_profit_yoy,q_netprofit_yoy,rd_exp')
            if not df.empty:
                _insert_stock_season_fina_indicator(conn, df)
                count = len(df)
                total_count += count
                logger.info(f"fina_indicator 写入成功: {code}, offset: {offset}, 记录数: {count}")
                if count < limit:
                    break
                offset += limit
            else:
                logger.info(f"fina_indicator 无数据: {code}, offset: {offset}")
                break
        logger.info(f"fina_indicator 总计写入: {code}, 总记录数: {total_count}")

if __name__ == "__main__":
    pro = tu_common.get_tushare_pro()

    conn = stock_common.get_db_conn(sql_echo=False)
    
    # 股票的季报 财务指标
    stock_season_fina_indicator_all(conn, pro)

    #增量更新
    #stock_season_fina_indicator_increase(conn, pro)

    conn.close()
