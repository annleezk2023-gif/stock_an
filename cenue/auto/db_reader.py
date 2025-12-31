import pymysql
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'stock',
    'charset': 'utf8mb4'
}

class DBReader:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.stock_daily_data = {}  # 内存中的日K数据，结构：{date: {code: data}}
        self.roe_data_cache = {}  # 股票roe数据，结构：{code: [{pubDate: datetime, statDate: datetime, roeAvg: float}, ...]}
        self.dividend_data_cache = {}  # 分红数据结构：{code: set(year1, year2, ...)}，存储股票每年的分红年份
    
    def connect_db(self):
        """连接数据库"""
        try:
            self.conn = pymysql.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
            # print("数据库连接成功")
            return True
        except Exception as e:
            print(f"数据库连接失败: {e}")
            return False
    
    def get_trading_days(self, start_date, end_date):
        """获取交易日历"""
        if not self.conn:
            print("数据库未连接，无法获取交易日历")
            return False
        
        sql = """
        SELECT calendar_date FROM bao_trade_date 
        WHERE is_trading_day = 1 AND calendar_date BETWEEN %s AND %s 
        ORDER BY calendar_date
        """
        try:
            self.cursor.execute(sql, (start_date, end_date))
            result = self.cursor.fetchall()
            trading_days = [row['calendar_date'].strftime('%Y-%m-%d') for row in result]
            # print(f"获取到{len(trading_days)}个交易日")
            return trading_days
        except Exception as e:
            print(f"获取交易日历失败: {e}")
            return []
    
    def get_stock_basic(self):
        """获取股票基本信息"""
        if not self.conn:
            print("数据库未连接，无法获取股票基本信息")
            return {}
        
        sql = """
        SELECT code, code_name, ipo_date FROM bao_stock_basic 
        WHERE status = 1 AND type = 1
        """
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            stock_basic = {row['code']: row for row in result}
            # print(f"获取到{len(stock_basic)}只股票的基本信息")
            return stock_basic
        except Exception as e:
            print(f"获取股票基本信息失败: {e}")
            return {}
    
    def get_table_name(self, code):
        """根据股票代码获取对应的表名"""
        suffix = code[-1] if code[-1].isdigit() else '0'
        return f'bao_stock_trade_{suffix}'
    
    def get_stock_daily(self, code, date):
        """从内存中获取单个股票的日K数据"""
        if date not in self.stock_daily_data:
            return None
        
        return self.stock_daily_data[date].get(code, None)
    
    def get_all_stocks_daily(self, date):
        """从内存中获取所有股票的日K数据"""
        if date not in self.stock_daily_data:
            # print(f"{date} 没有日K数据")
            return []
        
        all_data = list(self.stock_daily_data[date].values())
        # print(f"从内存中获取到{len(all_data)}条日K数据")
        return all_data
    
    def load_all_stock_daily_data(self, start_date, end_date):
        """一次性加载所有股票在回测期间的日K数据到内存"""
        if not self.conn:
            print("数据库未连接，无法加载日K数据")
            return False
        
        # print("开始加载所有股票日K数据...")
        
        # 确定需要的字段
        required_fields = ['code', 'date', 'total_market_value', 'peTTM', 'psTTM', 'pe_year_1_percent', 'ps_year_1_percent', 'close', 'stock_fenghong_percent', 'isST']
        fields_str = ', '.join(required_fields)
        
        # 构建union all查询
        union_sql = []
        for i in range(10):
            table_name = f'bao_stock_trade_{i}'
            union_sql.append(f"SELECT {fields_str} FROM {table_name} WHERE date BETWEEN %s AND %s")
        
        sql = " UNION ALL ".join(union_sql)
        
        try:
            # 执行查询
            self.cursor.execute(sql, (start_date, end_date) * 10)
            all_data = self.cursor.fetchall()
            # print(f"成功获取{len(all_data)}条日K数据")
            
            # 按日期和股票代码组织数据
            for data in all_data:
                # 处理date字段，可能是字符串或日期对象
                if isinstance(data['date'], str):
                    date_str = data['date']
                else:
                    date_str = data['date'].strftime('%Y-%m-%d')
                code = data['code']
                
                if date_str not in self.stock_daily_data:
                    self.stock_daily_data[date_str] = {}
                
                # 转换日期对象为字符串，方便后续使用
                data['date'] = date_str
                self.stock_daily_data[date_str][code] = data
            
            print(f"日K数据加载完成，覆盖{len(self.stock_daily_data)}个交易日")
            return True
        except Exception as e:
            print(f"加载日K数据失败: {e}")
            return False
    
    def load_all_roe_data(self, start_date, end_date):
        """一次性加载所有股票在回测期间及开始前6个月的roe数据到内存"""
        if not self.conn:
            print("数据库未连接，无法加载roe数据")
            return False
        
        # print("开始加载所有股票roe数据...")
        
        # 计算回测开始前6个月的日期
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        six_months_before_start = start_date_obj - timedelta(days=180)
        six_months_before_start_str = six_months_before_start.strftime('%Y-%m-%d')
        
        # 查询所有相关的roe数据
        sql = """
        SELECT code, pubDate, statDate, roeAvg FROM bao_stock_profit 
        WHERE pubDate BETWEEN %s AND %s and data_exist = 1
        ORDER BY code, pubDate DESC
        """
        
        try:
            # 执行查询
            self.cursor.execute(sql, (six_months_before_start_str, end_date))
            all_roe_data = self.cursor.fetchall()
            # print(f"成功获取{len(all_roe_data)}条roe数据")
            
            # 按股票代码组织数据，便于后续查询
            # 结构: {code: [{pubDate: datetime, statDate: datetime, roeAvg: float}, ...]}
            self.roe_data_cache = {}
            for data in all_roe_data:
                code = data['code']
                if code not in self.roe_data_cache:
                    self.roe_data_cache[code] = []
                
                # 确保日期对象格式统一
                if isinstance(data['pubDate'], str):
                    pub_date = datetime.strptime(data['pubDate'], '%Y-%m-%d')
                else:
                    pub_date = data['pubDate']
                
                if isinstance(data['statDate'], str):
                    stat_date = datetime.strptime(data['statDate'], '%Y-%m-%d')
                else:
                    stat_date = data['statDate']
                
                self.roe_data_cache[code].append({
                    'pubDate': pub_date,
                    'statDate': stat_date,
                    'roeAvg': data['roeAvg']
                })
           # 打印加载结果
            print(f"成功加载{len(self.roe_data_cache)}只股票的roe数据到内存")
            return True
        except Exception as e:
            print(f"加载roe数据失败: {e}")
            return False
    
    def load_dividend_data(self, start_date, end_date):
        """一次性加载回测开始时间2年前到回测结束时间的所有分红数据，按股票代码和年份存储"""
        if not self.conn:
            print("数据库未连接，无法加载分红数据")
            return False
        
        try:
            # 计算分红查询时间范围：回测开始时间2年前到回测结束时间
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            query_start_year = start_dt.year - 2
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            query_end_year = end_dt.year
            
            # 查询指定年份范围内的所有分红记录
            sql = """
            SELECT code, YEAR(dividOperateDate) as dividend_year
            FROM bao_stock_dividend
            WHERE YEAR(dividOperateDate) BETWEEN %s AND %s and data_exist = 1 and dividCashPsBeforeTax > 0
            GROUP BY code, dividend_year
            """
            self.cursor.execute(sql, (query_start_year, query_end_year))
            results = self.cursor.fetchall()
            
            # 按股票代码和年份组织数据
            self.dividend_data_cache = {}  # 重置缓存
            for row in results:
                code = row['code']
                dividend_year = row['dividend_year']
                if code not in self.dividend_data_cache:
                    self.dividend_data_cache[code] = set()
                self.dividend_data_cache[code].add(dividend_year)
            
            print(f"加载了{len(self.dividend_data_cache)}只有分红记录的股票，共{sum(len(years) for years in self.dividend_data_cache.values())}条分红年份记录")
            return True
        except Exception as e:
            print(f"加载分红数据失败: {e}")
            return False
    
    def has_dividend(self, code, date):
        """检查股票在当前交易日的前2年每年都有分红
        
        Args:
            code: 股票代码
            date: 当前交易日，格式为'YYYY-MM-DD'
            
        Returns:
            bool: 如果在当前交易日的前2年每年都有分红，返回True，否则返回False
        """
        if code not in self.dividend_data_cache:
            return False
        
        # 获取当前交易日的年份
        current_year = datetime.strptime(date, '%Y-%m-%d').year
        
        # 需要检查的年份：当前年份-1 和 当前年份-2
        years_to_check = [current_year - 1, current_year - 2]
        
        # 检查这些年份是否都有分红记录
        stock_dividend_years = self.dividend_data_cache[code]
        return all(year in stock_dividend_years for year in years_to_check)
    
    def get_roe_avg(self, code, date):
        """获取股票的roeAvg数据
        查询条件：code，pubDate小于交易日，且最靠近交易日的一条数据，statDate小于交易日且大于交易日减6个月
        从内存缓存中获取数据，不再直接查询数据库
        """
        # 转换日期字符串为datetime.date对象，方便比较
        trade_date = datetime.strptime(date, '%Y-%m-%d').date()
        six_months_ago = trade_date - timedelta(days=180)
        
        roe_avg = 0
        
        # 从内存缓存中查找符合条件的数据
        if code in self.roe_data_cache:
            roe_list = self.roe_data_cache[code]
            
            # 筛选符合条件的数据
            matched_roe = []
            for roe_item in roe_list:
                # 检查pubDate和statDate的类型，统一转换为date对象
                pub_date = roe_item['pubDate']
                if hasattr(pub_date, 'date'):
                    pub_date = pub_date.date()
                    
                stat_date = roe_item['statDate']
                if hasattr(stat_date, 'date'):
                    stat_date = stat_date.date()
                
                # 检查条件：pubDate < 交易日，statDate < 交易日，statDate > 交易日-6个月
                if (pub_date < trade_date and 
                    stat_date < trade_date and 
                    stat_date > six_months_ago):
                    matched_roe.append(roe_item)
            
            # 找到pubDate最靠近交易日的数据
            if matched_roe:
                # 按pubDate降序排序，第一个就是最靠近交易日的数据
                matched_roe.sort(key=lambda x: x['pubDate'], reverse=True)
                roe_avg = matched_roe[0]['roeAvg']

        return roe_avg
    
    def get_index_data(self, code, start_date, end_date):
        """从bao_nostock_trade表获取指数数据"""
        index_data = {}
        sql = """
        SELECT date, close FROM bao_nostock_trade 
        WHERE code = %s AND date BETWEEN %s AND %s
        ORDER BY date
        """
        try:
            self.cursor.execute(sql, (code, start_date, end_date))
            for row in self.cursor.fetchall():
                index_data[row['date'].strftime('%Y-%m-%d')] = row['close']
            print(f"获取到{len(index_data)}个交易日的{code}数据")
        except Exception as e:
            print(f"获取{code}数据失败: {e}")
        return index_data
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            print("数据库连接已关闭")
