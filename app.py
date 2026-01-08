from flask import Flask, request, render_template, flash, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from dotenv import load_dotenv
from sqlalchemy import text
import os
from datetime import datetime, timedelta
import json

# 加载环境变量
load_dotenv()

# 配置logger
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 创建Flask应用实例
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'your-secret-key')
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'mysql+mysqlconnector://root:123456@localhost/stock?charset=utf8mb4')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# 测试打印功能
logger.info("\n" + "="*50)
logger.info("应用初始化中...")
logger.info(f"数据库URL: {app.config['SQLALCHEMY_DATABASE_URI']}")
logger.info("="*50 + "\n")

# 使用SQLAlchemy原生的ECHO配置直接打印SQL语句
app.config['SQLALCHEMY_ECHO'] = True
logger.info("SQLALCHEMY_ECHO已设置为True")

# 初始化数据库
db = SQLAlchemy(app)
migrate = Migrate(app, db)

# 定义交易日数据模型
class TradeDate(db.Model):
    __tablename__ = 'bao_trade_date'
    
    id = db.Column(db.Integer, primary_key=True)
    calendar_date = db.Column(db.String(10), unique=True, nullable=False)
    is_trading_day = db.Column(db.Boolean, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

# 定义股票基本信息模型
class BaoStockBasic(db.Model):
    __tablename__ = 'bao_stock_basic'
    
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(20), unique=True, nullable=False)  # 证券代码
    code_name = db.Column(db.String(100), nullable=False)  # 证券名称
    ipo_date = db.Column(db.Date)  # 上市日期
    out_date = db.Column(db.Date)  # 退市日期
    type = db.Column(db.String(50))  # 证券类型
    status = db.Column(db.String(20))  # 上市状态
    tags = db.Column(db.String(100))  # 标签字段，逗号分隔的字符串
    auto_tags = db.Column(db.String(100))  # 自动生成的标签字段，逗号分隔的字符串
    remark = db.Column(db.String(1000))  # 备注字段，最多1000个文字
    risk_memo = db.Column(db.String(200))  # 风险备注字段，最多200个文字
    industry = db.Column(db.String(100))  # 所属行业
    industryClassification = db.Column(db.String(100))  # 所属行业类别
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)
    trade_score = db.Column(db.Float, comment='公司打分，用于量化交易；-1表示黑名单')
    trade_score_reason = db.Column(db.String(1000), comment='公司打分的原因')

# 定义非股票基本信息模型
class BaoNoStockBasic(db.Model):
    __tablename__ = 'bao_nostock_basic'
    
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(20), unique=True, nullable=False)  # 证券代码
    code_name = db.Column(db.String(100), nullable=False)  # 证券名称
    ipo_date = db.Column(db.Date)  # 上市日期
    out_date = db.Column(db.Date)  # 退市日期
    type = db.Column(db.String(50))  # 证券类型
    status = db.Column(db.String(20))  # 上市状态
    tags = db.Column(db.JSON)  # 标签字段，JSON格式，可以存储多个中文词
    remark = db.Column(db.String(1000))  # 备注字段，最多1000个文字
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

# 定义股票基本分析数据模型，与数据库表 stock_basic_ana 字段保持一致
class StockBasicAna(db.Model):
    __tablename__ = 'stock_basic_ana'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True, comment='主键ID')
    code = db.Column(db.String(20), unique=True, comment='证券代码')
    created_at = db.Column(db.DateTime, default=datetime.now, nullable=False, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, nullable=False, comment='更新时间')
    # 季度更新
    epsTTM = db.Column(db.Float, comment='最近1年每股收益')
    dividCashPsBeforeTax = db.Column(db.Float, comment='最近1年分红')
    MBRevenueRate = db.Column(db.Float, comment='季报营收同比')
    netProfitRate = db.Column(db.Float, comment='最近1季度净利润同比增长率')
    roeAvg = db.Column(db.Float, comment='最近1年净资产收益率(平均)')
    gpMargin = db.Column(db.Float, comment='销售毛利率')
    npMargin = db.Column(db.Float, comment='销售净利率')
    
    jijinNum = db.Column(db.Integer, comment='持仓基金数量')
    jijinPercent = db.Column(db.Float, comment='基金持仓百分比')

    # 每天更新
    curTotalprice = db.Column(db.Float, comment='当前总市值')

    # 每天计算
    dividCashPsPercent = db.Column(db.Float, comment='最近1年股息率')
    # 人工设置
    tradeBuyPE = db.Column(db.Float, comment='交易-买入pe')
    tradeBuyAllPE = db.Column(db.Float, comment='交易-全仓pe')
    tradeSalePE = db.Column(db.Float, comment='交易-卖出pe')
    tradeSaleAllPE = db.Column(db.Float, comment='交易-清仓pe')

# 定义行业分析数据模型
class FundAna(db.Model):
    __tablename__ = 'fund_ana'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True, comment='主键ID')
    industry = db.Column(db.String(100), unique=True, nullable=False, comment='所属行业')
    all_stock = db.Column(db.String(1000), comment='所有股票代码，逗号分隔')
    top_stock = db.Column(db.String(100), comment='行业TOP10股票的代码')
    sort_tag = db.Column(db.Integer, comment='排序标签，可选值：3重点、2观察、1不看')
    remark = db.Column(db.String(1000), comment='备注字段，最多1000个文字')
    risk_memo = db.Column(db.String(200), comment='风险备注')
    created_at = db.Column(db.DateTime, default=datetime.now, nullable=False, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, nullable=False, comment='更新时间')
    pe_year_1_percent = db.Column(db.Float, comment='1年PE分位值')
    pe_year_3_percent = db.Column(db.Float, comment='3年PE分位值')
    pe_year_5_percent = db.Column(db.Float, comment='5年PE分位值')
    pe_year_10_percent = db.Column(db.Float, comment='10年PE分位值')
    top_etf = db.Column(db.String(100), comment='行业TOP ETF的代码')
    etf_year_1_percent = db.Column(db.Float, comment='1年point分位值')
    etf_year_3_percent = db.Column(db.Float, comment='3年point分位值')
    etf_year_5_percent = db.Column(db.Float, comment='5年point分位值')
    etf_year_10_percent = db.Column(db.Float, comment='10年point分位值')
    
# 定义非股票K线数据模型
class BaoNoStockTrade(db.Model):
    __tablename__ = 'bao_nostock_trade'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    code = db.Column(db.String(20), nullable=False)  # 证券代码
    date = db.Column(db.Date, nullable=False)  # 日期
    open = db.Column(db.Float)  # 开盘价
    high = db.Column(db.Float)  # 最高价
    low = db.Column(db.Float)  # 最低价
    close = db.Column(db.Float)  # 收盘价
    preclose = db.Column(db.Float)  # 前收盘价
    volume = db.Column(db.BigInteger)  # 成交量
    amount = db.Column(db.Float)  # 成交额
    adjustflag = db.Column(db.Integer)  # 复权状态
    turn = db.Column(db.Float)  # 换手率
    tradestatus = db.Column(db.Integer)  # 交易状态
    pctChg = db.Column(db.Float)  # 涨跌幅
    peTTM = db.Column(db.Float)  # 滚动市盈率
    psTTM = db.Column(db.Float)  # 滚动市销率
    pcfNcfTTM = db.Column(db.Float)  # 滚动市现率
    pbMRQ = db.Column(db.Float)  # 市净率
    isST = db.Column(db.Integer)  # ST标识
    created_at = db.Column(db.DateTime)  # 创建时间
    updated_at = db.Column(db.DateTime)  # 更新时间
    
    # 定义联合唯一索引
    __table_args__ = (
        db.UniqueConstraint('code', 'date', name='unique_code_date'),
    )

# 导入API蓝图（延迟导入以避免循环依赖）
def register_api_blueprint():
    from api.routes import api_bp
    app.register_blueprint(api_bp)

# 显示主页
@app.route('/index')
def show_index():
    # 由于stock_data表已被删除，这里传递空列表给模板
    return render_template('index.html', stocks=[])

# 根路径路由
@app.route('/')
def root_index():
    return render_template('index.html', stocks=[])

# 显示按年份聚合的交易日数据
@app.route('/trade_dates/yearly')
def trade_dates_yearly():
    try:
        # 查询bao_trade_date表，按年份聚合交易日数量
        yearly_data = db.session.query(
            db.func.substr(TradeDate.calendar_date, 1, 4).label('year'),
            db.func.count(TradeDate.id).label('count')
        ).filter_by(
            is_trading_day=True
        ).group_by(
            'year'
        ).order_by(
            'year'
        ).all()
        
        return render_template('trade_dates_yearly.html', yearly_data=yearly_data)
    except Exception as e:
        flash('获取按年份聚合的交易日数据失败: ' + str(e))
        return render_template('trade_dates_yearly.html', yearly_data=[])

# 显示特定年份的所有交易日数据
@app.route('/trade_dates/by_year/<string:year>')
def trade_dates_by_year(year):
    try:
        # 查询特定年份的所有交易日数据
        trade_dates = TradeDate.query.filter(
            TradeDate.calendar_date.like(f'{year}%'),
            TradeDate.is_trading_day == True
        ).order_by(
            TradeDate.calendar_date
        ).all()
        
        return render_template('trade_dates_by_year.html', year=year, trade_dates=trade_dates)
    except Exception as e:
        flash('获取特定年份的交易日数据失败: ' + str(e))
        return render_template('trade_dates_by_year.html', year=year, trade_dates=[])


# 获取股票基本信息查询的通用方法
def get_stock_basic_query_sql(code_name='', code='', industry=None, status='1', tags=None, auto_tags=None, auto_tags_count=None, page=1, per_page=100, do_paginate=True, bukan=None):
    """
    使用SQL语句拼接方式构建股票基本信息的查询并支持分页
    
    参数:
    - code_name: 证券名称模糊查询
    - code: 证券代码模糊查询
    - industry: 行业列表过滤（支持多选）
    - status: 上市状态过滤
    - tags: 标签列表过滤
    - auto_tags: 自动标签列表过滤
    - auto_tags_count: 自动标签数量过滤
    - page: 页码（从1开始）
    - per_page: 每页数量
    - do_paginate: 是否执行分页，True返回分页对象，False返回查询对象
    
    返回:
    - do_paginate=True 时返回包含分页信息的结果
    - do_paginate=False 时返回查询结果列表
    """
    
    # 初始化industry为列表
    if industry is None:
        industry = []
    
    # 基础SQL查询
    base_sql = "SELECT * FROM bao_stock_basic"
    
    # 构建WHERE条件
    where_conditions = []
    params = {}
    
    # 添加证券名称模糊查询
    if code_name:
        where_conditions.append("code_name LIKE :code_name")
        params["code_name"] = f"%{code_name}%"
    
    # 添加证券代码模糊查询
    if code:
        where_conditions.append("code LIKE :code")
        params["code"] = f"%{code}%"
    
    # 添加行业过滤（支持多选）
    if industry:
        # 使用IN查询，更简洁高效
        # 动态生成占位符，处理MySQL IN查询
        placeholders = ', '.join([f":industry_{i}" for i in range(len(industry))])
        where_conditions.append(f"industry IN ({placeholders})")
        # 添加参数
        for i, ind in enumerate(industry):
            params[f"industry_{i}"] = ind
    
    # 添加上市状态过滤
    if status:
        where_conditions.append("status = :status")
        params["status"] = status
    
    # 添加自动标签数量过滤
    if auto_tags_count and auto_tags_count > 0:
        where_conditions.append(f"(LENGTH(TRIM(auto_tags)) - LENGTH(REPLACE(TRIM(auto_tags), ',', '')) + 1) >= {auto_tags_count}")

    # 添加bukan过滤
    if bukan == 1:
        where_conditions.append(f"(JSON_SEARCH(tags, 'one', '不看') IS NULL)")

    # 添加标签过滤 (多选)
    if tags:
        # 分离出_BLANK_标签和普通标签
        blank_tags = [tag for tag in tags if tag == '_BLANK_']
        normal_tags = [tag for tag in tags if tag != '_BLANK_']
        
        # 处理普通标签 (使用OR连接多个JSON_CONTAINS条件)
        if normal_tags:
            tag_conditions = []
            for idx, tag in enumerate(normal_tags):
                param_name = f"tag_{idx}"
                tag_conditions.append(f"JSON_CONTAINS(tags, :{param_name}, '$')")
                params[param_name] = f"\"{tag}\""
            
            if tag_conditions:
                where_conditions.append(f"({' OR '.join(tag_conditions)})")
        
        # 处理_BLANK_标签 (查询tags为null或空数组的情况)
        if blank_tags:
            where_conditions.append("(tags IS NULL OR tags = '[]')")
    
    # 添加自动标签过滤 (多选)
    if auto_tags:
        # 分离出_BLANK_自动标签和普通自动标签
        blank_auto_tags = [tag for tag in auto_tags if tag == '_BLANK_']
        normal_auto_tags = [tag for tag in auto_tags if tag != '_BLANK_']
        
        # 处理普通自动标签 (使用OR连接多个LIKE条件)
        if normal_auto_tags:
            auto_tag_conditions = []
            for idx, tag in enumerate(normal_auto_tags):
                param_name = f"auto_tag_{idx}"
                auto_tag_conditions.append(f"(auto_tags LIKE CONCAT('%', :{param_name}, '%') OR auto_tags LIKE CONCAT(:{param_name}, ',%') OR auto_tags LIKE CONCAT('%,', :{param_name}, '%') OR auto_tags = :{param_name})")
                params[param_name] = tag
            
            if auto_tag_conditions:
                where_conditions.append(f"({' OR '.join(auto_tag_conditions)})")
        
        # 处理_BLANK_自动标签 (查询auto_tags为null或空字符串的情况)
        if blank_auto_tags:
            where_conditions.append("(auto_tags IS NULL OR auto_tags = '')")
    
    # 组合WHERE子句
    if where_conditions:
        base_sql += " WHERE " + " AND ".join(where_conditions)
    
    # 添加排序
    base_sql += " ORDER BY (LENGTH(TRIM(auto_tags)) - LENGTH(REPLACE(TRIM(auto_tags), ',', '')) + 1) DESC, code DESC"
    
    # 执行查询
    if do_paginate:
        # 确保page和per_page是有效整数
        page = max(1, int(page))
        per_page = max(1, int(per_page))
        # 确保每页数量是可选的值之一
        if per_page not in [100, 200, 500]:
            per_page = 100
        
        # 计算偏移量
        offset = (page - 1) * per_page
        
        # 查询总记录数
        count_sql = "SELECT COUNT(*) FROM bao_stock_basic"
        if where_conditions:
            count_sql += " WHERE " + " AND ".join(where_conditions)
        
        total = db.session.execute(text(count_sql), params).scalar()
        
        # 添加分页LIMIT和OFFSET
        paginated_sql = base_sql + " LIMIT :limit OFFSET :offset"
        params["limit"] = per_page
        params["offset"] = offset
        
        # 执行分页查询
        results_old = db.session.execute(text(paginated_sql), params).mappings().all()

        #转为stock_basic对象
        results = [BaoStockBasic(**dict(item)) for item in results_old]
        # tags, auto_tags从字符串转为数组
        for stock in results:
            stock.tags = [tag.strip() for tag in stock.tags.split(',') if tag.strip()] if stock.tags else []
            stock.auto_tags = [tag.strip() for tag in stock.auto_tags.split(',') if tag.strip()] if stock.auto_tags else []
        
        # 创建自定义分页对象
        class Pagination:
            def __init__(self, items, page, per_page, total):
                self.items = items
                self.page = page
                self.per_page = per_page
                self.total = total
                self.pages = (total + per_page - 1) // per_page
                self.has_prev = page > 1
                self.has_next = page < self.pages
                
            def prev_num(self):
                return page - 1 if self.has_prev else None
                
            def next_num(self):
                return page + 1 if self.has_next else None
                
            def iter_pages(self, left_edge=2, left_current=2, right_current=3, right_edge=2):
                last = 0
                for num in range(1, self.pages + 1):
                    if num <= left_edge or (num > page - left_current - 1 and num < page + right_current) or num > self.pages - right_edge:
                        if last + 1 != num:
                            yield None
                        yield num
                        last = num
        
        return Pagination(results, page, per_page, total)
    else:
        # 返回所有结果
        results = db.session.execute(text(base_sql), params).mappings().all()
        return results


# 获取行业列表的API接口
@app.route('/api/industries')
def get_industries_api():
    """
    获取行业列表的API接口
    
    返回:
    - JSON格式的行业列表
    """
    try:
        industries_list = db.session.execute(
            text("SELECT DISTINCT industry FROM bao_stock_basic WHERE industry IS NOT NULL AND industry <> '' ORDER BY industry")
        ).scalars().all()
        return {
            'success': True,
            'data': industries_list,
            'message': '获取行业列表成功'
        }
    except Exception as e:
        logger.error(f"获取行业列表API失败: {e}")
        return {
            'success': False,
            'data': [],
            'message': f'获取行业列表失败: {str(e)}'
        }, 500


# 显示所有股票基本信息的页面
@app.route('/stock_basic')
def stock_basic_page():
    try:
        # 获取查询参数
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 100, type=int)
        code_name = request.args.get('code_name', '').strip()
        code = request.args.get('code', '').strip()
        industry = request.args.getlist('industry')
        status = request.args.get('status', '1').strip()  # 默认查询上市股票
        tags = request.args.getlist('tag')  # 获取所有选中的标签
        auto_tags = request.args.getlist('auto_tag')  # 获取所有选中的自动标签
        auto_tags_count = request.args.get('auto_tags_count', '', type=int)
        ana_type = request.args.get('ana_type', '0', type=int)
        bukan = request.args.get('bukan', '0', type=int)
        
        # 确保每页数量是可选的值之一
        if per_page not in [100, 200, 500]:
            per_page = 100
        
        # 调用通用方法获取分页对象
        pagination = get_stock_basic_query_sql(
            code_name=code_name,
            code=code,
            industry=industry,
            status=status,
            tags=tags,
            auto_tags=auto_tags,
            auto_tags_count=auto_tags_count,
            page=page,
            per_page=per_page,
            bukan=bukan
        )
        stock_basics = pagination.items
        
        if ana_type == 1:
            conn = db.session.connection()
            # 提取3年的K线信息
            for cur_stock in stock_basics:
                # 转换 tags 和 auto_tags 为列表
                if cur_stock.tags:
                    cur_stock.tags = [tag.strip() for tag in cur_stock.tags.split(',')] if cur_stock.tags else []
                if cur_stock.auto_tags:
                    cur_stock.auto_tags = [tag.strip() for tag in cur_stock.auto_tags.split(',')] if cur_stock.auto_tags else []
                
                trade_table_name = "bao_stock_trade_" + cur_stock.code[-1]
                kline_sql = f"""SELECT * FROM {trade_table_name} WHERE code = '{cur_stock.code}' AND date >= DATE_SUB(CURDATE(), INTERVAL 3 YEAR) ORDER BY date asc"""
                kline_results = conn.execute(text(kline_sql)).all()
                cur_stock.kline_results = kline_results
                # 最后一条数据
                if kline_results:
                    cur_stock.last_kline = kline_results[-1]
                else:
                    cur_stock.last_kline = None

                # 提取最近的标签
                tag_sql = f"""SELECT * FROM stock_auto_tags WHERE code = '{cur_stock.code}' and tags_type = 1 ORDER BY statDate DESC LIMIT 1"""
                tag_result = conn.execute(text(tag_sql)).fetchone()
                if tag_result:
                    cur_stock.bao_tag = tag_result
                else:
                    cur_stock.bao_tag = None

                tag_sql = f"""SELECT * FROM stock_auto_tags WHERE code = '{cur_stock.code}' and tags_type = 2 ORDER BY statDate DESC LIMIT 1"""
                tag_result = conn.execute(text(tag_sql)).fetchone()
                if tag_result:
                    cur_stock.bao_fenghong_tag = tag_result
                else:
                    cur_stock.bao_fenghong_tag = None

                # 取统计数据
                ana_sql = f"""SELECT * FROM stock_basic_ana WHERE code = '{cur_stock.code}' LIMIT 1"""
                ana_result = conn.execute(text(ana_sql)).fetchone()
                if ana_result:
                    cur_stock.ana = ana_result
                else:
                    cur_stock.ana = None
            conn.close()
            return render_template('stock_basic_ana.html', stock_basics=stock_basics, pagination=pagination, per_page=per_page)
        else:
            return render_template('stock_basic.html', stock_basics=stock_basics, pagination=pagination, per_page=per_page)
    except Exception as e:
        logger.error(f"获取股票基本信息失败: {e}")
        return render_template('stock_basic.html', stock_basics=[], pagination=None, per_page=100)

# 显示所有非股票基本信息的页面
@app.route('/nostock_basic')
def nostock_basic_page():
    try:
        # 获取查询参数
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 100, type=int)
        
        # 确保每页数量是可选的值之一
        if per_page not in [100, 200, 500]:
            per_page = 100
            
        # 分页查询，按code排序
        pagination = BaoNoStockBasic.query.order_by(BaoNoStockBasic.code).paginate(page=page, per_page=per_page, error_out=False)
        nostock_basics = pagination.items
        
        # 将tags字符串转换为数组，供模板显示
        for nostock in nostock_basics:
            if nostock.tags:
                nostock.tags = [tag.strip() for tag in nostock.tags.split(',') if tag.strip()]
            else:
                nostock.tags = []
        
        return render_template('nostock_basic.html', nostock_basics=nostock_basics, pagination=pagination, per_page=per_page)
    except Exception as e:
        flash('获取非股票基本信息失败: ' + str(e))
        return render_template('nostock_basic.html', nostock_basics=[], pagination=None, per_page=100)

# 显示特定非股票的日K数据页面
@app.route('/nostock_trade/<string:code>')
def nostock_trade_page(code):
    try:
        # 获取非股票基本信息
        nostock = BaoNoStockBasic.query.filter_by(code=code).first()
        if not nostock:
            flash(f'未找到代码为 {code} 的非股票信息')
            return redirect(url_for('nostock_basic_page'))
        
        # 查询该非股票的所有日K数据，按日期倒序排列，仅查询最近5年的数据
        five_years_ago = datetime.now() - timedelta(days=3*365)
        trades = BaoNoStockTrade.query.filter_by(code=code).filter(BaoNoStockTrade.date >= five_years_ago).order_by(BaoNoStockTrade.date.desc()).all()
        
        return render_template('nostock_trade.html', 
                             stock_code=code, 
                             stock_name=nostock.code_name, 
                             trades=trades, 
                             total_count=len(trades))
    except Exception as e:
        flash(f'获取非股票K线数据失败: {str(e)}')
        return redirect(url_for('nostock_basic_page'))


# 显示特定股票的日K数据页面
@app.route('/stock_trade/<string:code>')
def stock_trade_page(code):
    try:
        # 获取股票基本信息
        stock = BaoStockBasic.query.filter_by(code=code).first()
        if not stock:
            flash(f'未找到代码为 {code} 的股票信息')
            return redirect(url_for('stock_basic_page'))

        # 计算日期，默认为系统日期
        lastTradeDateStr = request.args.get('lastTradeDate', datetime.now().strftime('%Y-%m-%d'))
        
        # 查询该股票的所有日K数据，按日期倒序排列，仅查询最近3年的数据
        trade_table_name = "bao_stock_trade_" + code[-1]
        sql = f"""SELECT * FROM {trade_table_name} WHERE code = '{code}' and date between DATE_SUB('{lastTradeDateStr}', INTERVAL 3 YEAR) and '{lastTradeDateStr}' ORDER BY date desc"""
        trades = db.session.execute(text(sql)).fetchall()
        
        return render_template('stock_trade.html', 
                             stock_code=code, 
                             stock_name=stock.code_name, 
                             trades=trades, 
                             total_count=len(trades),
                             lastTradeDate=lastTradeDateStr)
    except Exception as e:
        flash(f'获取股票K线数据失败: {str(e)}')
        return redirect(url_for('stock_basic_page'))


# 保存股票备注的路由
@app.route('/stock_basic/save_remark/<int:id>', methods=['POST'])
def save_stock_remark(id):
    try:
        # 查找要更新的股票记录
        stock = BaoStockBasic.query.get_or_404(id)
        
        # 获取请求数据
        data = request.get_json()
        if not data or 'remark' not in data:
            return json.dumps({'success': False, 'message': '无效的请求数据'}), 400
        
        # 验证备注长度
        remark = data['remark']
        risk_memo = data.get('risk_memo', '')
        if len(remark) > 1000:
            return json.dumps({'success': False, 'message': '备注不能超过1000个文字'}), 400
        if len(risk_memo) > 200:
            return json.dumps({'success': False, 'message': '风险备注不能超过200个文字'}), 400
        
        # 更新备注
        stock.remark = remark
        stock.risk_memo = risk_memo
        stock.updated_at = datetime.now()
        
        # 保存到数据库
        db.session.commit()
        
        return json.dumps({'success': True, 'message': '备注更新成功'})
    except Exception as e:
        db.session.rollback()
        return json.dumps({'success': False, 'message': str(e)}), 500

# 保存股票标签的路由
@app.route('/stock_basic/save_tags/<int:id>', methods=['POST'])
def save_stock_tags(id):
    try:
        # 查找要更新的股票记录
        stock = BaoStockBasic.query.get_or_404(id)
        
        # 获取请求数据
        data = request.get_json()
        if not data or 'tags' not in data:
            return json.dumps({'success': False, 'message': '无效的请求数据'}), 400
        
        # 验证标签数据类型
        tags = data['tags']
        if not isinstance(tags, list):
            return json.dumps({'success': False, 'message': '标签必须是列表格式'}), 400
        
        # 确保标签只包含指定的中文词
        allowed_tags = ['重点', '观察', '垄断', '半垄断', '量化', '不看']
        for tag in tags:
            if tag not in allowed_tags:
                return json.dumps({'success': False, 'message': f'不允许的标签: {tag}'}), 400
        
        # 更新标签，排序后转换为逗号分隔字符串
        tags_sorted = sorted(tags)
        stock.tags = ','.join(tags_sorted)
        stock.updated_at = datetime.now()
        
        # 保存到数据库
        db.session.commit()
        
        return json.dumps({'success': True, 'message': '标签更新成功'})
    except Exception as e:
        db.session.rollback()
        return json.dumps({'success': False, 'message': str(e)}), 500


# 显示行业分析页面
@app.route('/fund_ana')
def fund_ana_page():
    try:
        # 查询所有行业分析数据
        # 先按sort_tag倒序（3重点 > 2观察 > 1不看 > 无标签），再按pe_year_1_percent升序
        fund_anas = FundAna.query.all()
        
        # 自定义排序：先按sort_tag倒序，再按pe_year_1_percent升序
        def sort_key(fund):
            # sort_tag的优先级：3 > 2 > 1 > 0
            sort_priority = fund.sort_tag if fund.sort_tag else 0
            
            # pe_year_1_percent越小优先级越高
            pe_value = fund.pe_year_1_percent if fund.pe_year_1_percent is not None else 999999
            
            return (-sort_priority, pe_value)
        
        fund_anas_sorted = sorted(fund_anas, key=sort_key)
        
        # 为每个行业查询成长型和分红型股票
        for fund in fund_anas_sorted:
            stocks = BaoStockBasic.query.filter(
                BaoStockBasic.industry == fund.industry,
                BaoStockBasic.status == '1'
            ).all()
            
            growth_stocks = []
            fenghong_stocks = []
            for stock in stocks:
                if stock.auto_tags:
                    if '成长' in stock.auto_tags:
                        growth_stocks.append(stock.code)
                    if '股息' in stock.auto_tags:
                        fenghong_stocks.append(stock.code)
            
            fund.growth_stock = ','.join(growth_stocks) if growth_stocks else None
            fund.fenghong_stock = ','.join(fenghong_stocks) if fenghong_stocks else None
        
        # 获取所有股票代码对应的名称
        all_codes = []
        for fund in fund_anas_sorted:
            if fund.top_stock:
                all_codes.extend(fund.top_stock.split(','))
            if fund.growth_stock:
                all_codes.extend(fund.growth_stock.split(','))
            if fund.fenghong_stock:
                all_codes.extend(fund.fenghong_stock.split(','))
        
        stock_dict = {}
        if all_codes:
            stocks = BaoStockBasic.query.filter(BaoStockBasic.code.in_(all_codes)).all()
            stock_dict = {stock.code: stock for stock in stocks}
        
        return render_template('fund_ana.html', fund_anas=fund_anas_sorted, stock_dict=stock_dict)
    except Exception as e:
        logger.error(f"获取行业分析数据失败: {e}")
        return render_template('fund_ana.html', fund_anas=[], stock_dict={})

# 保存行业分析数据的路由
@app.route('/fund_ana/save/<int:id>', methods=['POST'])
def save_fund_ana(id):
    try:
        # 查找要更新的行业分析记录
        fund_ana = FundAna.query.get_or_404(id)
        
        # 获取请求数据
        data = request.get_json()
        if not data:
            return json.dumps({'success': False, 'message': '无效的请求数据'}), 400
        
        # 验证备注长度
        remark = data.get('remark', '')
        risk_memo = data.get('risk_memo', '')
        sort_tag = data.get('sort_tag', None)
        
        if len(remark) > 1000:
            return json.dumps({'success': False, 'message': '备注不能超过1000个文字'}), 400
        if len(risk_memo) > 200:
            return json.dumps({'success': False, 'message': '风险备注不能超过200个文字'}), 400
        
        # 验证sort_tag值（只能是0、1、2、3）
        if sort_tag is not None and sort_tag not in [0, 1, 2, 3]:
            return json.dumps({'success': False, 'message': '排序标签只能是0、1、2、3'}), 400
        
        # 更新字段
        fund_ana.sort_tag = sort_tag
        fund_ana.remark = remark if remark else None
        fund_ana.risk_memo = risk_memo if risk_memo else None
        fund_ana.updated_at = datetime.now()
        
        # 保存到数据库
        db.session.commit()
        
        return json.dumps({'success': True, 'message': '更新成功'})
    except Exception as e:
        db.session.rollback()
        logger.error(f"保存行业分析数据失败: {e}")
        return json.dumps({'success': False, 'message': str(e)}), 500

# 保存非股票备注的路由
@app.route('/nostock_basic/save_remark/<int:id>', methods=['POST'])
def save_nostock_remark(id):
    try:
        # 查找要更新的非股票记录
        nostock = BaoNoStockBasic.query.get_or_404(id)
        
        # 获取请求数据
        data = request.get_json()
        if not data or 'remark' not in data:
            return json.dumps({'success': False, 'message': '无效的请求数据'}), 400
        
        # 验证备注长度
        remark = data['remark']
        if len(remark) > 1000:
            return json.dumps({'success': False, 'message': '备注不能超过1000个文字'}), 400
        
        # 更新备注
        nostock.remark = remark
        nostock.updated_at = datetime.now()
        
        # 保存到数据库
        db.session.commit()
        
        return json.dumps({'success': True, 'message': '备注更新成功'})
    except Exception as e:
        db.session.rollback()
        return json.dumps({'success': False, 'message': str(e)}), 500

# 保存non-stock标签路由
@app.route('/nostock_basic/save_tags/<int:id>', methods=['POST'])
def save_nostock_tags(id):
    try:
        # 从请求中获取标签数据
        data = request.get_json()
        if not data or 'tags' not in data:
            return json.dumps({'success': False, 'message': '无效的请求数据'}), 400
        
        # 验证标签数据类型
        tags = data['tags']
        if not isinstance(tags, list):
            return json.dumps({'success': False, 'message': '标签必须是列表格式'}), 400
        
        # 确保标签只包含指定的中文词
        allowed_tags = ['观察', '量化', '不看']
        for tag in tags:
            if tag not in allowed_tags:
                return json.dumps({'success': False, 'message': f'不允许的标签: {tag}'}), 400
        
        # 将标签数组转换为逗号分隔的字符串
        tags_str = ','.join(tags) if tags else None
        
        # 查找对应的记录
        nostock = BaoNoStockBasic.query.get_or_404(id)
        
        # 更新标签和时间戳
        nostock.tags = tags_str
        nostock.updated_at = datetime.now()
        
        # 提交到数据库
        db.session.commit()
        
        return json.dumps({'success': True, 'message': '标签保存成功'})
    except Exception as e:
        # 发生错误时回滚
        db.session.rollback()
        return json.dumps({'success': False, 'message': f'保存失败: {str(e)}'}), 500

@app.route('/save_pe_values', methods=['POST'])
def save_pe_values():
    try:
        data = request.get_json()
        code = data.get('code')
        
        # 查找对应的StockBasicAna记录
        stock_ana = StockBasicAna.query.filter_by(code=code).first()
        if not stock_ana:
            return json.dumps({'success': False, 'error': '未找到该股票的分析记录'}), 404
        
        # 更新PE值字段
        stock_ana.tradeBuyPE = data.get('tradeBuyPE')
        stock_ana.tradeBuyAllPE = data.get('tradeBuyAllPE')
        stock_ana.tradeSalePE = data.get('tradeSalePE')
        stock_ana.tradeSaleAllPE = data.get('tradeSaleAllPE')
        
        db.session.commit()
        logger.info(f"成功保存股票 {code} 的PE交易参数")
        return json.dumps({'success': True})
    except Exception as e:
        db.session.rollback()
        logger.error(f"保存PE值时发生错误: {str(e)}")
        return json.dumps({'success': False, 'error': str(e)}), 500

# 定义指数代码常量
INDEX_CODES = {
    'hs300': 'sh.000300',  # 沪深300
    'zz500': 'sh.000905',  # 中证500
    'zz1000': 'sh.000852'  # 中证1000
}

# 计算年度涨幅和最大回撤
def calculate_yearly_stats(data):
    if not data:
        return {}
    
    stats = {}
    yearly_data = {}
    
    # 按年份分组数据
    for item in data:
        year = item.date.strftime('%Y')
        if year not in yearly_data:
            yearly_data[year] = []
        yearly_data[year].append(item)
    
    # 计算每年的涨幅和最大回撤
    for year, year_data in yearly_data.items():
        if not year_data:
            continue
        
        # 按日期排序
        year_data.sort(key=lambda x: x.date)
        
        # 计算涨幅：(年末收盘价 - 年初收盘价) / 年初收盘价 * 100
        start_close = year_data[0].close
        end_close = year_data[-1].close
        gain = (end_close - start_close) / start_close * 100
        
        # 计算最大回撤：(年度最低收盘价 - 年初收盘价) / 年初收盘价 * 100
        # 找到年度最低收盘价
        min_close = min(item.close for item in year_data)
        # 计算最大回撤，结果应为负数
        max_drawdown = (min_close - start_close) / start_close * 100
        
        stats[year] = {
            'gain': gain,
            'max_drawdown': max_drawdown
        }
    
    return stats

# 指数对比页面
@app.route('/stock_index_trade', methods=['GET', 'POST'])
def stock_index_trade():
    from datetime import datetime, timedelta
    
    # 默认日期范围：最近一年
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    if request.method == 'POST':
        # 从表单获取日期范围
        start_date = request.form.get('start_date', start_date)
        end_date = request.form.get('end_date', end_date)
    
    # 查询三个指数的数据
    try:
        # 转换日期格式
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # 查询所有数据
        all_data = {}
        for name, code in INDEX_CODES.items():
            data = BaoNoStockTrade.query.filter(
                BaoNoStockTrade.code == code,
                BaoNoStockTrade.date >= start_dt,
                BaoNoStockTrade.date <= end_dt
            ).order_by(BaoNoStockTrade.date).all()
            all_data[name] = data
        
        # 准备图表数据
        chart_data = {
            'labels': [],
            'hs300': {'close': [], 'amount': []},
            'zz500': {'close': [], 'amount': []},
            'zz1000': {'close': [], 'amount': []}
        }
        
        # 获取所有日期
        dates = set()
        for name, data in all_data.items():
            for item in data:
                dates.add(item.date)
        
        # 按日期排序
        sorted_dates = sorted(dates)
        chart_data['labels'] = [date.strftime('%Y-%m-%d') for date in sorted_dates]
        
        # 为每个指数填充数据
        for name, data in all_data.items():
            # 创建日期到数据的映射
            data_map = {item.date: item for item in data}
            
            # 填充数据，缺失日期用None
            for date in sorted_dates:
                if date in data_map:
                    chart_data[name]['close'].append(data_map[date].close)
                    # 成交额转换为亿
                    chart_data[name]['amount'].append(data_map[date].amount / 100000000 if data_map[date].amount else 0)
                else:
                    chart_data[name]['close'].append(None)
                    chart_data[name]['amount'].append(None)
        
        # 计算年度统计数据
        yearly_stats = {}
        for year in sorted(set(date.strftime('%Y') for date in sorted_dates)):
            yearly_stats[year] = {
                'hs300': calculate_yearly_stats([item for item in all_data['hs300'] if item.date.strftime('%Y') == year]),
                'zz500': calculate_yearly_stats([item for item in all_data['zz500'] if item.date.strftime('%Y') == year]),
                'zz1000': calculate_yearly_stats([item for item in all_data['zz1000'] if item.date.strftime('%Y') == year])
            }
        
        # 处理年度统计数据的格式
        formatted_yearly_stats = {}
        for year, stats in yearly_stats.items():
            formatted_yearly_stats[year] = {
                'hs300': {
                    'gain': stats['hs300'].get(year, {}).get('gain', 0),
                    'max_drawdown': stats['hs300'].get(year, {}).get('max_drawdown', 0)
                },
                'zz500': {
                    'gain': stats['zz500'].get(year, {}).get('gain', 0),
                    'max_drawdown': stats['zz500'].get(year, {}).get('max_drawdown', 0)
                },
                'zz1000': {
                    'gain': stats['zz1000'].get(year, {}).get('gain', 0),
                    'max_drawdown': stats['zz1000'].get(year, {}).get('max_drawdown', 0)
                }
            }
        
        return render_template('stock_index_trade.html', 
                              start_date=start_date, 
                              end_date=end_date,
                              chart_data=chart_data,
                              yearly_stats=formatted_yearly_stats)
    
    except Exception as e:
        logger.error(f"查询指数数据时发生错误: {str(e)}")
        return render_template('stock_index_trade.html', 
                              start_date=start_date, 
                              end_date=end_date,
                              chart_data={'labels': [], 'hs300': {'close': [], 'amount': []}, 'zz500': {'close': [], 'amount': []}, 'zz1000': {'close': [], 'amount': []}},
                              yearly_stats={})

# 运行应用
if __name__ == '__main__':
    # 在应用启动时注册API蓝图
    # 暂时注释掉API蓝图注册，因为api模块依赖已删除的StockInfo模型
    # register_api_blueprint()
    app.run(debug=True)