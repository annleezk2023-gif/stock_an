from datetime import datetime

# 避免循环导入，这里不直接导入db
# 实际使用时，db会在app.py中初始化并注入

# StockInfo模型，对应stock_info表
class StockInfo:
    # 在app.py中会实际创建这个模型与db的关联
    __tablename__ = 'stock_info'
    
    # 字段定义（作为类变量）
    id = None
    stock_code = None
    stock_name = None
    market = None
    industry = None
    sector = None
    total_shares = None
    circulation_shares = None
    created_at = None
    updated_at = None
    
    # 用于API响应的静态方法，将数据库模型转换为字典
    @staticmethod
    def to_dict(stock_obj):
        if not stock_obj:
            return None
        
        return {
            'id': stock_obj.id,
            'stock_code': stock_obj.stock_code,
            'stock_name': stock_obj.stock_name,
            'market': stock_obj.market,
            'industry': stock_obj.industry,
            'sector': stock_obj.sector,
            'total_shares': stock_obj.total_shares,
            'circulation_shares': stock_obj.circulation_shares,
            'created_at': stock_obj.created_at.isoformat() if stock_obj.created_at else None,
            'updated_at': stock_obj.updated_at.isoformat() if stock_obj.updated_at else None
        }

# 用于API接口的请求体模型（非数据库模型）
class StockInfoRequest:
    def __init__(self, data):
        self.stock_code = data.get('stock_code')
        self.stock_name = data.get('stock_name')
        self.market = data.get('market')
        self.industry = data.get('industry')
        self.sector = data.get('sector')
        self.total_shares = data.get('total_shares')
        self.circulation_shares = data.get('circulation_shares')
    
    def to_dict(self):
        result = {
            'stock_code': self.stock_code,
            'stock_name': self.stock_name
        }
        
        # 只包含非None的字段
        if self.market:
            result['market'] = self.market
        if self.industry:
            result['industry'] = self.industry
        if self.sector:
            result['sector'] = self.sector
        if self.total_shares is not None:
            result['total_shares'] = self.total_shares
        if self.circulation_shares is not None:
            result['circulation_shares'] = self.circulation_shares
        
        return result