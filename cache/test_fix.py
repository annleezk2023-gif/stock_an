import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app, db, BaoNoStockBasic
import json

with app.app_context():
    try:
        print("测试修复后的代码...")
        
        # 模拟 app.py 中的查询逻辑
        page = 1
        per_page = 100
        
        pagination = BaoNoStockBasic.query.order_by(BaoNoStockBasic.code).paginate(page=page, per_page=per_page, error_out=False)
        nostock_basics = pagination.items
        
        print(f"查询结果数量: {len(nostock_basics)}")
        
        # 测试修复后的tags处理逻辑
        for nostock in nostock_basics[:5]:
            if nostock.tags:
                if isinstance(nostock.tags, str):
                    nostock.tags = json.loads(nostock.tags)
            else:
                nostock.tags = []
            
            print(f"Code={nostock.code}, Name={nostock.code_name}, Tags={nostock.tags}, Tags类型={type(nostock.tags)}")
        
        print("\n测试成功！tags字段处理正常。")
            
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
