import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app, db, BaoNoStockBasic
import json

with app.app_context():
    try:
        print("开始查询 bao_nostock_basic 表...")
        
        # 测试1: 直接查询所有记录
        all_records = BaoNoStockBasic.query.all()
        print(f"查询结果数量: {len(all_records)}")
        
        if len(all_records) > 0:
            print(f"第一条记录: ID={all_records[0].id}, Code={all_records[0].code}, Name={all_records[0].code_name}")
            print(f"Tags原始值: {all_records[0].tags}")
            print(f"Tags类型: {type(all_records[0].tags)}")
            
            # 测试2: 分页查询
            pagination = BaoNoStockBasic.query.order_by(BaoNoStockBasic.code).paginate(page=1, per_page=100, error_out=False)
            print(f"\n分页查询结果:")
            print(f"  总记录数: {pagination.total}")
            print(f"  当前页: {pagination.page}")
            print(f"  每页数量: {pagination.per_page}")
            print(f"  总页数: {pagination.pages}")
            print(f"  当前页记录数: {len(pagination.items)}")
            
            # 测试3: 测试tags字段处理
            print(f"\n测试tags字段处理:")
            for nostock in pagination.items[:3]:
                print(f"  Code={nostock.code}, Tags原始值={nostock.tags}, Tags类型={type(nostock.tags)}")
                # 模拟app.py中的处理逻辑
                if nostock.tags:
                    if isinstance(nostock.tags, str):
                        tags = json.loads(nostock.tags)
                        print(f"    处理后Tags: {tags}")
                    elif isinstance(nostock.tags, list):
                        print(f"    Tags已经是list: {nostock.tags}")
                    else:
                        print(f"    Tags类型未知: {type(nostock.tags)}")
                else:
                    print(f"    Tags为空")
        else:
            print("没有查询到任何记录！")
            
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
