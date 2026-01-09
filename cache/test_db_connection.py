import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import json

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL', 'mysql+mysqlconnector://root:123456@localhost/stock?charset=utf8mb4')
print(f"数据库URL: {DATABASE_URL}")

try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("数据库连接成功！")
        
        # 查询表中的数据数量
        result = conn.execute(text("SELECT COUNT(*) FROM bao_nostock_basic"))
        count = result.scalar()
        print(f"bao_nostock_basic 表中的记录数: {count}")
        
        if count > 0:
            # 查询前5条记录
            result = conn.execute(text("SELECT * FROM bao_nostock_basic LIMIT 5"))
            rows = result.mappings().all()
            print(f"\n前5条记录:")
            for row in rows:
                print(f"  ID: {row['id']}, Code: {row['code']}, Name: {row['code_name']}, Tags: {row['tags']}")
                if row['tags']:
                    try:
                        tags = json.loads(row['tags']) if isinstance(row['tags'], str) else row['tags']
                        print(f"    Tags类型: {type(tags)}, 内容: {tags}")
                    except Exception as e:
                        print(f"    Tags解析错误: {e}")
                        print(f"    Tags原始值: {repr(row['tags'])}")
        else:
            print("表中没有数据！")
            
except Exception as e:
    print(f"错误: {e}")
    import traceback
    traceback.print_exc()
