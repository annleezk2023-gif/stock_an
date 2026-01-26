from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv
import os
import json

# 加载环境变量
load_dotenv()

# 创建Flask应用实例
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'mysql+mysqlconnector://root:123456@localhost/stock?charset=utf8mb4')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# 初始化数据库
db = SQLAlchemy(app)

# 获取my_industry_fund表的industry字段所有值
def get_industry_values():
    with app.app_context():
        # 查询所有不重复的industry值
        sql = """SELECT DISTINCT industry FROM my_industry_fund WHERE industry IS NOT NULL AND industry != '' ORDER BY industry"""
        result = db.session.execute(db.text(sql))
        industries = [row[0] for row in result.fetchall()]
        return industries

if __name__ == '__main__':
    industries = get_industry_values()
    print("my_industry_fund表的industry字段值:")
    print(json.dumps(industries, indent=2, ensure_ascii=False))
    
    # 生成HTML选项，用于模板中
    print("\n生成的HTML选项:")
    for industry in industries:
        print(f"<option value='{industry}'>{industry}</option>")