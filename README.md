# 股票数据管理系统

这是一个基于Python和Flask的简单股票数据管理系统，可以实现对MySQL数据库中股票数据的添加和删除操作。

## 环境要求

- Python 3.8+ 
- MySQL 5.7+ 
- pip (Python包管理器)

## 安装步骤

### 1. 克隆或下载项目代码

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置MySQL数据库

1. 创建数据库：
   ```sql
   CREATE DATABASE stock;
   ```

2. 创建用户并授权（可选）：
   ```sql
   CREATE USER 'stock_user'@'localhost' IDENTIFIED BY 'your_password';
   GRANT ALL PRIVILEGES ON stock.* TO 'stock_user'@'localhost';
   FLUSH PRIVILEGES;
   ```

### 4. 配置环境变量

.env文件已配置为使用以下数据库连接信息：

```
# 数据库连接配置
DATABASE_URL=mysql+mysqlconnector://root:123456@localhost/stock
SECRET_KEY=your-secret-key
```

如果需要修改数据库连接信息，请编辑.env文件。

### 5. 初始化数据库

在项目根目录下运行以下命令初始化数据库表：

```bash
flask db init
flask db migrate -m "Initial migration"
flask db upgrade
```

## 运行项目

```bash
python app.py
```

项目将在`http://localhost:5000`启动。

## 使用方法

1. 访问`http://localhost:5000`进入主页
2. 点击"添加股票数据"按钮添加新的股票信息
3. 在主页可以查看所有已添加的股票数据
4. 点击"删除"按钮可以删除不需要的股票数据

## 项目结构

```
stock_an/
├── app.py              # 主应用文件
├── .env                # 环境变量配置
├── requirements.txt    # 项目依赖
├── README.md           # 项目说明文档
└── templates/          # HTML模板文件夹
    ├── index.html      # 主页模板
    └── add_stock.html  # 添加股票数据模板
```

## 功能说明

- 查看所有股票数据
- 添加新的股票数据（股票代码、股票名称、价格）
- 删除股票数据
- 操作成功或失败时显示提示信息

## 注意事项

1. 本项目仅作为学习示例，实际生产环境中需要添加更多的安全措施
2. 在使用前请确保已正确配置MySQL数据库连接信息
3. 如需部署到生产环境，请关闭debug模式并设置更安全的SECRET_KEY

## baostock己调用的接口
## 系统数据
9.1	交易日查询：query_trade_dates()

# 证券基本信息
9	获取证券元信息
9.2	证券代码查询：query_all_stock()  --未使用
9.3	证券基本资料：query_stock_basic()
公司的行业分类：行业分类：query_stock_industry()

## 个股交易信息
获取历史A股K线数据：query_history_k_data_plus()

## 公司财务信息
5	查询除权除息信息
5.1	除权除息信息：query_dividend_data()
6	查询复权因子信息
6.1	复权因子：query_adjust_factor()
7	查询季频财务数据信息
7.1	季频盈利能力：query_profit_data()
7.2	季频营运能力：query_operation_data()
7.3	季频成长能力：query_growth_data()
7.4	季频偿债能力：query_balance_data()
7.5	季频现金流量：query_cash_flow_data()
7.6	季频杜邦指数：query_dupont_data()
8	查询季频公司报告信息
8.1	季频公司业绩快报：query_performance_express_report()
8.2	季频公司业绩预告：query_forecast_report()


# 数据内容
day目录：
   nostock_trade每天执行，状态己退市的数据己全部下载，不用改状态；
   stock_basic_industry和stock_basic每天执行，新数据会更新旧数据；
   stock_trade每天执行


gen_data目录：
   先执行input_trade_data.py，再执行其它任务


season目录：
   所有数据全部完成，每季度执行一次。执行前要先删除该季度的数据。
   delete from bao_stock_balance where year = 2025 and quarter > 2 and data_exist = 0;
   delete from bao_stock_cash_flow where year = 2025 and quarter > 2 and data_exist = 0;
   delete from bao_stock_growth where year = 2025 and quarter > 2 and data_exist = 0;
   delete from bao_stock_operation where year = 2025 and quarter > 2 and data_exist = 0;
   delete from bao_stock_profit where year = 2025 and quarter > 2 and data_exist = 0;
   

year目录：
   trade_date每年执行一次，当前己执行到2025年。
   dividend_data每季度执行一次，当前己执行到2025年。执行前要先删除该年的数据。
   select * from bao_stock_dividend where year = 2025 and data_exist = 0 limit 10;
   select count(*) from bao_stock_dividend where year = 2025 and data_exist = 0;
   delete from bao_stock_dividend where year = 2025 and data_exist = 0;

