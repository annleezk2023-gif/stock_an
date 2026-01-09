# 修改计划：将 stock_auto_tags 表标签字段从 JSON 改为 varchar(2000)

## 1. 数据库修改
- 备份 `stock_auto_tags` 表数据
- 创建数据迁移 SQL 脚本，将 JSON 数组转换为逗号分隔字符串
- 修改字段类型：`bao_tags_loss` 和 `bao_tags_positive` 从 `json` 改为 `varchar(2000)`
- 执行数据迁移

## 2. Python 文件修改

### 2.1 [stock_gen_auto_tags_dividend.py](file:///d:/workspace_2/stock_an/api/gen_data/stock_gen_auto_tags_dividend.py)
- 修改 `save_bao_tags_2_db` 函数（第 101-152 行）
  - 移除 `json.loads` 解析逻辑
  - 直接使用逗号分隔的字符串
  - 确保写入前对标签进行排序

### 2.2 [my_cenue_fenghong.py](file:///d:/workspace_2/stock_an/cenue/my_cenue_fenghong.py)
- 修改第 217-228 行的标签解析逻辑
  - 移除 `json.loads` 调用
  - 改为字符串分割：`split(',')`

### 2.3 [cenue_db.py](file:///d:/workspace_2/stock_an/cenue/util/cenue_db.py)
- 修改 `get_stock_tags_by_trade_date` 函数（第 56-73 行）
  - 返回的数据保持字符串格式，不需要 JSON 解析

### 2.4 [app.py](file:///d:/workspace_2/stock_an/app.py)
- 修改第 492-504 行的标签查询逻辑
  - 移除 JSON 解析
  - 将字符串转换为列表：`split(',')`

## 3. HTML 文件修改

### 3.1 [stock_basic_ana.html](file:///d:/workspace_2/stock_an/templates/stock_basic_ana.html)
- 修改第 176-197 行的标签显示逻辑
  - 将字符串按逗号分割为数组
  - 遍历显示每个标签

## 4. 数据迁移 SQL 脚本
创建迁移脚本：
```sql
-- 1. 备份表
CREATE TABLE stock_auto_tags_backup AS SELECT * FROM stock_auto_tags;

-- 2. 将 JSON 数组转换为逗号分隔字符串
UPDATE stock_auto_tags 
SET bao_tags_loss = TRIM(BOTH '"' FROM REPLACE(REPLACE(bao_tags_loss, '","', ','), '["', ''))
WHERE bao_tags_loss IS NOT NULL;

UPDATE stock_auto_tags 
SET bao_tags_positive = TRIM(BOTH '"' FROM REPLACE(REPLACE(bao_tags_positive, '","', ','), '["', ''))
WHERE bao_tags_positive IS NOT NULL;

-- 3. 修改字段类型
ALTER TABLE stock_auto_tags 
MODIFY COLUMN bao_tags_loss varchar(2000) DEFAULT NULL COMMENT '负面标签';

ALTER TABLE stock_auto_tags 
MODIFY COLUMN bao_tags_positive varchar(2000) DEFAULT NULL COMMENT '依据bao计算的标签，正面标签';
```

## 5. 验证步骤
- 检查数据迁移是否正确
- 运行 Python 脚本测试标签生成和读取
- 访问 Web 页面验证标签显示