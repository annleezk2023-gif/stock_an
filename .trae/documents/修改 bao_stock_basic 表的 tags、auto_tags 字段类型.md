# bao_stock_basic 表 tags、auto_tags 字段类型修改计划

## 一、数据库修改

### 1. 备份数据库
```sql
-- 备份 bao_stock_basic 表
CREATE TABLE bao_stock_basic_backup_20260107 AS SELECT * FROM bao_stock_basic;
```

### 2. 修改 auto_tags 字段类型（tags 已经是 varchar）
```sql
-- 步骤1：创建临时列存储转换后的数据
ALTER TABLE bao_stock_basic ADD COLUMN auto_tags_new varchar(100) DEFAULT NULL COMMENT '自动标签，逗号分隔的字符串';

-- 步骤2：将 JSON 数组转换为逗号分隔的字符串
UPDATE bao_stock_basic
SET auto_tags_new = (
    CASE
        WHEN auto_tags IS NULL THEN NULL
        WHEN auto_tags = '[]' THEN NULL
        WHEN auto_tags = '' THEN NULL
        ELSE TRIM(BOTH '[]' FROM REPLACE(REPLACE(auto_tags, '"', ''), ',', ','))
    END
)
WHERE auto_tags IS NOT NULL;

-- 步骤3：删除旧的 auto_tags 列
ALTER TABLE bao_stock_basic DROP COLUMN auto_tags;

-- 步骤4：将新列重命名为 auto_tags
ALTER TABLE bao_stock_basic CHANGE COLUMN auto_tags_new auto_tags varchar(100) DEFAULT NULL COMMENT '自动标签，逗号分隔的字符串';
```

## 二、Python 文件修改

### 1. app.py
- **第60行**：修改 `BaoStockBasic.auto_tags` 字段类型从 `db.JSON` 改为 `db.String(100)`
- **第285-286行**：移除 `JSON_LENGTH` 相关查询逻辑
- **第314-332行**：修改 auto_tags 过滤逻辑，从 `JSON_CONTAINS` 改为 `LIKE` 或字符串匹配
- **第339行**：移除 `JSON_LENGTH` 排序
- **第370-373行**：移除 JSON 解析逻辑，改为字符串分割
- **第486、493行**：保持不变（查询 stock_auto_tags 表，该表保持 JSON 格式）

### 2. api/gen_data/stock_gen_auto_tags_dividend.py
- **第110-111行**：修改插入语句，将 `JSON_ARRAY` 改为逗号分隔字符串
- **第114-134行**：修改标签合并逻辑，从 JSON 数组操作改为字符串操作
- **第143-146行**：修改更新语句，将 `JSON_ARRAY` 改为逗号分隔字符串

### 3. api/gen_data/stock_auto_tags_by_file.py
- **第39-69行**：修改 auto_tags 更新逻辑，从 JSON 函数改为字符串操作
  - 移除 `JSON_REMOVE`、`JSON_SEARCH`、`JSON_ARRAY_APPEND` 等函数
  - 改为字符串的分割、添加、去重操作

### 4. api/stock_common.py
- **第23行**：修改查询逻辑，移除 `JSON_LENGTH` 条件
- 添加字符串分割和合并的辅助函数

## 三、HTML 模板修改

### 1. templates/stock_basic.html
- **第44-57行**：修改 auto_tags 显示逻辑
  - 将 `auto_tags_list` 改为字符串分割：`stock.auto_tags.split(',') if stock.auto_tags else []`
  - 保持显示样式不变

### 2. templates/stock_basic_ana.html
- **第192-195行**：修改 auto_tags 显示逻辑
  - 改为字符串分割和显示

### 3. templates/stock_common.html
- **第47-53行**：保持自动标签过滤复选框不变
- **第56行**：保持 auto_tags_count 输入框不变（需要修改后端逻辑支持字符串计数）

## 四、数据迁移验证

执行以下 SQL 验证数据转换：
```sql
-- 验证数据转换
SELECT id, code, code_name, tags, auto_tags 
FROM bao_stock_basic 
WHERE auto_tags IS NOT NULL 
LIMIT 10;

-- 统计数据
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN auto_tags IS NOT NULL THEN 1 ELSE 0 END) as has_auto_tags
FROM bao_stock_basic;
```

## 五、注意事项

1. **stock_auto_tags 表保持不变**：该表用于存储历史自动标签记录，保持 JSON 格式
2. **bao_stock_basic.auto_tags**：改为 varchar 后，存储逗号分隔的字符串，如："15成长,3股息"
3. **兼容性处理**：确保所有读取 auto_tags 的地方都改为字符串分割
4. **备份重要性**：执行数据库修改前务必做好备份