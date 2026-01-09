# bao_tags_loss 和 bao_tags_positive 字段类型变更检查报告

生成时间: 2026-01-08

## 1. 数据库表结构

### stock_auto_tags 表
```sql
CREATE TABLE `stock_auto_tags` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(20) NOT NULL COMMENT '股票代码',
  `pubDate` date DEFAULT NULL COMMENT '公告日期，即消息的最早日期',
  `statDate` date DEFAULT NULL COMMENT '统计日期',
  `year` int(11) DEFAULT NULL COMMENT '统计年份',
  `quarter` int(11) DEFAULT NULL COMMENT '统计季度',
  `bao_tags_loss` text DEFAULT NULL COMMENT '负面标签',
  `bao_tags_positive` text DEFAULT NULL COMMENT '依据bao计算的标签，正面标签',
  `tags_type` int(11) DEFAULT NULL COMMENT '类型1季度，2分红',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_code_statDate` (`code`,`statDate`,`tags_type`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=393217 DEFAULT CHARSET=utf8mb4 COMMENT='股票的自动计算好的标签';
```

**字段类型**: `bao_tags_loss` 和 `bao_tags_positive` 均为 `text` 类型

## 2. 代码使用情况分析

### 2.1 写入操作

#### stock_gen_auto_tags_dividend.py

**位置**: 第112-113行 (INSERT操作)
```python
conn.execute(text(f"""INSERT INTO stock_auto_tags (code, statDate, pubDate, tags_type, bao_tags_positive, bao_tags_loss)
    VALUES ('{stock_code}', '{statDateStr}', '{pubDateStr}', {tags_type}, '{','.join(positive_tags_sorted)}', '{','.join(loss_tags_sorted)}')"""))
```
- **写入方式**: 使用 `','.join()` 将列表转换为逗号分隔的字符串
- **兼容性**: ✅ 兼容 text 类型

**位置**: 第146-147行 (UPDATE操作)
```python
sql = f"""UPDATE stock_auto_tags
    SET bao_tags_positive = '{new_positive_tags_txt}',
        bao_tags_loss = '{new_loss_tags_txt}'
    WHERE code = '{stock_code}' and statDate = '{statDateStr}' and tags_type = {tags_type}"""
```
- **写入方式**: 直接写入字符串
- **兼容性**: ✅ 兼容 text 类型

### 2.2 读取操作

#### stock_gen_auto_tags_dividend.py

**位置**: 第117-118行
```python
new_positive_tags = [tag.strip() for tag in stock.bao_tags_positive.split(',')] if stock.bao_tags_positive else []
new_loss_tags = [tag.strip() for tag in stock.bao_tags_loss.split(',')] if stock.bao_tags_loss else []
```
- **读取方式**: 使用 `.split(',')` 分割字符串为列表
- **兼容性**: ✅ 兼容 text 类型

**位置**: 第141行
```python
if stock.bao_tags_positive == new_positive_tags_txt and stock.bao_tags_loss == new_loss_tags_txt:
```
- **读取方式**: 直接比较字符串值
- **兼容性**: ✅ 兼容 text 类型

#### my_cenue_fenghong.py

**位置**: 第217-226行
```python
if stock_auto_tags.bao_tags_loss:
    bao_tags_loss = [tag.strip() for tag in stock_auto_tags.bao_tags_loss.split(',')] if stock_auto_tags.bao_tags_loss else []
    if len(bao_tags_loss) > 0:
        self.stock_info_list.remove(stock_info)
        continue
if stock_auto_tags.bao_tags_positive:
    bao_tags_positive = [tag.strip() for tag in stock_auto_tags.bao_tags_positive.split(',')] if stock_auto_tags.bao_tags_positive else []
    if len(bao_tags_positive) > 0:
        has_fenghong_tag = True
```
- **读取方式**: 使用 `.split(',')` 分割字符串为列表
- **兼容性**: ✅ 兼容 text 类型

#### stock_basic_ana.html (Jinja2模板)

**位置**: 第176行
```jinja2
{% set loss_tags = item.bao_tag.bao_tags_loss.split(',') if item.bao_tag.bao_tags_loss else [] %}
```
- **读取方式**: 使用 `.split(',')` 分割字符串为列表
- **兼容性**: ✅ 兼容 text 类型

**位置**: 第186行
```jinja2
{% set positive_tags = item.bao_tag.bao_tags_positive.split(',') if item.bao_tag.bao_tags_positive else [] %}
```
- **读取方式**: 使用 `.split(',')` 分割字符串为列表
- **兼容性**: ✅ 兼容 text 类型

**位置**: 第203行
```jinja2
{% set fenghong_loss_tags = item.bao_fenghong_tag.bao_tags_loss.split(',') if item.bao_fenghong_tag.bao_tags_loss else [] %}
```
- **读取方式**: 使用 `.split(',')` 分割字符串为列表
- **兼容性**: ✅ 兼容 text 类型

**位置**: 第213行
```jinja2
{% set fenghong_positive_tags = item.bao_fenghong_tag.bao_tags_positive.split(',') if item.bao_fenghong_tag.bao_tags_positive else [] %}
```
- **读取方式**: 使用 `.split(',')` 分割字符串为列表
- **兼容性**: ✅ 兼容 text 类型

### 2.3 查询操作

#### app.py

**位置**: 第492行
```python
tag_sql = f"""SELECT * FROM stock_auto_tags WHERE code = '{cur_stock.code}' and tags_type = 1 ORDER BY statDate DESC LIMIT 1"""
```
- **操作**: 查询 stock_auto_tags 表
- **兼容性**: ✅ 兼容 text 类型

**位置**: 第499行
```python
tag_sql = f"""SELECT * FROM stock_auto_tags WHERE code = '{cur_stock.code}' and tags_type = 2 ORDER BY statDate DESC LIMIT 1"""
```
- **操作**: 查询 stock_auto_tags 表
- **兼容性**: ✅ 兼容 text 类型

#### stock_gen_auto_tags_season.py

**位置**: 第177行
```python
sql = f"""SELECT * FROM stock_auto_tags WHERE code = '{stock_code}' and statDate = '{cur_date_str}'"""
```
- **操作**: 查询 stock_auto_tags 表
- **兼容性**: ✅ 兼容 text 类型

**位置**: 第194行
```python
select 'dividend_num', count(*)  from stock_auto_tags where tags_type = 2
```
- **操作**: 统计 stock_auto_tags 表
- **兼容性**: ✅ 兼容 text 类型

#### cenue_db.py

**位置**: 第68行
```python
sql = f"""SELECT * FROM stock_auto_tags WHERE code = '{stock_code}' and pubDate < '{trade_date_str}' and pubDate >= '{month_ago_str}'
        and tags_type = {tags_type} order by pubDate desc limit 1"""
```
- **操作**: 查询 stock_auto_tags 表
- **兼容性**: ✅ 兼容 text 类型

## 3. 检查结论

### 3.1 字段类型变更影响

数据库表 `stock_auto_tags` 中的 `bao_tags_loss` 和 `bao_tags_positive` 字段已从原来的类型（可能是 varchar）变更为 `text` 类型。

### 3.2 代码兼容性分析

经过对所有.py和.html文件的全面检查,发现:

1. **所有代码均兼容 text 类型**
   - 写入操作: 使用字符串拼接,无长度限制
   - 读取操作: 使用 `.split(',')` 分割,适用于 text 类型
   - 查询操作: 标准 SQL 查询,不受字段类型影响

2. **数据格式保持一致**
   - 所有操作都基于逗号分隔的字符串格式
   - text 类型可以存储更长的字符串,不会影响现有逻辑

3. **无需修改代码**
   - 所有读取、写入、查询操作都与 text 类型完全兼容
   - 现有代码逻辑不需要任何调整

### 3.3 检查的文件列表

**Python文件**:
- `d:\workspace_2\stock_an\api\gen_data\stock_gen_auto_tags_dividend.py`
- `d:\workspace_2\stock_an\cenue\my_cenue_fenghong.py`
- `d:\workspace_2\stock_an\api\gen_data\stock_gen_auto_tags_season.py`
- `d:\workspace_2\stock_an\cenue\util\cenue_db.py`
- `d:\workspace_2\stock_an\app.py`

**HTML文件**:
- `d:\workspace_2\stock_an\templates\stock_basic_ana.html`

## 4. 总结

✅ **结论**: 代码无需修改

数据库字段类型从 varchar 变更为 text 后,所有现有代码都完全兼容,不需要进行任何修改。text 类型提供了更大的存储空间,可以存储更长的标签字符串,但不会影响现有的数据处理逻辑。

**建议**: 可以继续使用现有代码,无需进行任何调整。
