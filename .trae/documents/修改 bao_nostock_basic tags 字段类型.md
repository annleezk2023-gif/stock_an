## 修改 bao_nostock_basic 表 tags 字段从 JSON 改为 varchar

### 1. 数据库表结构修改
- 修改 `db_info.txt` 中 `bao_nostock_basic` 表的 `tags` 字段定义
- 将 `tags json DEFAULT NULL` 改为 `tags varchar(100) DEFAULT NULL COMMENT '标签，逗号分隔的字符串，可选值：观察、量化、不看'`

### 2. Python 文件修改

#### app.py
- **第81行**：修改 BaoNoStockBasic 模型的 tags 字段定义
  - `tags = db.Column(db.JSON)` → `tags = db.Column(db.String(100))`
  
- **第516-534行**：修改 `nostock_basic_page` 函数
  - 添加逻辑：将数据库中的逗号分隔字符串转换为列表，供模板显示
  
- **第765-798行**：修改 `save_nostock_tags` 函数
  - 将前端传来的标签列表转换为逗号分隔的字符串
  - 修改验证逻辑，确保标签值在允许范围内

### 3. HTML 文件修改

#### templates/nostock_basic.html
- **第132-139行**：修改 tags 显示逻辑
  - 将字符串按逗号分割为数组进行显示
  
- **第146行**：修改 editTags 调用参数
  - 传递字符串格式的 tags 而不是数组
  
- **第231-256行**：修改 editTags 函数
  - 将字符串按逗号分割为数组，用于选中对应的复选框
  
- **第264-291行**：修改 saveTags 函数
  - 保持不变（前端仍然发送数组），后端负责转换

### 4. 数据迁移 SQL
- 编写 SQL 脚本将现有的 JSON 数组转换为逗号分隔的字符串
- 使用 `JSON_UNQUOTE(JSON_EXTRACT(tags, '$[*]'))` 或类似函数提取值并用 `GROUP_CONCAT` 连接

### 5. 执行顺序
1. 先备份数据库
2. 修改 Python 和 HTML 代码
3. 执行数据迁移 SQL
4. 修改数据库表结构
5. 测试验证功能正常