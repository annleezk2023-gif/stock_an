# 修改计划：表格宽度改为百分比，列宽在tr标签中定义

## 需要修改的文件

### 1. CSS文件修改

* **styles.css**: 确保th,td样式允许换行（已完成）

### 2. HTML文件修改

#### fund\_ana.html

* 将table标签的`style="width: 1920px;"`改为`style="width: 100%;"`

* 将thead中tr标签的th标签内的`style="width: xxxpx;"`移除

* 在thead的tr标签中添加`style="width: 150px 450px 120px 200px 250px 200px 100px;"`

* 移除tbody中所有td标签的`style="width: xxxpx;"`

#### stock\_basic.html

* 将table标签的`style="width: 1920px;"`改为`style="width: 100%;"`

* 将thead中tr标签的th标签内的`style="width: xxxpx;"`移除

* 在thead的tr标签中添加`style="width: 200px 150px 150px 300px 120px;"`

* 移除tbody中所有td标签的`style="width: xxxpx;"`

#### stock\_basic\_ana.html

* 将table标签的`style="width: 1920px;"`改为`style="width: 100%;"`

* 将thead中tr标签的th标签内的`style="width: xxxpx;"`移除

* 在thead的tr标签中添加`style="width: 200px 200px 180px 180px 350px 180px 280px;"`

* 移除tbody中所有td标签的`style="width: xxxpx;"`

#### nostock\_basic.html

* 将table标签的`style="width: 1920px;"`改为`style="width: 100%;"`

* 将thead中tr标签的th标签内的`style="width: xxxpx;"`移除

* 在thead的tr标签中添加`style="width: 120px 150px 120px 120px 100px 100px 200px 300px 120px;"`

* 移除tbody中所有td标签的`style="width: xxxpx;"`

#### stock\_trade.html

* 将table标签的`style="width: 1920px;"`改为`style="width: 100%;"`

* 将thead中tr标签的th标签内的`style="width: xxxpx;"`移除

* 在thead的tr标签中添加`style="width: 120px 100px 100px 100px 100px 120px 150px 100px 200px;"`

* 移除tbody中所有td标签的`style="width: xxxpx;"`

#### stock\_index\_trade.html

* 将table标签的`style="width: 1920px;"`改为`style="width: 100%;"`

* 将thead中tr标签的th标签内的`style="width: xxxpx;"`移除

* 在thead的tr标签中添加`style="width: 100px 150px 150px 150px 150px 150px 150px 150px;"`

* 移除tbody中所有td标签的`style="width: xxxpx;"`

#### trade\_dates\_yearly.html

* 将table标签的`style="width: 1920px;"`改为`style="width: 100%;"`

* 将thead中tr标签的th标签内的`style="width: xxxpx;"`移除

* 在thead的tr标签中添加`style="width: 960px 960px;"`（保持固定宽度，因为只有2列）

* 移除tbody中所有td标签的`style="width: xxxpx;"`

#### trade\_dates\_by\_year.html

* 将table标签的`style="width: 1920px;"`改为`style="width: 100%;"`

* 将thead中tr标签的th标签内的`style="width: xxxpx;"`移除

* 在thead的tr标签中添加`style="width: 480px 480px 480px 480px;"`（保持固定宽度，因为只有4列）

* 移除tbody中所有td标签的`style="width: xxxpx;"`

#### nostock\_trade.html

* 将table标签的`style="width: 1920px;"`改为`style="width: 100%;"`

* 将thead中tr标签的th标签内的`style="width: xxxpx;"`移除

* 在thead的tr标签中添加`style="width: 120px 100px 100px 100px 100px 120px 150px 100px;"`

* 移除tbody中所有td标签的`style="width: xxxpx;"`

## 修改原则

1. 表格宽度使用百分比（100%）
2. 列宽在tr标签中定义（使用百分比或固定宽度）
3. 移除td标签中的宽度样式
4. 移除th标签中的宽度样式
5. 内容超出时自动换行（CSS已配置）

