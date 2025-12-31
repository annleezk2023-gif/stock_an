import matplotlib.pyplot as plt
from datetime import datetime

# 指数代码常量定义，作为共用代码
INDEX_CODES = {
    'hs300': 'sh.000300',  # 沪深300
    'zz500': 'sh.000905',  # 中证500
    'zz1000': 'sh.000852'  # 中证1000
}

class TXTWriter:
    def __init__(self, prefix, db_reader=None):
        self.prefix = prefix
        self.db_reader = db_reader
    
    def generate_trade_report(self, trade_records):
        """生成交易记录报告"""
        if not trade_records:
            print("没有交易记录，无法生成交易报告")
            return
        
        file_name = f'logs/{self.prefix}_trade_records.txt'
        with open(file_name, 'w', encoding='utf-8') as f:
            f.write("交易日期,股票代码,股票名称,操作类型,交易价格,交易数量,交易金额,手续费,持仓数量,剩余资金,交易原因\n")
            for record in trade_records:
                f.write(f"{record['date']},{record['code']},{record['code_name']},{record['action']},{record['price']:.2f},{record['shares']},{record['amount']:.2f},{record['fee']:.2f},{record['total_shares']},{record['capital']:.2f},{record.get('reason', '')}\n")
        print(f"交易记录已保存到{file_name}，共{len(trade_records)}条记录")
    
    def generate_daily_report(self, daily_records):
        """生成每日持仓和收益报告"""
        if not daily_records:
            print("没有每日记录，无法生成每日报告")
            return
        
        file_name = f'logs/{self.prefix}_daily_results.txt'
        with open(file_name, 'w', encoding='utf-8') as f:
            f.write("日期,可用资金,持仓市值,总资产,累计收益率(%)\n")
            for record in daily_records:
                f.write(f"{record['date']},{record['capital']:.2f},{record['position_value']:.2f},{record['total_asset']:.2f},{record['total_return']:.2f}\n")
        print(f"每日结果已保存到{file_name}，共{len(daily_records)}条记录")
    
    def calculate_annual_stats(self, daily_records):
        """按年统计收益、最大收益、最大回撤"""
        if not daily_records:
            return []
        
        # 按年份分组
        annual_groups = {}
        for record in daily_records:
            # 提取年份
            year = record['date'][:4]
            if year not in annual_groups:
                annual_groups[year] = []
            annual_groups[year].append(record)
        
        # 计算每年的统计数据
        annual_stats = []
        for year, records in sorted(annual_groups.items()):
            # 计算年度收益
            start_asset = records[0]['total_asset']
            end_asset = records[-1]['total_asset']
            annual_return = (end_asset - start_asset) / start_asset * 100
            
            # 计算最大收益
            max_return = max(record['total_return'] for record in records)
            
            # 计算最大回撤
            max_drawdown = 0.0
            peak = records[0]['total_asset']
            for record in records:
                current_asset = record['total_asset']
                # 更新峰值
                if current_asset > peak:
                    peak = current_asset
                # 计算当前回撤
                drawdown = (peak - current_asset) / peak * 100
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
            
            annual_stats.append({
                'year': year,
                'annual_return': annual_return,
                'max_return': max_return,
                'max_drawdown': max_drawdown
            })
        
        return annual_stats
    
    def generate_equity_curve(self, daily_records, start_date, end_date):
        """生成收益曲线HTML页面，包含收益曲线图和年度统计表格"""
        if not daily_records:
            print("没有每日记录，无法生成收益曲线")
            return
        
        # 计算回测时间范围
        first_date = datetime.strptime(daily_records[0]['date'], '%Y-%m-%d')
        last_date = datetime.strptime(daily_records[-1]['date'], '%Y-%m-%d')
        total_days = (last_date - first_date).days
        
        # 根据时间范围确定采样频率
        resampled_records = []
        
        if total_days < 90:
            # 小于90天，按日绘制，使用所有数据
            resampled_records = daily_records
        else:
            # 分组采样
            groups = {}
            
            for record in daily_records:
                date = datetime.strptime(record['date'], '%Y-%m-%d')
                
                if 90 <= total_days <= 365:
                    # 90天-1年，按周绘制，使用每周最后一个交易日数据
                    year, week, _ = date.isocalendar()
                    key = (year, week)
                else:
                    # 1年以上，按月绘制，使用每月最后一个交易日数据
                    key = (date.year, date.month)
                
                if key not in groups:
                    groups[key] = []
                groups[key].append(record)
            
            # 取每组最后一个交易日数据
            for group_list in groups.values():
                resampled_records.append(group_list[-1])
            
            # 按日期排序
            resampled_records.sort(key=lambda x: x['date'])
        
        # 提取重新采样后的日期和收益率
        resampled_dates = [record['date'] for record in resampled_records]
        resampled_returns = [record['total_return'] for record in resampled_records]
        
        # 获取指数数据
        hs300_data = self.db_reader.get_index_data(INDEX_CODES['hs300'], start_date, end_date) if self.db_reader else None
        zz500_data = self.db_reader.get_index_data(INDEX_CODES['zz500'], start_date, end_date) if self.db_reader else None
        zz1000_data = self.db_reader.get_index_data(INDEX_CODES['zz1000'], start_date, end_date) if self.db_reader else None
        
        # 准备指数数据
        hs300_returns = []
        if hs300_data and resampled_dates and resampled_dates[0] in hs300_data:
            hs300_base = hs300_data[resampled_dates[0]]
            hs300_returns = [(hs300_data[date] / hs300_base - 1) * 100 if date in hs300_data else 0 for date in resampled_dates]
        
        zz500_returns = []
        if zz500_data and resampled_dates and resampled_dates[0] in zz500_data:
            zz500_base = zz500_data[resampled_dates[0]]
            zz500_returns = [(zz500_data[date] / zz500_base - 1) * 100 if date in zz500_data else 0 for date in resampled_dates]
        
        zz1000_returns = []
        if zz1000_data and resampled_dates and resampled_dates[0] in zz1000_data:
            zz1000_base = zz1000_data[resampled_dates[0]]
            zz1000_returns = [(zz1000_data[date] / zz1000_base - 1) * 100 if date in zz1000_data else 0 for date in resampled_dates]
        
        # 计算年度统计数据
        annual_stats = self.calculate_annual_stats(daily_records)
        
        # 生成HTML内容，使用字符串拼接避免模板语法冲突
        html_content = '<!DOCTYPE html>\n'
        html_content += '<html lang="zh-CN">\n'
        html_content += '<head>\n'
        html_content += '    <meta charset="UTF-8">\n'
        html_content += '    <meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        html_content += '    <title>策略收益与指数对比</title>\n'
        html_content += '    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>\n'
        html_content += '    <style>\n'
        html_content += '        body {\n'
        html_content += '            font-family: Arial, sans-serif;\n'
        html_content += '            margin: 0;\n'
        html_content += '            padding: 20px;\n'
        html_content += '            background-color: #f5f5f5;\n'
        html_content += '        }\n'
        html_content += '        .container {\n'
        html_content += '            max-width: 1200px;\n'
        html_content += '            margin: 0 auto;\n'
        html_content += '            background-color: white;\n'
        html_content += '            padding: 20px;\n'
        html_content += '            border-radius: 8px;\n'
        html_content += '            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);\n'
        html_content += '        }\n'
        html_content += '        h1 {\n'
        html_content += '            text-align: center;\n'
        html_content += '            color: #333;\n'
        html_content += '            margin-bottom: 30px;\n'
        html_content += '        }\n'
        html_content += '        .chart-container {\n'
        html_content += '            margin: 30px 0;\n'
        html_content += '            height: 400px;\n'
        html_content += '        }\n'
        html_content += '        .stats-table {\n'
        html_content += '            width: 100%;\n'
        html_content += '            border-collapse: collapse;\n'
        html_content += '            margin: 30px 0;\n'
        html_content += '        }\n'
        html_content += '        .stats-table th, .stats-table td {\n'
        html_content += '            border: 1px solid #ddd;\n'
        html_content += '            padding: 12px;\n'
        html_content += '            text-align: right;\n'
        html_content += '        }\n'
        html_content += '        .stats-table th {\n'
        html_content += '            background-color: #f2f2f2;\n'
        html_content += '            text-align: center;\n'
        html_content += '        }\n'
        html_content += '        .stats-table tr:hover {\n'
        html_content += '            background-color: #f5f5f5;\n'
        html_content += '        }\n'
        html_content += '        .summary {\n'
        html_content += '            margin: 20px 0;\n'
        html_content += '            padding: 15px;\n'
        html_content += '            background-color: #e8f4f8;\n'
        html_content += '            border-radius: 5px;\n'
        html_content += '        }\n'
        html_content += '        .summary p {\n'
        html_content += '            margin: 5px 0;\n'
        html_content += '        }\n'
        html_content += '    </style>\n'
        html_content += '</head>\n'
        html_content += '<body>\n'
        html_content += '    <div class="container">\n'
        html_content += '        <h1>策略收益与指数对比</h1>\n'
        html_content += '        \n'
        html_content += '        <div class="summary">\n'
        
        # 添加回测摘要信息
        html_content += f'            <p>回测时间：{start_date} 至 {end_date}</p>\n'
        html_content += f'            <p>总交易日：{len(daily_records)} 天</p>\n'
        html_content += f'            <p>初始资金：{daily_records[0]["total_asset"]:.2f} 元</p>\n'
        html_content += f'            <p>最终资金：{daily_records[-1]["total_asset"]:.2f} 元</p>\n'
        html_content += f'            <p>总收益率：{daily_records[-1]["total_return"]:.2f}%</p>\n'
        
        html_content += '        </div>\n'
        html_content += '        \n'
        html_content += '        <div class="chart-container">\n'
        html_content += '            <canvas id="equityChart"></canvas>\n'
        html_content += '        </div>\n'
        html_content += '        \n'
        html_content += '        <h2>年度统计</h2>\n'
        html_content += '        <table class="stats-table">\n'
        html_content += '            <thead>\n'
        html_content += '                <tr>\n'
        html_content += '                    <th>年份</th>\n'
        html_content += '                    <th>年度收益(%)</th>\n'
        html_content += '                    <th>最大收益(%)</th>\n'
        html_content += '                    <th>最大回撤(%)</th>\n'
        html_content += '                </tr>\n'
        html_content += '            </thead>\n'
        html_content += '            <tbody>\n'
        
        # 添加年度统计数据行
        for stats in annual_stats:
            html_content += '                <tr>\n'
            html_content += f'                    <td style="text-align: center;">{stats["year"]}</td>\n'
            html_content += f'                    <td>{stats["annual_return"]:.2f}</td>\n'
            html_content += f'                    <td>{stats["max_return"]:.2f}</td>\n'
            html_content += f'                    <td>{stats["max_drawdown"]:.2f}</td>\n'
            html_content += '                </tr>\n'
        
        # 完成表格
        html_content += '            </tbody>\n'
        html_content += '        </table>\n'
        html_content += '        \n'
        
        # 添加JavaScript部分
        html_content += '        <script>\n'
        html_content += '            // 准备数据\n'
        html_content += f'            const dates = {resampled_dates};\n'
        html_content += f'            const strategyReturns = {resampled_returns};\n'
        html_content += f'            const hs300Returns = {hs300_returns};\n'
        html_content += f'            const zz500Returns = {zz500_returns};\n'
        html_content += f'            const zz1000Returns = {zz1000_returns};\n'
        html_content += '            \n'
        html_content += '            // 创建图表\n'
        html_content += '            const ctx = document.getElementById(\'equityChart\').getContext(\'2d\');\n'
        html_content += '            const equityChart = new Chart(ctx, {\n'
        html_content += '                type: \'line\',\n'
        html_content += '                data: {\n'
        html_content += '                    labels: dates,\n'
        html_content += '                    datasets: [\n'
        html_content += '                        {\n'
        html_content += '                            label: \'策略收益\',\n'
        html_content += '                            data: strategyReturns,\n'
        html_content += '                            borderColor: \'blue\',\n'
        html_content += '                            backgroundColor: \'rgba(0, 0, 255, 0.1)\',\n'
        html_content += '                            borderWidth: 2,\n'
        html_content += '                            tension: 0.1,\n'
        html_content += '                            fill: false\n'
        html_content += '                        },\n'
        html_content += '                        {\n'
        html_content += '                            label: \'沪深300\',\n'
        html_content += '                            data: hs300Returns,\n'
        html_content += '                            borderColor: \'red\',\n'
        html_content += '                            backgroundColor: \'rgba(255, 0, 0, 0.1)\',\n'
        html_content += '                            borderWidth: 2,\n'
        html_content += '                            tension: 0.1,\n'
        html_content += '                            fill: false,\n'
        html_content += '                            borderDash: [5, 5]\n'
        html_content += '                        },\n'
        html_content += '                        {\n'
        html_content += '                            label: \'中证500\',\n'
        html_content += '                            data: zz500Returns,\n'
        html_content += '                            borderColor: \'green\',\n'
        html_content += '                            backgroundColor: \'rgba(0, 255, 0, 0.1)\',\n'
        html_content += '                            borderWidth: 2,\n'
        html_content += '                            tension: 0.1,\n'
        html_content += '                            fill: false,\n'
        html_content += '                            borderDash: [5, 5]\n'
        html_content += '                        },\n'
        html_content += '                        {\n'
        html_content += '                            label: \'中证1000\',\n'
        html_content += '                            data: zz1000Returns,\n'
        html_content += '                            borderColor: \'purple\',\n'
        html_content += '                            backgroundColor: \'rgba(128, 0, 128, 0.1)\',\n'
        html_content += '                            borderWidth: 2,\n'
        html_content += '                            tension: 0.1,\n'
        html_content += '                            fill: false,\n'
        html_content += '                            borderDash: [5, 5]\n'
        html_content += '                        }\n'
        html_content += '                    ]\n'
        html_content += '                },\n'
        html_content += '                options: {\n'
        html_content += '                    responsive: true,\n'
        html_content += '                    maintainAspectRatio: false,\n'
        html_content += '                    plugins: {\n'
        html_content += '                        legend: {\n'
        html_content += '                            position: \'top\'\n'
        html_content += '                        },\n'
        html_content += '                        title: {\n'
        html_content += '                            display: true,\n'
        html_content += '                            text: \'策略收益与指数对比\'\n'
        html_content += '                        }\n'
        html_content += '                    },\n'
        html_content += '                    scales: {\n'
        html_content += '                        y: {\n'
        html_content += '                            beginAtZero: true,\n'
        html_content += '                            title: {\n'
        html_content += '                                display: true,\n'
        html_content += '                                text: \'收益率(%)\'\n'
        html_content += '                            }\n'
        html_content += '                        },\n'
        html_content += '                        x: {\n'
        html_content += '                            title: {\n'
        html_content += '                                display: true,\n'
        html_content += '                                text: \'日期\'\n'
        html_content += '                            },\n'
        html_content += '                            ticks: {\n'
        html_content += '                                maxRotation: 45,\n'
        html_content += '                                minRotation: 45\n'
        html_content += '                            }\n'
        html_content += '                        }\n'
        html_content += '                    },\n'
        html_content += '                    interaction: {\n'
        html_content += '                        mode: \'index\',\n'
        html_content += '                        intersect: false\n'
        html_content += '                    },\n'
        html_content += '                    elements: {\n'
        html_content += '                        point: {\n'
        html_content += '                            radius: 2,\n'
        html_content += '                            hoverRadius: 6\n'
        html_content += '                        }\n'
        html_content += '                    }\n'
        html_content += '                }\n'
        html_content += '            });\n'
        html_content += '        </script>\n'
        html_content += '    </div>\n'
        html_content += '</body>\n'
        html_content += '</html>\n'
        
        # 保存HTML文件
        file_name = f'logs/{self.prefix}_equity_curve.html'
        with open(file_name, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"收益曲线HTML已保存到{file_name}")
