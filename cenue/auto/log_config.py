import os
import sys

class LogConfig:
    def __init__(self, prefix):
        self.prefix = prefix
        self.log_file = None
        self.original_stdout = None
        
        # 初始化日志文件
        self.init_log_file()
    
    def init_log_file(self):
        """初始化日志文件，确保logs目录存在"""
        # 确保logs目录存在
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        # 初始化日志文件
        self.log_file = open(f'logs/{self.prefix}_backtest_log.txt', 'w', encoding='utf-8')
        self.original_stdout = sys.stdout
        sys.stdout = self
    
    def write(self, message):
        """重定向stdout，将输出同时写入控制台和日志文件"""
        self.original_stdout.write(message)
        self.log_file.write(message)
    
    def flush(self):
        """刷新输出"""
        self.original_stdout.flush()
        self.log_file.flush()
    
    def close(self):
        """关闭日志文件"""
        if hasattr(self, 'log_file'):
            self.log_file.close()
            sys.stdout = self.original_stdout
