
import os
import baostock as bs

import sys
# 获取当前脚本的绝对路径并向上回溯到根目录
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(root_path)  # 添加根目录到搜索路径

# 配置logger
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 登录baostock系统
def login_baostock():
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"登录失败: {lg.error_msg}")
        return False
    logger.info("登录成功")
    return True

# 登出baostock系统
def logout_baostock():
    bs.logout()
    logger.info("登出成功")
