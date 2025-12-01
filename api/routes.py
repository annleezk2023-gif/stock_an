from flask import Blueprint, jsonify, request
from .models import StockInfo as StockInfoConverter  # 导入转换器类
import json

# 创建API蓝图
api_bp = Blueprint('api', __name__, url_prefix='/api')
