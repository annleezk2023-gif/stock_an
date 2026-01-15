import tushare as ts

def get_tushare_pro():
    ts.set_token('dab46015dee241ef1a23b5a417c2ffdc37d4f5d8ce0ddeba8ae538c2')
    pro = ts.pro_api()
    return pro

def convert_stock_code(code):
    """
    转换股票代码格式：数据库格式为sh.600000，tushare格式为600000.SH
    考虑sh和sz两种情况
    """
    if code.startswith('sh.'):
        return code.replace('sh.', '').replace('.SH', '') + '.SH'
    elif code.startswith('sz.'):
        return code.replace('sz.', '').replace('.SZ', '') + '.SZ'
    else:
        return code
