import tushare as ts

def get_tushare_pro():
    ts.set_token('dab46015dee241ef1a23b5a417c2ffdc37d4f5d8ce0ddeba8ae538c2')
    pro = ts.pro_api()
    return pro

def convert_stock_code_2tu(code):
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

def convert_stock_code_2bao(code):
    """
    转换股票代码格式：数据库格式为sh.600000，tushare格式为600000.SH
    考虑sh和sz两种情况
    """
    if code.endswith('.SH'):
        return 'sh.' + code.replace('sh.', '').replace('.SH', '')
    elif code.endswith('.SZ'):
        return 'sz.' + code.replace('sz.', '').replace('.SZ', '')
    else:
        return code

if __name__ == "__main__":
    print(convert_stock_code_2tu('sh.600000'))
    print(convert_stock_code_2tu('sz.000001'))
    print(convert_stock_code_2bao('600000.SH'))
    print(convert_stock_code_2bao('000001.SZ'))