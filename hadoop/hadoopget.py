# coding=utf-8
__author__ = 'GaoJie'
# 加载hadoop结果集，计算

def get_sum(date_time, field_map, table, sum_field='Requests', date_field='Datetime', is_train=True, where='1'):
    """
    获取多维度组合的总量
    """
    return  0