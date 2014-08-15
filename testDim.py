#!/usr/bin/python
# coding=utf-8

from decimal import Decimal
from pprint import pprint

import chronic
import matplotlib.pyplot as plt

from core.dbget import *
from core.term_test import TermTestManage
from core.term_combiner import *


__author__ = 'GaoJie'
TERM_FILE = 'data/term_list.txt'
DIM_FILE = 'data/dim_list.txt'
DB_NAME = 'RTBApp'

if __name__ == '__main__':
    test_time = '2014-06-10'
    table_name = 'RTBApp'
    # 实际中
    total = get_sum(test_time, {}, DB_NAME, is_train=False)
    manage = TermTestManage(total)

    # 加载基础维度
    fo = open(DIM_FILE, 'r')
    lines = fo.readlines()
    manage.load_dim(lines)
    fo.close()

    # 加载修正组合
    fo = open(TERM_FILE, 'r')
    lines = fo.readlines()
    manage.load(lines)
    fo.close()

    result = []
    result_term = []

    load_dim()
    #dim_dict_combine = dim_combine()
    dim_dict_combine = test_combine()
    '''
    for i in range(0, 200):
        # term_map = get_termmap(dim_dict_combine)
        #term_map = get_termmap_single_dim('Categorys', dim_dict_combine)
        #term_map = get_termmap_dim_single()
        result_term.append(term_map)
        result.append(
            manage.estimate_test2(term_map=term_map, real=get_sum(test_time, term_map, test_table, is_train=False),
                                  p=False))
        # result.append(manage.estimate_test(term_map=term_map, real=get_sum(test_time, term_map, test_table, is_train=False), p=False))
    '''

    for i in range(0, 200):
        with chronic.Timer('test1'):
            term_map=get_termmap_x(dim_dict_combine,2)
            result_term.append(term_map)
            result.append(
                manage.estimate_test2(term_map=term_map, real=get_sum(test_time, term_map, DB_NAME, is_train=False),
                                      p=False))

    for i in range(0, 100):
        with chronic.Timer('test2'):
            term_map=get_termmap_x(dim_dict_combine,3)
            result_term.append(term_map)
            result.append(
                manage.estimate_test2(term_map=term_map, real=get_sum(test_time, term_map, DB_NAME, is_train=False),
                                      p=False))

    for i in range(0, 50):
        with chronic.Timer('test3'):
            term_map=get_termmap_x(dim_dict_combine,4)
            result_term.append(term_map)
            result.append(
                manage.estimate_test2(term_map=term_map, real=get_sum(test_time, term_map, DB_NAME, is_train=False),
                                      p=False))

    # x_list, y1_list = zip(*result)
    x_list, y1_list, y2_list = zip(*result)
    # 分档
    # 100,000,000-100
    for LBound in [10 ** x for x in range(1, 12)]:
        list = [[i, x] for i, x in enumerate(x_list) if x >= LBound and x < LBound * 10]
        sum_rate = 0
        # print list
        for item in list:
            rate = (y2_list[item[0]] - item[1]) / Decimal(item[1])
            #if abs(rate) > 0.4:
            #	print result_term[item[0]]
            #	print result[item[0]]
            sum_rate += rate
        if len(list) > 0:
            diff = sum_rate / len(list)
            print "[%s,%s) avg_diff: %s" % (LBound, LBound * 10, diff)

    plt.plot(x_list, y1_list, 'og')
    plt.plot(x_list, y2_list, 'ob')

    plt.plot([0, total / 2], [0, total / 2], 'r')
    # plt.plot([0, 1], [0, 1], 'r')
    plt.grid(True)
    # 修正最大值
    #max_value = round(max(max(x_list), max(y1_list), max(y2_list)), 2)
    #plt.xlim(0.0, max_value)
    #plt.ylim(0.0, max_value)
    plt.show()
    pprint(chronic.timings)

