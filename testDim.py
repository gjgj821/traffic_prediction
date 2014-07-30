#!/usr/bin/python
# coding=utf-8

import time
from decimal import Decimal
from core.dbget import *
from core.term_test import TermTestManage
import matplotlib.pyplot as plt
import pylab as pl
from term_combiner import *

__author__ = 'GaoJie'
TERM_FILE = 'term_list.txt'
DIM_FILE = 'dim_list'

if __name__ == '__main__':
	test_time = '2014-06-01'
	test_table = 'RTBLocation'
	# 实际中
	total = get_sum(test_time, {}, test_table, is_train=False)
	manage = TermTestManage(total)

	# 加载修正组合
	fo = open(TERM_FILE, 'r')
	lines = fo.readlines()
	manage.load(lines)
	fo.close()

	# 加载基础维度
	filename = DIM_FILE + '_' + test_table + '.txt'
	fo = open(filename, 'r')
	lines = fo.readlines()
	manage.load_dim(lines)
	fo.close()

	result = []

	'''
	term_map = {'OS': ['iOS'], 'DeviceType': ['Pad', 'Phone'], 'Categorys': [u'娱乐', u'音乐']}
	result.append(manage.estimate_test(term_map=term_map, real=get_sum(test_time, term_map, test_table, is_train=False), p=False))
	term_map = {'OS': ['iOS'], 'DeviceType': ['Pad'], 'Categorys': [u'娱乐', u'图书']}
	result.append(manage.estimate_test(term_map=term_map, real=get_sum(test_time, term_map, test_table, is_train=False), p=False))
	term_map = {'OS': ['Android'], 'DeviceType': ['Phone'], 'Categorys': [u'图书', u'音乐']}
	result.append(manage.estimate_test(term_map=term_map, real=get_sum(test_time, term_map, test_table, is_train=False), p=False))
	term_map = {'DeviceType': ['Phone'], 'Categorys': [u'娱乐']}
	result.append(manage.estimate_test(term_map=term_map, real=get_sum(test_time, term_map, test_table, is_train=False), p=False))
	term_map = {'DeviceType': ['Phone']}
	result.append(manage.estimate_test(term_map=term_map, real=get_sum(test_time, term_map, test_table, is_train=False), p=False))
	'''

	load_dim()
	dim_dict_combine = dim_combine()
	for i in range(0, 200):
		# term_map = get_termmap(dim_dict_combine)
		#term_map = get_termmap_single_dim('City',dim_dict_combine)
		term_map = get_termmap_dim_single(dim_dict_combine)
		result.append(
			manage.estimate_test2(term_map=term_map, real=get_sum(test_time, term_map, test_table, is_train=False),
			                      p=False))
	# result.append(manage.estimate_test(term_map=term_map, real=get_sum(test_time, term_map, test_table, is_train=False), p=False))

	# x_list, y1_list = zip(*result)
	x_list, y1_list, y2_list = zip(*result)
	# 分档
	# 100,000,000-100
	for LBound in [10 ** x for x in range(1, 12)]:
		list = [[i, x] for i, x in enumerate(x_list) if x >= LBound and x < LBound * 10]
		sum_rate = 0
		# print list
		for item in list:
			sum_rate += (y2_list[item[0]] - item[1]) / Decimal(item[1])
		if sum_rate > 0:
			diff = sum_rate / len(list)
			print "[%s,%s) avg_diff: %s" % (LBound, LBound * 10, diff)

	plt.plot(x_list, y1_list, 'og')
	plt.plot(x_list, y2_list, 'ob')

	plt.plot([0, total / 2], [0, total / 2], 'r')
	#plt.plot([0, 1], [0, 1], 'r')
	plt.grid(True)
	# 修正最大值
	#max_value = round(max(max(x_list), max(y1_list), max(y2_list)), 2)
	#plt.xlim(0.0, max_value)
	#plt.ylim(0.0, max_value)
	plt.show()