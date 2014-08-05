#!/usr/bin/python
# coding=utf-8
from pprint import pprint
import chronic
from core.dbget import *
from core.term_test import TermTestManage
from core.flow import Flow
from term_combiner import get_termmap, load_dim, dim_combine

__author__ = 'GaoJie'
TERM_FILE = 'term_list.txt'
DIM_FILE = 'dim_list.txt'


if __name__ == '__main__':
	test_time = '2014-06-02'
	test_table = 'RTBApp'
	with chronic.Timer('create'):
		manage = TermTestManage()

		# 加载修正组合
		fo = open(TERM_FILE, 'r')
		lines = fo.readlines()
		manage.load(lines)
		fo.close()

		# 加载基础维度
		fo = open(DIM_FILE, 'r')
		lines = fo.readlines()
		manage.load_dim(lines)
		fo.close()
		f = Flow(test_time, test_table)
		#load_dim()
		#dim_dict_combine = dim_combine()
		#term_map = get_termmap(dim_dict_combine)

	with chronic.Timer('test'):
		value = f.total_current(7)
		manage.estimate(term_map={'DeviceType': ['Phone']}, total=value)

	# 计算单次执行时间
	pprint(chronic.timings)