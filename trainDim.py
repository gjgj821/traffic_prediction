#!/usr/bin/python
# coding=utf-8
from core.term import TermManage

__author__ = 'GaoJie'

# 最大维度，最多几个定向条件的组合 #
MAX_DIM = 3
TERM_FILE = 'term_list.txt'
DIM_FILE = 'dim_list'

if __name__ == '__main__':
	train_time = '2014-06-10'
	term_map_all = {}

	m1 = TermManage('RTBApp', train_time)
	m1.add_dim('OS').add_dim('Categorys').add_dim('DeviceType').add_dim('AppName')
	term_map_list = m1.union_dim(2, ['OS', 'DeviceType', 'Categorys', 'AppName'])
	term_map_all = dict(term_map_all, **term_map_list)
	term_map_list = m1.union_dim(3, ['OS', 'DeviceType', 'Categorys'])
	term_map_all = dict(term_map_all, **term_map_list)

	# 只读取日表信息
	m2 = TermManage('RTBLocation', train_time, where='`Type` = 1')
	m2.add_dim('OS').add_dim('DeviceType').add_dim('CarrierName').add_dim('City')
	# 不再进行OS和DeviceType的聚合查询
	term_map_list = m2.union_dim(2, ['OS', 'CarrierName', 'City'])
	term_map_all = dict(term_map_all, **term_map_list)
	term_map_list = m2.union_dim(2, ['DeviceType', 'CarrierName', 'City'])
	term_map_all = dict(term_map_all, **term_map_list)
	term_map_list = m2.union_dim(3, ['OS', 'DeviceType', 'CarrierName'])
	term_map_all = dict(term_map_all, **term_map_list)

	type_list = [value.get_line(relative=False) for key, value in term_map_all.items()]
	output = open(TERM_FILE, 'w')
	output.writelines(type_list)
	output.close()

	filename = DIM_FILE + '_RTBApp' + '.txt'
	output = open(filename, 'w')
	output.writelines(m1.export_dim(['OS', 'DeviceType', 'Categorys', 'AppName'], relative=False))
	output.close()
	filename = DIM_FILE + '_RTBLocation' + '.txt'
	output = open(filename, 'w')
	output.writelines(m2.export_dim(['OS', 'DeviceType', 'CarrierName', 'City'], relative=False))
	output.close()