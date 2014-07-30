#!/usr/bin/python
#encoding: utf-8
__author__ = 'WangWei'
import os
import time
import math
import random

# single dim list
table_name = 'RTBLocation'
dim_list_name = './dim_list_' + table_name + '.txt'
# dim_list_name = './dim_list_'+table_name+'_noappname.txt'

dim_dict = {}

#{key:[[...],[...],...]}
#dim_dict_combine = {}


def load_dim():
	global dim_dict
	if not os.path.exists(dim_list_name):
		return False
	fo = open(dim_list_name, 'r')
	for line in fo:
		line = line.decode('utf-8')
		#print line
		line.strip()
		if line[:2] == '##':
			d = line[2:].strip('\n')
			dim_dict[d] = []
		#parse
		else:
			list = line.split(':')
			items = list[1].split('|')
			for i in items:
				if i == '':
					continue
				kv = i.split('=')
				key = kv[0]
				value = kv[1]
				#print key,value
				#if not key in dim_dict:
				#	dim_dict[key] = []
				dim_dict[key].append(value)
	return True

max_combinations = 10
max_possible = 1


def dim_combine():
	global max_possible
	#{key:[[...],[...],...]}
	dim_dict_combine = {}
	#random.seed(int(time.time()))
	for key in dim_dict:
		size = len(dim_dict[key])
		#print 'key:%s size:%s'%(key,size)
		limit=min(size,max_combinations)
		max_possible=max_possible*limit

		rset=[0]
		for i in range(0,limit):
			t_com = []
			r = 0

			while not rset.count(r) == 0:
				r = random.randint(1,(1 << size)-1)
			rset.append(r)
			#print 'randint:%s'%(r)
			for shift in range(0, size):
				b = (r >> shift) & 1
				if b == 1:
					t_com.append(dim_dict[key][shift])
			#for value in t_com:
			#	print value,'|',
			#print ''
			if not key in dim_dict_combine:
				dim_dict_combine[key] = []
			dim_dict_combine[key].append(t_com)

	return dim_dict_combine


def get_termmap(dim_dict_combine):
	tm={}
	for key in dim_dict_combine:
		r=random.randint(0,len(dim_dict_combine[key])-1)
		tm[key]=dim_dict_combine[key][r]
	return tm

#load_dim()
#print dim_dict
#dim_combine()
