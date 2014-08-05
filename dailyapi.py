#!/usr/bin/python
# -*- coding:utf-8 -*-

__author__ = 'wangwei'
import time
from core.dbget import *

def get_daily(days,queryDate):
	"""
	:param days:
	:param queryDate: format in "%Y-%m-%d"
	:return: a list of the total amount in forward days
	"""
	timeArray = time.strptime(queryDate, "%Y-%m-%d")
	queryStamp = int(time.mktime(timeArray))
	lastWeek=[]
	l=range(1,days+2)
	l.reverse()
	for i in l:
		timeStamp = queryStamp-86400*i
		timeArray = time.localtime(timeStamp)
		date = time.strftime("%Y-%m-%d", timeArray)
		lastWeek.append(get_sum(date, {}, "RTBApp", is_train=False))
	print lastWeek
	rates=[]
	for i in range(1,len(lastWeek)):
		rates.append((lastWeek[i]-lastWeek[i-1])/lastWeek[i-1])
	print rates
	ret=[]
	for r in rates:
		if len(ret) == 0:
			ret.append(lastWeek[-1]*(1+r))
		else:
			ret.append(ret[-1]*(1+r))
	return ret

if __name__ == '__main__':
    ret=get_daily(7,"2014-6-20")
    print ret