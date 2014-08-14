#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
from org.apache.pig.scripting import *
__author__ = 'wangwei'

def getParams():
	params=[]
	cdate=startDate
	while cmp(cdate, endDate) != 0:
		d={"DATE":cdate}
		params.append(d)
		timeArray = time.strptime(cdate, "%Y-%m-%d")
		timeStamp = int(time.mktime(timeArray))
		timeStamp += 86400
		timeArray = time.localtime(timeStamp)
		cdate = time.strftime("%Y-%m-%d", timeArray)
	return params


if __name__ == '__main__':
	startDate = '2014-08-01'
	endDate = '2014-08-02'  #not include in
	Pig.registerUDF("./getDateHour.py", "timeUDFS")
	pig = Pig.compileFromFile('./priceFluctuate.pig')
	bound = pig.bind(getParams())
	bound.run()