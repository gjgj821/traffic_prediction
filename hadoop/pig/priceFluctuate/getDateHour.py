import time

__author__ = 'wangwei'

@outputSchema("datehour:chararray")
def getDateHour(timeStamp):
	timeArray = time.localtime(timeStamp)
	otherStyleTime = time.strftime("%Y-%m-%d %H", timeArray)
	return otherStyleTime