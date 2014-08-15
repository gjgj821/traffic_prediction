import time
import matplotlib.pyplot as plt
__author__ = 'wangwei'

# {adx:[[timeStamp],[deal_rate],[minp],[dealp],[dealp-dealp]]}
data = {}

# (3,2014-08-04 10)|0.009637561|215509|1112|1045
def parseRecord(line):
	arr = line.split('|')
	adx_time = arr[0].strip('(').strip(')').split(',')
	adx = 0 if adx_time[0]=='' else int(adx_time[0])
	datehour = 0 if adx_time[1]=='' else adx_time[1]
	timeArray = time.strptime(datehour, "%Y-%m-%d %H")
	timeStamp = int(time.mktime(timeArray))
	deal_rate = 0 if arr[1]=='' else float(arr[1])
	minp = 0 if arr[2]=='' else long(arr[2])
	dealp = 0 if arr[3]=='' else long(arr[3])
	deald = 0 if arr[4]=='' else long(arr[4])
	if data.get(adx)==None:
		data[adx] = [[timeStamp],[deal_rate],[minp],[dealp],[deald]]
	else:
		data[adx][0].append(timeStamp)
		data[adx][1].append(deal_rate)
		data[adx][2].append(minp)
		data[adx][3].append(dealp)
		data[adx][4].append(deald)

def readData():
	din=open('../../data/priceSummary/total.txt','r')
	for line in din:
		parseRecord(line.strip('\n'))

def uniform():
	for k in data:
		for l in data[k]:
			m=max(l)
			print m
			if m==0:
				continue
			for i in l:
				i = i/float(m)

readData()
#uniform()

def visualData(adx):
	plt.subplot('411')
	plt.plot(data[adx][0], data[adx][4], 'b.')
	plt.grid()
	plt.title('deal price - min cpm/1000 adx=%s'%(adx))
	plt.subplot('412')
	plt.plot(data[adx][0], data[adx][1], 'b.')
	plt.grid()
	plt.title('deal rate adx=%s'%(adx))
	plt.subplot('413')
	plt.plot(data[adx][0], data[adx][2], 'b.')
	plt.grid()
	plt.title('min cpm adx=%s'%(adx))
	plt.subplot('414')
	plt.plot(data[adx][0], data[adx][3], 'b.')
	plt.grid()
	plt.title('deal price adx=%s'%(adx))
	plt.show()

visualData(3)
