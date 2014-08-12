#!/usr/bin/python

__author__ = 'wangwei'

# +----+----------+---------+-----------+
# | id | parentId | osName  | osVersion |
# +----+----------+---------+-----------+
# |  2 |        0 | iOS     | 0         |
# | 11 |        2 | iOS     | 4.x       |
# | 12 |        2 | iOS     | 5.x       |
# | 13 |        2 | iOS     | 6.x       |
# |  1 |        0 | Android | 0         |
# | 31 |        1 | Android | 1.x       |
# | 32 |        1 | Android | 2.x       |
# | 33 |        1 | Android | 3.x       |
# | 34 |        1 | Android | 4.x       |
# | 14 |        2 | iOS     | 7.x       |
# +----+----------+---------+-----------+
os_dict = {'24':11, '25':12, '26':13, '27':14, '11':31, '12':32, '13':33, '14':34}

class CrossCombine(object):
	def __init__(self):
		self.res = ['']
		self.res_chain = [0]
		self.count = [0]
	def cross(self, L, Limit, level):
		"""
		:param L: this dim list
		:param Limit: allowed combined dims before combine this dim
		:return: /
		"""
		#assert len(L) < len(Limit)
		res_len = len(self.res)
		L_len = len(L)
		for i in range(0, res_len):
			for j in range(0, L_len):
				if( self.count[i] <= Limit[j] ):
					self.res.append(self.res[i]+'.%s'%(L[j]))
					self.res_chain.append(self.res_chain[i] | 1 << level)
					self.count.append(self.count[i]+1)

Limits=[[99],[99,99],[99,1],[99],[99],[99],[99,99,1],[99],[99]]

# def combine_udf(a0,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12):
def combine_merged_udf(*args):
	la=[]
	#adx
	la.append([args[0]])
	#device_os device_os_version
	l=[]
	if args[1] != None:
		l.append(args[1])
		if args[2] != None:
			k = args[1]+args[2][0]
			if os_dict.get(k)!=None:
				l.append(os_dict[k])
	la.append(l)
	#device_brand device_model
	l=[]
	if args[3] != None:
		l.append(args[3])
		if args[4] != None:
			l.append(args[4])
	la.append(l)
	# device_device_type
	la.append([args[5]])
    # detworkConnection_connection_type
	la.append([args[6]])
    # detworkConnection_carrier_id
	la.append([args[7]])
    # location_country_id location_region_id location_city_id
	l=[]
	if args[8] != None:
		l.append(args[8])
		if args[9] != None:
			l.append(args[9])
			if args[10] != None:
				l.append(args[10])
	la.append(l)
    # app_category_id
	la.append([args[11]])
    # app_limei_app_id
	la.append([args[12]])
	#----------------------------
	c = CrossCombine()
	for i in range(0,len(la)):
		#print la[i],Limits[i]
		c.cross(la[i],Limits[i],i)
	outBag = []
	for i in range(0,len(c.res)):
		tup=('%s%s'%(c.res_chain[i],c.res[i]),)
		outBag.append(tup)
	return outBag

# merge -------- should map to id
# adx                                   :int,          --U 0    0
# device_os                             :chararray,    --U 1    1
# device_os_version                     :chararray,    --U 2    1
# device_brand                          :chararray,    --u 3    2
# device_model                          :chararray,    --u 4    2
# device_device_type                    :int,          --u 5    3
# detworkConnection_connection_type     :int,          --U 6    4
# detworkConnection_carrier_id          :int,          --U 7    5
# location_country_id                   :int,          --U 8    6
# location_region_id                    :int,          --U 9    6
# location_city_id                      :int,          --U 10   6
# app_category_id                       :int,          --U 11   7
# app_limei_app_id                      :int,          --U 12   8

r = combine_merged_udf('0','1','2','3','4','5','6','7','8','9','10','11','12')
for i in r:
	print i
print 'length:',len(r)