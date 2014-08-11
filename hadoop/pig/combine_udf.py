#!/usr/bin/python

__author__ = 'wangwei'

# def combine_udf(a0,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12):
@outputSchema("combination:bag{t:tuple(comb:chararray)}")
def combine_udf(*args):
	count = 0
	key_dict = {}
	for i,item in enumerate(args):
		if item != None:
			key_dict[i]=item
			count+=1
	outBag = []
	tup=('0',)
	outBag.append(tup)
	for i in range(1,1 << count):
		tmp=i
		dim=0
		keys=''
		for k in key_dict:
			t = tmp & 1
			tmp = tmp >> 1
			if t == 1:
				dim |= 1 << k
				keys += '.%s'%(key_dict[k])
		if dim != 0:
			tup = ('%s%s'%(dim, keys),)
			outBag.append(tup)
	return outBag

# adx                                   :int,          --U 0
# device_os                             :chararray,    --U 1
# device_os_version                     :chararray,    --U 2
# device_brand                          :chararray,    --u 3
# device_model                          :chararray,    --u 4
# device_device_type                    :int,          --u 5
# detworkConnection_connection_type     :int,          --U 6
# detworkConnection_carrier_id          :int,          --U 7
# location_country_id                   :int,          --U 8
# location_region_id                    :int,          --U 9
# location_city_id                      :int,          --U 10
# app_category_id                       :int,          --U 11
# app_limei_app_id                      :int,          --U 12