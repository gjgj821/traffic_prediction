# coding=utf-8
import os
from sklearn import linear_model
DATA_DIR = 'data/mapred/'
clf = linear_model.LinearRegression()
files = os.listdir(DATA_DIR)
x = []
y = []
DIM_LIST = ['adx', 'DeviceType', 'ConnectionType', 'OS', 'CarrierName', 'Categorys', 'Citys', 'DeviceModel', 'AppId']
NEED_RELATE = ['DeviceModel']
NEED_PATENT = ['Citys', 'OS', 'DeviceModel']
need_parent_index = []
for item in NEED_PATENT:
    need_parent_index.append(DIM_LIST.index(item))
need_relate_index = []
for item in NEED_RELATE:
    need_relate_index.append(DIM_LIST.index(item))
yy = 0
for f in files:
    fo = open(DATA_DIR + f, 'r')
    lines = fo.readlines()
    fo.close()
    for line in lines:
        yy += 1
        if yy > 1000:
            break
        info = line.split("\t")
        dim_info = info[0].split('.')
        dim_sum = len(dim_info) - 1
        dim_key = int(dim_info[0])
        value_list = dim_info[1:]
        if len(value_list) != dim_sum:
            continue
        item = []
        for index, value in enumerate(DIM_LIST):
            if dim_key & ( 1 << index ):
                t = 0
                for i in xrange(index):
                    if dim_key & ( 1 << i):
                        t += 1
                item.append(float(value_list[t]))
            else:
                item.append(0)
        x.append(item)
        y.append(int(info[1]))
clf.fit (x, y) #拟合
print clf.coef_ #获取拟合参数