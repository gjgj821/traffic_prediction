# coding=utf-8
import os
from core.relative import Relative,  Parent

__author__ = 'GaoJie'
# 加载hadoop结果集，计算

DATA_DIR = 'data/mapred/'
# merge -------- should map to id
# adx                                   :int,          --U 0
# device_device_type                    :int,          --u 1
# detworkConnection_connection_type     :int,          --U 2
# device_os                             :int,          --U 3
# device_os_version                     :int,          --U 3
# detworkConnection_carrier_id          :int,          --U 4
# app_category_id                       :int,          --U 5
# location_geo_criteria_id              :int,          --U 6
# device_brand                          :chararray,    --u 7
# device_model                          :chararray,    --u 7
# app_limei_app_id                      :int,          --U 8
DIM_LIST = ['adx', 'DeviceType', 'ConnectionType', 'OS', 'CarrierName', 'Categorys', 'Citys', 'DeviceModel', 'AppId']
NEED_RELATE = ['DeviceModel']
NEED_PATENT = ['Citys', 'OS', 'DeviceModel']

data = None

def get_sum(date_time, field_map, table, sum_field='Requests', date_field='Datetime', is_train=True, where='1'):
    """
    获取多维度组合的总量，代理接口，与dbget同步
    """
    return  data.get_sum(field_map)


def get_group(date_time, field, table, sum_field='Requests', date_field='Datetime', is_train=True, where='1'):
    """
    获取多维度组合的分组信息，代理接口，与dbget同步
    """
    return data.get_group(field)


class HadoopData:
    def __init__(self):
        self.term_map = [0,{},{},{},{},{}]
        self.need_parent_index = []
        for item in NEED_PATENT:
            self.need_parent_index.append(DIM_LIST.index(item))
        self.need_relate_index = []
        for item in NEED_RELATE:
            self.need_relate_index.append(DIM_LIST.index(item))
        pass

    def load(self, lines):
        """
        加载数据
        """
        for line in lines:
            line = line.strip().decode('utf-8')
            dim_sum, dim_key, value_list, value_sum = self.parse(line)
            this_map = self.term_map[dim_sum]
            if dim_sum == 0:
                self.term_map[dim_sum] = value_sum
                continue
            if dim_key not in this_map:
                this_map[dim_key] = {}
            this_dim = this_map[dim_key]
            this_dim[u'.'.join(value_list)] = value_sum
            # 添加父级统计
            for key, index in enumerate(self.need_parent_index):
                if dim_key & ( 1 << index ):
                    t = 0
                    for i in xrange(index):
                        if dim_key & ( 1 << i):
                            t += 1
                    # print t
                    # print value_list
                    # 获取所有的父级value
                    parent = Parent.mapping_value(DIM_LIST[index], value_list[t])
                    if not parent:
                        continue
                    parent_value_list = value_list
                    for p in parent:
                        parent_value_list[t] = p
                        parent_value_key = '.'.join(parent_value_list)
                        if parent_value_key not in this_dim:
                            this_dim[parent_value_key] = 0
                        this_dim[parent_value_key] += value_sum

    def get_sum(self, field_map):
        """
        获取定向总数
        """
        dim_sum = len(field_map.keys())
        this_map = self.term_map[dim_sum]
        if dim_sum == 0:
            #print this_map
            return this_map
        dim_key, value_list = self.get_key(field_map)
        value_key = u'.'.join(value_list)
        #print field_map
        #print dim_key, value_key

        if dim_key not in this_map:
            return 0
        if value_key not in this_map[dim_key]:
            return 0
        return this_map[dim_key][value_key]

    def get_group(self, dim):
        index = DIM_LIST.index(dim)
        dim_key = 0 | 1 << index
        this_map = self.term_map[1]
        if dim_key not in this_map:
            return []
        group_list = []
        for key, value in this_map[dim_key].items():
            group_list.append((key,value))
        return group_list

    @staticmethod
    def get_key(term_map):
        dim_key = 0
        value_list = []
        #print term_map

        for dim, value in term_map.items():
            index = DIM_LIST.index(dim)
            dim_key |= 1 << index
            value_list.append(value)
        return dim_key, value_list

    def parse(self, string):
        info = string.split("\t")
        dim_info = info[0].split('.')
        dim_sum = len(dim_info) - 1
        dim_key = int(dim_info[0])
        value_list = dim_info[1:]
        for key, index in enumerate(self.need_relate_index):
            if dim_key & ( 1 << index ):
                t = 0
                for i in xrange(index):
                    if dim_key & ( 1 << i):
                        t += 1
                value_list[t] = str(Relative.mapping_value(DIM_LIST[index], value_list[t]))
        return dim_sum, dim_key, value_list, int(info[1])

    def reload(self):
        self.term_map = [0, {}, {}, {}, {}, {}]
        files = os.listdir(DATA_DIR)
        for f in files:
            fo = open(DATA_DIR + f, 'r')
            lines = fo.readlines()
            fo.close()
            self.load(lines)

def init_hapood():
    data = HadoopData()
    data.reload()
