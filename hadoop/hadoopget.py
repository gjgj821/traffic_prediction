# coding=utf-8
import os

__author__ = 'GaoJie'
# 加载hadoop结果集，计算

DATA_DIR = __package__.replace('.','/')+'/data/piece/'
# merge -------- should map to id
# adx                                   :int,          --U 0
# device_device_type                    :int,          --u 1
# detworkConnection_connection_type     :int,          --U 2
# detworkConnection_carrier_id          :int,          --U 3
# app_category_id                       :int,          --U 4
# device_os                             :chararray,    --U 5
# device_os_version                     :chararray,    --U 5
# location_country_id                   :int,          --U 6
# location_region_id                    :int,          --U 6
# location_city_id                      :int,          --U 6
# device_brand                          :chararray,    --u 7
# device_model                          :chararray,    --u 7
# app_limei_app_id                      :int,          --U 8
DIM_LIST = ['adx', 'DeviceType', 'ConnectionType', 'CarrierName', 'Categorys', 'OS', 'City', 'DeviceModel', 'AppId']
NEED_RELATE = []

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


class HadoopData(object):
    def __init__(self):
        self.term_map = [0,{},{},{},{},{}]
        pass

    def load(self, lines):
        """
        加载数据
        """
        for line in lines:
            line = line.strip().decode('utf-8')
            dim_sum, dim_key, value_key, value_sum = self.parse(line)
            this_map = self.term_map[dim_sum]
            if dim_sum == 0:
                self.term_map[dim_sum] = value_sum
                continue
            if dim_key not in this_map:
                this_map[dim_key] = {}
            this_map[dim_key][value_key] = value_sum

    def get_sum(self, field_map):
        """
        获取定向总数
        """
        dim_sum = len(field_map.keys())
        this_map = self.term_map[dim_sum]
        if dim_sum == 0:
            print this_map
            return this_map
        dim_key, value_key = self.get_key(field_map)
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
        term_key = u''
        print term_map
        for dim, value in term_map.items():
            index = DIM_LIST.index(dim)
            dim_key |= 1 << index
            term_key += value + u'|'
        return dim_key, term_key

    @staticmethod
    def parse(string):
        info = string.split('|')
        dim_info = info[0].split('.')
        dim_sum = len(dim_info) - 1
        return dim_sum, int(dim_info[0]), u'|'.join(dim_info[1:]), int(info[1])


data = HadoopData()
files = os.listdir(DATA_DIR)
for f in files:
    fo = open(DATA_DIR+f, 'r')
    lines = fo.readlines()
    fo.close()
    data.load(lines)