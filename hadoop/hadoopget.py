# coding=utf-8
__author__ = 'GaoJie'
# 加载hadoop结果集，计算

DATA_FILE = 'file.txt'
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
DIM_LIST = ['adx', 'DeviceType', 'ConnectionType', 'Carrier', 'Categorys', 'OS', 'City', 'DeviceModel', 'AppId']

def get_sum(date_time, field_map, table, sum_field='Requests', date_field='Datetime', is_train=True, where='1'):
    """
    获取多维度组合的总量，代理接口，与dbget同步
    """

    return  data.get_sum(field_map)


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
                this_map = value_sum
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
            return this_map
        dim_key, value_key = self.get_key(field_map)
        if dim_key not in this_map:
            return 0
        if value_key not in this_map[dim_key]:
            return 0
        return this_map[dim_key][value_key]

    @staticmethod
    def get_key(term_map):
        dim_key = 0
        term_key = u''
        for dim, value in term_map.items():
            index = DIM_LIST.index(dim)
            dim_key |= 1 << index
            term_key += value + u'|'
        return dim_key, term_key

    @staticmethod
    def parse(string):
        info = string.split('|')
        dim_info = info[0].splite(',')
        if len(dim_info) == 1:
            dim_sum = 0
        else:
            dim_sum = 0
        return dim_sum, dim_info[0], u'|'.join(dim_info[1:]), info[1]


data = HadoopData()
filename = DATA_FILE
fo = open(filename, 'r')
lines = fo.readlines()
fo.close()
data.load(lines)