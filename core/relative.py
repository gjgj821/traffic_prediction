# coding=utf-8
from dbget import get_relate, get_parent

__author__ = 'GaoJie'

dim_relative_map = {}

class Relative(object):
    @staticmethod
    def get_os():
        return get_relate('osName', 'b_base_os', where='osVersion="0"')

    @staticmethod
    def get_categorys():
        return get_relate('zhName', 'b_base_media_category', where='status=1')

    @staticmethod
    def get_devicetype():
        return {'Phone': 1, 'Pad': 2}

    @staticmethod
    def get_appname():
        return get_relate('zhName', 'b_base_media', where='status=1')

    @staticmethod
    def get_carriername():
        return {'China Mobile': 43, 'China Telecom': 45, 'UNICOM': 47}

    @staticmethod
    def get_devicemodel():
        ## todo
        return get_relate('modelName', 'b_base_device')

    @staticmethod
    def get_province():
        return get_relate('zhName', 'b_base_geo', where='level=1')

    @staticmethod
    def get_city():
        return get_relate('zhName', 'b_base_geo', where='level=2 or firstcity = 1')

    @staticmethod
    def get_citys():
        """
        获取所有层级
        """
        return get_relate('zhName', 'b_base_geo')

    @classmethod
    def mapping_value(cls, dim, value):
        global dim_relative_map
        if dim not in dim_relative_map:
            relative = getattr(cls, 'get_%s' % dim.lower())
            relative_map = relative()
            dim_relative_map[dim] = relative_map
        else:
            relative_map = dim_relative_map[dim]
        return relative_map[value] if value in relative_map else value

dim_father_map = {}

class Parent(object):
    @classmethod
    def get_citys(cls):
        map_dict = get_parent('id', 'b_base_geo')
        return cls._filter(map_dict)

    @classmethod
    def get_carriername(cls):
        map_dict1 = get_parent('id', 'b_base_operator', parent_field='categoryId')
        map_dict2 = get_parent('id', 'b_base_operator_category', parent_field='parentId')
        return cls._filter(map_dict1, map_dict2)

    @classmethod
    def get_devicemodel(cls):
        map_dict = get_parent('id', 'b_base_device')
        return cls._filter(map_dict)

    @classmethod
    def get_os(cls):
        map_dict = get_parent('id', 'b_base_os')
        return cls._filter(map_dict)

    @staticmethod
    def _filter(*args):
        """
        解析子类所有的上层父类
        """
        map_dict = dict(*args)
        all_list = {}
        for key, value in map_dict.items():
            if value is 0:
                continue
            key = str(key)
            if key not in all_list:
                all_list[key] = []
            parent = value
            while parent and parent in map_dict:
                all_list[key].append(str(parent))
                parent = map_dict[parent]

        return all_list

    @classmethod
    def mapping_value(cls, dim, value):
        global dim_father_map
        if dim not in dim_father_map:
            relative = getattr(cls, 'get_%s' % dim.lower())
            father_map = relative()
            dim_father_map[dim] = father_map
        else:
            father_map = dim_father_map[dim]
        return father_map[value] if value in father_map else []