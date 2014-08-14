#!/usr/bin/python
# coding=utf-8
import itertools
from core.term import TermManage
from hadoop.hadoopget import DIM_LIST

__author__ = 'GaoJie'

TERM_FILE = 'term_list.txt'
DIM_FILE = 'dim_list'

LEVEL_DIM = [
    ['AppId', 'DeviceModel', 'City'],
    ['OS', 'Categorys', 'CarrierName'],
    ['adx', 'DeviceType', 'ConnectionType']
]

if __name__ == '__main__':
    train_time = '2014-06-10'
    term_map_all = {}

    m1 = TermManage('RTBApp', train_time)
    for dim in DIM_LIST:
        m1.add_dim(dim)
    for dim in LEVEL_DIM[0]:
        for other_dim in LEVEL_DIM[1] + LEVEL_DIM[2]:
            term_map_list = m1.union_dim(2, [dim, other_dim])
            term_map_all = dict(term_map_all, **term_map_list)
    comb_list = []
    for x in range(2, len(LEVEL_DIM[2]) + 1):
        comb_list += itertools.combinations(LEVEL_DIM[2], x)
    for dim in LEVEL_DIM[1]:
        for second_dim in LEVEL_DIM[1] + LEVEL_DIM[2]:
            if dim == second_dim:
                continue
            term_map_list = m1.union_dim(2, [dim, second_dim])
            term_map_all = dict(term_map_all, **term_map_list)

        for comb in comb_list:
            for second_dim in LEVEL_DIM[1]:
                dim_list_tmp = [dim, second_dim] + list(comb)
                term_map_list = m1.union_dim(len(dim_list_tmp), dim_list_tmp)
                term_map_all = dict(term_map_all, **term_map_list)
            dim_list_tmp = [dim] + list(comb)
            term_map_list = m1.union_dim(len(dim_list_tmp), dim_list_tmp)
            term_map_all = dict(term_map_all, **term_map_list)
    for comb in comb_list:
        term_map_list = m1.union_dim(len(comb), comb)
        term_map_all = dict(term_map_all, **term_map_list)

    type_list = [value.get_line(relative=False) for key, value in term_map_all.items()]
    output = open(TERM_FILE, 'w')
    output.writelines(type_list)
    output.close()

    filename = DIM_FILE + '.txt'
    output = open(filename, 'w')
    output.writelines(m1.export_dim(DIM_LIST, relative=False))
    output.close()