#!/usr/bin/python
# coding=utf-8
import datetime
from core.flow import Flow
import matplotlib.pyplot as plt

__author__ = 'GaoJie'
DB_NAME = 'RTBApp'

if __name__ == '__main__':
    train_time = '2014-06-01'
    now = datetime.datetime.strptime(train_time, '%Y-%m-%d')
    x_list = []
    y1_list = []
    y2_map = {}
    for i in range(50):
        date_time_array = now - datetime.timedelta(days=-1 * i)
        current_date = date_time_array.strftime("%Y-%m-%d")
        f = Flow(current_date, DB_NAME)
        x_list.append(current_date)
        y1_list.append(f.total)
        i = 0
        for y in range(6, 18):
            y += 1
            i += 1
            value = f.total_current(y)
            if y not in y2_map:
                y2_map[y] = []
            y2_map[y].append(value)

    i = 1
    for key, value in y2_map.items():
        ax = plt.subplot((len(y2_map.keys())/2)+1, 2, i)
        i += 1
        plt.sca(ax)
        plt.plot(range(50), y1_list, 'r')
        plt.plot(range(50), value, 'o')

    plt.show()