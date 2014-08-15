# coding=utf-8
import datetime
from dbget import *
from decimal import Decimal
from hadoop.hadoopget import *

__author__ = 'GaoJie'


class Flow(object):
    """
    假设每天的每小时所占比例差异不会太大
    """
    def __init__(self, date_time, table):
        self.date_time = date_time
        self.table = table
        self.hourly_list, self.total = self.amend(get_sum_hourly(date_time, table))

    def load_yesterday(self):
        now = datetime.datetime.strptime(self.date_time, '%Y-%m-%d')
        date_time_array = now - datetime.timedelta(days=1)
        hourly_list, total = self.amend(get_sum_hourly(date_time_array.strftime("%Y-%m-%d"), self.table))
        return self.ratio(hourly_list, total), total

    def total_current(self, hour=None, total=None):
        """
        计算当前流量
        """
        if not hour:
            hour = len(self.hourly_list) - 1
            total = self.total
        ratio_list = self.ratio(self.hourly_list, self.total)[:hour]
        if not total:
            total = int(reduce(lambda x, y: x + y, ratio_list) * self.total)
        yes_ratio_list, yes_total = self.load_yesterday()
        return self.fix(yes_ratio_list, yes_total, hour, ratio_list, total)

    def future(self, length=1):
        """
            :param length:
            :return: a list of the total amount in forward days
            """
        time_array = time.strptime(self.date_time, "%Y-%m-%d")
        query_stamp = int(time.mktime(time_array))
        last_week = []
        l = range(1, length + 2)
        l.reverse()
        for i in l:
            time_stamp = query_stamp - 86400 * i
            time_array = time.localtime(time_stamp)
            date = time.strftime("%Y-%m-%d", time_array)
            last_week.append(get_sum(date, {}, self.table, is_train=False))
        #print last_week
        rates = []
        for i in range(1, len(last_week)):
            rates.append((last_week[i] - last_week[i - 1]) / last_week[i - 1])
        #print rates
        ret = []
        for r in rates:
            if len(ret) == 0:
                ret.append(last_week[-1] * (1 + r))
            else:
                ret.append(ret[-1] * (1 + r))
        return ret

    @staticmethod
    def amend(hourly_list):
        # 修正数据
        hourly_list_tmp = []
        i = 0
        total = 0
        for item in hourly_list:
            key = int(item[0])
            value = int(item[1])
            if key > i:
                # 缺失数据，取2个时间节点的中间值
                hourly_list_tmp.append((hourly_list_tmp[i-1] + value) / 2)
                total += value
                i += 1
            hourly_list_tmp.append(value)
            total += value
            i += 1
        return hourly_list_tmp, total

    @staticmethod
    def ratio(hourly_list, total):
        # 计算每小时所占比例
        ratio_list = []
        for value in hourly_list:
            ratio_list.append(Decimal(value) / Decimal(total))
        return ratio_list

    @staticmethod
    def fix(yes_ratio_list, yes_total, hour, current_ratio_list, current_total):
        if not current_total:
            return 0
        change_ratio = (current_total - reduce(lambda x, y: x + y, yes_ratio_list[:hour]) * yes_total) / yes_total
        return int(yes_total * (1 + change_ratio))

    @staticmethod
    def fix2(yes_ratio_list, yes_total, hour, current_ratio_list, current_total):
        if not current_total:
            return 0
        return 0