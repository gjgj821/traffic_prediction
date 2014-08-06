# coding=utf-8
import time

__author__ = 'GaoJie'

from mysql.connector.connection import MySQLConnection
from mysql.connector.conversion import MySQLConverter

# 日期的统计周期
STAT_CYCLE = 0

STAT_DB_HOST = '192.168.168.144'
STAT_DB_PORT = '3306'
STAT_DB_USER = 'root'
STAT_DB_PASS = 'gaojie'
STAT_DB_NAME = 'test'

SOURCE_DB_HOST = 'dsp.corp.limei.com'
SOURCE_DB_PORT = '3306'
SOURCE_DB_USER = 'kaiden'
SOURCE_DB_PASS = 'qwe123'
SOURCE_DB_NAME = 'bunny'

mysqlConnectStat = MySQLConnection(host=STAT_DB_HOST, user=STAT_DB_USER, passwd=STAT_DB_PASS, db=STAT_DB_NAME,
                                   port=STAT_DB_PORT)
mysqlConnectSource = MySQLConnection(host=SOURCE_DB_HOST, user=SOURCE_DB_USER, passwd=SOURCE_DB_PASS, db=SOURCE_DB_NAME,
                                     port=SOURCE_DB_PORT)
mysqlConverterUTF8 = MySQLConverter(charset='utf8')


def get_sum(date_time, field_map, table, sum_field='Requests', date_field='Datetime', is_train=True, where='1'):
    """
    获取多维度组合的总量
    """
    cursor = mysqlConnectStat.cursor()
    select = 'sum(%s)' % sum_field
    time_stamp = int(time.mktime(time.strptime(date_time, '%Y-%m-%d'))) - (STAT_CYCLE * 86400 if is_train else 0)
    if not where:
        where = '1'
    where += ' and %s between %s and %s' % (date_field, time_stamp, time_stamp + 86400 - 1)
    if field_map:
        for key, value in field_map.items():
            reverse = False
            if key[:1] == '!':
                key = key[1:]
                reverse = True
            if type(value) is list:
                op = ' not in ' if reverse else ' in '
                where += " and %s %s ('%s')" % (
                key, op, "','".join([mysqlConverterUTF8.escape(v if v != 'other' else '') for v in value]))
            else:
                op = ' != ' if reverse else ' = '
                where += " and %s %s '%s'" % (key, op, mysqlConverterUTF8.escape(value))
    sql = 'select %s from %s where %s ' % (select, table, where)
    # print sql
    cursor.execute(sql)
    result = cursor.fetchone()
    cursor.close()
    sum_value = result[0] if result else 0
    return sum_value if sum_value else 0


def get_group(date_time, field, table, sum_field='Requests', date_field='Datetime', is_train=True, where='1'):
    """
    获取单维度条件及其总量
    """
    cursor = mysqlConnectStat.cursor()
    select = 'IF(`%s`="", "other",`%s`) as %s,sum(`%s`),count(`%s`)' % (field, field, field, sum_field, field)
    time_stamp = int(time.mktime(time.strptime(date_time, '%Y-%m-%d'))) - (STAT_CYCLE * 86400 if is_train else 0)
    if not where:
        where = '1'
    where += ' and `%s` between %s and %s' % (date_field, time_stamp, time_stamp + 86400 - 1)
    sql = 'select %s from `%s` where %s group by `%s`' % (select, table, where, field)
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result


def get_sum_hourly(date_time, table, sum_field='Requests', date_field='Datetime'):
    """
    获取单天每小时总量
    """
    cursor = mysqlConnectStat.cursor()
    select = 'FROM_UNIXTIME(`%s`, "%%k"),sum(`%s`)' % (date_field, sum_field)
    time_stamp = int(time.mktime(time.strptime(date_time, '%Y-%m-%d')))
    where = '`%s` between %s and %s ' % (date_field, time_stamp, time_stamp + 86400 - 1)
    sql = 'select %s from `%s` where %s group by `%s` order by `%s` asc' % (
    select, table, where, date_field, date_field)
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result


def get_relate(field, table, id_field='id', where='1'):
    """
    获取字段对应的键值关系
    """
    cursor = mysqlConnectSource.cursor()
    select = '`%s`,`%s`' % (id_field, field)
    sql = 'select %s from `%s` where %s ' % (select, table, where)
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    relative_map = {}
    for item in result:
        relative_map[item[1]] = item[0]
    return relative_map