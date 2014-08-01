# coding=utf-8
from decimal import getcontext
from dbget import *
from relative import Relative

__author__ = 'GaoJie'
import itertools
import math
import uuid


## 放大流量，10为将流量比例放大10倍，并且10分之一以上的流量都考虑相关性
REQUEST_UP = 100000
## 以support为x轴，流量比例为y轴，对于半径为1的集合过滤
EFFECT_RADIO = 0

## 过滤精确度，过滤不足该量的定向条件，对于单维度很多条件的则过滤一定比例的条件（按总请求量升序过滤）并且计算精度也是该值与总量的比率
ACCURACY_VALUE = 100

## 得出计算的精确度
ACCURACY_F = 15
getcontext().prec = ACCURACY_F


def round_f(d):
	"""
	取精度
	"""
	return d


class TermManage(object):
	def __init__(self, table, date_time, where=False):
		self.table = table
		self.date_time = date_time
		self.where = where
		self.total = get_sum(date_time, {}, table, where=where)
		self.max_dim_result = int(int(self.total / ACCURACY_VALUE) ** 0.5)
		self.term_map = {}
		self.dim_list = []
		self.dim_map_list = []
		self.dim_relative_map = {}
		return

	def add_dim(self, dim):
		"""

		"""
		self.dim_map_list.append(dim)
		result = get_group(self.date_time, dim, self.table, where=self.where)
		##对数据量少的条件过滤
		result = filter(self.need_filter, result)
		if len(result) > self.max_dim_result:
			## 对结果进行排序，在迭代中会按需过滤
			result = sorted(result, key=lambda x: x[1], reverse=True)

		##全部加入基础词
		for item in result:
			term_map = {}
			term_map[dim] = item[0]
			self.add_term(term_map, item[1])
		print (dim, len(result))
		self.dim_list.append(result)
		return self

	def add_term(self, term_map, sum_value):
		"""
		添加词，词组
		"""
		string = Term.get_string(term_map)
		self.term_map[string] = Term(term_map, self, sum_value)
		return self.term_map[string]

	def union_dim(self, r, dim_list=None):
		"""
		多维度并集
		"""
		need_map = {}
		if dim_list:
			dim_list = [self.dim_map_list.index(dim) for dim in dim_list if dim in self.dim_map_list]
			if len(dim_list) < 2 or r > len(dim_list):
				return need_map
		else:
			dim_num = len(self.dim_map_list)
			if r > dim_num:
				return need_map
			dim_list = range(0, dim_num)
		#获取不重复的排列组合
		comb_list = itertools.combinations(dim_list, r)
		for comb in comb_list:
			dl = []
			for index in comb:
				##过滤影响较小的条件
				dl.append(self.dim_list[index][:self.max_dim_result])
			## 判断的是相对支持度，所以需要进行笛卡尔乘积
			for item in itertools.product(*dl):
				##去除重复，计算有效的
				term_map = {}
				for i in range(0, len(item)):
					term_map[self.dim_map_list[comb[i]]] = item[i][0]
				if self.has_term(term_map, self.term_map):
					continue
				sum_value = get_sum(self.date_time, term_map, self.table)
				term = self.add_term(term_map, sum_value)
				#if not self.need_filter(term):
				#	continue
				#term.effect()
				#print term.format()
				#print term.format2()
				if self.is_effect(term):
					need_map[term.uuid()] = term
		return need_map

	def get_term(self, term_map):
		string = Term.get_string(term_map)
		return self.term_map[string] if string in self.term_map else None

	def export_dim(self, dim_list=None, relative=True):
		"""
		导出维度数据
		"""
		if dim_list:
			dim_list = [self.dim_map_list.index(dim) for dim in dim_list if dim in self.dim_map_list]
		else:
			dim_list = range(0, len(self.dim_map_list))

		lines = []
		for index in dim_list:
			dim = self.dim_map_list[index]
			lines.append(u'##%s\n' % dim)
			for item in self.dim_list[index]:
				term_map = {}
				term_map[dim] = item[0]
				term = self.get_term(term_map)
				lines.append(term.get_line(relative=relative))
		return lines

	def get_relative_value(self, dim, value):
		if dim not in self.dim_relative_map:
			relative = getattr(Relative, 'get_%s' % dim.lower())
			relative_map = relative()
			self.dim_relative_map[dim] = relative_map
		else:
			relative_map = self.dim_relative_map[dim]
		return relative_map[value] if value in relative_map else value

	@staticmethod
	def has_term(term_map, map_list):
		"""
		词是否存在
		"""
		return False if Term.get_string(term_map) not in map_list else True

	@staticmethod
	def is_effect(term):
		"""
		是否符合影响范围，符合的则加入集合
		"""
		return True if term.effect() > EFFECT_RADIO else False

	@staticmethod
	def need_filter(item):
		"""
		是否过滤该Term，过滤的则无需考虑后续的组合情况，因为精度低
		"""
		if isinstance(item, Term):
			sum_value = item.sum_value
		elif type(item) == list or type(item) == tuple:
			sum_value = item[1]
		else:
			return False
		if sum_value < ACCURACY_VALUE:
			return False
		return True


class Term(object):
	def __init__(self, term_map, manage, sum_value=None):
		self._sum = len(term_map.keys())
		self.term_map = term_map
		self.manage = manage
		if sum_value:
			self.sum_value = sum_value
			#占总量的比例
			self.ratio = None
			self.support()
		else:
			self.sum_value = None
			self.ratio = None
		#相对支持度
		self.ratio2 = None
		#父交集比例
		if self._sum == 1:
			self.ratio3 = self.support()
		else:
			self.ratio3 = None
		#影响度
		self.ratio4 = None
		self.string = self.get_string(self.term_map)
		self.relative_string = self.get_relative_string(self.term_map)

	def support(self):
		"""
		支持度
		"""
		if self.ratio:
			return self.ratio
		sum_int = get_sum(self.manage.date_time, self.term_map, self.manage.table) if self.sum_value is None else self.sum_value
		self.ratio = sum_int / self.manage.total
		return self.ratio

	def support_relative(self):
		"""
		相对支持度
		"""
		if self.ratio2:
			return self.ratio2
		ratio = self.support()
		term_list = []
		for key, value in self.term_map.items():
			term_map = {}
			term_map[key] = value
			term_list.append(self.manage.get_term(term_map).support())
		self.ratio3 = reduce(lambda x, y: x * y, term_list)
		ratio2 = (ratio - self.ratio3) / max(ratio, self.ratio3)
		self.ratio2 = ratio2
		return self.ratio2

	def effect(self):
		"""
		影响度
		"""
		if self.ratio4:
			return self.ratio4
		ratio = (self.support()) * REQUEST_UP
		ratio2 = self.support_relative()
		self.ratio4 = math.sqrt(ratio ** 2 + ratio2 ** 2)
		#print self.ratio4, ratio, ratio2
		return self.ratio4

	def amend(self, ratio):
		"""
		修正支持度，不考虑
		"""
		_ratio = self.support()
		self.ratio = round((ratio + _ratio) / 2, ACCURACY_F)
		self.ratio2 = self.ratio3 = self.ratio4 = None
		return self.ratio

	def __str__(self):
		return self.string

	def uuid(self):
		return uuid.uuid3(uuid.NAMESPACE_DNS, self.string.encode('utf-8'))

	def format(self, encode=False, relative=False):
		"""
		自身比例，父交集比例，影响度
		"""
		string = u'%s:%s:%s:%s:%s' % (self._sum, self.relative_string if relative else self.string, round_f(self.ratio), round_f(self.ratio3), self.ratio4 if self.ratio4 else 1)
		return string.encode("utf-8") if encode else string

	def get_line(self, relative=True):
		return self.format(encode=True, relative=relative)+"\n"

	def get_relative_string(self, term_map):
		string = ''
		for key, value in term_map.items():
			string += u'%s=%s|' % (key, self.manage.get_relative_value(key, value))
		return string

	@staticmethod
	def get_string(term_map, relative=False):
		string = ''
		for key, value in term_map.items():
			string += u'%s=%s|' % (key, value)
		return string
