# coding=utf-8
from decimal import Decimal, getcontext
import itertools
from term import Term, TermManage, ACCURACY_F

__author__ = 'GaoJie'
getcontext().prec = ACCURACY_F


class TermTestManage(object):
	def __init__(self, total=0):
		self.total = total
		self.ratio = None
		self.ratio_fix = None
		self.term_map = {}
		self.dim_list = []
		self.dim_map_list = []
		self.need_map = []
		self.ok = False
		return

	def load(self, term_list):
		"""
		加载词组
		"""
		for line in term_list:
			line = line.strip()
			term = TermTest.parse(line.decode('utf-8'), self)
			self.term_map[term.string] = term
		return self

	def load_dim(self, lines):
		"""
		加载基础维度
		"""
		result_map = {}
		dim = None
		for line in lines:
			line = line.strip()
			if not line:
				continue
			if line[:2] == '##':
				if result_map:
					self.dim_map_list.append(dim)
					self.dim_list.append(result_map)
				dim = line[2:]
				result_map = {}
			else:
				term = TermTest.parse(line.decode('utf-8'), self)
				result_map[term.term_map[dim]] = term.ratio
		if result_map:
			self.dim_map_list.append(dim)
			self.dim_list.append(result_map)
		# print self.dim_list
		return self

	def analyze(self, term_map=None, fix=True):
		"""
		分析定向条件
		"""
		if term_map is None and self.ok:
			return self.ratio_fix if fix else self.ratio
		if not term_map:
			self.ratio, self.ratio_fix = [1, 1]
		else:
			self.ratio, self.ratio_fix = self.parse_term_map(term_map, fix)
		self.ok = True
		return self.ratio_fix if fix else self.ratio

	def estimate(self, term_map=None, total=None):
		"""
		根据总流量计算
		"""
		if not total:
			total = self.total
		ratio = self.analyze(term_map)
		return int(ratio * Decimal(total))

	def estimate_test(self, term_map=None, total=None, real=None, p=True):
		"""
		根据总流量计算，测试用
		"""
		if not total:
			total = self.total
		self.analyze(term_map)
		total_source = int(self.ratio * Decimal(total))
		total_fix = int(self.ratio_fix * Decimal(total))
		if p:
			print "条件：%s" % term_map
			print "总量：%s,实际量：%s,所占比例：%s" % (total, real, real / total)
			# 不修正的差值
			print "未修正：%s(比例),%s(预估量),%s(差比)" % (self.ratio, total_source, (total_source - real) / real)
			# 修正的差值
			print "修正后：%s(比例),%s(预估量),%s(差比)" % (self.ratio_fix, total_fix, (total_fix - real) / real)
		return real / total, self.ratio, self.ratio_fix

	def estimate_test2(self, term_map=None, total=None, real=None, p=True):
		"""
		根据总流量计算，测试用
		"""
		if not total:
			total = self.total
		self.analyze(term_map)
		total_source = int(self.ratio * Decimal(total))
		total_fix = int(self.ratio_fix * Decimal(total))
		if p:
			print "条件：%s" % term_map
			print "总量：%s,实际量：%s,所占比例：%s" % (total, real, real / total)
			# 不修正的差值
			print "未修正：%s(比例),%s(预估量),%s(差比)" % (self.ratio, total_source, (total_source - real) / real)
			# 修正的差值
			print "修正后：%s(比例),%s(预估量),%s(差比)" % (self.ratio_fix, total_fix, (total_fix - real) / real)
		return real, total_source, total_fix

	def parse_term_map(self, term_map, fix=True):
		"""
		解析条件，并获取比例值
		"""
		ratio_list = []
		fix_list = []
		fix_dim = []
		for dim, value in term_map.items():
			reverse = False
			## 取反符号
			if dim[:1] == '!':
				dim = dim[1:]
				reverse = True
			index = self.dim_map_list.index(dim)
			fix_dim.append(dim)
			if not reverse and type(value[0]) is not list:
				# 对于单维度的复合集，也需要排除
				# 添加0，用于笛卡尔乘积中判断任意维度组合情况
				value_tmp = value[:]
				value_tmp.append(0)
				fix_list.append(value_tmp)
			ratio_list.append(abs(self.parse_or(index, value) + (-1 if reverse else 0)))
		ratio = Decimal(reduce(lambda x, y: x * y, ratio_list))
		# 优化
		if fix and len(fix_list) > 1:
			fix_wait_list = []
			## 获取可以产生影响的term
			for item in itertools.product(*fix_list):
				term_map_tmp = {}
				for i in range(0, len(item)):
					if item[i] is 0:
						continue
					term_map_tmp[fix_dim[i]] = item[i]
				term = self.get_term(term_map_tmp)
				if not term:
					continue
				fix_wait_list.append(term)
			if fix_wait_list:
				# 执行优化算法
				ratio_list = self.fix3(fix_wait_list, ratio_list, fix_dim)

		#print ratio_list
		return ratio, Decimal(reduce(lambda x, y: x * y, ratio_list))

	def parse_or(self, dim, ll):
		"""
		求并集
		"""
		ratio_dim = []
		for value in ll:
			if type(value) is list:
				return self.parse_and(dim, ll)
			if value in self.dim_list[dim]:
				ratio_dim.append(self.dim_list[dim][value])
			else:
				ratio_dim.append(0)
		return Decimal(reduce(lambda x, y: x + y, ratio_dim))

	def parse_and(self, dim, ll):
		"""
		求交集
		"""
		ratio_dim = []
		for value in ll:
			if type(value) is list:
				ratio_dim.append(self.parse_or(dim, value))
			else:
				return self.parse_or(dim, ll)
		#print ratio_dim
		return Decimal(reduce(lambda x, y: x * y, ratio_dim))

	def get_term(self, term_map):
		string = Term.get_string(term_map)
		return self.term_map[string] if string in self.term_map else None

	@classmethod
	def fix(cls, fix_wait_list, ratio_list, fix_dim):
		"""
		同一维度中，修正影响度最高的条件组合
		"""
		## 对影响度进行排序
		fix_wait_list = sorted(fix_wait_list, key=lambda x: 1/x.ratio4)
		for term in fix_wait_list:
			need = True
			for key, value in term.term_map.items():
				index = fix_dim.index(key)
				if ratio_list[index] == 1:
					need = False
					break
			if not need:
				continue
			cls.fix_base(term, ratio_list, fix_dim)
		return ratio_list

	@classmethod
	def fix2(cls, fix_wait_list, ratio_list, fix_dim):
		"""
		遍历所有组合
		"""
		## 对影响度进行排序
		fix_wait_list = sorted(fix_wait_list, key=lambda x: 1/x.ratio4)
		ratio_plus_list = []
		for term in fix_wait_list:
			ratio_list_tmp = ratio_list[:]
			fix_dim_tmp = fix_dim[:]
			cls.fix_base(term, ratio_list_tmp, fix_dim_tmp)
			ratio_plus_list.append(Decimal(reduce(lambda x, y: x * y, ratio_list_tmp)))
		return [1, Decimal(reduce(lambda x, y: x + y, ratio_plus_list)) / len(ratio_plus_list)]

	@classmethod
	def fix3(cls, fix_wait_list, ratio_list, fix_dim):
		"""
		只叠加相同2维组合权重
		"""
		## 只计算2维情况
		fix_wait_list = filter(lambda x: x.sum == 2, fix_wait_list)
		## 对影响度进行排序
		fix_wait_list = sorted(fix_wait_list, key=lambda x: x.ratio4, reverse=True)
		for term in fix_wait_list:
			bin_number = 0
			i = 0
			for key, value in term.term_map.items():
				index = fix_dim.index(key)
				if ratio_list[index] == 1:
					bin_number |= (1 << i)
				i += 1
			if not ~bin_number:
				bin_number = 0
			if bin_number and ~bin_number:
				continue
			cls.fix_base(term, ratio_list, fix_dim)
		return [1, Decimal(reduce(lambda x, y: x * y, ratio_list))]

	@staticmethod
	def fix_base(term, ratio_list, fix_dim):
		ratio_tmp = term.fix()
		fix_ratio = []
		keys = []
		for key, value in term.term_map.items():
			index = fix_dim.index(key)
			keys.append(key)
			fix_ratio.append(ratio_list[index])
			ratio_list[index] = 1
		index_string = '|'.join(keys)
		if index_string in fix_dim:
			index = fix_dim.index(index_string)
			ratio_list[index] = ratio_list[index] + ratio_tmp
		else:
			ratio_list.append(ratio_tmp + Decimal(reduce(lambda x, y: x * y, fix_ratio)))
			fix_dim.append(index_string)
			index = len(fix_dim) - 1
		if ratio_list[index] > 1:
			ratio_list[index] = 1


class TermTest(object):
	def __init__(self, term_map, manage, ratio, ratio3, ratio4):
		self.sum = len(term_map.keys())
		self.term_map = term_map
		self.manage = manage
		self.ratio = Decimal(ratio)
		self.ratio3 = Decimal(ratio3)
		self.ratio4 = Decimal(ratio4)
		self.string = self.get_string(self.term_map)

	def fix(self):
		"""
		获取该词的差率
		"""
		return self.ratio - self.ratio3

	@staticmethod
	def parse(string, manage):
		info_list = string.split(':')
		term_info = info_list[1].split('|')
		term_info.pop()
		term_map = {}
		if len(term_info) == int(info_list[0]):
			for term_item in term_info:
				item = term_item.split('=')
				term_map[item[0]] = item[1]
		else:
			return None
		return TermTest(term_map, manage, info_list[2], info_list[3], info_list[4])

	@staticmethod
	def get_string(term_map):
		return Term.get_string(term_map)

	def __str__(self):
		return self.string