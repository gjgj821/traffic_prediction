from dbget import get_relate

__author__ = 'GaoJie'


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
	def get_province():
		return get_relate('zhName', 'b_base_geo', where='level=1')

	@staticmethod
	def get_city():
		return get_relate('zhName', 'b_base_geo', where='level=2 or firstcity = 1')