#!/usr/bin/python
# coding=utf-8
import SimpleHTTPServer
from pprint import pprint
import threading
from multiprocessing import Process, Array
import SocketServer
import datetime
import urllib
import json
import chronic
from core.term_test import TermTestManage
from core.flow import Flow

DATE_FORMAT = '%Y-%m-%d'

TERM_FILE = 'term_list.txt'
DIM_FILE = 'dim_list.txt'

manage=None
f=None

def parse_param(s):
	param = {}
	content = s.split('&')
	for text in content:
		print text
		key, value = text.split('=')
		param[key] = value
	return param


def exec_result(length, data):
	value_list = []
	value = f.total_current()
	# manage = TermTestManage()
	# # 加载修正组合
	# fo = open(TERM_FILE, 'r')
	# lines = fo.readlines()
	# manage.load(lines)
	# fo.close()
	#
	# # 加载基础维度
	# fo = open(DIM_FILE, 'r')
	# lines = fo.readlines()
	# manage.load_dim(lines)
	# fo.close()
	# f = Flow(datetime.datetime.now().strftime(DATE_FORMAT), 'RTPApp')
	# value = f.total_current()
	with chronic.Timer('exec_result'):
		for i in range(length):
			value_list.append(str(manage.estimate(term_map=data, total=value)))
			value = f.future(value, i)

	# 计算单次执行时间
	pprint(chronic.timings)

	print ','.join(value_list)
	return ','.join(value_list)


class HTTPHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
	def do_GET(self):
		return

	def do_POST(self):
		print "do_POST: thread",threading.current_thread().getName()
		length = int(self.headers.getheader('content-length'))
		content = urllib.unquote(self.rfile.read(length))
		# param = parse_param(content)
		param = json.loads(content)
		#param['data'] = json.loads(param['data'])
		print param
		if 'data' not in param:
			return
		elif 'length' in param:
			self.wfile.write(exec_result(param['length'], param['data']))
			#self.wfile.write("response")
			return
		else:
			self.wfile.write(exec_result(1, param['data']))
			return

if __name__ == '__main__':
	PORT = 9112
	httpd = SocketServer.TCPServer(("", PORT), HTTPHandler)
	print "serving at port", PORT

	manage = TermTestManage()

	# 加载基础维度
	fo = open(DIM_FILE, 'r')
	lines = fo.readlines()
	manage.load_dim(lines)
	fo.close()
	f = Flow('2014-06-10', 'RTBApp')

	# 加载修正组合
	fo = open(TERM_FILE, 'r')
	lines = fo.readlines()
	manage.load(lines)
	fo.close()
	print "==========load finish=========="
	print "MAIN: thread",threading.current_thread().getName()

	httpd.serve_forever()