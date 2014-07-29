#!/usr/bin/python
# coding=utf-8
import SimpleHTTPServer
from multiprocessing import Process, Array
import SocketServer
import datetime
import urllib
import json
from core.term_test import TermTestManage
from core.flow import Flow

DATE_FORMAT = '%Y-%m-%d'

TERM_FILE = 'term_list.txt'
DIM_FILE = 'dim_list.txt'


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
	manage = TermTestManage()
	# 加载修正组合
	fo = open(TERM_FILE, 'r')
	lines = fo.readlines()
	manage.load(lines)
	fo.close()

	# 加载基础维度
	fo = open(DIM_FILE, 'r')
	lines = fo.readlines()
	manage.load_dim(lines)
	fo.close()
	f = Flow(datetime.datetime.now().strftime(DATE_FORMAT), 'RTPApp')
	value = f.total_current()
	for i in range(length):
		value_list.append(manage.estimate(term_map=data, total=value))
		value = f.future()
	return ','.join(value_list)


class HTTPHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
	def do_GET(self):
		return

	def do_POST(self):
		length = int(self.headers.getheader('content-length'))
		content = urllib.unquote(self.rfile.read(length))
		param = parse_param(content)
		param['data'] = json.loads(param['data'])
		if 'data' not in param:
			return
		elif 'length' in param:
			self.wfile.write(exec_result(param['length'], param['data']))
			return
		else:
			self.wfile.write(exec_result(1, param['data']))
			return

if __name__ == '__main__':
	PORT = 9111
	httpd = SocketServer.TCPServer(("", PORT), HTTPHandler)
	print "serving at port", PORT
	httpd.serve_forever()