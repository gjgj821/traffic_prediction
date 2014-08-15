#!/usr/bin/python
# coding=utf-8
import SimpleHTTPServer
import threading
import SocketServer
import datetime
import urllib
import json
from core.term_test import TermTestManage
from core.flow import Flow

#import chronic
#from pprint import pprint
#import hadoop.hadoopget

DATE_FORMAT = '%Y-%m-%d'

TERM_FILE = 'data/term_list.txt'
DIM_FILE = 'data/dim_list.txt'

DB_NAME = 'RTBApp'

manage=None
f=None

lock = threading.Lock()

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
    #with chronic.Timer('exec_result'):
    for i in range(length):
        value_list.append(str(manage.estimate(term_map=data, total=value)))
        value = f.future(value, i)

    # 计算单次执行时间
    #pprint(chronic.timings)

    #print ','.join(value_list)
    return ','.join(value_list)


def load():
    global manage, f
    # 锁定
    lock.acquire()
    manage = TermTestManage()

    now = datetime.datetime.now()
    day = now.strftime(DATE_FORMAT)
    f = Flow(day, DB_NAME)

    # 加载基础维度
    fo = open(DIM_FILE, 'r')
    lines = fo.readlines()
    manage.load_dim(lines)
    fo.close()

    # 加载修正组合
    fo = open(TERM_FILE, 'r')
    lines = fo.readlines()
    manage.load(lines)
    fo.close()

    #释放
    lock.release()

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
        if 'reload' in param:
            load()
            return
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

    load()

    print "==========load finish=========="
    print "MAIN: thread",threading.current_thread().getName()

    httpd.serve_forever()