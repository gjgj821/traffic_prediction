#!/usr/bin/python
import SimpleHTTPServer
from multiprocessing import Process, Array
import SocketServer
import datetime
import time
import os
import re
import urllib

DATE_FORMAT = "%Y-%m-%d"

PRO_PATH = "/data7/liubo/traffic_prediction/"
SHELL_PATH = PRO_PATH + "online/script/"
cmd = "python " + SHELL_PATH + "process_main.py %s %s"


def parse_param(s):
    param = {}
    content = s.split('&')
    for text in content:
        print text
        key, value = text.split('=')
        param[key] = value
    return param


def exec_result(day, data):
    data_tmp = (data + '||' if data else '') + 'DateTime:(' + day + ')'
    han = os.popen(cmd % (PRO_PATH, data_tmp))
    info = han.readline()
    #info = 'request:12312 user:12312312'
    if info.find('request') >= 0:
        m = re.findall(r'\d+', info)
        return ','.join(m)
    else:
        return '0,0'


def m_exec_result(day, data, result):
    result.value = exec_result(day, data)
    return


class HTTPHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_GET(self):
        return

    def do_POST(self):
        length = int(self.headers.getheader('content-length'))
        content = urllib.unquote(self.rfile.read(length))
        param = parse_param(content)
        if 'data' not in param:
            return
        if 'start' in param and 'end' in param:
            plist = []
            index = 0
            day_list = []
            start = int(time.mktime(time.strptime(param['start'], DATE_FORMAT)))
            end = int(time.mktime(time.strptime(param['end'], DATE_FORMAT)))
            while start <= end:
                day = time.strftime(DATE_FORMAT, time.localtime(start))
                day_list.append(day)
                start += 86400
            result = []
            for day in day_list:
                result.append(Array('c', 20))
                p = Process(target=m_exec_result, args=(day, param['data'], result[index]))
                p.start()
                plist.append(p)
                index += 1
            for p in plist:
                p.join()
            self.wfile.write('|'.join([i.value for i in result]))
            return
        elif 'day' in param:
            self.wfile.write(exec_result(param['day'], param['data']))
            return
        else:
            now = datetime.datetime.now()
            day = now.strftime(DATE_FORMAT)
            self.wfile.write(exec_result(day, param['data']))
            return

if __name__ == '__main__':
    PORT = 9111
    httpd = SocketServer.TCPServer(("", PORT), HTTPHandler)
    print "serving at port", PORT
    httpd.serve_forever()