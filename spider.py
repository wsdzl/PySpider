#!/usr/bin/env python3
#coding: utf-8
#http://blog.knownsec.com/2012/02/knownsec-recruitment/

import os
import sys
import getopt
import logging as _log
from string import printable
from urllib import request as urllib
from SqliteThreadSafe import DbHandler, sqlite3
from ThreadPool import Pool, Lock
from AnchorParser import AnchorParser

_help = '''
Usage: spider.py [-u url] [-d deep] [-f logfile] [-l loglevel(1-5)]
                 [--thread number] [--dbfile filepath]
                 [--key="<keyword>"] [--testself]
Options: 
    -h, --help         查看帮助信息。
    -u url             指定爬虫开始地址，必选参数。
    -d deep            指定爬虫深度，可选参数，默认为7。
    -f logfile         保存日志到指定文件，可选参数，默认为spider.log。
    -l loglevel(1-5)   日志记录文件记录详细程度，数字越大记录越详细，可选参数，默认为5。
    --thread number    指定线程池大小，多线程爬取页面，可选参数，默认为20。
    --dbfile filepath  存放结果数据到指定的数据库（sqlite）文件中，可选参数，默认为data.db。
    --key="<keyword>"  页面内的关键词，获取满足该关键词的网页，可选参数，默认为所有页面。
    --testself         程序自测，可选参数。
'''

class _db(DbHandler):
    def __init__(self, host, dbname=None):
        super().__init__(dbname)
        self.table = '_%s' % host
        sql = "create table if not exists '%s' (\
        id integer primary key autoincrement, \
        url text key, \
        keyword text, \
        html blob \
        )" % self.table
        self.execute(sql)

    class Writer(object):
        def __init__(self, db, url, keyword):
            self.db = db
            self.table = db.table
            self.url = url
            self.keyword = keyword

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def write(self, html):
            with self.db.lock:
                cursor = self.db.conn.cursor()
                cursor.execute("insert into '%s' (url,keyword,html) values(?,?,?)" % self.table,
                    (
                    self.url,
                    self.keyword,
                    sqlite3.Binary(html))
                    )
                self.db.conn.commit()
            cursor.close()
    def get_writer(self, url, keyword):
        if keyword==None:
            keyword = ''
        return self.Writer(self, url, keyword)

def request_url(url, fn=None, save_as=None):
    if not save_as:
        assert fn
        save_as = open
    if fn is None:
        f = save_as
    else:
        f = save_as(fn, 'wb')
    try:
        url = urllib.quote(url, safe=printable)
        r = urllib.urlopen(url)
        data = r.read()
        with f:
            f.write(data)
        ct = r.headers.get('Content-type')
        charset = None
        if ct:
            ct = ct.split(';',1)
            if len(ct) > 1:
                charset = ct[1].split('charset=')
                if len(charset) > 1:
                    charset = charset[1]
                else:
                    charset = None
            ct = ct[0]
        retval = ('ok', ct, data, charset)
    except Exception as e:
        retval = (('*** ERROR: bad URL "%s": %s' % (url, e)), None)
    return retval

class Spider(object):

    _filter = {'.css', '.js', '.jpg', '.jpeg', '.jpe', '.gif', '.bmp',
               '.exe', '.avi', '.rmvb', '.mp4', '.mp3', '.wav'}

    def __init__(self, url, deep=7, threads=20, dbname='data.db', keyword=None):
        if not url.startswith('http://') and not url.startswith('https://'):
            url = 'http://%s' % url
        while url.endswith('/'):
            url = url[:-1]
        parsed = urllib.urlparse(url)
        self.host = parsed.netloc.split('@')[-1].split(':')[0]
        ext = os.path.splitext(parsed.path)[1]
        if not ext:
            ext = '.html'
        self.dom = '.'.join(self.host.split('.')[-2:])
        self.deep = deep
        self.pool = Pool(threads)
        self.db = _db(parsed.netloc, dbname)
        self.keyword = keyword
        self.lock = Lock()
        self.queue = [(url, ext, 0)]
        self.seen = set()
        self.seen.add(url)
        self.count = 0
        
    def get_page(self, url, _filter=True, dom=True):
        url, ext, deep = url
        with self.lock:
            self.count += 1
            count = self.count
        _log.info('No.%s URL: %s starting to handle' % (count, url))
        if _filter:
            if _filter == True:
                _filter = self._filter
            if ext in _filter:
                _log.debug('No.%s URL: %s skipping download' % (count, url))
                exit()
                return
        result = request_url(url, save_as=self.db.get_writer(url,self.keyword))
        if result[0][0] == '*':
            _log.warning(result[0])
            return
        else:
            _log.debug('No.%s URL: %s has been downloaded' % (count, url))
        if deep == self.deep:
            _log.debug('No.%s URL: %s skipping parse' % (count, url))
            return
        mime = result[1]
        if mime and not mime.startswith('text/html'):
            _log.debug('No.%s URL: %s skipping parse' % (count, url))
            return
        links = set(AnchorParser(result[2], url, result[3])())
        for link in links:
            parsed = urllib.urlparse(link)
            _ext = os.path.splitext(parsed.path)[1]
            if not _ext:
                _ext = '.html'
            if dom and link.startswith('http'):
                if dom == True:
                    dom = self.dom
                host = parsed.netloc.split('@')[-1].split(':')[0]
                if not host.endswith(dom):
                    _log.debug('LINK: discarded link %s' % link)
                    continue
            if link not in self.seen:
                with self.lock:
                    if link not in self.seen:
                        self.seen.add(link)
                        _log.debug('LINK: found link %s' % link)
                        self.queue.append((link, _ext, deep+1))

    def run(self, _filter=True, dom=True):
        while True:
            if len(self.queue):
                with self.lock:
                    url = self.queue[0]
                    del self.queue[0]
                self.pool.add(self.get_page,(url, _filter, dom))
            elif not self.pool.running():
                with self.lock, self.pool._lock:
                    flag = False
                    if not (len(self.pool.tasks) or len(self.queue) or self.pool.running()):
                        flag = True
                if flag:
                    self.pool.close()
                    break

def _setlog(loglevel=5, filename='spider.log'):
    loglevels = [_log.CRITICAL, _log.ERROR, _log.WARNING, _log.INFO, _log.DEBUG, _log.NOTSET]
    _log.basicConfig(level=loglevels[loglevel],
                     format='%(asctime)s %(message)s',
                     datefmt='%Y-%m-%d %H:%M:%S',
                     filename=filename,
                     filemode='a')
    console = _log.StreamHandler()
    console.setLevel(loglevels[loglevel])
    formatter = _log.Formatter('%(asctime)s %(message)s')
    console.setFormatter(formatter)
    _log.getLogger('').addHandler(console)
    # _log.debug('This is debug message')
    # _log.info('This is info message')
    # _log.warning('This is warning message')

def _getopt(opts, key, func, default):
    try:
        opt = func(opts[key])
    except:
        opt = default
    return opt

def main():
    if len(sys.argv) == 1:
        print(_help)
        exit()
    try:
        opts, args = getopt.getopt(sys.argv[1:],
            'hu:d:f:l:', ['help', 'testself', 'thread=', 'dbfile=', 'key='])
    except getopt.GetoptError as e:
        print('Error:', e)
        print('Use -h or --help for more information.')
        exit(1)
    opts = dict(opts)
    if '-h' in opts or '--help' in opts:
        print(_help)
        exit()
    if '-u' in opts:
        start_url = opts['-u']
        if start_url[0:7].lower() != 'http://':
            start_url = 'http://' + start_url
    else:
        print('Error: option -u must not be null')
        print('Use -h or --help for more information.')
        exit(1)
    deep = _getopt(opts, '-d', int, 7)
    logfile = _getopt(opts, '-f', str, 'spider.log')
    loglevel = _getopt(opts, '-l', int, 5)
    _setlog(loglevel, logfile)
    thread = _getopt(opts, '--thread', int, 20)
    dbfile = _getopt(opts, '--dbfile', str, 'data.db')
    keyword = _getopt(opts, '--key', str, None)
    testself = _getopt(opts, '--testself', lambda x:not x, False)
    if testself:
        print('...............ok.................')
        exit()
    spider = Spider(start_url, deep, thread, dbfile, keyword)
    spider.run()
if __name__ == '__main__':
    main()
    

    # text = urllib.urlopen('http://www.baidu.com').read()
    # print(AnchorParser(text)())