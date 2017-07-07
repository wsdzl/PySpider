#!/usr/bin/env python3
#coding: utf-8
#http://blog.knownsec.com/2012/02/knownsec-recruitment/

import os
import sys
import getopt
import logging as _log
from string import printable
from urllib import request as urllib
from gzip import GzipFile
from io import BytesIO
from SqliteThreadSafe import DbHandler, sqlite3
from ThreadPool import Pool, Lock
from AnchorParser import AnchorParser, get_charset

_help = '''
Usage: spider.py [-u url] [-d deep] [-f logfile] [-l loglevel(1-5)]
                 [--thread number] [--dbfile filepath]
                 [--key="<keyword>"] [--pridomain/-p] [--testself]
Options: 
    -h, --help         查看帮助信息。
    -u url             指定爬虫开始地址，必选参数。
    -d deep            指定爬虫深度，可选参数，默认为7。
    -f logfile         保存日志到指定文件，可选参数，默认为spider.log。
    -l loglevel(1-5)   日志记录文件记录详细程度，数字越大记录越详细，可选参数，默认为5。
    --thread number    指定线程池大小，多线程爬取页面，可选参数，默认为20。
    --dbfile filepath  存放结果数据到指定的数据库（sqlite）文件中，可选参数，默认为data.db。
    --key="<keyword>"  页面内的关键词，获取满足该关键词的网页，可选参数，默认为所有页面。
    --pridomain/-p     仅爬行主域名，可选参数，默认爬行主域名及所有子域名链接。
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

def request_url(url, fn=None, save_as=None, keyword=''):
    if not save_as:
        assert fn
        save_as = open
    if fn is None:
        f = save_as
    else:
        f = save_as(fn, 'wb')
    try:
        headers = {
                'Connection': 'keep-alive',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip',
                'User-Agent': 'Mozilla/5.0 (X11; Linux i686)\
                      AppleWebKit/537.36 (KHTML, like Gecko)\
                      Chrome/35.0.1916.153 Safari/537.36',
                'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
            }
        url = urllib.quote(url, safe=printable)
        req = urllib.Request(url, headers=headers)
        r = urllib.urlopen(req, timeout=5)
        data = r.read()
        ce = r.headers.get('Content-Encoding')
        if ce == 'gzip':
            data = GzipFile(fileobj=BytesIO(data)).read()
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
        if not charset:
            charset = get_charset(data)
        has_key = True
        if keyword:
            if charset:
                keyword = keyword.encode(charset)
            else:
                keyword = keyword.encode('utf-8')
            if data.find(keyword) == -1:
                has_key = False
        if has_key:
            with f:
                f.write(data)
        retval = ('ok', ct, data, charset)
    except Exception as e:
        # raise e
        retval = (('*** ERROR: bad URL "%s": %s' % (url, e)), None)
    return retval

class Spider(object):

    _filter = {'.css', '.js', '.jpg', '.jpeg', '.jpe', '.gif', '.bmp',
               '.exe', '.avi', '.rmvb', '.mp4', '.mp3', '.wav'}

    def __init__(self, url, deep=7, threads=20, dbname='data.db', keyword=None, pridomain=False):
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
        self.pridomain = pridomain
        self.deep = deep
        self.pool = Pool(threads)
        self.db = _db(parsed.netloc, dbname)
        self.keyword = keyword
        self.lock = Lock()
        self.queue = [(url, ext, 0)]
        self.seen = set()
        self.seen.add(url)
        self.count = 0
        
    def get_page(self, url, _filter):
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
        keyword = self.keyword if deep > 0 else None
        result = request_url(url, save_as=self.db.get_writer(url,keyword), keyword=keyword)
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
            if link.startswith('http'):
                host = parsed.netloc.split('@')[-1].split(':')[0]
                if self.pridomain:
                    if host != self.host:
                        _log.debug('LINK: discarded link %s' % link)
                        continue
                else:
                    if not host.endswith(self.dom):
                        _log.debug('LINK: discarded link %s' % link)
                        continue
            if link not in self.seen:
                with self.lock:
                    if link not in self.seen:
                        self.seen.add(link)
                        _log.debug('LINK: found link %s' % link)
                        self.queue.append((link, _ext, deep+1))

    def run(self, _filter=True):
        try:
            while True:
                if len(self.queue):
                    with self.lock:
                        url = self.queue[0]
                        del self.queue[0]
                    self.pool.add(self.get_page,(url, _filter))
                elif not self.pool.running():
                    with self.lock, self.pool._lock:
                        flag = False
                        if not (len(self.pool.tasks) or len(self.queue) or self.pool.running()):
                            flag = True
                    if flag:
                        self.pool.close()
                        break
        except KeyboardInterrupt as e:
            with self.lock, self.pool._lock:
                self.pool.close()
                self.pool.tasks.clear()
                for i in self.pool.workers: i.kill()
                self.queue.clear()
            while not all(map(lambda x:x.done, self.pool.workers)):
                pass
            self.db.close()
            _log.warning('*** ERROR: KeyboardInterrupt')
            exit(1)
        self.db.close()

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
            'hpu:d:f:l:', ['help', 'pridomain', 'testself', 'thread=', 'dbfile=', 'key='])
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
    pridomain = ('-p' in opts or '--pridomain' in opts)
    if testself:
        print('...............ok.................')
        exit()
    spider = Spider(start_url, deep, thread, dbfile, keyword, pridomain)
    spider.run()
if __name__ == '__main__':
    main()