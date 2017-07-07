# 封装了sqlite数据库增删改查等常用操作，线程安全

import sqlite3
import threading  as _t

__all__ = ('DbHandler',)

class DbHandler(object):
    __lock = _t.Lock()
    _instance = None    # 唯一实例
    _dbname = 'data.db' # 数据库文件名

    # ***单例模式
    def __new__(cls, *args, **kwargs):
        if cls._instance == None:
            with cls.__lock:
                if cls._instance == None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, dbname=None):
        if dbname: type(self)._dbname = dbname
        self.conn = sqlite3.connect(self._dbname, check_same_thread=False)
        self.lock = _t.Lock()

    # 关闭数据库链接，初始化类
    def close(self):
        self.conn.close()
        cls = type(self)
        cls._instance = None
        cls._dbname = 'data.db'

    # 改变数据库，注意在实例化对象之前使用本方法，否则
    # 需要重新实例化对象
    # dbname str 数据库文件名
    @classmethod
    def change_db(cls, dbname):
        if cls._dbname != dbname:
            cls._dbname = dbname
            cls._instance = super().__new__(cls, *(), **{})

    # 递归转义sql语句
    # data str/list/tuple/* 要转义的数据，例："I'm Li Ming."，
    # '"Hello!"'，['Nice"', "'to", ('\\meet', '"you')]
    @classmethod
    def _addslashes(cls, data):
        t = type(data)
        if t == str:
            if data == 'NULL':
                return '\\NULL'
            handle_chars = ["'", "\"" , "\\"]
            bfs = []
            for i in data:
                if i in handle_chars:
                    bfs.append('\\')
                bfs.append(i)
            return ''.join(bfs)
        if t == list or t == tuple:
            return tuple(map(lambda x:cls._addslashes(x), data))
        return data

    # 向数据库中插入数据，返回影响行数
    # table   str          要操作的表名
    # colimns str/iterable 要操作的列，例："name,age"，['name', 'age']
    # data    iterable     要插入的数据，例：[('Li Ming', 18), ('Wang Mei', 19)]
    def insert(self, table, columns, data):
        table, columns, data = self._addslashes((table, columns, data))
        sql = "insert into '%s' (%s) values %s" % (
            table,
            columns if type(columns)==str else ','.join(columns),
            ','.join(["('%s')" % "','".join(i) for i in data])
            )
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            rowcount = cursor.rowcount
            self.conn.commit()
            cursor.close()
        return rowcount

    # 向数据库中插入一行数据，返回影响行数
    # table   str          要操作的表名
    # colimns str/iterable 要操作的列，例："name,age"，['name', 'age']
    # data    iterable     要插入的数据，例：('Li Ming', 18)
    insert_line = lambda self, table, columns, data: self.insert(table, columns, (data,))

    # 从数据库中删除数据，返回影响行数
    # table str          要操作的表名
    # where str/iterable 条件，例："name='Li Ming'"，['name', 'Li Ming']
    def delete(self, table, where='1=1'):
        table, where = self._addslashes((table, where))
        sql = "delete from '%s' where %s" % (
            table,
            where if type(where)==str else ("%s='%s'" % (where[0], where[1]))
            )
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            rowcount = cursor.rowcount
            self.conn.commit()
            cursor.close()
        return rowcount

    # 从数据库中更新数据，返回影响行数
    # table str          要操作的表名
    # data  iterable     要更新的数据，例：[('name', 'Li Ming'), ('age', 18)]
    # where str/iterable 条件，例："name='Li Ming'"，['name', 'Li Ming']
    def update(self, table, data, where='1=1'):
        table, data, where = self._addslashes((table, data, where))
        sql = "update '%s' set %s where %s" % (
            table,
            ','.join([("%s='%s'" % column,value) for column,value in data]),
            where if type(where)==str else ("%s='%s'" % (where[0], where[1]))
            )
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            rowcount = cursor.rowcount
            self.conn.commit()
            cursor.close()
        return rowcount

    # 从数据库中查询数据，返回数据列表
    # table    str          要操作的表名
    # colimns  str/iterable 要操作的列，例："name,age"，['name', 'age']
    # where    str/iterable 条件，例："name='Li Ming'"，['name', 'Li Ming']
    # with_key bool         返回数据是否带有列名信息
    def select(self, table, columns='*', where='1=1', with_key=False):
        table, columns, where = self._addslashes((table, columns, where))
        sql = "select %s from '%s' where %s" % (
            columns if type(columns)==str else ','.join(columns),
            table,
            where if type(where)==str else ("%s='%s'" % (where[0], where[1]))
            )
        with self.lock:
            cursor = self.conn.cursor()
            if with_key:
                cursor.row_factory = sqlite3.Row
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
        return result

    # 从数据库中查询一行数据，返回数据列表
    # table    str          要操作的表名
    # colimns  str/iterable 要操作的列，例："name,age"，['name', 'age']
    # where    str/iterable 条件，例："name='Li Ming'"，['name', 'Li Ming']
    # with_key bool         返回数据是否带有列名信息
    def select_line(self, table, columns='*', where='1=1', with_key=False):
        try:
            return self.select(table, columns, where, with_key)[0]
        except:
            return None

    # 从数据库中查询单个数据，返回结果的第一行第一列数据
    # table    str          要操作的表名
    # colimns  str/iterable 要操作的列，例："name,age"，['name', 'age']
    # where    str/iterable 条件，例："name='Li Ming'"，['name', 'Li Ming']
    def select_one(self, table, columns='*', where='1=1'):
        try:
            return self.select(table, columns, where, False)[0][0]
        except:
            return None

    # 执行sql语句
    # sql  str      要执行的sql语句
    # call function 执行sql语句后会执行此函数，并传入cursor，可选参数，如
    # 果提供此参数则本方法最后会返回传入函数的返回值，否则将返回None。
    def execute(self, sql, call=None):
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            retval = call(cursor) if call else None
            self.conn.commit()
            cursor.close()
        return retval