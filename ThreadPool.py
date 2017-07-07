# 线程池封装

import threading as _t
from time import sleep

__all__ = ('Pool', '_Task', '_Worker', 'Lock')

Lock = _t.Lock

class _Task(object):
    '''任务执行类
    对象属性说明：
    result  *           执行结果
    func    function    任务包装函数
    is_done bool        是否已执行
    '''

    def __init__(self, func, args=(), kwargs={}):
        '''参数说明：
        func   callable    可执行对象
        args   iterable    参数，可选
        kwargs dict        关键字参数，可选
        '''
        self.func = lambda:func(*args, **kwargs) # 任务包装函数
        self.result = None # 执行结果
        self.is_done = False # 是否已执行

    def __call__(self):
        '''执行任务'''
        self.result = self.func()
        self.is_done = True

class _Worker(_t.Thread):
    '''任务监听线程类
    对象属性说明：
    lock    threading.Lock()    锁
    tasks   list                任务列表
    running bool                监听线程是否正在执行任务
    done    bool                监听线程是否执行完毕
    '''

    def __init__(self, lock, tasks, pool):
        '''参数说明：
        lock  threading.Lock()    锁
        tasks list                任务列表
        pool  Pool                线程池
        '''
        self.lock = lock
        self.tasks = tasks
        self.pool = pool
        self.running = False # 监听线程是否正在执行任务
        self.done = False # 监听线程是否执行完毕
        self.killed = False
        super().__init__()

    def kill(self):
        self.killed = True

    def run(self):
        sleep(0.1)
        while True: # 循环监听任务列表
            if self.killed:
                self.done = True
                return
            if len(self.tasks) == 0:
                if self.pool.closed: # 无任务时若线程池关闭则退出
                    self.done = True
                    return
                continue
            with self.lock:
                if len(self.tasks) > 0:
                    #print('I am', self.name)
                    func = self.tasks[0]
                    del self.tasks[0]
                else:
                    func = None
            if func:
                self.running = True
                func()
                self.running = False

class Pool(object):
    '''线程池类
    对象属性说明：
    num     int     线程数
    tasks   list    任务对象列表
    results list    map执行结果
    closed  bool    线程池是否关闭
    workers list    任务监听线程对象列表
    '''

    def __init__(self, num):
        '''参数说明：
        num int    线程数
        '''
        self._lock = _t.Lock()
        if num < 1: num = 1
        self.num = num
        self.tasks = []
        self.results = None
        self.closed = False
        self.workers = [_Worker(self._lock, self.tasks, self) for i in range(num)]
        for i in self.workers: i.start()

    def running(self):
        '''返回同时执行任务的数量'''
        return sum(map(lambda x:int(x.running), self.workers))

    def close(self):
        '''关闭线程池，添加任务后必须关闭，否则程序将不会退出，
        建议使用with结构语句，线程池将自动关闭。
        线程池关闭后则不能添加新任务。
        '''
        self.closed = True

    def join(self):
        '''等待所有任务执行完毕'''
        assert self.closed, 'The pool must be closed'
        while True:
            if all(map(lambda x:x.done, self.workers)):
                return

    def add(self, func, args=(), kwargs={}):
        '''添加任务，返回任务对象
        参数说明：
        func   callable    可执行对象
        args   iterable    参数
        kwargs dict        关键字参数
        '''
        assert not self.closed, 'The pool must not be closed'
        task = _Task(func, args, kwargs)
        with self._lock:
            self.tasks.append(task)
        return task

    def map(self, func, *iterables, async=False):
        '''多线程版map函数
        参数说明：
        func       function    要执行的函数
        *iterables iterable    要操作的列表
        async      bool        是否异步执行，非异步执行则返回结果
        '''
        if not iterables:
            return ()
        iterables = list(map(lambda x:x if type(x) in (list, tuple) else list(x), iterables))
        with self._lock:
            maps = []
            for i in range(len(iterables[0])):
                args = [iterables[key][i] for key in range(len(iterables))]
                task = _Task(func, args)
                maps.append(task)
                self.tasks.append(task)
        if async:
            _t.Thread(target=self._map_fetch, args=(maps,), daemon=True).start()
        else:
            return self._map_fetch(maps)
    
    def map_async(self, func, *iterables):
        '''多线程版map函数，异步执行
        参数说明：
        func       function    要执行的函数
        *iterables iterable    要操作的列表
        '''
        self.map(func, *iterables, async=True)

    def _map_fetch(self, maps):
        '''获取map执行结果
        参数说明：
        maps iterable    任务对象列表
        '''
        while True:
            if all(map(lambda x:x.is_done, maps)):
                results = list(map(lambda x:x.result, maps))
                self.results = results
                return results

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

if __name__ == '__main__':
    with Pool(5) as p:
        p.add(print, ('test1', 'test1'))
        p.add(print, ('test2', 'test2'))
        p.map_async(print, range(10))
    print('running:',p.running())
    p.join()
    print('All done!')