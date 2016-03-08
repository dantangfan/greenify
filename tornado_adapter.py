# coding:utf-8

__author__ = 'huangjin'

import tornado.ioloop
import tornado.gen
import greenlet
import sys


class Watcher(object):
    """
    这里传递过来的都是一些就绪的事件
    watcher 用来替换 libev 中的 watcher
    直接实例化一个 Watcher(fd, event) 对象，可以替换 greenify.pyx.wait_gevent 中的 hub.loop.io(fd, event)
    """
    def __init__(self, fd, event):
        self._fd = fd
        self._event = tornado.ioloop.IOLoop.READ if event == 1 else tornado.ioloop.IOLoop.WRITE  # 因为 greenify 中 libgreenify.c 只定义了两种事件
        self._greenlet = greenlet.getcurrent()
        self._parent = self._greenlet.parent
        self._ioloop = tornado.ioloop.IOLoop.current()
        self._callback = None
        self._args = None
        self._kwargs = None

    def start(self, callback, *args, **kwargs):
        self._callback = callback
        self._args = args
        self._kwargs = kwargs
        self._ioloop.add_handler(self._fd, self._handle_event, self._event)

    def _handle_event(self, *args, **kwargs):
        self._callback(*self._args, **self._kwargs)

    def stop(self):
        # 到此为止，处理完一个io事件
        self._ioloop.remove_handler(self._fd)


class Waiter(object):
    def __init__(self):
        self._greenlet = greenlet.getcurrent()
        self._parent = self._greenlet.parent

    def switch(self, unique):
        # 把控制权限交给当前这个写程
        # 这个函数往往作为回调函数使用
        # 在gevent中，只有hub有资格调用这个函数
        # 在这里随便谁都可调用(其实也只有注册在tornado.ioloop中的callback可用调用)
        self._greenlet.switch(unique)

    def get(self):
        # 这里仅仅需要将控制权交给父协程
        # 当前协程就在本函数调用的地方开始挂起
        # 实际挂起时间是在return之前
        # 挂起之后，事件循环(这里指的是tornado的ioloop)会监听watcher中指定的描述符
        # 一旦可用描述符被执行完毕，会调用Watcher._handler_event，而正好，这里的callback参数就是Watier.switch
        # Waiter.switch会直接返回unique(其实会返回所有传入的参数，见greenlet.greenlet.switch注释)
        # 这个return值会直接返回到父greenlet中，而不管是否当前greenlet是否已经执行完。也就是直接返回到了get挂起的地方
        return self._parent.switch()

    def clear(self):
        self._greenlet = None
        self._parent = None


class TorGreenlet(greenlet.greenlet):
    def __init__(self, run=None, *args, **kwargs):
        super(TorGreenlet, self).__init__()
        self._run = run
        self._args = args
        self._kwargs = kwargs
        self._future = tornado.gen.Future()

    def run(self):
        try:
            result = self._run(*self._args, **self._kwargs)
            self._future.set_result(result)
        except:
            exex_info = sys.exc_info()
            self._future.set_exc_info(exex_info)

    @classmethod
    def spawn(cls, callable_obj, *args, **kwargs):
        g = TorGreenlet(callable_obj, *args, **kwargs)
        # 调用switch，开始执行这个 greenlet
        # 在这个c-based-socket协程的执行中，如果遇到IOblock，会让出权限给root-greenlet，也就是主程序
        g.switch()
        return g._future


def spawn(callable_obj, *args, **kwargs):
    # 首先生成一个TorGreenlet对象g，然后执行其start函数
    # start函数会执行这里的 callable_obj ，这里我们的callable_obj会是一个C-based-Socket对象（因为本库也只对这个起作用）
    # greenify获取到这个socket，然后开始执行patch过的socket
    # 最终返回一个 Future 对象，让给 yield 解析
    return TorGreenlet.spawn(callable_obj, *args, **kwargs)
