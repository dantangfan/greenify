#from gevent.hub import get_hub, getcurrent, Waiter
#from gevent.timeout import Timeout
from greenlet import getcurrent
from tornado_adapter import Waiter, Watcher, spawn as Spawn

cdef extern from "libgreenify.h":
    struct greenify_watcher:
        int fd
        int events
    ctypedef int (*greenify_wait_callback_func_t) (greenify_watcher* watchers, int nwatchers, int timeout)
    cdef void greenify_set_wait_callback(greenify_wait_callback_func_t callback)

cdef extern from "hook_greenify.h":

    ctypedef enum greenified_function_t:
        FN_CONNECT
        FN_READ
        FN_WRITE
        FN_PREAD
        FN_PWRITE
        FN_READV
        FN_WRITEV
        FN_RECV
        FN_SEND
        FN_RECVMSG
        FN_SENDMSG
        FN_RECVFROM
        FN_SENDTO
        FN_SELECT
        FN_POLL

    void* greenify_patch_lib(const char* library_filename, greenified_function_t fn)

cdef int wait_event(greenify_watcher* watchers, int nwatchers, int timeout_in_ms) with gil:
    cdef int fd, event
    cdef float timeout_in_s
    cdef int i

    watchers_list = []
    for i in range(nwatchers):
        fd = watchers[i].fd
        event = watchers[i].events
        watcher = Watcher(fd, event)
        watchers_list.append(watcher)

        wait(watchers_list)
        return 0

def greenify():
    greenify_set_wait_callback(wait_event)

def wait(watchers):
    waiter = Waiter()
    switch = waiter.switch
    unique = object()
    try:
        count = len(watchers)
        for watcher in watchers:
            watcher.start(switch, unique)
        result = waiter.get()
        assert result is unique, 'Invalid switch into %s: %r' % (getcurrent(), result)
        waiter.clear()
        return result
    finally:
        for watcher in watchers:
            watcher.stop()

cpdef patch_lib(bytes library_path):
    cdef char* path = library_path
    cdef bint result = False
    for fn in (FN_CONNECT, FN_READ, FN_WRITE, FN_PREAD, FN_PWRITE, FN_READV,
               FN_WRITEV, FN_RECV, FN_SEND, FN_RECVMSG, FN_SENDMSG,
               FN_RECVFROM, FN_SENDTO, FN_SELECT, FN_POLL):
        if NULL != greenify_patch_lib(path, fn):
            result = True

    return result

def spawn(callable_obj, *args, **kwargs):
    return Spawn(callable_obj, *args, **kwargs)