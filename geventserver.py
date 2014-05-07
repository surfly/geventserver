#!/usr/bin/python
"""
Inside a worker, to access server's information:

    import geventserver
    geventserver.quit_handler: default SIGQUIT handler; you can cancel() it.
    geventserver.socket: listening _socket.socket() instance (do not close it!)
    geventserver.server: server instance (call its close() method to close the listening socket)

"""
# TODO:
# handle: max_accept
# handle: wsgi.multiprocess

import gevent

if gevent.version_info[:1] < 1:
    raise ImportError('Require gevent of version at least 1.0. Your version is %r' % (gevent.__version__, ))

import sys
import os
import time
import signal
import traceback
import fcntl
from errno import EAGAIN
from datetime import timedelta
import gc
from gevent.baseserver import parse_address
from gevent.subprocess import MAXFD, linkproxy, fork
from gevent import get_hub, getcurrent, sleep
from gevent.event import AsyncResult


server_classes = {'wsgi': 'gevent.pywsgi.WSGIServer',
                  'tcp': 'gevent.server.StreamServer'}

_shortcuts = {'gevent.pywsgi.WSGIServer': 'WSGIServer',
              'gevent.server.StreamServer': 'StreamServer'}


PREFIX = ''


class Interrupt(BaseException):
    """Raised when worker receives SIGINT"""


def log(message, *args):
    try:
        string = message % args
    except Exception:
        traceback.print_exc()
        try:
            message = '%r %% %r\n\n' % (message, args)
        except Exception:
            pass
        try:
            sys.stderr.write(PREFIX + message)
        except Exception:
            traceback.print_exc()
    else:
        sys.stderr.write(PREFIX + string + '\n')


def setproctitle(prefix, server_class_name, socket_txt, application):
    try:
        from setproctitle import setproctitle as settitle
    except ImportError:
        return
    server_class_name = _shortcuts.get(server_class_name, server_class_name)
    if application:
        application = str(application).rsplit('/', 1)[-1]
    settitle('%s: %s %s' % (prefix, socket_txt, application or server_class_name))


class Worker(object):

    TIMEOUT_TERM = 1
    TIMEOUT_INT = 1

    def __init__(self, fileno, server_class, pythonpath, setuid, setgid, application, preexec, timeout_shutdown, timeout_pipe):
        self.info = ''
        self.signal_time = None
        self.signals_sent = []
        self.killed = False

        if timeout_pipe:
            pipe_read, pipe_write = os.pipe()
            _set_cloexec_flag(pipe_read)
        try:
            args = [sys.executable, '-u', __file__,
                    '--fileno',
                    str(fileno),
                    '--server-class',
                    server_class]
            if pythonpath:
                args += ['--pythonpath', pythonpath]
            if setuid:
                args += ['--setuid', str(setuid)]
            if setgid:
                args += ['--setgid', str(setgid)]
            if preexec:
                args += ['--preexec', preexec]
            if timeout_pipe:
                args += ['--pipe', str(pipe_write), '--notify-period', str(timeout_pipe / 2.0)]
            if application:
                args.append(application)

            self.start = time.time()
            if timeout_pipe:
                self.popen = Popen(args, pass_fds=(fileno, pipe_write,))
                loop = gevent.get_hub().loop
                fcntl.fcntl(pipe_read, fcntl.F_SETFL, os.O_NONBLOCK)
                self.io_watcher = loop.io(pipe_read, 1, ref=False)
                self.kill_timer = loop.timer(timeout_pipe + timeout_pipe + 1, timeout_pipe, ref=False)
                self.io_watcher.start(self._read_pipe)
                self.kill_timer.start(self._timeout_kill)
            else:
                self.popen = Popen(args)
        except:
            if timeout_pipe:
                os.close(pipe_read)
            raise
        finally:
            if timeout_pipe:
                os.close(pipe_write)
                self.pipe_read = pipe_read

    def _timeout_kill(self):
        self.info = 'timed out; '
        gevent.spawn(self.interrupt)

    def _read_pipe(self):
        if read(self.pipe_read):
            self.kill_timer.again(self._timeout_kill)

    def _close(self):
        if self.pipe_read is not None:
            self.io_watcher.stop()
            self.kill_timer.stop()
            try:
                os.close(self.pipe_read)
            except EnvironmentError:
                pass
            self.pipe_read = None

    def send_signal(self, signum):
        try:
            self.popen.send_signal(signum)
        except OSError, ex:
            if ex.errno == 3:  # No such process
                return False
            else:
                log('kill(%r, %r): %s', self.popen.pid, signum, ex)
        else:
            self.signals_sent.append(signum)
        return True

    def kill(self):
        if self.killed:
            return

        self._close()
        self.killed = True

        if self.popen.poll() is not None:
            return

        if not self.send_signal(signal.SIGQUIT):
            return

        self.popen.wait(self.timeout_shutdown)

        if self.popen.poll() is not None:
            return

        self.send_signal(signal.SIGTERM)

        self.popen.wait(self.TIMEOUT_TERM)

        if self.popen.poll() is not None:
            return

        if not self.send_signal(signal.SIGKILL):
            return

        self.popen.wait(5)
        assert self.popen.poll() is not None

    def interrupt(self):
        if self.killed:
            return

        self._close()
        self.killed = True

        if self.popen.poll() is not None:
            return

        if not self.send_signal(signal.SIGINT):
            return

        self.popen.wait(self.TIMEOUT_INT)

        if self.popen.poll() is not None:
            return

        self.send_signal(signal.SIGTERM)

        self.popen.wait(self.TIMEOUT_TERM)

        if self.popen.poll() is not None:
            return

        if not self.send_signal(signal.SIGKILL):
            return

        self.popen.wait(5)
        assert self.popen.poll() is not None

    def report(self, status):
        current = time.time()
        info = self.info + 'running time: ' + format_time(current - self.start)

        if self.signal_time is not None:
            exit_time = current - self.signal_time
            if exit_time > 1:
                info += '; exited in ' + format_time(exit_time)

        if not self.signals_sent:
            info += '; unexpected'
        elif self.signals_sent == [signal.SIGQUIT]:
            pass
        else:
            info += '; signalled=%s' % ','.join(str(x) for x in self.signals_sent)

        if not status:
            log('Worker %s exited cleanly (%s)', self.popen.pid, info)
        elif os.WIFSIGNALED(status):
            log('Worker %s killed by signal %s (%s)', self.popen.pid, os.WTERMSIG(status), info)
        else:
            log('Worker %s exited with status %s (%s)', self.popen.pid, os.WEXITSTATUS(status), info)


def read(fd):
    result = False
    while True:
        try:
            data = os.read(fd, 65536)
        except OSError, ex:
            if ex.errno == EAGAIN:
                break
            raise
        if data:
            result = True
        else:
            break
    return result


class RateCount(object):

    def __init__(self, period):
        self.period = period
        assert self.period
        self.reset()

    def reset(self):
        self.time = 0
        self.count = 0

    def _time(self):
        return int(time.time() / self.period)

    def getcount(self):
        if self._time() == self.time:
            return self.count
        else:
            return 0

    def incr(self):
        current = self._time()
        if current == self.time:
            self.count += 1
        else:
            self.time = current
            self.count = 1


class Master(object):
    """Master process that forks off and manages one or more worker processes"""
    backlog = 5

    # when number of forks per minute becomes bigger than the number of workers requested
    # an additional delay is introduced between forks
    fork_wait = 5

    DEFAULT_TIMEOUT_SHUTDOWN = 32
    DEFAULT_TIMEOUT_PIPE = 16

    def __init__(self, address,
                 application=None,
                 server_class='wsgi',
                 number=1,
                 user=None,
                 pythonpath=None,
                 preexec=None,
                 respawn=True,
                 timeout_shutdown=None,
                 timeout_pipe=None):
        self.server_class = server_class
        self.family, self.address = parse_address(address)
        self.application = application
        self.number = number
        self.setuid, self.setgid = self.get_uidgid(user)
        self.pythonpath = pythonpath
        self.preexec = preexec
        self.respawn = respawn

        if timeout_shutdown is None:
            timeout_shutdown = self.DEFAULT_TIMEOUT_SHUTDOWN
        self.timeout_shutdown = timeout_shutdown

        if timeout_pipe is None:
            timeout_pipe = self.DEFAULT_TIMEOUT_PIPE
        self.timeout_pipe = timeout_pipe

        self.workers = []
        self.greenlet = None
        self.last_exit_code = None
        self.fork_rate = RateCount(60)

    def handle_term(self):
        log('(SIGTERM) Closing listener and shutting down workers')
        self.close()

    def handle_hup(self):
        log('(SIGHUP) Replacing workers')
        self.upgrade()

    def setproctitle(self):
        setproctitle('geventserver master', self.server_class, ':'.join(str(x) for x in self.address), self.application)

    def setsignals(self):
        gevent.signal(signal.SIGTERM, self.handle_term)
        gevent.signal(signal.SIGHUP, self.handle_hup)

    def setup_and_wait(self):
        self.setproctitle()
        os.setpgrp()
        self.setsignals()
        self.start()
        gevent.wait()
        if self.last_exit_code:
            log('Exiting with code %s.' % self.last_exit_code)
            sys.exit(self.last_exit_code)
        else:
            log('Exiting cleanly.')

    def start_listening(self):
        socket = self.get_listener(self.address, family=self.family)
        socket_txt = ':'.join(str(x) for x in socket.getsockname())
        log('Listening on %s', socket_txt)
        self.fileno = socket.fileno()
        self._socket = socket  # keep ref

    def get_listener(self, address, family):
        from gevent.server import _tcp_listener
        return _tcp_listener(address, backlog=self.backlog, reuse_addr=1, family=family)

    def get_uidgid(self, user):
        if not user:
            return None, None
        import pwd
        pw = pwd.getpwnam(user)
        return pw.pw_uid, pw.pw_gid

    def close(self):
        if self.fileno is None:
            return
        os.close(self.fileno)
        self.fileno = None
        for worker in self.workers:
            gevent.spawn(worker.kill)

    def start(self):
        self.start_listening()
        self.schedule_fork()

    def schedule_fork(self):
        if len(self.workers) >= self.number:
            return
        if self.fileno is None:
            return
        if self.greenlet:
            return
        if self.fork_rate.getcount() > self.number:
            log("WARNING: fork() is called too often, next one will be delayed")
            delay = self.fork_wait
        else:
            delay = 0
        self.greenlet = gevent.spawn_later(delay, self.do_fork)

    def do_fork(self):
        if len(self.workers) >= self.number:
            return
        if self.fileno is None:
            return
        worker = Worker(self.fileno,
                        self.server_class,
                        self.pythonpath,
                        self.setuid,
                        self.setgid,
                        self.application,
                        preexec=self.preexec,
                        timeout_shutdown=self.timeout_shutdown,
                        timeout_pipe=self.timeout_pipe)
        self.fork_rate.incr()
        self.workers.append(worker)
        worker.popen.result.rawlink(lambda *args: self.on_death(worker))
        self.greenlet = None
        self.schedule_fork()

    def on_death(self, worker):
        try:
            self.workers.remove(worker)
        except ValueError:
            pass

        status = worker.popen._watcher.rstatus
        worker.report(status)

        if os.WIFEXITED(status):
            self.last_exit_code = os.WEXITSTATUS(status)

        if self.respawn:
            self.schedule_fork()
        else:
            self.close()

    def upgrade(self):
        period = 0
        for worker in self.workers:
            if worker.killed:
                continue
            gevent.spawn_later(period, worker.kill)
            period += 0.05
        self.fork_rate.reset()


def format_time(seconds):
    result = str(timedelta(seconds=seconds))
    if result[-7:-6] == '.' and result[-6:].isdigit():
        result = result[:-3]
    if result.startswith('0:'):
        result = result[2:]
    return result


def _set_cloexec_flag(fd, cloexec=True):
    try:
        cloexec_flag = fcntl.FD_CLOEXEC
    except AttributeError:
        cloexec_flag = 1

    old = fcntl.fcntl(fd, fcntl.F_GETFD)
    if cloexec:
        fcntl.fcntl(fd, fcntl.F_SETFD, old | cloexec_flag)
    else:
        fcntl.fcntl(fd, fcntl.F_SETFD, old & ~cloexec_flag)


class Popen(object):
    # copy of gevent.subprocess.Popen; won't be needed once gevent's subprocess supports pass_fds

    def __init__(self, args, pass_fds=()):
        hub = get_hub()
        self._loop = hub.loop
        self.pid = None
        self.returncode = None
        self.result = AsyncResult()
        self._execute_child(args, pass_fds)

    def __repr__(self):
        return '<%s at 0x%x pid=%r returncode=%r>' % (self.__class__.__name__, id(self), self.pid, self.returncode)

    def _on_child(self, watcher):
        watcher.stop()
        status = watcher.rstatus
        if os.WIFSIGNALED(status):
            self.returncode = -os.WTERMSIG(status)
        else:
            self.returncode = os.WEXITSTATUS(status)
        self.result.set(self.returncode)

    def rawlink(self, callback):
        self.result.rawlink(linkproxy(callback, self))

    def _execute_child(self, args, pass_fds):
        executable = args[0]

        self._loop.install_sigchld()

        gc_was_enabled = gc.isenabled()
        # Disable gc to avoid bug where gc -> file_dealloc ->
        # write to stderr -> hang.  http://bugs.python.org/issue1336
        gc.disable()
        try:
            self.pid = fork()
        except:
            if gc_was_enabled:
                gc.enable()
            raise

        if self.pid == 0:

            _close_fds(pass_fds)

            try:
                os.execvp(executable, args)
            finally:
                os._exit(1)

        # Parent
        self._watcher = self._loop.child(self.pid)
        self._watcher.start(self._on_child, self._watcher)

        if gc_was_enabled:
            gc.enable()

    def poll(self):
        if self.returncode is None:
            if get_hub() is not getcurrent():
                sig_pending = getattr(self._loop, 'sig_pending', True)
                if sig_pending:
                    sleep(0.00001)
        return self.returncode

    def wait(self, timeout=None):
        return self.result.wait(timeout=timeout)

    def send_signal(self, sig):
        os.kill(self.pid, sig)


def _close_fds(pass_fds):
    pass_fds = sorted(pass_fds)
    previous = 3
    for fd in pass_fds:
        os.closerange(previous, fd)
        previous = fd + 1
    os.closerange(previous, MAXFD)


def die_if_parent_dies(signum):
    # this is linux-only
    # QQQ: add cross-platform that sets up a periodic check of ppid
    if 'linux' not in sys.platform:
        return
    try:
        import ctypes
        libc = ctypes.CDLL('libc.so.6', use_errno=True)
        PR_SET_PDEATHSIG = 1
        result = libc.prctl(PR_SET_PDEATHSIG, signum)
        if result == 0:
            return True
        else:
            log('prctl failed: %s', os.strerror(ctypes.get_errno()))
    except StandardError, ex:
        log(str(ex))


def _reseed_random():
    if 'random' not in sys.modules:
        return
    import random
    from binascii import hexlify
    try:
        seed = long(hexlify(os.urandom(16)), 16)
    except NotImplementedError:
        seed = int(time.time() * 1000) ^ os.getpid()
    random.seed(seed)


def drop_privileges(setuid, setgid):
    if setgid is not None:
        os.setgid(setgid)
    if setuid is not None:
        os.setuid(setuid)


def worker_handle_quit(server):
    if server is None:
        sys.exit(103)  # 100 + SIGQUIT
    server.close()
    activecnt = getattr(gevent.get_hub().loop, 'activecnt', 'n/a')
    log('(SIGQUIT) Closed listener (active watchers: %s)', activecnt)


def worker_handle_term(*args):
    activecnt = getattr(gevent.get_hub().loop, 'activecnt', 'n/a')
    log('(SIGTERM) Dying now (active watchers: %s).', activecnt)
    os._exit(115)  # 100 + SIGTERM


def worker_handle_int(*args):
    current = gevent.getcurrent()
    hub = gevent.get_hub()
    activecnt = getattr(hub.loop, 'activecnt', 'n/a')
    if current is hub:
        log('(SIGINT) Interrupting run (active watchers: %s)', activecnt)
        current.parent.throw(Interrupt('Interrupted by SIGINT'))
    else:
        log('(SIGINT) Interrupting %r (active watchers: %s)', current, activecnt)
        raise Interrupt('Interrupted by SIGINT')


def worker_main():
    global PREFIX
    PREFIX = '[W%05d] ' % os.getpid()
    import optparse
    parser = optparse.OptionParser()
    parser.add_option('--fileno', type=int)
    parser.add_option('--pipe', type=int)
    parser.add_option('-u', '--user')
    parser.add_option('-s', '--server-class', default='wsgi')
    parser.add_option('--preexec')
    parser.add_option('--pythonpath', help='Colon-separated entries to prepend to sys.path')
    parser.add_option('--setuid', type=int)
    parser.add_option('--setgid', type=int)
    parser.add_option('--maxaccept', type=int)
    parser.add_option('--notify-period', type=float)
    options, args = parser.parse_args()
    if args:
        if len(args) > 1:
            sys.exit('Too many arguments. Expected at most one: application')
        application_name = args[0]
    else:
        application_name = None
    server_class = server_classes.get(options.server_class, options.server_class)
    if server_class in server_classes.values():
        if application_name is None:
            sys.exit('Expected argument: application or handler')

    if options.pipe:
        fcntl.fcntl(options.pipe, fcntl.F_SETFL, os.O_NONBLOCK)

    if options.pythonpath:
        sys.path = options.pythonpath.split(':') + sys.path

    import _socket
    socket = _socket.fromfd(options.fileno, _socket.AF_INET, _socket.SOCK_STREAM)
    # _socket.fromfd dups the file descriptor; it does not seem possible to avoid it
    # therefore close the old filenot that we no longer need
    os.close(options.fileno)
    del options.fileno

    import geventserver
    geventserver.socket = socket

    from gevent.hub import _import

    if options.preexec:
        preexec = _import(options.preexec)
        preexec()

    socket_txt = ':'.join(str(x) for x in socket.getsockname())
    setproctitle('geventserver worker', options.server_class, socket_txt, application_name)

    drop_privileges(options.setuid, options.setgid)

    # master must always outlive its workers; if the master died, something
    # awful happened and SIGKILL for worker is warranted
    # Note, that die_if_parent_dies must be called after dropping privileges,
    # otherwise it does not work
    die_if_parent_dies(signal.SIGKILL)
    ppid = os.getppid()
    if ppid == 1:
        sys.exit(PREFIX + 'ppid cannot be 1')

    _reseed_random()

    from gevent import monkey
    monkey.patch_all()

    geventserver.quit_handler = gevent.signal(signal.SIGQUIT, worker_handle_quit, None)

    signal.signal(signal.SIGTERM, worker_handle_term)
    signal.signal(signal.SIGINT, worker_handle_int)
    # signal handlers are installed before importing user code so that imported modules could override signals

    geventserver.looptimer = LoopTimer()

    server_class = _import(options.server_class)
    if application_name:
        application = _import(application_name)
        args = (socket, application)
    else:
        application = None
        args = (socket, )

    server = server_class(*args)
    if options.maxaccept:
        # XXX unused currently?
        server.max_accept = options.max_accept

    geventserver.server = server

    server.start()
    geventserver.quit_handler.args = (server, )

    log('Serving %s', application_name or options.server_class)

    if callable(getattr(application, 'on_start', None)):
        application.on_start()

    if options.pipe is not None:
        gevent.get_hub().loop.timer(0, options.notify_period, ref=False).start(write_pipe, options.pipe)

    try:
        gevent.wait()
    except Interrupt:
        sys.exit(102)  # 100 + SIGINT
    else:
        log('Exiting cleanly.')


class LoopTimer(object):
    """Time how much a loop iteration takes.

    Report it every report_period seconds if it's over the threshold.
    """

    def __init__(self, threshold=0.2, report_period=10):
        self.report_period = report_period
        self.threshold = threshold
        loop = gevent.get_hub().loop
        self.finish = loop.prepare(priority=loop.MINPRI, ref=False)
        self.start = loop.check(priority=loop.MAXPRI, ref=False)
        self.loop = loop
        self.init_counters()
        self.iter_start = None

    def start(self):
        self.finish.start(self.on_iter_finish)
        self.start.start(self.on_iter_start)

    def stop(self):
        self.finish.stop()
        self.start.stop()

    def init_counters(self):
        self.period_end = time.time() + self.report_period
        self.max = 0
        self.count = 0
        self.above_count = 0
        self.total = 0

    def flush_counters(self):
        self.log()
        self.init_counters()

    def log(self):
        if self.max < self.threshold:
            return
        avg = self.total / self.count
        loop = self.loop._format()
        m = 'WARNING: block time: avg=%.1fms max=%.1fms above=%d/%d (%s)'
        log(m, avg * 1000., self.max * 1000., self.above_count, self.count, loop)

    def on_iter_start(self):
        self.iter_start = time.time()

    def on_iter_finish(self):
        if self.iter_start is None:
            return
        current = time.time()
        delta = current - self.iter_start
        if delta > self.max:
            self.max = delta
        if delta >= self.threshold:
            self.above_count += 1
        self.count += 1
        self.total += delta
        if current > self.period_end:
            self.flush_counters()


def write_pipe(pipe):
    try:
        try:
            os.write(pipe, 'x')
        except EnvironmentError, ex:
            if ex.errno == 32:  # Broken pipe
                os._exit(132)
            raise
    except Exception, ex:
        sys.exit('Failed to write to pipe: %s' % ex)


def master_main():
    global PREFIX
    PREFIX = '[Master] '
    import optparse
    parser = optparse.OptionParser()
    parser.add_option('-b', '--bind')
    parser.add_option('-u', '--user')
    parser.add_option('-n', '--workers', type=int, default=1)
    parser.add_option('-s', '--server-class', default='wsgi')
    parser.add_option('--timeout-shutdown')
    parser.add_option('--timeout-pipe')
    parser.add_option('--pythonpath', help='Colon-separated entries to prepend to sys.path')
    parser.add_option('--preexec')
    parser.add_option('--no-respawn', dest='respawn', default=True, action='store_false')
    parser.add_option('--test-arguments', action='store_true', help='Parse command-line arguments and exit')

    options, args = parser.parse_args()

    if args:
        if len(args) > 1:
            sys.exit('Too many arguments. Expected at most one: application')
        application = args[0]
    else:
        application = None

    server_class = server_classes.get(options.server_class, options.server_class)
    if server_class in server_classes.values():
        if application is None:
            sys.exit('Expected argument: application or handler')

    if not options.bind:
        sys.exit('Please provide --bind')

    master = Master(options.bind,
                    application=application,
                    server_class=server_class,
                    number=options.workers,
                    user=options.user,
                    pythonpath=options.pythonpath,
                    preexec=options.preexec,
                    respawn=options.respawn,
                    timeout_shutdown=options.timeout_shutdown,
                    timeout_pipe=options.timeout_pipe)
    if options.test_arguments:
        return
    master.setup_and_wait()


if __name__ == '__main__':
    if '--fileno' in sys.argv:
        worker_main()
    else:
        master_main()
