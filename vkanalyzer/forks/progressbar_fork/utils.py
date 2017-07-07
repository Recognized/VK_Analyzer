import io
import os
import sys
import math
import logging
import datetime

from . import six


# There might be a better way to get the epoch with tzinfo, please create
# a pull request if you know a better way that functions for Python 2 and 3
epoch = datetime.datetime(year=1970, month=1, day=1)


class StreamWrapper(object):
    '''Wrap stdout and stderr globally'''

    def __init__(self):
        self.stdout = self.original_stdout = sys.stdout
        self.stderr = self.original_stderr = sys.stderr
        self.wrapped_stdout = 0
        self.wrapped_stderr = 0

        if os.environ.get('WRAP_STDOUT'):  # pragma: no cover
            self.wrap_stdout()

        if os.environ.get('WRAP_STDERR'):  # pragma: no cover
            self.wrap_stderr()

    def wrap(self, stdout=False, stderr=False):
        if stdout:
            self.wrap_stdout()

        if stderr:
            self.wrap_stderr()

    def wrap_stdout(self):
        if not self.wrapped_stdout:
            self.stdout = sys.stdout = six.StringIO()
        self.wrapped_stdout += 1

        return sys.stdout

    def wrap_stderr(self):
        if not self.wrapped_stderr:
            self.stderr = sys.stderr = six.StringIO()
        self.wrapped_stderr += 1

        return sys.stderr

    def unwrap(self, stdout=False, stderr=False):
        if stdout:
            self.unwrap_stdout()

        if stderr:
            self.unwrap_stderr()

    def unwrap_stdout(self):
        if self.wrapped_stdout > 1:
            self.wrapped_stdout -= 1
        else:
            sys.stdout = self.original_stdout
            self.wrapped_stdout = 0

    def unwrap_stderr(self):
        if self.wrapped_stderr > 1:
            self.wrapped_stderr -= 1
        else:
            sys.stderr = self.original_stderr
            self.wrapped_stderr = 0

    def flush(self):
        if self.wrapped_stdout:
            try:
                self.original_stdout.write(self.stdout.getvalue())
                self.stdout.seek(0)
                self.stdout.truncate(0)
            except (io.UnsupportedOperation,
                    AttributeError):  # pragma: no cover
                self.wrapped_stdout = False
                logger.warn('Disabling stdout redirection, %r is not seekable',
                            sys.stdout)

        if self.wrapped_stderr:
            try:
                self.original_stderr.write(self.stderr.getvalue())
                self.stderr.seek(0)
                self.stderr.truncate(0)
            except (io.UnsupportedOperation,
                    AttributeError):  # pragma: no cover
                self.wrapped_stderr = False
                logger.warn('Disabling stderr redirection, %r is not seekable',
                            sys.stderr)


streams = StreamWrapper()
logger = logging.getLogger(__name__)


def timedelta_to_seconds(delta):
    '''Convert a timedelta to seconds with the microseconds as fraction
    >>> from datetime import timedelta
    >>> '%d' % timedelta_to_seconds(timedelta(days=1))
    '86400'
    >>> '%d' % timedelta_to_seconds(timedelta(seconds=1))
    '1'
    >>> '%.6f' % timedelta_to_seconds(timedelta(seconds=1, microseconds=1))
    '1.000001'
    >>> '%.6f' % timedelta_to_seconds(timedelta(microseconds=1))
    '0.000001'
    '''
    # Only convert to float if needed
    if delta.microseconds:
        total = delta.microseconds * 1e-6
    else:
        total = 0
    total += delta.seconds
    total += delta.days * 60 * 60 * 24
    return total


def scale_1024(x, n_prefixes):
    '''Scale a number down to a suitable size, based on powers of 1024.

    Returns the scaled number and the power of 1024 used.

    Use to format numbers of bytes to KiB, MiB, etc.

    >>> scale_1024(310, 3)
    (310.0, 0)
    >>> scale_1024(2048, 3)
    (2.0, 1)
    >>> scale_1024(0, 2)
    (0.0, 0)
    >>> scale_1024(0.5, 2)
    (0.5, 0)
    >>> scale_1024(1, 2)
    (1.0, 0)
    '''
    if x <= 0:
        power = 0
    else:
        power = min(int(math.log(x, 2) / 10), n_prefixes - 1)
    scaled = float(x) / (2 ** (10 * power))
    return scaled, power


def get_terminal_size():  # pragma: no cover
    '''Get the current size of your terminal

    Multiple returns are not always a good idea, but in this case it greatly
    simplifies the code so I believe it's justified. It's not the prettiest
    function but that's never really possible with cross-platform code.

    Returns:
        width, height: Two integers containing width and height
    '''

    try:
        # Default to 79 characters for IPython notebooks
        from IPython import get_ipython
        ipython = get_ipython()
        from ipykernel import zmqshell
        if isinstance(ipython, zmqshell.ZMQInteractiveShell):
            return 79, 24
    except Exception:  # pragma: no cover
        pass

    try:
        # This works for Python 3, but not Pypy3. Probably the best method if
        # it's supported so let's always try
        import shutil
        w, h = shutil.get_terminal_size()
        if w and h:
            # The off by one is needed due to progressbars in some cases, for
            # safety we'll always substract it.
            return w - 1, h
    except Exception:  # pragma: no cover
        pass

    try:
        w = int(os.environ.get('COLUMNS'))
        h = int(os.environ.get('LINES'))
        if w and h:
            return w, h
    except Exception:  # pragma: no cover
        pass

    try:
        import blessings
        terminal = blessings.Terminal()
        w = terminal.width
        h = terminal.height
        if w and h:
            return w, h
    except Exception:  # pragma: no cover
        pass

    try:
        w, h = _get_terminal_size_linux()
        if w and h:
            return w, h
    except Exception:  # pragma: no cover
        pass

    try:
        # Windows detection doesn't always work, let's try anyhow
        w, h = _get_terminal_size_windows()
        if w and h:
            return w, h
    except Exception:  # pragma: no cover
        pass

    try:
        # needed for window's python in cygwin's xterm!
        w, h = _get_terminal_size_tput()
        if w and h:
            return w, h
    except Exception:  # pragma: no cover
        pass

    return 79, 24


def _get_terminal_size_windows():  # pragma: no cover
    res = None
    try:
        from ctypes import windll, create_string_buffer

        # stdin handle is -10
        # stdout handle is -11
        # stderr handle is -12

        h = windll.kernel32.GetStdHandle(-12)
        csbi = create_string_buffer(22)
        res = windll.kernel32.GetConsoleScreenBufferInfo(h, csbi)
    except Exception:
        return None

    if res:
        import struct
        (_, _, _, _, _, left, top, right, bottom, _, _) = \
            struct.unpack("hhhhHhhhhhh", csbi.raw)
        w = right - left
        h = bottom - top
        return w, h
    else:
        return None


def _get_terminal_size_tput():  # pragma: no cover
    # get terminal width src: http://stackoverflow.com/questions/263890/
    try:
        import subprocess
        proc = subprocess.Popen(
            ['tput', 'cols'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        output = proc.communicate(input=None)
        w = int(output[0])
        proc = subprocess.Popen(
            ['tput', 'lines'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        output = proc.communicate(input=None)
        h = int(output[0])
        return w, h
    except Exception:
        return None


def _get_terminal_size_linux():  # pragma: no cover
    def ioctl_GWINSZ(fd):
        try:
            import fcntl
            import termios
            import struct
            size = struct.unpack(
                'hh', fcntl.ioctl(fd, termios.TIOCGWINSZ, '1234'))
        except Exception:
            return None
        return size

    size = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)

    if not size:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            size = ioctl_GWINSZ(fd)
            os.close(fd)
        except Exception:
            pass
    if not size:
        try:
            size = os.environ['LINES'], os.environ['COLUMNS']
        except Exception:
            return None

    return int(size[1]), int(size[0])


def format_time(time, precision=datetime.timedelta(seconds=1)):
    '''Formats timedelta/datetime/seconds

    >>> format_time('1')
    '0:00:01'
    >>> format_time(1.234)
    '0:00:01'
    >>> format_time(1)
    '0:00:01'
    >>> format_time(datetime.datetime(2000, 1, 2, 3, 4, 5, 6))
    '2000-01-02 03:04:05'
    >>> format_time(datetime.date(2000, 1, 2))
    '2000-01-02'
    >>> format_time(datetime.timedelta(seconds=3661))
    '1:01:01'
    >>> format_time(None)
    '--:--:--'
    >>> format_time(format_time)  # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    TypeError: Unknown type ...

    '''
    precision_seconds = precision.total_seconds()

    if isinstance(time, six.basestring + six.numeric_types):
        try:
            time = datetime.timedelta(seconds=six.long_int(time))
        except OverflowError:  # pragma: no cover
            time = None

    if isinstance(time, datetime.timedelta):
        seconds = time.total_seconds()
        # Truncate the number to the given precision
        seconds = seconds - (seconds % precision_seconds)

        return str(datetime.timedelta(seconds=seconds))
    elif isinstance(time, datetime.datetime):
        # Python 2 doesn't have the timestamp method
        seconds = timestamp(time)
        # Truncate the number to the given precision
        seconds = seconds - (seconds % precision_seconds)

        try:  # pragma: no cover
            if six.PY3:
                dt = datetime.datetime.fromtimestamp(seconds)
            else:
                dt = datetime.datetime.utcfromtimestamp(seconds)
        except ValueError:  # pragma: no cover
            dt = datetime.datetime.max
        return str(dt)
    elif isinstance(time, datetime.date):
        return str(time)
    elif time is None:
        return '--:--:--'
    else:
        raise TypeError('Unknown type %s: %r' % (type(time), time))


def timestamp(dt):  # pragma: no cover
    if hasattr(dt, 'timestamp'):
        return dt.timestamp()
    else:
        return (dt - epoch).total_seconds()
