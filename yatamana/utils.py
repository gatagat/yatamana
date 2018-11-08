"""
yatamana utility functions
"""

from __future__ import (
        division, print_function, unicode_literals, absolute_import)

import datetime
import errno
import logging
import numbers
import os
import signal
import subprocess
import sys

from base64 import b64encode
from math import ceil


def parse_walltime(value):
    """Parse walltime in the setup file.

    Parameters
    ----------
    value : int | str
        Walltime specified in any of the formats below.

    Returns
    -------
    seconds : int
        Walltime in seconds.

    Notes
    -----

    Acceptable time formats include:

    - minutes (int)
    - 'minutes'
    - 'minutes:seconds'
    - 'hours:minutes:seconds'
    - 'days-hours'
    - 'days-hours:minutes'
    - 'days-hours:minutes:seconds'
    """
    if isinstance(value, numbers.Integral):
        minutes = int(value)
        return minutes * 60
    days, hours, minutes, seconds = 0, 0, 0, 0
    if '-' in value:
        days, value = value.split('-')
        if ':' in value:
            value = value.split(':')
        else:
            value = [value]
        hours = int(value[0])
        if len(value) > 1:
            minutes = int(value[1])
        if len(value) > 2:
            seconds = int(value[2])
    elif ':' in value:
        value = value.split(':')
        if len(value) == 2:
            minutes = int(value[0])
        else:
            hours = int(value[0])
            minutes = int(value[1])
        seconds = int(value[-1])
    else:
        minutes = int(value)
    return (((days * 24 + hours) * 60) + minutes) * 60 + seconds


def make_salt(n=4):
    """Generate a random alphanumeric string.

    Arguments
    ---------
    n : int
        String length.

    Returns
    -------
    Random string of the given length.
    """
    n_bytes = int(ceil(6/8. * n))
    # XXX: use only alphanumeric chars replacing +/ with random chars.
    enc = b64encode(os.urandom(n_bytes), 'TK')
    enc = enc.rstrip('=')[:n]
    return enc


def run_cmd(cmd):
    """ Run a given command as a subprocess.

    Parameters
    ----------
    cmd : str
        Command to run.

    Returns
    -------
    out : str
        Stdout of the command.

    Raises
    ------
    RuntimeError
        If the command return a non-zero error code.

    Notes
    -----
    Special care is taken to kill both the child process and the parent when a
    keyboard interrupt is detected.
    """
    try:
        # Capture both stout and stderr.
        p = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out = p.communicate()[0]
        if p.returncode != 0:
            out = out.decode('ascii', 'ignore').encode('ascii')
            raise RuntimeError('Error running "%s" (%d): %s' % (
                ' '.join(cmd), p.returncode, out))
        return out
    except KeyboardInterrupt:
        os.kill(p.pid, signal.SIGTERM)
        sys.exit(signal.SIGTERM)


def which(name):
    """ Find a full path of an executable.

    The full path to an executable is found by scanning the contents of the
    environment variable ``$PATH``.  Similar to the ``which`` command-line
    utility

    Parameters
    ----------
    name : str
        Name of the executable, e.g., 'less'.

    Returns
    -------
    path : str
        Full filename to the executable, or None if it was not found.
    """
    def is_exe(path):
        return os.path.isfile(path) and os.access(path, os.X_OK)

    path = os.path.split(name)[0]
    if path:
        if is_exe(name):
            return name
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            path = os.path.join(path, name)
            if is_exe(path):
                return path
    return None


def makedirs(path):
    """Make directories without failing if they already exist.

    Parameters
    ----------
    path : str
        Directory to be created.

    Returns
    -------
    created : bool
        Whether or not the directories were newly created.

    Raises
    ------
    OSError
        If the directories do not exist but cannot be created.
    """
    # Allow lazy calls to makedirs without checking if path == ''.
    if not path:
        return True
    try:
        os.makedirs(path)
        return True
    except OSError as e:
        if e.errno == errno.EEXIST:
            return False
        else:
            raise


def setup_log(level=logging.WARNING):
    """
    Setup logging setting the logging level.

    Parameters
    ----------
    level : int, optional
        Root logging level.

    Returns
    -------
    log : Logger
        Root logger.
    """
    logging.basicConfig()
    log = logging.getLogger()
    log.handlers = []
    try:
        handler = logging.StreamHandler(stream=sys.stderr)
    except TypeError:
        handler = logging.StreamHandler(strm=sys.stderr)
    formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y.%m.%d %H:%M:%S'
        )
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(level)
    return log


def get_timestamp():
    """ Get current time stamp as YYYYMMDDHHMMSS.

    Returns
    -------
    stamp : str
        Current time stamp.
    """
    dt = datetime.datetime.now()
    stamp = dt.strftime('%y%m%d%H%M%S')
    return stamp


def make_executable(path):
    """ Make a file executable.

    This is intended to be a Python counterpart of ``chmod +x /some/path``.

    Parameters
    ----------
    path : str
        Full path.

    References
    ----------
    .. [1] https://stackoverflow.com/a/30463972/791435
    """
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2    # copy R bits to X
    os.chmod(path, mode)
