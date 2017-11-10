import datetime
import errno
import logging
import os
import signal
import subprocess
import sys

from base64 import b64encode
from math import ceil


def make_salt(n=4):
    n_bytes = int(ceil(6/8. * n))
    # XXX: use only alphanumeric chars replacing +/ with random chars.
    enc = b64encode(os.urandom(n_bytes), 'TK')
    enc = enc.rstrip('=')[:n]
    return enc


def run_cmd(cmd):
    try:
        # Capture both stout and stderr.
        p = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out = p.communicate()[0]
        if p.returncode != 0:
            raise RuntimeError('Error submitting "%s" (%d): %s' % (
                ' '.join(cmd), p.returncode, out))
        return out
    except KeyboardInterrupt:
        os.kill(p.pid, signal.SIGTERM)
        sys.exit(signal.SIGTERM)


def which(program):
    def is_exe(path):
        return os.path.isfile(path) and os.access(path, os.X_OK)

    path = os.path.split(program)[0]
    if path:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            path = os.path.join(path, program)
            if is_exe(path):
                return path
    return None


def makedirs(path):
    try:
        os.makedirs(path)
        return True
    except OSError as e:
        if e.errno == errno.EEXIST:
            return False
        else:
            raise


def setup_log(level=logging.WARNING):
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
    dt = datetime.datetime.now()
    stamp = dt.strftime('%y%m%d%H%M%S')
    return stamp


def make_executable(path):
    # by Jonathon Reinhart
    # https://stackoverflow.com/questions/12791997/how-do-you-do-a-simple-chmod-x-from-within-python
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2    # copy R bits to X
    os.chmod(path, mode)
