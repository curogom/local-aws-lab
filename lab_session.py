"""Serialize scripts sharing the fixed local ports across repository checkouts."""
import fcntl
import os
from pathlib import Path
import stat
import sys


def lock_path():
    cache = Path(os.environ.get('XDG_CACHE_HOME', Path.home() / '.cache'))
    return cache / 'local-aws-lab' / '9298-19092.lock'


def inherited_lock(path):
    value = os.environ.get('LOCAL_AWS_LAB_LOCK_FD')
    if value is None:
        return None
    fd = int(value)
    opened, current = os.fstat(fd), path.stat()
    if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
        raise ValueError('Invalid inherited lab lock')
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return fd


def main():
    path = lock_path()
    checking = sys.argv[1:] == ['--check']
    try:
        fd = inherited_lock(path)
        if fd is None:
            if checking:
                raise ValueError('Missing inherited lab lock')
            path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if checking:
            return 0
        script = Path(sys.argv[1]).resolve(strict=True)
        os.set_inheritable(fd, True)
        env = dict(os.environ, LOCAL_AWS_LAB_LOCK_FD=str(fd))
        os.execvpe(str(script), [str(script), '--lab-lock-held', *sys.argv[2:]], env)
    except BlockingIOError:
        print('Another local AWS lab session is running; no containers were changed.', file=sys.stderr)
        return 75
    except (OSError, ValueError, IndexError):
        print('Cannot acquire or validate the local AWS lab session lock.', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
