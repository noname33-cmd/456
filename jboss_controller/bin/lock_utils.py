# FILE: lock_utils.py
#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
lock_utils.py — общие файловые блокировки для Py2.7
Использование:
  from lock_utils import with_flock
  with with_flock("/tmp/pattern_controller/locks/job.lock"):
      # критическая секция
"""
import os, errno
import contextlib

try:
    import fcntl
except ImportError:
    fcntl = None

def ensure_dir(p):
    if p and not os.path.isdir(p):
        os.makedirs(p)

@contextlib.contextmanager
def with_flock(lock_path, nonblock=True):
    """
    Эксклюзивная блокировка файла (LOCK_EX). При nonblock=True — без ожидания.
    """
    ensure_dir(os.path.dirname(lock_path))
    fh = open(lock_path, "ab+")
    try:
        if fcntl is None:
            # На системах без fcntl — «мягкая» блокировка (небезопасно для нескольких хостов)
            yield fh
        else:
            flags = fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblock else 0)
            try:
                fcntl.flock(fh.fileno(), flags)
            except IOError as e:
                # занято
                if e.errno in (errno.EACCES, errno.EAGAIN):
                    raise SystemExit(0)
                raise
            try:
                yield fh
            finally:
                try:
                    fh.flush(); os.fsync(fh.fileno())
                except: pass
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                except: pass
    finally:
        try: fh.close()
        except: pass
