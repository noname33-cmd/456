#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
lock_utils.py — файловые блокировки (flock) для критических секций.
Пример:
  from lock_utils import with_flock
  with with_flock(LOGS_DIR / "locks" / "job.lock"):
      # критическая секция
"""
from __future__ import annotations
import os
import errno
import contextlib
from pathlib import Path

try:
    import fcntl  # POSIX
except Exception:  # pragma: no cover
    fcntl = None  # type: ignore

@contextlib.contextmanager
def with_flock(path: str | os.PathLike, timeout_sec: float | None = None, nonblock: bool = False):
    """Эксклюзивная блокировка файла. Создаёт каталоги при необходимости."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fh = open(p, "a+", encoding="utf-8")
    try:
        if fcntl is None:
            # На несущ. платформах — просто "глушим" (неблокирующая совместимость)
            yield fh
            return
        flags = fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblock else 0)
        if not timeout_sec or timeout_sec <= 0:
            while True:
                try:
                    fcntl.flock(fh.fileno(), flags)
                    break
                except OSError as e:
                    if e.errno in (errno.EAGAIN, errno.EACCES):
                        # подождём 50мс
                        import time as _t
                        _t.sleep(0.05)
                        continue
                    raise
        else:
            import time as _t
            end = _t.time() + float(timeout_sec)
            while True:
                try:
                    fcntl.flock(fh.fileno(), flags)
                    break
                except OSError as e:
                    if e.errno in (errno.EAGAIN, errno.EACCES):
                        if _t.time() >= end:
                            raise TimeoutError(f"flock timeout: {p}")
                        _t.sleep(0.05)
                        continue
                    raise
        try:
            yield fh
        finally:
            try:
                fh.flush(); os.fsync(fh.fileno())
            finally:
                try:
                    if fcntl: fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                except Exception:
                    pass
    finally:
        try: fh.close()
        except Exception: pass
