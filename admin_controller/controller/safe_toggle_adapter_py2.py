#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Адаптер для старого Py2-монитора: вызывает Py3-скрипт через subprocess.
"""

import os
import subprocess

PY3  = os.environ.get("PY3_BIN", "/usr/bin/python3")
SAFE = os.environ.get("SAFE_TOGGLE", "/tmp/pattern_controller/controller/safe_haproxy_toggle.py")

def haproxy_safe(action, backend, server):
    # action in {"drain","disable","enable"}
    cmd = [PY3, SAFE, "--action", action, "--backend", backend, "--server", server]
    try:
        # устойчивый вызов (не падаем при ненулевом коде, логи пишет сам safe_haproxy_toggle)
        subprocess.call(cmd)
    except Exception:
        pass
