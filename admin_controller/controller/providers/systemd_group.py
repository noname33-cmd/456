# -*- coding: utf-8 -*-
"""
Пул как набор systemd-юнитов. enable/disable: старт/стоп конкретного юнита.
server — имя юнита, например "svc-a.service".
"""
import subprocess
from typing import Tuple

def _run(cmd: list) -> str:
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.decode("utf-8","ignore"))
    return r.stdout.decode("utf-8","ignore")

def count_enabled(conf) -> Tuple[int,int]:
    units = conf["units"]
    enabled = 0
    for u in units:
        try:
            out = _run(["systemctl","is-active",u])
            if out.strip() in ("active","activating"):
                enabled += 1
        except Exception:
            pass
    return (enabled, len(units))

def set_state(conf, server: str, action: str):
    if server not in conf["units"]:
        raise RuntimeError("unknown unit")
    if action == "enable":
        _run(["systemctl","start",server])
    elif action in ("disable","drain"):
        # для drain можно сделать graceful-stop через ExecStop внутри сервиса
        _run(["systemctl","stop",server])
    else:
        raise ValueError("unknown action")
