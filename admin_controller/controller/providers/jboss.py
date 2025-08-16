# -*- coding: utf-8 -*-
"""
Минимальный провайдер для JBoss-нод, если пул — это список хостов.
count_enabled: пингуем health-URL или CLI read-attribute server-state.
set_state: можно выключать ноду из балансера через внешние механизмы
(например, HAProxy/nginx), либо выполнить cli-команду suspend/shutdown.
Здесь показан CLI-скелет; адаптируйте под ваш домен/standalone.
"""
import subprocess
from typing import Tuple

def _run(cmd: list) -> str:
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.decode("utf-8","ignore"))
    return r.stdout.decode("utf-8","ignore")

def count_enabled(conf) -> Tuple[int,int]:
    # Простейшая эвристика: считаем нодой "enabled", если cli вернул server-state=running
    cli = conf.get("cli","/opt/jboss/bin/jboss-cli.sh")
    hosts = conf["hosts"]
    enabled = 0
    for h in hosts:
        try:
            # пример: /host=h1/server-config=server-one:read-attribute(name=status)
            out = _run([cli,"--connect",f"/host={h}/server-config=server-one:read-attribute(name=status)"])
            if "running" in out:
                enabled += 1
        except Exception:
            pass
    return (enabled, len(hosts))

def set_state(conf, server: str, action: str):
    cli = conf.get("cli","/opt/jboss/bin/jboss-cli.sh")
    host = server  # server: имя хоста из списка
    if action == "enable":
        _run([cli,"--connect",f"/host={host}/server-config=server-one:start()"])
    elif action in ("disable","drain"):
        # drain: suspend; disable: stop (упростим)
        if action == "drain":
            _run([cli,"--connect",f"/host={host}/server-config=server-one:suspend(timeout=60)"])
        _run([cli,"--connect",f"/host={host}/server-config=server-one:stop()"])
    else:
        raise ValueError("unknown action")
