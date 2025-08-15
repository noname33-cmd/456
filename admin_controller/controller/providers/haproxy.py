# -*- coding: utf-8 -*-
import csv, subprocess

def _run(cmd: str, socket: str) -> str:
    full = f'echo "{cmd}" | socat - UNIX-CONNECT:{socket}'
    r = subprocess.run(full, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.decode("utf-8","ignore"))
    return r.stdout.decode("utf-8","ignore")

def count_enabled(conf) -> tuple:
    socket = conf["socket"]
    out = _run("show stat -1 2 -1", socket)
    lines = [ln for ln in out.splitlines() if ln and not ln.startswith("#")]
    reader = csv.reader(lines)
    rows = list(reader)
    if not rows: return (0,0)
    headers = rows[0]
    enabled = 0; total = 0
    for r in rows[1:]:
        row = {headers[i]: (r[i] if i<len(r) else "") for i in range(len(headers))}
        sv = row.get("svname","")
        if sv in ("BACKEND",""): continue
        total += 1
        status = (row.get("status","") or "").upper()
        admin  = (row.get("admin","")  or "").upper()
        if status in ("UP","OPEN") and "MAINT" not in admin:
            enabled += 1
    return (enabled, total)

def set_state(conf, server: str, action: str):
    socket = conf["socket"]
    backend, srv = server.split("/",1) if "/" in server else (conf["backend"], server)
    if action == "enable":
        _run(f"enable server {backend}/{srv}", socket)
    elif action == "disable":
        _run(f"disable server {backend}/{srv}", socket)
    elif action == "drain":
        _run(f"set server {backend}/{srv} state drain", socket)
    else:
        raise ValueError("unknown action")
