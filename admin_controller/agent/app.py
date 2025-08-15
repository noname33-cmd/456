#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import subprocess
from typing import Dict, Any
from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
import yaml

import haproxy as hx
import haproxy_cfg as hcfg

BASE = os.path.dirname(__file__)
CFG_PATH_YAML = os.path.join(BASE, "config.yaml")

with open(CFG_PATH_YAML, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f) or {}

# Межсервисный токен (обязателен, должен совпадать с мастером)
INTER_TOKEN = str(CONFIG.get("x_auth_token", ""))

BASE_DIR        = CONFIG.get("base_dir", "/tmp/pattern_controller")
RUNTIME_SOCK    = CONFIG.get("haproxy_runtime_socket", "/run/haproxy/admin.sock")
HAPROXY_CFG     = CONFIG.get("haproxy_cfg_path", "/etc/haproxy/haproxy.cfg")
SYNC_CFG_RELOAD = bool(CONFIG.get("sync_cfg_reload", False))

os.makedirs(BASE_DIR, exist_ok=True)

app = FastAPI(title="JBoss Controller Agent (35072)")

def check_xauth(x_auth_token: str = Header(default="")):
    if INTER_TOKEN and x_auth_token != INTER_TOKEN:
        raise HTTPException(401, "Unauthorized")

class WaitReq(BaseModel):
    timeout_sec: int = 300

class RestartReq(BaseModel):
    service: str

class CfgToggleReq(BaseModel):
    backend: str
    server: str
    enable: bool

class XAuthSetReq(BaseModel):
    token: str
    persist: bool = True

def jboss_restart(service: str):
    p = subprocess.run(["/bin/systemctl", "restart", service],
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        raise HTTPException(500, f"jboss restart failed:\n{p.stdout}")
    return p.stdout

def jboss_health_ok() -> bool:
    health: Dict[str, Any] = CONFIG.get("jboss_health") or {}
    if not health: return True
    if health.get("tcp_port"):
        import socket as pysock
        try:
            with pysock.create_connection(("127.0.0.1", int(health["tcp_port"])), timeout=2):
                return True
        except Exception:
            return False
    if health.get("http_url"):
        import httpx
        try:
            r = httpx.get(health["http_url"], timeout=2, verify=False)
            return r.status_code == 200
        except Exception:
            return False
    return True

@app.get("/health")
def health():
    # health оставляем открытым
    return {"status": "ok", "base_dir": BASE_DIR}

@app.get("/haproxy/stat", dependencies=[Depends(check_xauth)])
def haproxy_stat():
    csv = hx.show_stat_csv(RUNTIME_SOCK)
    table = hx.parse_stat(csv)  # dict[(backend,server)] = row
    nested: Dict[str, Dict[str, Dict[str, str]]] = {}
    for (bk, srv), row in table.items():
        nested.setdefault(bk, {})[srv] = row
    return {"data": nested}

@app.post("/node/{backend}/{server}/drain", dependencies=[Depends(check_xauth)])
def node_drain(backend: str, server: str):
    out = hx.set_state(RUNTIME_SOCK, backend, server, "drain")
    return {"ok": True, "out": out}

@app.post("/node/{backend}/{server}/enable", dependencies=[Depends(check_xauth)])
def node_enable(backend: str, server: str):
    out = hx.set_state(RUNTIME_SOCK, backend, server, "ready")
    if SYNC_CFG_RELOAD:
        text = hcfg.read_file(HAPROXY_CFG)
        text2 = hcfg.toggle_server_in_backend(text, backend, server, enable=True)
        if text2 != text:
            hcfg.write_atomic(HAPROXY_CFG, text2)
            hcfg.validate_and_reload(HAPROXY_CFG)
    return {"ok": True, "out": out}

@app.post("/node/{backend}/{server}/wait-empty", dependencies=[Depends(check_xauth)])
def wait_empty(backend: str, server: str, req: WaitReq):
    deadline = time.time() + req.timeout_sec
    last = -1
    while time.time() < deadline:
        csv = hx.show_stat_csv(RUNTIME_SOCK)
        table = hx.parse_stat(csv)
        row = table.get((backend, server)) or {}
        scur = int((row.get("scur") or "0") or "0")
        last = scur
        if scur == 0:
            return {"ok": True, "last_scur": last}
        time.sleep(2)
    raise HTTPException(408, f"Timeout waiting empty. last_scur={last}")

@app.post("/haproxy/reload", dependencies=[Depends(check_xauth)])
def haproxy_reload():
    hcfg.validate_and_reload(HAPROXY_CFG)
    return {"ok": True}

@app.post("/haproxy/cfg-toggle", dependencies=[Depends(check_xauth)])
def haproxy_cfg_toggle(req: CfgToggleReq):
    text = hcfg.read_file(HAPROXY_CFG)
    text2 = hcfg.toggle_server_in_backend(text, req.backend, req.server, enable=req.enable)
    if text2 != text:
        hcfg.write_atomic(HAPROXY_CFG, text2)
    hcfg.validate_and_reload(HAPROXY_CFG)
    return {"ok": True, "changed": text2 != text}

@app.post("/jboss/restart", dependencies=[Depends(check_xauth)])
def do_jboss_restart(req: RestartReq):
    out = jboss_restart(req.service)
    return {"ok": True, "out": out}

@app.post("/jboss/wait-health", dependencies=[Depends(check_xauth)])
def wait_health(req: WaitReq):
    deadline = time.time() + req.timeout_sec
    while time.time() < deadline:
        if jboss_health_ok():
            return {"ok": True}
        time.sleep(2)
    raise HTTPException(408, "Timeout waiting JBoss health")

# Приём нового межсервисного токена (мастер шлёт по "старому" X-Auth-Token)
@app.post("/xauth/set", dependencies=[Depends(check_xauth)])
def xauth_set(req: XAuthSetReq):
    global INTER_TOKEN, CONFIG
    if not req.token or len(req.token) < 6:
        raise HTTPException(400, "token too short")
    INTER_TOKEN = req.token
    if req.persist:
        with open(CFG_PATH_YAML, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        cfg["x_auth_token"] = req.token
        with open(CFG_PATH_YAML, "w", encoding="utf-8") as f:
            yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)
        CONFIG = cfg
    return {"ok": True}
