#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import time
import secrets
from typing import Dict, Any, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Depends, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
import yaml

# ========= Paths =========
BASE = os.path.dirname(__file__)
CFG  = os.path.join(BASE, "config.yaml")

# ========= Config =========
def load_config() -> Dict[str, Any]:
    if not os.path.exists(CFG):
        return {}
    with open(CFG, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

CONFIG = load_config()

def clusters() -> Dict[str, Any]:
    return CONFIG.get("clusters", {})

# Межсервисный токен (мастер → агенты)
INTER_TOKEN: str = str(CONFIG.get("x_auth_token", ""))

# Аккаунты для входа в UI (roles: root | admin)
# auth.accounts: [{name, role, token}, ...]
ACCOUNTS = CONFIG.get("auth", {}).get("accounts", [])

# ========= App =========
app = FastAPI(title="JBoss Controller Master")

# ========= Sessions / bans (для UI) =========
SESSIONS: Dict[str, Dict[str, Any]] = {}     # sid -> {exp, user, role}
SESSION_TTL_SEC = 12 * 3600                  # 12 часов

ATTEMPTS: Dict[str, int] = {}                # ip -> попытки
BANS: Dict[str, float]   = {}                # ip -> забанен_до
MAX_ATTEMPTS = 3
BAN_SEC = 5 * 60

def _client_ip(req: Request) -> str:
    return (req.client.host if req.client else "0.0.0.0")

def _is_banned(ip: str) -> bool:
    until = BANS.get(ip, 0.0)
    if until <= time.time():
        BANS.pop(ip, None)
        return False
    return True

def _fail_attempt(ip: str):
    n = ATTEMPTS.get(ip, 0) + 1
    ATTEMPTS[ip] = n
    if n >= MAX_ATTEMPTS:
        BANS[ip] = time.time() + BAN_SEC
        ATTEMPTS[ip] = 0

def _success_attempt(ip: str):
    ATTEMPTS.pop(ip, None)
    BANS.pop(ip, None)

def _new_session(user: str, role: str) -> str:
    sid = secrets.token_urlsafe(32)
    SESSIONS[sid] = {"exp": time.time() + SESSION_TTL_SEC, "user": user, "role": role}
    return sid

def _check_session(session_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not session_id:
        return None
    sess = SESSIONS.get(session_id)
    if not sess:
        return None
    if sess["exp"] <= time.time():
        SESSIONS.pop(session_id, None)
        return None
    # пролонгируем
    sess["exp"] = time.time() + SESSION_TTL_SEC
    return sess

def _find_account_by_token(tok: str) -> Optional[Tuple[str, str]]:
    for acc in ACCOUNTS:
        if str(acc.get("token", "")) == tok:
            role = acc.get("role", "")
            if role not in ("root", "admin"):
                continue
            return acc.get("name", "user"), role
    return None

# ========= RBAC helpers =========
def require_session(req: Request) -> Dict[str, Any]:
    sid = req.cookies.get("session")
    sess = _check_session(sid)
    if not sess:
        raise HTTPException(401, "Unauthorized")
    return sess

def require_admin(sess: Dict[str, Any] = Depends(require_session)) -> Dict[str, Any]:
    # admin или root
    if sess["role"] not in ("admin", "root"):
        raise HTTPException(403, "Forbidden: admin or root required")
    return sess

def require_root(sess: Dict[str, Any] = Depends(require_session)) -> Dict[str, Any]:
    if sess["role"] != "root":
        raise HTTPException(403, "Forbidden: root required")
    return sess

# ========= HTTP client → agents (с X-Auth-Token) =========
async def call_agent(url: str, method: str = "GET", json_body=None, timeout: int = 20) -> Any:
    async with httpx.AsyncClient(timeout=timeout, verify=False) as client:
        headers = {}
        if INTER_TOKEN:
            headers["X-Auth-Token"] = INTER_TOKEN
        r = await client.request(method, url, json=json_body, headers=headers)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text)
        ctype = (r.headers.get("content-type") or "").lower()
        return r.json() if ctype.startswith("application/json") else r.text

def _find_node(cid: str, name: str) -> Dict[str, Any]:
    c = clusters().get(cid)
    if not c:
        raise HTTPException(404, "Cluster not found")
    for n in c.get("nodes", []):
        if n.get("name") == name:
            return n
    raise HTTPException(404, "Node not found")

# ========= Auth pages =========
@app.get("/login", response_class=HTMLResponse)
async def login_page(req: Request):
    ip = _client_ip(req)
    banned = _is_banned(ip)
    left = max(0, int(BANS.get(ip, 0) - time.time()))
    attempts_left = MAX_ATTEMPTS - ATTEMPTS.get(ip, 0)
    warn = ""
    if banned:
        warn = f"<p style='color:#b00'>Вы заблокированы на {left} сек.</p>"
    elif not ACCOUNTS:
        warn = "<p style='color:#b00'>В master/config.yaml нет auth.accounts.</p>"
    return f"""
<!doctype html><html><head><meta charset="utf-8"><title>Login</title>
<style>
body{{font-family:Arial;margin:40px}}
form{{max-width:460px}}
input,button{{padding:8px;font-size:16px}}
button{{margin-top:10px}}
.note{{color:#666}}
</style></head><body>
<h2>Вход в JBoss Controller Master</h2>
{warn}
<form method="post" action="/login">
  <label>Токен:</label><br>
  <input name="token" type="password" placeholder="введите токен (root/admin)" required style="width:100%"/>
  <button type="submit">Войти</button>
</form>
<p class="note">Осталось попыток: {attempts_left if not banned else 0} / {MAX_ATTEMPTS}. После 3 попыток — бан на 5 минут.</p>
</body></html>
"""

@app.post("/login")
async def do_login(req: Request, token: str = Form(...)):
    ip = _client_ip(req)
    if _is_banned(ip):
        raise HTTPException(429, "Вы временно заблокированы, попробуйте позже")

    acc = _find_account_by_token(token)
    if acc:
        _success_attempt(ip)
        name, role = acc
        sid = _new_session(name, role)
        resp = RedirectResponse(url="/", status_code=303)
        resp.set_cookie(
            "session", sid,
            httponly=True, samesite="lax", secure=False, path="/", max_age=SESSION_TTL_SEC
        )
        return resp

    _fail_attempt(ip)
    return RedirectResponse(url="/login", status_code=303)

@app.get("/logout")
async def logout(req: Request):
    sid = req.cookies.get("session")
    if sid:
        SESSIONS.pop(sid, None)
    resp = RedirectResponse(url="/login", status_code=303)
    resp.delete_cookie("session", path="/")
    return resp

# ========= UI (admin/root) =========
@app.get("/", response_class=HTMLResponse)
async def ui_root(sess: Dict[str, Any] = Depends(require_admin)):
    data = json.dumps(clusters())
    role = sess["role"]; user = sess["user"]
    show_tokens_link = (role == "root")
    settings_link = '<p><a href="/settings/tokens">Управление межсервисным токеном</a></p>' if show_tokens_link else ""
    return f"""
<!doctype html><html><head><meta charset="utf-8"><title>JBoss Master</title>
<style>
body{{font-family:Arial,sans-serif;margin:20px}}
.card{{border:1px solid #ccc;padding:12px;border-radius:10px;margin:8px 0}}
button{{margin-right:8px}} h3{{margin-top:18px}}
.meta{{color:#555}}
</style></head><body>
<h2>JBoss Controller Master</h2>
<p class="meta">Пользователь: <b>{user}</b> (роль: <b>{role}</b>) — <a href="/logout">Выйти</a></p>
{settings_link}
<div id="app"></div>
<script>
const CFG = {data};
function btn(t,cb){{const b=document.createElement('button');b.textContent=t;b.onclick=cb;return b;}}
async function api(path,m="GET",body=null){{
  const r = await fetch(path, {{method:m, headers:{{'Content-Type':'application/json'}}, body: body?JSON.stringify(body):undefined}});
  if(!r.ok) throw new Error(await r.text());
  const ct = r.headers.get('content-type')||''; return ct.startsWith('application/json')? r.json(): r.text();
}}
function render(){{
  const root=document.getElementById('app'); root.innerHTML='';
  Object.entries(CFG).forEach(([cid, c])=>{{
    const h=document.createElement('h3'); h.textContent=`Cluster: ${"{"+'cid'+"}"}`.replace("{cid}", cid); root.appendChild(h);
    const cont=document.createElement('div'); root.appendChild(cont);
    (c.nodes||[]).forEach(n=>{{
      const card=document.createElement('div'); card.className='card';
      card.innerHTML = `<b>${"{"+'name'+"}"} — backend=${"{"+'bk'+"}"} server=${"{"+'sv'+"}"}`.replace("{name}",n.name).replace("{bk}",n.haproxy_backend).replace("{sv}",n.haproxy_server)+"<br>";
      card.appendChild(btn('Drain', ()=>api(`/clusters/${cid}/node/${n.name}/drain`,'POST').then(()=>alert('OK')).catch(e=>alert(e))));
      card.appendChild(btn('Wait empty', ()=>api(`/clusters/${cid}/node/${n.name}/wait-empty`,'POST',{{timeout_sec:300}}).then(()=>alert('OK')).catch(e=>alert(e))));
      card.appendChild(btn('Restart JBoss', ()=>api(`/clusters/${cid}/node/${n.name}/restart`,'POST',{{}}).then(()=>alert('OK')).catch(e=>alert(e))));
      card.appendChild(btn('Enable', ()=>api(`/clusters/${cid}/node/${n.name}/enable`,'POST').then(()=>alert('OK')).catch(e=>alert(e))));
      card.appendChild(btn('SAFE ROLL', ()=>api(`/clusters/${cid}/node/${n.name}/safe-roll`,'POST').then(()=>alert('DONE')).catch(e=>alert(e))));
      card.appendChild(btn('CFG Disable+Reload', ()=>api(`/clusters/${cid}/node/${n.name}/cfg-disable`,'POST').then(()=>alert('OK')).catch(e=>alert(e))));
      card.appendChild(btn('CFG Enable+Reload', ()=>api(`/clusters/${cid}/node/${n.name}/cfg-enable`,'POST').then(()=>alert('OK')).catch(e=>alert(e))));
      cont.appendChild(card);
    }});
  }});
}}
render();
</script></body></html>
"""

# ========= Tokens management (ONLY root) =========
@app.get("/settings/tokens", response_class=HTMLResponse)
async def tokens_page(sess: Dict[str, Any] = Depends(require_root)):
    return """
<!doctype html><html><head><meta charset="utf-8"><title>X-Auth Token</title>
<style>body{font-family:Arial;margin:30px} input,button{padding:8px;font-size:16px} form{max-width:560px}</style></head>
<body>
<h2>Межсервисный X-Auth-Token (мастер ⇄ агенты)</h2>
<form method="post" action="/settings/tokens/xauth-rotate">
  <label>Новый X-Auth-Token:</label><br>
  <input name="token" type="password" placeholder="вставь новый токен" required style="width:100%"/>
  <button type="submit">Применить (мастер + агенты)</button>
</form>
<p><a href="/">← Назад</a></p>
</body></html>
"""

@app.post("/settings/tokens/xauth-rotate")
async def xauth_rotate(token: str = Form(...), sess: Dict[str, Any] = Depends(require_root)):
    if not token or len(token) < 6:
        raise HTTPException(400, "token too short")

    global INTER_TOKEN, CONFIG
    old = INTER_TOKEN
    # 1) обновляем мастер (runtime)
    INTER_TOKEN = token
    # 2) сохраняем в YAML
    cfg = load_config()
    cfg["x_auth_token"] = token
    with open(CFG, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)
    CONFIG = cfg  # обновим кластеры/аккаунты, если правили

    # 3) разослать агентам по старому токену
    async with httpx.AsyncClient(timeout=15, verify=False) as client:
        for cid, c in clusters().items():
            for n in c.get("nodes", []):
                base = n["agent_base_url"]
                headers = {"X-Auth-Token": old} if old else {}
                try:
                    r = await client.post(f"{base}/xauth/set", headers=headers, json={"token": token, "persist": True})
                    r.raise_for_status()
                except Exception:
                    # агент недоступен — пропустим
                    pass

    return RedirectResponse(url="/settings/tokens", status_code=303)

# ========= API (admin/root) =========
@app.get("/health")
async def health():
    # открытый хелсчек мастера
    return {"status": "ok", "clusters": list(clusters().keys())}

@app.get("/clusters")
async def get_clusters(sess: Dict[str, Any] = Depends(require_admin)):
    return clusters()

@app.get("/clusters/{cid}/nodes")
async def get_nodes(cid: str, sess: Dict[str, Any] = Depends(require_admin)):
    c = clusters().get(cid)
    if not c:
        raise HTTPException(404, "Cluster not found")
    return c.get("nodes", [])

@app.get("/clusters/{cid}/agents/health")
async def agents_health(cid: str, sess: Dict[str, Any] = Depends(require_admin)):
    c = clusters().get(cid)
    if not c:
        raise HTTPException(404, "Cluster not found")
    tasks = [call_agent(f'{n["agent_base_url"]}/health') for n in c.get("nodes", [])]
    results = await httpx.AsyncClient.gather(*tasks, return_exceptions=True)  # type: ignore
    # ↑ если ругнётся, замени на asyncio.gather:
    # results = await asyncio.gather(*[call_agent(f'{n["agent_base_url"]}/health') for n in c.get("nodes", [])], return_exceptions=True)
    out = {}
    for n, res in zip(c.get("nodes", []), results):
        ok = not isinstance(res, Exception)
        out[n["name"]] = {"ok": ok, "data": None if not ok else res, "error": None if ok else str(res)}
    return out

@app.post("/clusters/{cid}/node/{name}/drain")
async def node_drain(cid: str, name: str, sess: Dict[str, Any] = Depends(require_admin)):
    node = _find_node(cid, name)
    return await call_agent(
        f'{node["agent_base_url"]}/node/{node["haproxy_backend"]}/{node["haproxy_server"]}/drain', method="POST"
    )

@app.post("/clusters/{cid}/node/{name}/enable")
async def node_enable(cid: str, name: str, sess: Dict[str, Any] = Depends(require_admin)):
    node = _find_node(cid, name)
    return await call_agent(
        f'{node["agent_base_url"]}/node/{node["haproxy_backend"]}/{node["haproxy_server"]}/enable', method="POST"
    )

@app.post("/clusters/{cid}/node/{name}/wait-empty")
async def node_wait_empty(cid: str, name: str, timeout_sec: int = 300, sess: Dict[str, Any] = Depends(require_admin)):
    node = _find_node(cid, name)
    return await call_agent(
        f'{node["agent_base_url"]}/node/{node["haproxy_backend"]}/{node["haproxy_server"]}/wait-empty',
        method="POST", json_body={"timeout_sec": timeout_sec}
    )

@app.post("/clusters/{cid}/node/{name}/restart")
async def node_restart(cid: str, name: str, sess: Dict[str, Any] = Depends(require_admin)):
    node = _find_node(cid, name)
    return await call_agent(
        f'{node["agent_base_url"]}/jboss/restart', method="POST",
        json_body={"service": node["jboss_service"]}
    )

@app.post("/clusters/{cid}/node/{name}/safe-roll")
async def safe_roll(cid: str, name: str, sess: Dict[str, Any] = Depends(require_admin)):
    node = _find_node(cid, name)
    base = node["agent_base_url"]
    await call_agent(f'{base}/node/{node["haproxy_backend"]}/{node["haproxy_server"]}/drain', method="POST")
    await call_agent(
        f'{base}/node/{node["haproxy_backend"]}/{node["haproxy_server"]}/wait-empty',
        method="POST", json_body={"timeout_sec": 300}
    )
    await call_agent(f'{base}/jboss/restart', method="POST", json_body={"service": node["jboss_service"]})
    await call_agent(f'{base}/jboss/wait-health', method="POST", json_body={"timeout_sec": 300})
    return await call_agent(f'{base}/node/{node["haproxy_backend"]}/{node["haproxy_server"]}/enable', method="POST")

@app.post("/clusters/{cid}/node/{name}/cfg-enable")
async def node_cfg_enable(cid: str, name: str, sess: Dict[str, Any] = Depends(require_admin)):
    n = _find_node(cid, name)
    return await call_agent(
        f'{n["agent_base_url"]}/haproxy/cfg-toggle', method="POST",
        json_body={"backend": n["haproxy_backend"], "server": n["haproxy_server"], "enable": True}
    )

@app.post("/clusters/{cid}/node/{name}/cfg-disable")
async def node_cfg_disable(cid: str, name: str, sess: Dict[str, Any] = Depends(require_admin)):
    n = _find_node(cid, name)
    return await call_agent(
        f'{n["agent_base_url"]}/haproxy/cfg-toggle', method="POST",
        json_body={"backend": n["haproxy_backend"], "server": n["haproxy_server"], "enable": False}
    )
