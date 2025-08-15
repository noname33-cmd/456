#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, time, hmac, hashlib, threading
from typing import Optional, Tuple

# Секрет берём из TOGGLE_SECRET или из файла (systemd credentials)
def _load_secret() -> str:
    path = os.environ.get("TOGGLE_SECRET_FILE")
    if path and os.path.exists(path):
        with open(path, "rb") as f:
            return f.read().decode("utf-8").strip()
    val = os.environ.get("TOGGLE_SECRET", "")
    return val.strip()

_SECRET = _load_secret()
_NONCE_TTL = int(os.environ.get("SIG_TTL_SEC", "60"))
_nonce_cache = {}
_lock = threading.Lock()

def set_secret_for_tests(value: str):
    global _SECRET
    _SECRET = value.strip()

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def sign_v1(method: str, path_qs: str, ts: str, nonce: str, body: bytes, secret: Optional[str]=None) -> str:
    sec = (secret if secret is not None else _SECRET).encode("utf-8")
    msg = "\n".join([method.upper(), path_qs, ts, nonce, sha256_hex(body)]).encode("utf-8")
    return hmac.new(sec, msg, hashlib.sha256).hexdigest()

def _parse_sig_header(hv: str) -> Optional[Tuple[str,str,str]]:
    # формат:  v=1;ts=1699999999;nonce=...;sig=<hex>
    parts = dict(x.split("=",1) for x in hv.replace(" ", "").split(";") if "=" in x)
    if parts.get("v") != "1": return None
    ts, nonce, sig = parts.get("ts"), parts.get("nonce"), parts.get("sig")
    if not (ts and nonce and sig): return None
    return ts, nonce, sig

def _check_replay_and_store(nonce: str, now: int) -> bool:
    # простая in-mem защита от повторов по nonce с TTL
    with _lock:
        # cleanup
        to_del = [k for k,v in _nonce_cache.items() if v < now - _NONCE_TTL]
        for k in to_del:
            _nonce_cache.pop(k, None)
        if nonce in _nonce_cache:
            return False
        _nonce_cache[nonce] = now
        return True

class AuthError(Exception): pass

def verify_request(method: str, path_qs: str, body: bytes, headers: dict):
    """
    Правила:
      1) Разрешаем простой токен: X-Auth-Token == TOGGLE_SECRET  ИЛИ ?secret=...
      2) Или HMAC: X-Signature: v=1;ts=...;nonce=...;sig=...
         - |now - ts| <= SIG_TTL_SEC (60 по умолчанию)
         - nonce не использовался ранее (in-mem)
    """
    # 1) Простой токен
    if _SECRET:
        token = headers.get("X-Auth-Token")
        if token and token == _SECRET:
            return
        # запасной канал — query (?secret=) передан сервером в headers['X-Query-Secret']
        qsec = headers.get("X-Query-Secret")
        if qsec and qsec == _SECRET:
            return

    # 2) HMAC v=1
    sig_hdr = headers.get("X-Signature")
    if not (sig_hdr and _SECRET):
        raise AuthError("missing auth")

    parsed = _parse_sig_header(sig_hdr)
    if not parsed: raise AuthError("bad signature header format")
    ts_str, nonce, sig = parsed
    try:
        ts = int(ts_str)
    except Exception:
        raise AuthError("bad ts")

    now = int(time.time())
    if abs(now - ts) > _NONCE_TTL:
        raise AuthError("ts out of window")
    if not _check_replay_and_store(nonce, now):
        raise AuthError("replay detected")

    expected = sign_v1(method, path_qs, ts_str, nonce, body)
    if not hmac.compare_digest(expected, sig):
        raise AuthError("invalid signature")
