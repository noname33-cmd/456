# core/proxy.py
import os
import random
import logging
from typing import List, Optional
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)

DEFAULT_PROXIES_PATH = "proxies.txt"
DEFAULT_TOKENS_PATH = "tokens.txt"
_SCHEMES = {"http", "https", "socks5", "socks4"}

def _looks_like_url(s: str) -> bool:
    try:
        p = urlparse(s)
        return bool(p.scheme and p.netloc)
    except Exception:
        return False

def _ensure_scheme(url: str, default: str = "http") -> str:
    return url if "://" in url else f"{default}://{url}"

def _normalize_host_port(host: str, port: str) -> str:
    host = host.strip(" []"); port = port.strip()
    if not host or not port or not port.isdigit():
        raise ValueError("invalid host/port")
    return f"{host}:{port}"

def _build_url(scheme: str, user: Optional[str], pwd: Optional[str], host_port: str) -> str:
    netloc = f"{user}:{pwd}@{host_port}" if user is not None else host_port
    return urlunparse((scheme, netloc, "", "", "", ""))

def normalize_proxy(raw: str, default_scheme: str = "http") -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip()

    # Уже URL?
    url = _ensure_scheme(raw, default_scheme)
    if _looks_like_url(url):
        try:
            p = urlparse(url)
            scheme = p.scheme.lower()
            if scheme not in _SCHEMES:
                url = url.replace(f"{p.scheme}://", f"{default_scheme}://", 1)
            if not p.hostname or not p.port:
                return None
            return url
        except Exception:
            return None

    # user:pass@ip:port
    if "@" in raw:
        creds, host_port = raw.split("@", 1)
        if ":" in creds and ":" in host_port:
            user, pwd = creds.split(":", 1)
            host, port = host_port.rsplit(":", 1)
            try:
                return _build_url(default_scheme, user, pwd, _normalize_host_port(host, port))
            except Exception:
                return None

    parts = raw.split(":")
    if len(parts) == 4:
        host, port, user, pwd = parts
        try:
            return _build_url(default_scheme, user, pwd, _normalize_host_port(host, port))
        except Exception:
            return None

    if len(parts) == 2:
        host, port = parts
        try:
            return _build_url(default_scheme, None, None, _normalize_host_port(host, port))
        except Exception:
            return None

    return None

def load_list_from_file(path: str, *, shuffle: bool = True, unique: bool = True) -> List[str]:
    if not os.path.exists(path):
        logger.warning("File not found: %s", path)
        return []
    with open(path, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip()]

    if unique:
        seen, uniq = set(), []
        for x in lines:
            if x in seen: continue
            seen.add(x); uniq.append(x)
        lines = uniq

    if shuffle:
        random.shuffle(lines)
    logger.info("Loaded %d items from %s", len(lines), path)
    return lines

def shuffle_list(items: List[str]) -> List[str]:
    random.shuffle(items)
    return items

def load_proxies_raw(path: Optional[str] = None, *, shuffle=True, unique=True) -> List[str]:
    return load_list_from_file(path or DEFAULT_PROXIES_PATH, shuffle=shuffle, unique=unique)

def load_tokens_raw(path: Optional[str] = None, *, shuffle=True, unique=True) -> List[str]:
    return load_list_from_file(path or DEFAULT_TOKENS_PATH, shuffle=shuffle, unique=unique)

def load_proxies_normalized(path: Optional[str] = None, *, shuffle=True, unique=True,
                            default_scheme: str = "http") -> List[str]:
    raw = load_proxies_raw(path, shuffle=shuffle, unique=unique)
    out, bad = [], 0
    for line in raw:
        url = normalize_proxy(line, default_scheme=default_scheme)
        if url: out.append(url)
        else: bad += 1
    logger.info("Proxies normalized: %d ok, %d bad", len(out), bad)
    return out

class RoundRobin:
    def __init__(self, items: list[str]):
        self.items = list(items)
        self.index = -1

    def next(self) -> str:
        if not self.items:
            raise IndexError("RoundRobin is empty")
        self.index = (self.index + 1) % len(self.items)
        return self.items[self.index]

    def add(self, item: str):
        self.items.append(item)

    def remove(self, item: str):
        self.items = [x for x in self.items if x != item]
        if self.index >= len(self.items):
            self.index = -1

    def __len__(self): return len(self.items)
    def __bool__(self): return bool(self.items)

def to_proxy_url(raw: str, default_scheme: str = "http"):
    return normalize_proxy(raw, default_scheme=default_scheme)
