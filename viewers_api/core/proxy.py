# core/proxy.py
import os
import re
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
    if "://" not in url:
        return f"{default}://{url}"
    return url


def _normalize_host_port(host: str, port: str) -> str:
    """
    Возвращает host:port, минимально проверяя валидность.
    """
    host = host.strip(" []")
    port = port.strip()
    if not host or not port or not port.isdigit():
        raise ValueError("invalid host/port")
    return f"{host}:{port}"


def _build_url(scheme: str, user: Optional[str], pwd: Optional[str], host_port: str) -> str:
    """
    Собираем URL: scheme://[user:pwd@]host:port
    """
    netloc = host_port
    if user:
        if pwd is None:
            pwd = ""
        netloc = f"{user}:{pwd}@{host_port}"
    return urlunparse((scheme, netloc, "", "", "", ""))


def normalize_proxy(raw: str, default_scheme: str = "http") -> Optional[str]:
    """
    Превращает разные форматы прокси в URL пригодный для aiohttp:
      - "ip:port"
      - "ip:port:user:pass"
      - "user:pass@ip:port"
      - "http://user:pass@ip:port"
      - "socks5://ip:port" и т.п.

    Возвращает нормализованный URL или None, если не удалось распарсить.
    """
    if not raw:
        return None
    raw = raw.strip()

    # 1) Уже полноценный url?
    if _looks_like_url(_ensure_scheme(raw, default_scheme)):
        url = _ensure_scheme(raw, default_scheme)
        try:
            p = urlparse(url)
            if p.scheme.lower() not in _SCHEMES:
                # если схема нестандартная — меняем на default_scheme
                url = url.replace(f"{p.scheme}://", f"{default_scheme}://", 1)
            # простая проверка, что есть хост и порт
            if not p.hostname or not p.port:
                return None
            return url
        except Exception:
            return None

    # 2) Форматы без схемы
    # 2.1 user:pass@ip:port
    if "@" in raw:
        creds, host_port = raw.split("@", 1)
        if ":" in creds and ":" in host_port:
            user, pwd = creds.split(":", 1)
            host, port = host_port.rsplit(":", 1)
            try:
                host_port_norm = _normalize_host_port(host, port)
                return _build_url(default_scheme, user, pwd, host_port_norm)
            except Exception:
                return None

    # 2.2 ip:port:user:pass
    parts = raw.split(":")
    if len(parts) == 4:
        host, port, user, pwd = parts
        try:
            host_port_norm = _normalize_host_port(host, port)
            return _build_url(default_scheme, user, pwd, host_port_norm)
        except Exception:
            return None

    # 2.3 ip:port
    if len(parts) == 2:
        host, port = parts
        try:
            host_port_norm = _normalize_host_port(host, port)
            return _build_url(default_scheme, None, None, host_port_norm)
        except Exception:
            return None

    return None


def load_list_from_file(path: str, *, shuffle: bool = True, unique: bool = True) -> List[str]:
    """
    Читает строки из файла, чистит пустые, убирает дубли (опционально) и перемешивает (опционально).
    Возвращает как есть, без нормализации.
    """
    if not os.path.exists(path):
        logger.warning("File not found: %s", path)
        return []

    with open(path, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip()]

    if unique:
        seen = set()
        uniq = []
        for x in lines:
            if x in seen:
                continue
            seen.add(x)
            uniq.append(x)
        lines = uniq

    if shuffle:
        random.shuffle(lines)

    logger.info("Loaded %d items from %s (unique=%s, shuffled=%s)", len(lines), path, unique, shuffle)
    return lines


def shuffle_list(items: List[str]) -> List[str]:
    random.shuffle(items)
    return items


def load_proxies_raw(path: Optional[str] = None, *, shuffle: bool = True, unique: bool = True) -> List[str]:
    path = path or DEFAULT_PROXIES_PATH
    return load_list_from_file(path, shuffle=shuffle, unique=unique)


def load_tokens_raw(path: Optional[str] = None, *, shuffle: bool = True, unique: bool = True) -> List[str]:
    path = path or DEFAULT_TOKENS_PATH
    return load_list_from_file(path, shuffle=shuffle, unique=unique)


def load_proxies_normalized(path: Optional[str] = None, *, shuffle: bool = True, unique: bool = True,
                            default_scheme: str = "http") -> List[str]:
    """
    Загружает прокси и нормализует каждую строку к URL (scheme://[user:pass@]host:port).
    Пропускает невалидные строки.
    """
    raw = load_proxies_raw(path, shuffle=shuffle, unique=unique)
    out: List[str] = []
    bad = 0
    for line in raw:
        url = normalize_proxy(line, default_scheme=default_scheme)
        if url:
            out.append(url)
        else:
            bad += 1
    logger.info("Proxies normalized: %d ok, %d bad", len(out), bad)
    return out

class RoundRobin:
    """
    Циклический перебор значений из списка.
    При каждом вызове next() возвращает следующий элемент,
    доходя до конца — начинает сначала.
    """
    def __init__(self, items: list[str]):
        self.items = list(items)
        self.index = -1

    def next(self) -> str:
        if not self.items:
            raise IndexError("RoundRobin is empty")
        self.index = (self.index + 1) % len(self.items)
        return self.items[self.index]

    def add(self, item: str):
        """Добавить элемент в список."""
        self.items.append(item)

    def remove(self, item: str):
        """Удалить элемент из списка."""
        self.items = [x for x in self.items if x != item]
        if self.index >= len(self.items):
            self.index = -1

    def __len__(self):
        return len(self.items)

    def __bool__(self):
        return bool(self.items)

def to_proxy_url(raw: str, default_scheme: str = "http"):
    """Совместимый алиас для normalize_proxy()."""
    return normalize_proxy(raw, default_scheme=default_scheme)