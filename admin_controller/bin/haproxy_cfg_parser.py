#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
haproxy_cfg_parser.py — лёгкая работа с haproxy.cfg (Py3.11)

Класс: HAProxyCfg
  - comment_server(backend, server)   → (ok_bool, note)
  - uncomment_server(backend, server) → (ok_bool, note)

Особенности:
  * Разрешённые backends (allowed set в конструкторе)
  * Сохраняет форматирование (добавляет/убирает '# ' перед 'server ...')
  * Бэкап <cfg>.bak_YYYYmmdd_HHMMSS и атомарная запись
"""
from __future__ import annotations

import os, time, tempfile, re
from pathlib import Path
from typing import Iterable, Optional

class HAProxyCfg:
    def __init__(self, cfg_path: str | os.PathLike, allowed_backends: Optional[Iterable[str]] = None):
        self.cfg_path = str(cfg_path)
        self.allowed = set(allowed_backends or [])

    def _read_text(self) -> tuple[bytes, str]:
        raw = Path(self.cfg_path).read_bytes()
        try:
            return raw, raw.decode("utf-8")
        except Exception:
            return raw, raw.decode("latin-1", "ignore")

    def _write_text(self, old_raw: bytes, new_text: str) -> None:
        try:
            bpath = self.cfg_path + ".bak_" + time.strftime("%Y%m%d_%H%M%S")
            Path(bpath).write_bytes(old_raw)
        except Exception:
            pass
        d = os.path.dirname(self.cfg_path) or "."
        fd, tmp = tempfile.mkstemp(prefix=".haproxy_cfg.", dir=d, text=True)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(new_text)
        os.replace(tmp, self.cfg_path)

    def _toggle(self, backend: str, server: str, make_comment: bool) -> tuple[bool, str]:
        if self.allowed and backend not in self.allowed:
            return (False, f"backend {backend} not allowed")

        raw, text = self._read_text()
        lines = text.splitlines(True)
        in_bk = False
        bk_re = re.compile(r"^\s*backend\s+" + re.escape(backend) + r"\b")
        srv_re = re.compile(r"^(\s*)(#\s*)?(server\s+" + re.escape(server) + r"\b.*)$")
        changed = False

        for i, line in enumerate(lines):
            if bk_re.match(line):
                in_bk = True
                continue
            if in_bk and re.match(r"^\s*(frontend|backend|listen|global|defaults)\b", line):
                in_bk = False
            if in_bk:
                m = srv_re.match(line)
                if not m:
                    continue
                indent, hashpart, rest = m.groups()
                if make_comment:
                    if hashpart:
                        return (False, "already commented")
                    lines[i] = indent + "# " + rest + ("" if line.endswith("\n") else "")
                    changed = True
                    break
                else:
                    if not hashpart:
                        return (False, "already active")
                    lines[i] = indent + rest + ("" if line.endswith("\n") else "")
                    changed = True
                    break

        if not changed:
            return (False, f"no matching 'server {server}' in backend {backend}")

        new_text = "".join(lines)
        try:
            self._write_text(raw, new_text)
            return (True, "cfg updated")
        except Exception as e:
            return (False, f"write error: {e}")

    def comment_server(self, backend: str, server: str) -> tuple[bool, str]:
        return self._toggle(backend, server, True)

    def uncomment_server(self, backend: str, server: str) -> tuple[bool, str]:
        return self._toggle(backend, server, False)
