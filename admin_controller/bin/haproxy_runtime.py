#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
haproxy_runtime.py — работа с HAProxy Runtime Socket (Py3.11)
"""
from __future__ import annotations

import socket

class HAProxyRuntime:
    def __init__(self, socket_path: str, timeout: float = 3.0):
        self.socket_path = socket_path
        self.timeout = float(timeout)

    def _talk(self, cmd: str) -> str:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.connect(self.socket_path)
        try:
            s.sendall((cmd.strip() + "\n").encode("ascii"))
            data = b""
            while True:
                try:
                    chunk = s.recv(65536)
                except socket.timeout:
                    break
                if not chunk:
                    break
                data += chunk
            return data.decode("utf-8", "replace")
        finally:
            try:
                s.close()
            except Exception:
                pass

    def show_stat(self) -> str:
        return self._talk("show stat")

    def show_servers_state(self) -> str:
        return self._talk("show servers state")

    def set_state(self, backend: str, server: str, state: str) -> str:
        return self._talk(f"set server {backend}/{server} state {state}")

    def set_weight(self, backend: str, server: str, weight: int) -> str:
        return self._talk(f"set weight {backend}/{server} {int(weight)}")

    def get_backends(self) -> list[str]:
        txt = self.show_servers_state()
        res: set[str] = set()
        for line in (txt or "").splitlines():
            parts = line.strip().split()
            if len(parts) >= 3 and parts[0] in ("#", "srv", "be"):
                if parts[0] == "be":
                    res.add(parts[1])
                elif parts[0] == "srv":
                    res.add(parts[1])
        return sorted(res)
