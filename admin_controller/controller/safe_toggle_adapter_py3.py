#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тонкий адаптер для вашего Py3-контроллера: централизованные вызовы toggle.
"""

from controller.safe_haproxy_toggle import safe_toggle

def haproxy_drain(backend: str, server: str):
    safe_toggle("drain", backend, server)

def haproxy_disable(backend: str, server: str):
    safe_toggle("disable", backend, server)

def haproxy_enable(backend: str, server: str):
    safe_toggle("enable", backend, server)
