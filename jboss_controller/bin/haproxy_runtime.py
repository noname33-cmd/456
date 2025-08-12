# FILE: haproxy_runtime.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Py2.7
import socket
import errno

class HAProxyRuntime(object):
    def __init__(self, socket_path, timeout=3.0):
        self.socket_path = socket_path
        self.timeout = float(timeout)

    def _talk(self, cmd):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        try:
            s.connect(self.socket_path)
            s.sendall((cmd.strip() + "\n").encode("utf-8"))
            chunks = []
            while True:
                try:
                    data = s.recv(65536)
                except socket.timeout:
                    break
                if not data:
                    break
                chunks.append(data)
            out = b"".join(chunks)
            try:
                return out.decode("utf-8", "ignore")
            except:
                return out.decode("latin-1", "ignore")
        finally:
            try:
                s.close()
            except:
                pass

    # ---- high level ----
    def show_stat_csv(self):
        return self._talk("show stat")

    def enable_server(self, backend, server):
        return self._talk("enable server %s/%s" % (backend, server))

    def disable_server(self, backend, server):
        return self._talk("disable server %s/%s" % (backend, server))

    def drain_server(self, backend, server):
        # перевод в drain (не принимать новые, но добежать активным)
        return self._talk("set server %s/%s state drain" % (backend, server))

    def set_weight(self, backend, server, weight):
        return self._talk("set server %s/%s weight %s" % (backend, server, str(int(weight))))

    def show_servers_state(self):
        return self._talk("show servers state")

    def get_backends(self):
        # быстрая выжимка бэкендов из show servers state
        txt = self.show_servers_state()
        res = set()
        for line in (txt or "").splitlines():
            # # be <backend> <....> или srv <backend> <server> ...
            parts = line.strip().split()
            if len(parts) >= 3 and parts[0] in ("#","srv","be"):
                if parts[0] == "be":
                    res.add(parts[1])
                elif parts[0] == "srv":
                    res.add(parts[1])
        return sorted(res)
