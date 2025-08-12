# FILE: haproxy_cfg_parser.py
#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
haproxy_cfg_parser.py — лёгкая библиотека для работы с haproxy.cfg (Py2.7).

Класс: HAProxyCfg
  - comment_server(backend, server)   → (ok_bool, note)
  - uncomment_server(backend, server) → (ok_bool, note)

Особенности:
  * Без внешних зависимостей
  * Работает только внутри указанных backend-ов (allowed set в конструкторе)
  * Сохраняет форматирование строки (добавляет/убирает '# ' перед 'server ...')
  * Делает бэкап <cfg>.bak_YYYYmmdd_HHMMSS
  * Атомарная запись файла
"""

import os, re, time, tempfile

class HAProxyCfg(object):
    def __init__(self, cfg_path, backends=None):
        """
        backends: set(['Jboss_client', ...]) — если пусто/None, разрешены все.
        """
        self.cfg_path = cfg_path
        self.allowed = set(backends or [])

    # --- io helpers ---
    def _read_text(self):
        raw = open(self.cfg_path, "rb").read()
        try:
            return raw.decode("utf-8")
        except:
            try:
                return raw.decode("latin-1")
            except:
                return raw.decode("utf-8", "ignore")

    def _write_text(self, old_raw, new_text):
        # backup
        try:
            bpath = self.cfg_path + ".bak_" + time.strftime("%Y%m%d_%H%M%S")
            open(bpath, "wb").write(old_raw)
        except:
            pass
        # atomic write
        d = os.path.dirname(self.cfg_path) or "."
        fd, tmp = tempfile.mkstemp(prefix=".haproxy_cfg.", dir=d)
        os.write(fd, new_text.encode("utf-8"))
        os.close(fd)
        os.rename(tmp, self.cfg_path)

    # --- toggle helpers ---
    def _toggle(self, backend, server, do_comment):
        if not os.path.isfile(self.cfg_path):
            return (False, "cfg not found: %s" % self.cfg_path)

        raw = open(self.cfg_path, "rb").read()
        try:
            text = raw.decode("utf-8")
        except:
            try: text = raw.decode("latin-1")
            except: text = raw.decode("utf-8", "ignore")

        lines = text.splitlines(True)  # keepends
        cur_be = None
        changed = False
        be_allowed = (not self.allowed) or (backend in self.allowed)

        # шаблон server-строки
        rx = re.compile(r'^(\s*)(#\s*)?(server\s+%s\b.*)$' % re.escape(server))

        for i in range(len(lines)):
            line = lines[i]
            st = line.strip()
            if st.startswith("backend "):
                cur_be = st.split(None,1)[1].strip()
                continue
            if cur_be != backend:
                continue
            m = rx.match(line)
            if not m:
                continue
            if not be_allowed:
                return (False, "backend not allowed by worker: %s" % backend)
            indent, hashpart, rest = m.group(1), m.group(2), m.group(3)
            if do_comment:
                if hashpart:
                    # уже закомментировано
                    return (False, "already commented")
                lines[i] = indent + "# " + rest + ("\n" if not line.endswith("\n") else "")
                changed = True
                break
            else:
                if not hashpart:
                    return (False, "already active")
                lines[i] = indent + rest + ("\n" if not line.endswith("\n") else "")
                changed = True
                break

        if not changed:
            return (False, "no matching 'server %s' in backend %s" % (server, backend))

        new_text = "".join(lines)
        try:
            self._write_text(raw, new_text)
            return (True, "cfg updated")
        except Exception as e:
            return (False, "write error: %s" % e)

    def comment_server(self, backend, server):
        return self._toggle(backend, server, True)

    def uncomment_server(self, backend, server):
        return self._toggle(backend, server, False)
