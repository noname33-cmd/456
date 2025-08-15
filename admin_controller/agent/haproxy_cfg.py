import os
import re
import tempfile
import subprocess
from fastapi import HTTPException

SERVER_RE = re.compile(r"^\s*server\s+(?P<name>\S+)\s+(?P<addr>\S+)(?P<opts>.*)$")

def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()

def write_atomic(path: str, text: str):
    d = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(prefix=".haproxy_", dir=d, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        try:
            os.unlink(tmp)
        except Exception:
            pass

def _patch_server_line(line: str, enable: bool) -> str:
    m = SERVER_RE.match(line)
    if not m:
        return line
    name = m.group("name")
    addr = m.group("addr")
    opts = (m.group("opts") or "").strip()

    # удаляем явные "disabled"
    opts = re.sub(r"\bdisabled\b", "", opts)
    opts = re.sub(r"\s+", " ", opts).strip()

    if not enable:
        if "disabled" not in (opts.split() if opts else []):
            opts = (opts + " disabled").strip()

    base = f"server {name} {addr}"
    return base if not opts else f"{base} {opts}"

def toggle_server_in_backend(cfg_text: str, backend: str, server: str, enable: bool) -> str:
    """
    Находим backend <name> и там правим строку 'server <server> ...' (добавляем/убираем 'disabled').
    """
    lines = cfg_text.splitlines()
    out = []
    in_bk = False

    for ln in lines:
        if re.match(r"^\s*backend\s+" + re.escape(backend) + r"\b", ln):
            in_bk = True
            out.append(ln)
            continue
        if in_bk and re.match(r"^\s*(frontend|backend|listen|global|defaults)\b", ln):
            in_bk = False

        if in_bk and SERVER_RE.match(ln):
            m = SERVER_RE.match(ln)
            if m and m.group("name") == server:
                ln = _patch_server_line(ln, enable)
        out.append(ln)

    # сохраняем конечный перевод строки как в исходнике
    return "\n".join(out) + ("\n" if cfg_text.endswith("\n") else "")

def validate_and_reload(cfg_path: str):
    p = subprocess.run(
        ["/usr/sbin/haproxy", "-c", "-f", cfg_path],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    if p.returncode != 0:
        raise HTTPException(400, f"haproxy -c failed:\n{p.stdout}")
    p2 = subprocess.run(
        ["/bin/systemctl", "reload", "haproxy"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    if p2.returncode != 0:
        raise HTTPException(500, f"systemctl reload failed:\n{p2.stdout}")
