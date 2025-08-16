import socket

def _sock(path: str):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(path)
    return s

def cmd(path: str, command: str) -> str:
    s = _sock(path)
    try:
        s.sendall((command + "\n").encode("ascii"))
        out = b""
        while True:
            ch = s.recv(65536)
            if not ch:
                break
            out += ch
        return out.decode("utf-8", "replace")
    finally:
        s.close()

def show_stat_csv(path: str) -> str:
    return cmd(path, "show stat")

def parse_stat(csv_text: str):
    """
    Возвращает: dict[(backend, server)] -> {column: value}
    FRONTEND/BACKEND строки пропускаем.
    """
    lines = csv_text.splitlines()
    header = None
    table = {}
    for ln in lines:
        if ln.startswith("# "):
            header = ln[2:].split(",")
            continue
        if not ln or ln.startswith("#"):
            continue
        parts = ln.split(",")
        if not header or len(parts) < 2:
            continue
        row = {header[i]: parts[i] if i < len(parts) else "" for i in range(len(header))}
        px = row.get("pxname", "")
        sv = row.get("svname", "")
        if not px or not sv or sv in ("FRONTEND", "BACKEND"):
            continue
        table[(px, sv)] = row
    return table

def set_state(path: str, backend: str, server: str, state: str) -> str:
    # state: ready | drain | maint
    return cmd(path, f"set server {backend}/{server} state {state}")
