#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Единые проверки здоровья кластера (без внешних зависимостей):

1) Диски:    check_disk()
2) Сервисы:  check_services()
3) Порты:    check_ports()
4) JBoss:    check_jboss_deploys()
5) Память/CPU: check_system_load()

Все функции возвращают словари с полями: ts, items, и детальной информацией.
"""

from __future__ import annotations
import os, json, time, socket, subprocess
from typing import List, Dict, Any

# ---------- helpers ----------
def _now_ts() -> str:
    import datetime as dt
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ---------- 1) Диски ----------
def _read_mounts() -> list[tuple[str, str, str]]:
    """[(fsname, mountpoint, fstype), ...] — только «реальные» FS, без tmpfs/cgroup и т.п."""
    mounts = []
    try:
        with open("/proc/mounts", "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 3:
                    continue
                fsname, mnt, fstype = parts[0], parts[1], parts[2]
                if fstype.startswith(("proc", "sysfs", "cgroup", "tmpfs", "devpts", "overlay", "squashfs", "debugfs", "tracefs")):
                    continue
                mounts.append((fsname, mnt, fstype))
    except Exception:
        pass
    return mounts

def _disk_usage(path: str) -> dict:
    st = os.statvfs(path)
    total = st.f_blocks * st.f_frsize
    avail = st.f_bavail * st.f_frsize
    used = total - avail
    pct = 0.0 if total <= 0 else (used / total) * 100.0
    return {"total": total, "used": used, "avail": avail, "pct": pct}

def check_disk(mounts: List[str] | None = None, warn_pct: float = 90.0, crit_pct: float = 100.0) -> Dict[str, Any]:
    """
    mounts: список путей монтирования; если None — авто из /proc/mounts
    status: ok | warn (>=warn_pct) | crit (>=crit_pct)
    """
    mp_all = _read_mounts()
    if not mounts:
        mounts = [m for _, m, _ in mp_all] or ["/"]

    out = {"ts": _now_ts(), "warn": warn_pct, "crit": crit_pct, "items": []}
    need = {m for m in mounts}
    by_mnt = {m: (fs, m, t) for (fs, m, t) in mp_all}
    for m in sorted(need):
        if m not in by_mnt:
            out["items"].append({"mount": m, "status": "unknown", "error": "not mounted"})
            continue
        (fs, mnt, fstype) = by_mnt[m]
        try:
            u = _disk_usage(mnt)
            status = "ok"
            if u["pct"] >= crit_pct:
                status = "crit"
            elif u["pct"] >= warn_pct:
                status = "warn"
            out["items"].append({
                "mount": mnt, "fs": fs, "fstype": fstype,
                "pct": round(u["pct"], 2), "used": u["used"], "total": u["total"], "avail": u["avail"],
                "status": status
            })
        except Exception as e:
            out["items"].append({"mount": mnt, "status": "unknown", "error": str(e)})
    return out

# ---------- 2) Сервисы ----------
def check_services(services: List[str]) -> Dict[str, Any]:
    """systemd is-active для каждого сервиса."""
    items = []
    for svc in services or []:
        svc = svc.strip()
        if not svc:
            continue
        try:
            r = subprocess.run(["systemctl", "is-active", svc],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
            status = (r.stdout.strip() or r.stderr.strip() or "").strip()
            items.append({"name": svc, "active": (status == "active"), "status": status})
        except Exception as e:
            items.append({"name": svc, "active": False, "status": f"error:{e}"})
    return {"ts": _now_ts(), "items": items}

# ---------- 3) Порты ----------
def check_ports(targets: List[str], timeout_sec: float = 1.0) -> Dict[str, Any]:
    """targets: ["host:port", ...] — проверка TCP-connect."""
    items = []
    for t in targets or []:
        t = t.strip()
        if not t or ":" not in t:
            continue
        host, port_s = t.rsplit(":", 1)
        try:
            port = int(port_s)
            with socket.create_connection((host, port), timeout=timeout_sec):
                items.append({"target": t, "open": True, "error": ""})
        except Exception as e:
            items.append({"target": t, "open": False, "error": str(e)})
    return {"ts": _now_ts(), "items": items}

# ---------- 4) JBoss deploys ----------
def _jboss_cli_query(cli_path: str, controller: str, user: str | None, password: str | None, timeout: int = 8) -> dict:
    """
    Вызов jboss-cli.sh для получения статуса деплоев:
      :read-children-resources(child-type=deployment, include-runtime=true)
    Возвращает распарсенный JSON ответа CLI (если удалось).
    """
    cmd = [cli_path, "--connect"]
    if controller:
        cmd += [f"--controller={controller}"]
    if user:
        cmd += [f"--user={user}"]
    if password:
        cmd += [f"--password={password}"]
    cmd += ['command=/:read-children-resources(child-type=deployment,include-runtime=true,recursive=true)']
    # Некоторые версии поддерживают --output-json напрямую:
    env = os.environ.copy()
    env["JBOSS_LOGGING_CONFIG"] = env.get("JBOSS_LOGGING_CONFIG","")  # тише
    try:
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout, env=env)
        txt = r.stdout or r.stderr
        # Ответ CLI часто текстовый с ключом "outcome" и "result". Попробуем найти JSON.
        # Самый надёжный способ — отрезать лишний мусор и попытаться распарсить.
        start = txt.find("{")
        end = txt.rfind("}")
        if start >= 0 and end > start:
            js = txt[start:end+1]
            return json.loads(js)
    except Exception:
        pass
    return {}

def check_jboss_deploys(cli_path: str,
                        controller: str = "127.0.0.1:9990",
                        user: str | None = None,
                        password: str | None = None,
                        deployments_filter: List[str] | None = None,
                        timeout: int = 8) -> Dict[str, Any]:
    """
    Проверяет деплои JBoss через jboss-cli.sh.
    - cli_path: путь к jboss-cli.sh (например, /opt/jboss/bin/jboss-cli.sh)
    - controller: host:port management
    - user/password: при необходимости
    - deployments_filter: если задан — показывать только эти имена деплоев (WAR/ear)
    Возвращает статус выдачи CLI + список деплоев: name, enabled, status.
    """
    raw = _jboss_cli_query(cli_path, controller, user, password, timeout=timeout)
    items = []
    if raw.get("outcome") == "success":
        result = raw.get("result") or {}
        for name, meta in (result.items() if isinstance(result, dict) else []):
            if deployments_filter and name not in deployments_filter:
                continue
            # признак активного/загруженного из runtime:
            enabled = bool(meta.get("enabled", True))
            status = meta.get("status") or meta.get("enabled") or "unknown"
            items.append({"name": name, "enabled": bool(enabled), "status": str(status)})
        ok = True
    else:
        ok = False
    return {"ts": _now_ts(), "ok": ok, "controller": controller, "items": items, "raw_outcome": raw.get("outcome", "unknown")}

# ---------- 5) Память и CPU ----------
def _meminfo() -> dict:
    out = {}
    try:
        with open("/proc/meminfo","r",encoding="utf-8",errors="ignore") as f:
            for line in f:
                if ":" not in line: 
                    continue
                k,v = line.split(":",1)
                out[k.strip()] = v.strip()
    except Exception:
        pass
    return out

def _cpu_times() -> dict:
    try:
        with open("/proc/stat","r") as f:
            for line in f:
                if line.startswith("cpu "):
                    parts = line.split()
                    # cpu user nice system idle iowait irq softirq steal guest guest_nice
                    nums = list(map(int, parts[1:]))
                    return {
                        "user": nums[0], "nice": nums[1], "system": nums[2], "idle": nums[3],
                        "iowait": nums[4] if len(nums)>4 else 0, "irq": nums[5] if len(nums)>5 else 0,
                        "softirq": nums[6] if len(nums)>6 else 0, "steal": nums[7] if len(nums)>7 else 0
                    }
    except Exception:
        pass
    return {}

def _cpu_usage_percent(sample_sec: float = 0.25) -> float:
    a = _cpu_times()
    time.sleep(sample_sec)
    b = _cpu_times()
    if not a or not b:
        return -1.0
    def tot(d): return sum(d.values())
    idle_delta = b.get("idle",0) - a.get("idle",0)
    total_delta = tot(b) - tot(a)
    if total_delta <= 0:
        return 0.0
    usage = 100.0 * (1.0 - (idle_delta / total_delta))
    return round(usage, 2)

def _top_processes(n: int = 5) -> list[dict]:
    """Топ потребителей CPU/MEM через ps (без внеш. либ)"""
    items = []
    try:
        r = subprocess.run(["bash","-lc","ps -eo pid,comm,%cpu,%mem --sort=-%cpu | head -n $((1+{}))".format(n+1)],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=3)
        lines = (r.stdout or "").strip().splitlines()
        for line in lines[1:]:
            parts = line.split(None,4)
            if len(parts) >= 4:
                pid, comm, cpu, mem = parts[0], parts[1], parts[2], parts[3]
                items.append({"pid": int(pid), "comm": comm, "cpu_pct": float(cpu), "mem_pct": float(mem)})
    except Exception:
        pass
    return items

def check_system_load() -> Dict[str, Any]:
    """
    Возвращает:
      mem: total, available, used, used_pct
      cpu: usage_pct (на интервале ~0.25с)
      top_procs: [{pid,comm,cpu_pct,mem_pct} ...]
    """
    mi = _meminfo()
    def _kB(x: str) -> int:
        # строки вида '16313860 kB'
        try:
            return int((x or "0").split()[0]) * 1024
        except Exception:
            return 0

    total = _kB(mi.get("MemTotal","0"))
    avail = _kB(mi.get("MemAvailable","0"))
    used = total - avail if total>0 else 0
    used_pct = round((used/total)*100.0, 2) if total>0 else 0.0

    cpu_pct = _cpu_usage_percent(0.25)
    top = _top_processes(5)

    return {
        "ts": _now_ts(),
        "mem": {"total": total, "available": avail, "used": used, "used_pct": used_pct},
        "cpu": {"usage_pct": cpu_pct},
        "top_procs": top,
        "hints": [
            "Высокая память: смотри top_procs по mem_pct и dmesg (OOM-killer).",
            "Высокий CPU: проверь top_procs по cpu_pct, iowait (vmstat 1), и активности GC/JIT у JVM.",
        ]
    }
