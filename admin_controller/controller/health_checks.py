#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Единые проверки здоровья кластера (без внешних зависимостей).

Функции:
  1) check_disk()
  2) check_services()
  3) check_ports()
  4) check_jboss_deploys()
  5) check_system_load()

Все функции возвращают словари с ключами: ts, items/ok/...,
и ДОПОЛНИТЕЛЬНО сохраняют результат в:
  /tmp/pattern_controller/report/<HOST>/health/{disk,services,ports,jboss,system}.json
(Путь переопределяем через ENV HEALTH_OUT_DIR).

CLI:
  python3 controller/health_checks.py --all
  python3 controller/health_checks.py --disk
  python3 controller/health_checks.py --services haproxy,admin-controller
  python3 controller/health_checks.py --ports 127.0.0.1:35073,127.0.0.1:35009
  python3 controller/health_checks.py --jboss
  python3 controller/health_checks.py --system
"""

from __future__ import annotations
import os, json, time, socket, subprocess, argparse, shutil
from typing import List, Dict, Any, Tuple

# Единые пути (report/<HOST>/...)
try:
    from bin.path_utils import REPORT_DIR
    _DEFAULT_OUT_DIR = str((REPORT_DIR / "health"))
except Exception:
    # запасной вариант, если запуск не из рабочей директории:
    _DEFAULT_OUT_DIR = "/tmp/pattern_controller/report/local/health"

# ---- ENV-настройки по умолчанию -------------------------------------------------

DISK_WARN = float(os.environ.get("DISK_WARN", "90"))
DISK_CRIT = float(os.environ.get("DISK_CRIT", "100"))

DEFAULT_SERVICES = [s for s in os.environ.get("SERVICES", "haproxy,admin-controller,pattern-ui").split(",") if s.strip()]
DEFAULT_PORTS    = [p for p in os.environ.get("PORTS", "127.0.0.1:35073,127.0.0.1:35009").split(",") if p.strip()]

DEFAULT_JBOSS_CLI        = os.environ.get("JBOSS_CLI", "/u01/jboss/bin/jboss-cli.sh")
DEFAULT_JBOSS_CONTROLLER = os.environ.get("JBOSS_CONTROLLER", "127.0.0.1:9990")
DEFAULT_JBOSS_USER       = os.environ.get("JBOSS_USER") or None
DEFAULT_JBOSS_PASS       = os.environ.get("JBOSS_PASS") or None
DEFAULT_JBOSS_DEPLOYS    = [x for x in os.environ.get("JBOSS_DEPLOYS","").split(",") if x.strip()]

HEALTH_OUT_DIR = os.environ.get("HEALTH_OUT_DIR", _DEFAULT_OUT_DIR)

# ---- утилиты --------------------------------------------------------------------

def _now_ts() -> str:
    import datetime as dt
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _ensure_outdir() -> str:
    try:
        os.makedirs(HEALTH_OUT_DIR, exist_ok=True)
    except Exception:
        pass
    return HEALTH_OUT_DIR

def _write_json(name: str, data: dict):
    """Пишем JSON в health/<name>.json (best-effort)."""
    outdir = _ensure_outdir()
    try:
        path = os.path.join(outdir, f"{name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass

# ---- 1) Диски -------------------------------------------------------------------

def _read_mounts() -> list[tuple[str, str, str]]:
    """[(fsname, mountpoint, fstype), ...] — без tmpfs/cgroup/overlay и т.п."""
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

def check_disk(mounts: List[str] | None = None, warn_pct: float = DISK_WARN, crit_pct: float = DISK_CRIT) -> Dict[str, Any]:
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
    _write_json("disk", out)
    return out

# ---- 2) Сервисы (systemd) -------------------------------------------------------

def check_services(services: List[str] | None = None) -> Dict[str, Any]:
    """systemd is-active для каждого сервиса."""
    if services is None:
        services = DEFAULT_SERVICES
    items = []
    systemctl = shutil.which("systemctl")
    for svc in services or []:
        svc = svc.strip()
        if not svc:
            continue
        if not systemctl:
            items.append({"name": svc, "active": False, "status": "no-systemctl"})
            continue
        try:
            r = subprocess.run([systemctl, "is-active", svc],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
            status = (r.stdout.strip() or r.stderr.strip() or "").strip()
            items.append({"name": svc, "active": (status == "active"), "status": status})
        except Exception as e:
            items.append({"name": svc, "active": False, "status": f"error:{e}"})
    out = {"ts": _now_ts(), "items": items}
    _write_json("services", out)
    return out

# ---- 3) Порты (TCP CONNECT) -----------------------------------------------------

def check_ports(targets: List[str] | None = None, timeout_sec: float = 1.0) -> Dict[str, Any]:
    """targets: ["host:port", ...] — проверка TCP-connect."""
    if targets is None:
        targets = DEFAULT_PORTS
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
    out = {"ts": _now_ts(), "items": items}
    _write_json("ports", out)
    return out

# ---- 4) JBoss deploys (через jboss-cli) ----------------------------------------

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
    env = os.environ.copy()
    env["JBOSS_LOGGING_CONFIG"] = env.get("JBOSS_LOGGING_CONFIG","")
    try:
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout, env=env)
        txt = r.stdout or r.stderr
        # Пытаемся вытащить JSON из текстового ответа
        start = txt.find("{"); end = txt.rfind("}")
        if start >= 0 and end > start:
            js = txt[start:end+1]
            return json.loads(js)
    except Exception:
        pass
    return {}

def check_jboss_deploys(cli_path: str | None = None,
                        controller: str = DEFAULT_JBOSS_CONTROLLER,
                        user: str | None = DEFAULT_JBOSS_USER,
                        password: str | None = DEFAULT_JBOSS_PASS,
                        deployments_filter: List[str] | None = None,
                        timeout: int = 8) -> Dict[str, Any]:
    """
    Проверяет деплои JBoss через jboss-cli.sh.
      - cli_path: путь к jboss-cli.sh (по умолчанию /u01/jboss/bin/jboss-cli.sh)
      - controller: host:port management
      - user/password: при необходимости
      - deployments_filter: если задан — показывать только указанные имена деплоев
    """
    cli = cli_path or DEFAULT_JBOSS_CLI
    if not os.path.exists(cli):
        out = {"ts": _now_ts(), "ok": False, "controller": controller, "items": [],
               "raw_outcome": "cli_not_found", "error": f"jboss-cli not found: {cli}"}
        _write_json("jboss", out)
        return out

    raw = _jboss_cli_query(cli, controller, user, password, timeout=timeout)
    items = []
    ok = (raw.get("outcome") == "success")
    result = raw.get("result") or {}
    if ok and isinstance(result, dict):
        for name, meta in result.items():
            if deployments_filter and name not in deployments_filter:
                continue
            enabled = bool(meta.get("enabled", True))
            status = meta.get("status") or meta.get("enabled") or "unknown"
            items.append({"name": name, "enabled": bool(enabled), "status": str(status)})

    out = {"ts": _now_ts(), "ok": ok, "controller": controller, "items": items,
           "raw_outcome": raw.get("outcome", "unknown")}
    _write_json("jboss", out)
    return out

# ---- 5) Память и CPU ------------------------------------------------------------

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
    """Топ потребителей CPU/MEM через ps (без внешних либ)."""
    items = []
    try:
        r = subprocess.run(
            ["bash","-lc", f"ps -eo pid,comm,%cpu,%mem --sort=-%cpu | head -n $((1+{n}))"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=3
        )
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

    out = {
        "ts": _now_ts(),
        "mem": {"total": total, "available": avail, "used": used, "used_pct": used_pct},
        "cpu": {"usage_pct": cpu_pct},
        "top_procs": top,
        "hints": [
            "Высокая память: смотри top_procs по mem_pct и dmesg (OOM-killer).",
            "Высокий CPU: проверь top_procs по cpu_pct, iowait (vmstat 1), и активности GC/JIT у JVM.",
        ]
    }
    _write_json("system", out)
    return out

# ---- CLI ------------------------------------------------------------------------

def _run_and_print(name: str, fn, *args, **kwargs):
    res = fn(*args, **kwargs)
    print(json.dumps({name: res}, ensure_ascii=False))
    return res

def main():
    ap = argparse.ArgumentParser(description="Health checks aggregator (no external deps)")
    ap.add_argument("--all", action="store_true", help="run all checks")
    ap.add_argument("--disk", action="store_true", help="disk usage")
    ap.add_argument("--services", help="comma list of systemd services")
    ap.add_argument("--ports", help="comma list of host:port")
    ap.add_argument("--jboss", action="store_true", help="check jboss deployments")
    ap.add_argument("--system", action="store_true", help="memory/cpu/top processes")
    args = ap.parse_args()

    ran = False
    if args.all or args.disk:
        _run_and_print("disk", check_disk)
        ran = True
    if args.all or args.services is not None:
        svcs = DEFAULT_SERVICES if args.services is None else [s for s in args.services.split(",") if s.strip()]
        _run_and_print("services", check_services, svcs)
        ran = True
    if args.all or args.ports is not None:
        ports = DEFAULT_PORTS if args.ports is None else [p for p in args.ports.split(",") if p.strip()]
        _run_and_print("ports", check_ports, ports)
        ran = True
    if args.all or args.jboss:
        _run_and_print("jboss", check_jboss_deploys,
                       cli_path=DEFAULT_JBOSS_CLI,
                       controller=DEFAULT_JBOSS_CONTROLLER,
                       user=DEFAULT_JBOSS_USER, password=DEFAULT_JBOSS_PASS,
                       deployments_filter=(DEFAULT_JBOSS_DEPLOYS or None))
        ran = True
    if args.all or args.system:
        _run_and_print("system", check_system_load)
        ran = True

    if not ran:
        ap.print_help()

if __name__ == "__main__":
    main()
