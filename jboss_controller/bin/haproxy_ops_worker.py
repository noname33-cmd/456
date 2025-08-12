# FILE: haproxy_ops_worker.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Py2.7 — HAProxy ops queue worker
import os, sys, time, json, pipes, subprocess, threading
import fcntl
import argparse

BASE = "/tmp/pattern_controller"
DEFAULT_QDIR   = os.path.join(BASE, "signals", "haproxy_ops")
DEFAULT_IPDIR  = os.path.join(BASE, "signals", "haproxy_ops_inprogress")
DEFAULT_DDIR   = os.path.join(BASE, "signals", "haproxy_ops_done")
DEFAULT_FDIR   = os.path.join(BASE, "signals", "haproxy_ops_failed")
DEFAULT_REP    = os.path.join(BASE, "report")
DEFAULT_LOGS   = os.path.join(BASE, "logs")

def ensure_dir(p):
    if p and not os.path.isdir(p): os.makedirs(p)

def ts():
    import datetime as dt
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log_line(log_path, line):
    ensure_dir(os.path.dirname(log_path))
    data = ("[%s] %s\n" % (ts(), line)).encode("utf-8")
    try:
        f = open(log_path, "ab")
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            f.write(data); f.flush(); os.fsync(f.fileno())
        finally:
            try: fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except: pass
            f.close()
    except: pass
    try:
        sys.stdout.write(line + "\n")
    except: pass

def sh(cmd, timeout=10):
    p = subprocess.Popen(["/bin/sh","-lc", cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    killed=[False]
    def _kill():
        try: killed[0]=True; p.kill()
        except: pass
    tmr = threading.Timer(timeout, _kill); tmr.start()
    out, err = p.communicate()
    tmr.cancel()
    rc = 124 if killed[0] else p.returncode
    return rc, (out or b"").decode("utf-8","ignore"), (err or b"").decode("utf-8","ignore")

def write_csv_row(csv_path, headers, row):
    new = not os.path.exists(csv_path)
    f = open(csv_path, "ab")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        import csv
        w = csv.writer(f)
        if new: w.writerow(headers)
        w.writerow(row)
        f.flush(); os.fsync(f.fileno())
    finally:
        try: fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except: pass
        f.close()

def list_queue(qdir):
    try:
        items = [os.path.join(qdir, x) for x in os.listdir(qdir) if x.endswith(".json")]
        items.sort()
        return items
    except:
        return []

# ---- runtime / cfg helpers ----
def do_runtime(op, rt, backend, server, payload):
    if op == "enable":   return True, rt.enable_server(backend, server)
    if op == "disable":  return True, rt.disable_server(backend, server)
    if op == "drain":    return True, rt.drain_server(backend, server)
    if op == "weight":
        w = int(payload.get("weight", 1))
        return True, rt.set_weight(backend, server, w)
    return False, "unknown runtime op"

def do_cfg(op, cfg, backend, server):
    if op == "comment":     return cfg.comment_server(backend, server)
    if op == "uncomment":   return cfg.uncomment_server(backend, server)
    return (False, "unknown cfg op")

def main():
    ap = argparse.ArgumentParser(description="HAProxy ops queue worker (Py2.7)")
    ap.add_argument("--socket", default="/var/lib/haproxy/haproxy.sock", help="HAProxy runtime socket")
    ap.add_argument("--cfg", default="/etc/haproxy/haproxy.cfg")
    ap.add_argument("--backends", default="Jboss_client", help="comma list, optional filter")
    ap.add_argument("--queue-dir", default=DEFAULT_QDIR)
    ap.add_argument("--inprogress-dir", default=DEFAULT_IPDIR)
    ap.add_argument("--done-dir", default=DEFAULT_DDIR)
    ap.add_argument("--failed-dir", default=DEFAULT_FDIR)
    ap.add_argument("--report-dir", default=DEFAULT_REP)
    ap.add_argument("--log-dir", default=DEFAULT_LOGS)
    ap.add_argument("--reload-cmd", default="systemctl reload haproxy")
    ap.add_argument("--poll-sec", type=int, default=2)
    args = ap.parse_args()

    # lazy imports to keep deps local
    from haproxy_runtime import HAProxyRuntime
    from haproxy_cfg_parser import HAProxyCfg

    ensure_dir(args.queue_dir); ensure_dir(args.inprogress_dir)
    ensure_dir(args.done_dir);  ensure_dir(args.failed_dir)
    ensure_dir(args.report_dir); ensure_dir(args.log_dir)

    allowed = set([x.strip() for x in (args.backends or "").split(",") if x.strip()])
    rt = HAProxyRuntime(args.socket, timeout=3.0)
    cfg = HAProxyCfg(args.cfg, backends=allowed)

    ops_log = os.path.join(args.log_dir, "haproxy_ops_worker.log")
    csv_path = os.path.join(args.report_dir, "controller_summary.csv")
    HEAD = ["timestamp","host","node","phase","severity","action","result","note","op_log","logfile","line_snippet"]

    host = os.uname()[1]

    log_line(ops_log, "START haproxy_ops_worker on %s" % host)

    while True:
        jobs = list_queue(args.queue_dir)
        if not jobs:
            time.sleep(max(1,int(args.poll_sec))); continue

        for p in jobs:
            base = os.path.basename(p)
            try:
                # move to inprogress
                ip = os.path.join(args.inprogress_dir, base)
                try: os.rename(p, ip)
                except OSError:
                    # кто-то успел взять
                    continue

                try:
                    meta = json.load(open(ip, "rb"))
                except Exception as e:
                    log_line(ops_log, "BROKEN %s: %s" % (base, e))
                    os.rename(ip, os.path.join(args.failed_dir, base))
                    continue

                op     = (meta.get("op") or "").strip()
                scope  = (meta.get("scope") or "runtime").strip()   # runtime | cfg | both
                backend= (meta.get("backend") or "").strip()
                server = (meta.get("server") or "").strip()
                note   = (meta.get("note") or "").strip()
                payload= meta

                if allowed and backend not in allowed:
                    log_line(ops_log, "SKIP %s: backend %s not in allowed" % (base, backend))
                    os.rename(ip, os.path.join(args.failed_dir, base))
                    continue

                ok_all = True
                msg_all = []

                # RUNTIME
                if scope in ("runtime","both"):
                    ok1, msg1 = do_runtime(op, rt, backend, server, payload)
                    msg_all.append("runtime: %s" % (msg1.strip() if isinstance(msg1, basestring) else str(msg1)))
                    ok_all = ok_all and ok1

                # CFG
                if scope in ("cfg","both"):
                    ok2, msg2 = do_cfg(op, cfg, backend, server)
                    msg_all.append("cfg: %s" % (msg2,))
                    ok_all = ok_all and ok2
                    if ok2 and args.reload_cmd:
                        rc, out, err = sh(args.reload_cmd, timeout=20)
                        msg_all.append("reload rc=%s out=%s err=%s" % (rc, out.strip(), err.strip()))
                        ok_all = ok_all and (rc == 0)

                # запись в csv (node = server)
                write_csv_row(csv_path, HEAD, [
                    ts(), host, server or "-", "haproxy_op", "info",
                    "%s/%s" % (scope, op), ("OK" if ok_all else "FAIL"), "; ".join(msg_all)[:3000], "-", "-", "-"
                ])

                log_line(ops_log, "%s %s/%s %s -> %s" % (base, scope, op, ("%s/%s" % (backend, server)), "OK" if ok_all else "FAIL"))

                # move to done/failed
                os.rename(ip, os.path.join(args.done_dir if ok_all else args.failed_dir, base))

            except Exception as e:
                log_line(ops_log, "ERROR processing %s: %s" % (base, e))
                try:
                    os.rename(ip, os.path.join(args.failed_dir, base))
                except:
                    pass

        # короткая пауза между батчами
        time.sleep(0.2)

if __name__ == "__main__":
    main()
