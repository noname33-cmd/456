#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Py2.7 — systemctl stop/start jboss, verify, done-flag + error logs to logs/worker_<node>.log

BASE = os.environ.get("PC_BASE", "/tmp/pattern_controller")
DEFAULT_FLAG_DIR   = os.path.join(BASE, "signals")
DEFAULT_REPORT_DIR = os.path.join(BASE, "report")
DEFAULT_LOG_DIR    = os.path.join(BASE, "logs")

import argparse, datetime as dt, os, sys, time, subprocess, threading, pipes, socket, re
import urllib, urllib2

def now(): return dt.datetime.now()
def ts():  return now().strftime("%Y-%m-%d %H:%M:%S")
def ensure_dir(p):
    if not os.path.exists(p): os.makedirs(p)

def _log(fh, line):
    try:
        if fh: fh.write(line + "\n"); fh.flush()
        else: sys.stdout.write(line + "\n")
    except: pass

def append_worker_err(log_dir, node, line):
    ensure_dir(log_dir)
    path = os.path.join(log_dir, "worker_%s.log" % node)
    try:
        with open(path, "ab") as f: f.write((line + "\n").encode("utf-8"))
    except: pass

def run_cmd(cmd_list, timeout=120, log=None, errlog=None, node=None):
    line=" ".join([pipes.quote(s) for s in cmd_list]); _log(log, "[%s] RUN: %s" % (ts(), line))
    try:
        proc=subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        killed=[False]
        def _kill():
            try: killed[0]=True; proc.kill()
            except: pass
        timer=None
        if timeout and timeout>0:
            timer=threading.Timer(timeout,_kill); timer.start()
        out,err=proc.communicate()
        if timer: timer.cancel()
        if out: _log(log, "[%s] STDOUT: %s" % (ts(), out.strip()))
        if err:
            s = "[%s] STDERR: %s" % (ts(), err.strip())
            _log(log, s)
            if errlog and node: append_worker_err(errlog, node, s)
        if killed[0]:
            s = "[%s] ERROR: timeout" % ts()
            _log(log, s)
            if errlog and node: append_worker_err(errlog, node, s)
            return 124
        return proc.returncode
    except Exception as e:
        s = "[%s] ERROR: %s" % (ts(), e)
        _log(log, s)
        if errlog and node: append_worker_err(errlog, node, s)
        return 1

def tg_send(token, chat_id, text):
    if not token or not chat_id or not text: return
    try:
        url="https://api.telegram.org/bot%s/sendMessage" % token
        data=urllib.urlencode({"chat_id": chat_id, "text": text})
        urllib2.urlopen(urllib2.Request(url, data), timeout=10).read()
    except Exception as e:
        pass

def http_ok(url, timeout=10):
    try:
        r=urllib2.urlopen(urllib2.Request(url), timeout=timeout)
        code=getattr(r, 'getcode', lambda: 200)()
        return (200 <= code < 400)
    except:
        return False

def tcp_open(host, port, timeout=2):
    try:
        s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout); s.connect((host, int(port))); s.close(); return True
    except:
        return False

def tail_find_regex(path, rx, bytes_limit=200000):
    try:
        fsz=os.path.getsize(path)
        with open(path,"rb") as f:
            if fsz>bytes_limit: f.seek(-bytes_limit, os.SEEK_END)
            data=f.read()
        try: text=data.decode("utf-8","ignore")
        except:
            try: text=data.decode("latin-1","ignore")
            except: text=""
        return rx.search(text) is not None
    except:
        return False

def systemd_active(service):
    try:
        rc=subprocess.call(["systemctl","is-active","--quiet", service])
        return rc==0
    except:
        return False

def verify_jboss(health_url, log_path, log_ok_regex, tcp_host, tcp_port, timeout_total, sleep_every, log=None):
    deadline=time.time()+max(5,int(timeout_total))
    rx=None
    if log_ok_regex:
        try: rx=re.compile(log_ok_regex, re.I|re.U)
        except: 
            if log: _log(log, "[%s] WARN bad log_ok_regex" % ts())
            rx=None
    while time.time()<deadline:
        active=systemd_active("jboss")
        cond2=False
        if health_url and http_ok(health_url, timeout=min(10, sleep_every)): cond2=True
        elif log_path and rx and tail_find_regex(log_path, rx): cond2=True
        elif tcp_host and tcp_port and tcp_open(tcp_host, tcp_port, timeout=min(3, sleep_every)): cond2=True
        if log: _log(log, "[%s] VERIFY: active=%s cond2=%s" % (ts(), str(active), str(cond2)))
        if active and cond2: return True
        time.sleep(max(1,int(sleep_every)))
    return False

def main():
    ap = argparse.ArgumentParser(description="Worker: stop/start jboss, verify, done-flag, Telegram")
    ap.add_argument("--node", required=True)
    ap.add_argument("--flag-dir", default=DEFAULT_FLAG_DIR)
    ap.add_argument("--report-dir", default=DEFAULT_REPORT_DIR)
    ap.add_argument("--log-dir", default=DEFAULT_LOG_DIR, help="куда писать worker_<node>.log (ошибки)")
    ap.add_argument("--poll-sec", type=int, default=2)
    ap.add_argument("--health-url")
    ap.add_argument("--log-path", help="например: /opt/jboss/standalone/log/server.log")
    ap.add_argument("--log-ok-regex", default=r"(started in|JBoss .* started|WildFly .* started)")
    ap.add_argument("--tcp-host")
    ap.add_argument("--tcp-port", type=int)
    ap.add_argument("--verify-timeout", type=int, default=240)
    ap.add_argument("--verify-every", type=int, default=5)
    ap.add_argument("--tg-token")
    ap.add_argument("--tg-chat")
    args = ap.parse_args()

    ensure_dir(args.flag_dir); ensure_dir(args.report_dir); ensure_dir(args.log_dir)
    host, node = socket.gethostname(), args.node
    day_log = os.path.join(args.report_dir, "worker_%s_%s.log" % (node, now().strftime("%Y%m%d")))
    log = open(day_log, "ab")
    _log(log, "[%s] START worker node=%s host=%s" % (ts(), node, host))

    restart_flag = os.path.join(args.flag_dir, "restart_%s.txt" % node)
    done_flag    = os.path.join(args.flag_dir, "done_%s.txt" % node)

    try:
        while True:
            if os.path.exists(restart_flag):
                _log(log, "[%s] DETECTED %s" % (ts(), restart_flag))
                try:
                    with open(restart_flag,"rb") as rf: _ = rf.read()
                except: pass
                try:
                    os.unlink(restart_flag); _log(log, "[%s] REMOVED %s" % (ts(), restart_flag))
                except Exception as e:
                    msg = "[%s] WARN cannot remove flag: %s" % (ts(), e)
                    _log(log, msg); append_worker_err(args.log_dir, node, msg)

                tg_send(args.tg_token, args.tg_chat, "[%s] %s/%s: stopping JBoss..." % (ts(), host, node))
                rc_stop = run_cmd(["systemctl","stop","jboss"], timeout=120, log=log, errlog=args.log_dir, node=node)

                rc_start=-1; verified_ok=False
                if rc_stop==0:
                    time.sleep(5)
                    tg_send(args.tg_token, args.tg_chat, "[%s] %s/%s: starting JBoss..." % (ts(), host, node))
                    rc_start = run_cmd(["systemctl","start","jboss"], timeout=180, log=log, errlog=args.log_dir, node=node)

                    verified_ok = verify_jboss(
                        health_url=args.health_url,
                        log_path=args.log_path,
                        log_ok_regex=args.log_ok_reg
                        if hasattr(args,"log_ok_reg") else args.log_ok_regex,
                        tcp_host=args.tcp_host,
                        tcp_port=args.tcp_port,
                        timeout_total=args.verify_timeout,
                        sleep_every=args.verify_every,
                        log=log
                    )
                    tg_send(args.tg_token, args.tg_chat, "[%s] %s/%s: verify %s" % (ts(), host, node, ("OK" if verified_ok else "FAIL")))
                    if not verified_ok:
                        append_worker_err(args.log_dir, node, "[%s] VERIFY FAIL host=%s node=%s" % (ts(), host, node))
                else:
                    msg = "[%s] ERROR stop failed, skip start" % ts()
                    _log(log, msg); append_worker_err(args.log_dir, node, msg)

                try:
                    with open(done_flag,"wb") as df:
                        df.write("ts=%s host=%s node=%s stop_rc=%s start_rc=%s verify=%s\n" %
                                 (ts(), host, node, str(rc_stop), str(rc_start), ("OK" if verified_ok else "FAIL")))
                    _log(log, "[%s] CREATED %s" % (ts(), done_flag))
                    tg_send(args.tg_token, args.tg_chat,
                            "[%s] %s/%s: done stop_rc=%s start_rc=%s verify=%s" %
                            (ts(), host, node, str(rc_stop), str(rc_start), ("OK" if verified_ok else "FAIL")))
                except Exception as e:
                    msg = "[%s] ERROR cannot create done flag: %s" % (ts(), e)
                    _log(log, msg); append_worker_err(args.log_dir, node, msg)

            time.sleep(max(1,int(args.poll_sec)))
    finally:
        try: log.close()
        except: pass

if __name__ == "__main__":
    sys.exit(main())
