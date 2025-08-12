#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Queue dispatcher for controlled JBoss restarts (Py2.7)

import os, sys, time, json, subprocess, threading, pipes, datetime as dt, socket, traceback

BASE_DIR = "/tmp/pattern_controller"
DEFAULT_FLAG_DIR   = BASE_DIR + "/signals"
DEFAULT_REPORT_DIR = BASE_DIR + "/report"
DEFAULT_LOG_DIR    = BASE_DIR + "/logs"

def now(): return dt.datetime.now()
def ts():  return now().strftime("%Y-%m-%d %H:%M:%S")

def ensure_dir(p):
    if p and not os.path.isdir(p): os.makedirs(p)

def sh(cmd, timeout=90, log=None):
    line = " ".join([pipes.quote(s) for s in ["/bin/sh","-lc",cmd]])
    if log: log("[RUN] " + cmd)
    try:
        p = subprocess.Popen(["/bin/sh","-lc",cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        killed=[False]
        def _kill():
            try: killed[0]=True; p.kill()
            except: pass
        tmr = threading.Timer(timeout, _kill); tmr.start()
        out, err = p.communicate()
        tmr.cancel()
        if log:
            if out: log("[OUT] " + out.strip())
            if err: log("[ERR] " + err.strip())
        if killed[0]:
            if log: log("[ERROR] timeout")
            return 124
        return p.returncode
    except Exception as e:
        if log: log("[ERROR] %s" % e)
        return 1

def tg_send(token, chat_id, text):
    if not token or not chat_id: return
    try:
        import urllib, urllib2
        url="https://api.telegram.org/bot%s/sendMessage" % token
        data=urllib.urlencode({"chat_id": chat_id, "text": text})
        urllib2.urlopen(urllib2.Request(url, data), timeout=10).read()
    except: pass

class Dispatcher(object):
    def __init__(self, flag_dir, report_dir, log_dir,
                 max_concurrent, stagger_sec,
                 per_node_cooldown, burst_window, burst_limit,
                 per_group_max, groups_file, worker_wait_sec):
        self.flag_dir, self.report_dir, self.log_dir = flag_dir, report_dir, log_dir
        ensure_dir(flag_dir); ensure_dir(report_dir); ensure_dir(log_dir)
        self.qdir = os.path.join(flag_dir, "queue")
        self.ipdir = os.path.join(flag_dir, "inprogress")
        self.ddir  = os.path.join(flag_dir, "done")
        self.fdir  = os.path.join(flag_dir, "failed")
        for d in (self.qdir, self.ipdir, self.ddir, self.fdir): ensure_dir(d)
        self.max_concurrent = int(max_concurrent)
        self.stagger_sec = int(stagger_sec)
        self.per_node_cooldown = int(per_node_cooldown)
        self.burst_window = int(burst_window)
        self.burst_limit  = int(burst_limit)
        self.per_group_max = int(per_group_max)
        self.worker_wait_sec = int(worker_wait_sec)
        self.groups = {}
        if groups_file and os.path.exists(groups_file):
            try: self.groups = json.load(open(groups_file,"rb"))
            except: self.groups = {}
        self.active = {}       # node -> start_ts
        self.node_last = {}    # node -> last_finish_ts
        self.group_active = {} # group -> count
        self.history = []      # [(ts, node, ok_bool)]
        self.hostname = socket.gethostname()

    # ---- helpers ----
    def log_path(self): return os.path.join(self.log_dir, "dispatcher.log")
    def log(self, line):
        path = self.log_path()
        try:
            with open(path,"ab") as f:
                f.write(("[%s] " % ts() + line + "\n").encode("utf-8"))
        except: pass
        try: sys.stdout.write(line + "\n")
        except: pass

    def write_csv(self, row):
        headers = ["timestamp","host","node","phase","severity","action","result","note","op_log","logfile","line_snippet"]
        csv_path = os.path.join(self.report_dir, "controller_summary.csv")
        new = not os.path.exists(csv_path)
        f = open(csv_path, "ab")
        try:
            import csv
            w = csv.writer(f)
            if new: w.writerow(headers)
            w.writerow(row)
        finally:
            f.close()

    def list_queue(self):
        try:
            items = [os.path.join(self.qdir, x) for x in os.listdir(self.qdir) if x.endswith(".json")]
            items.sort()  # по ts в имени
            return items
        except: return []

    def read_json(self, path):
        try: return json.load(open(path,"rb"))
        except: return None

    def move(self, src, dst_dir):
        try:
            base = os.path.basename(src)
            dst = os.path.join(dst_dir, base)
            os.rename(src, dst)
            return dst
        except Exception as e:
            self.log("move error: %s -> %s: %s" % (src, dst_dir, e))
            return None

    def cleanup_flags(self, node):
        rf = os.path.join(self.flag_dir, "restart_%s.txt" % node)
        df = os.path.join(self.flag_dir, "done_%s.txt" % node)
        for p in (rf, df):
            try:
                if os.path.exists(p): os.unlink(p)
            except: pass

    def can_start(self, node):
        # global limit
        if len(self.active) >= self.max_concurrent: return (False, "global_limit")
        # per-node cooldown
        last = self.node_last.get(node, 0)
        if (time.time() - last) < self.per_node_cooldown: return (False, "node_cooldown")
        # burst window
        t0 = time.time() - self.burst_window
        recent = [1 for (t,_,_) in self.history if t >= t0]
        if len(recent) >= self.burst_limit: return (False, "burst_limit")
        # per-group
        grp = self.groups.get(node)
        if grp:
            if self.group_active.get(grp,0) >= self.per_group_max:
                return (False, "group_limit")
        return (True, "ok")

    def mark_active(self, node):
        self.active[node] = time.time()
        grp = self.groups.get(node)
        if grp: self.group_active[grp] = self.group_active.get(grp,0) + 1

    def unmark_active(self, node):
        if node in self.active: del self.active[node]
        grp = self.groups.get(node)
        if grp and self.group_active.get(grp,0) > 0:
            self.group_active[grp] -= 1

    # ---- core ----
    def run_one(self, rq_path):
        rq = self.read_json(rq_path)
        if not rq:
            self.move(rq_path, self.fdir); return
        node = rq.get("node")
        comment_cmd   = rq.get("comment_cmd","true")
        uncomment_cmd = rq.get("uncomment_cmd","true")
        tg_token = rq.get("tg_token","")
        tg_chat  = rq.get("tg_chat","")
        reason   = rq.get("reason","pattern")

        def _log(m): self.log("node=%s %s" % (node, m))
        self.log("TAKE %s" % os.path.basename(rq_path))

        can, why = self.can_start(node)
        if not can:
            self.log("DEFER node=%s because %s" % (node, why))
            time.sleep(2)
            return

        # move to inprogress
        ipath = self.move(rq_path, self.ipdir)
        if not ipath: return
        self.mark_active(node)

        # comment
        rc_c = sh(comment_cmd, timeout=90, log=_log)
        self.write_csv([ts(), self.hostname, node, "comment", "critical", "comment_node", ("OK" if rc_c==0 else "FAIL"), reason, "-", "-", "-"])
        if tg_token and tg_chat: tg_send(tg_token, tg_chat, "[%s] %s/%s: COMMENT -> %s" % (ts(), self.hostname, node, "OK" if rc_c==0 else "FAIL"))
        if rc_c != 0:
            self.unmark_active(node)
            self.move(ipath, self.fdir)
            return

        # ensure clean + create restart flag
        self.cleanup_flags(node)
        rflag = os.path.join(self.flag_dir, "restart_%s.txt" % node)
        try:
            with open(rflag,"wb") as f:
                f.write("dispatch %s node=%s reason=%s\n" % (ts(), node, reason))
            self.write_csv([ts(), self.hostname, node, "signal", "critical", "create_restart_flag", "OK", rflag, "-", "-", "-"])
            if tg_token and tg_chat: tg_send(tg_token, tg_chat, "[%s] %s/%s: signal RESTART" % (ts(), self.hostname, node))
        except Exception as e:
            self.unmark_active(node)
            self.move(ipath, self.fdir)
            self.log("flag create error: %s" % e)
            return

        # wait done from worker
        dflag = os.path.join(self.flag_dir, "done_%s.txt" % node)
        deadline = time.time() + self.worker_wait_sec
        verified = "UNKNOWN"
        stop_rc = start_rc = "-"
        while time.time() < deadline:
            if os.path.exists(dflag):
                try:
                    raw = open(dflag,"rb").read().strip()
                    s = raw.lower()
                    verified = "OK" if "verify=ok" in s else ("FAIL" if "verify=fail" in s else "UNKNOWN")
                    parts = raw.replace("\n"," ").split()
                    for p in parts:
                        if p.startswith("stop_rc="):  stop_rc  = p.split("=",1)[1]
                        if p.startswith("start_rc="): start_rc = p.split("=",1)[1]
                except: pass
                break
            time.sleep(2)

        ok = (verified == "OK")
        self.node_last[node] = time.time()
        self.history.append((time.time(), node, ok))

        # uncomment only if OK
        if ok:
            rc_u = sh(uncomment_cmd, timeout=90, log=_log)
            self.write_csv([ts(), self.hostname, node, "uncomment", "critical", "uncomment_node", ("OK" if rc_u==0 else "FAIL"), "verify=OK", "-", "-", "-"])
            if tg_token and tg_chat: tg_send(tg_token, tg_chat, "[%s] %s/%s: UNCOMMENT -> %s" % (ts(), self.hostname, node, "OK" if rc_u==0 else "FAIL"))
        else:
            self.write_csv([ts(), self.hostname, node, "uncomment", "critical", "uncomment_node", "SKIP", "verify=%s" % verified, "-", "-", "-"])
            if tg_token and tg_chat: tg_send(tg_token, tg_chat, "[%s] %s/%s: verify=%s — node LEFT DISABLED" % (ts(), self.hostname, node, verified))

        # cleanup flags
        self.cleanup_flags(node)

        # archive rq
        self.unmark_active(node)
        self.move(ipath, self.ddir if ok else self.fdir)

        # pacing
        time.sleep(self.stagger_sec)

    def loop(self):
        self.log("dispatcher start on %s" % self.hostname)
        while True:
            try:
                queue = self.list_queue()
                if not queue:
                    time.sleep(1); continue
                for q in queue:
                    self.run_one(q)
            except KeyboardInterrupt:
                self.log("stopping by user"); break
            except Exception as e:
                self.log("loop error: %s\n%s" % (e, traceback.format_exc()))
                time.sleep(2)

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Queue Dispatcher for JBoss restarts (Py2.7)")
    ap.add_argument("--flag-dir", default=DEFAULT_FLAG_DIR)
    ap.add_argument("--report-dir", default=DEFAULT_REPORT_DIR)
    ap.add_argument("--log-dir", default=DEFAULT_LOG_DIR)
    ap.add_argument("--max-concurrent", type=int, default=1)
    ap.add_argument("--stagger-sec", type=int, default=30)
    ap.add_argument("--per-node-cooldown", type=int, default=600)
    ap.add_argument("--burst-window", type=int, default=900)
    ap.add_argument("--burst-limit", type=int, default=3)
    ap.add_argument("--groups-file", help="JSON: {\"nodeA\":\"group1\", \"nodeB\":\"group1\"}")
    ap.add_argument("--per-group-max", type=int, default=1)
    ap.add_argument("--worker-wait-sec", type=int, default=900, help="сколько ждать done_<node>.txt от воркера")
    args = ap.parse_args()

    d = Dispatcher(args.flag_dir, args.report_dir, args.log_dir,
                   args.max_concurrent, args.stagger_sec,
                   args.per_node_cooldown, args.burst_window, args.burst_limit,
                   args.per_group_max, args.groups_file, args.worker_wait_sec)
    d.loop()

if __name__ == "__main__":
    sys.exit(main())
