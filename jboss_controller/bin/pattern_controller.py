#!/usr/bin/env python
# -*- coding: utf-8 -*-

# === Defaults under your tree ===
BASE_DIR = os.environ.get("PC_BASE", "/tmp/pattern_controller")
DEFAULT_FLAG_DIR   = os.path.join(BASE_DIR, "signals")
DEFAULT_REPORT_DIR = os.path.join(BASE_DIR, "report")
DEFAULT_LOG_DIR    = os.path.join(BASE_DIR, "logs")

import argparse, csv, datetime as dt, os, sys, time, subprocess, threading, pipes, socket, re, json, uuid
import urllib, urllib2, traceback

# --- Default rules (если не передан --rules-file) ---
DEFAULT_RULES = [
    {"pattern": r"\b(ERROR|Exception|CRITICAL|FATAL)\b", "severity": "critical", "action": "restart"},
    {"pattern": r"\bWARN(ING)?\b",                        "severity": "warn",     "action": "notify"},
    {"pattern": r"connection reset by peer",              "severity": "critical", "action": "restart"},
    {"pattern": r"connection reset",                      "severity": "critical", "action": "restart"},
    {"pattern": r"\bRST_STREAM\b",                        "severity": "critical", "action": "restart"},
    {"pattern": r"\bECONNRESET\b",                        "severity": "critical", "action": "restart"},
]

# ==== Utils ====
def now(): return dt.datetime.now()
def ts():  return now().strftime("%Y-%m-%d %H:%M:%S")

def ensure_dir(p):
    if p and not os.path.exists(p):
        os.makedirs(p)

def _log(fh, line):
    try:
        (fh or sys.stdout).write(line + "\n")
        (fh or sys.stdout).flush()
    except:
        pass

def append_node_log(log_dir, node, line):
    ensure_dir(log_dir)
    path = os.path.join(log_dir, "node_%s.log" % node)
    try:
        with open(path, "ab") as f:
            f.write((line + "\n").encode("utf-8"))
    except:
        pass

def append_controller_error(log_dir, line):
    ensure_dir(log_dir)
    path = os.path.join(log_dir, "pattern_errors.log")
    try:
        with open(path, "ab") as f:
            f.write((line + "\n").encode("utf-8"))
    except:
        pass

def run_cmd(cmd_list, timeout=60, log=None):
    line = " ".join([pipes.quote(s) for s in cmd_list])
    _log(log, "[%s] RUN: %s" % (ts(), line))
    try:
        proc = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        killed = [False]
        def _kill():
            try:
                killed[0] = True
                proc.kill()
            except:
                pass
        timer = None
        if timeout and timeout > 0:
            timer = threading.Timer(timeout, _kill); timer.start()
        out, err = proc.communicate()
        if timer: timer.cancel()
        if out:
            _log(log, "[%s] STDOUT: %s" % (ts(), out.strip()))
        if err:
            _log(log, "[%s] STDERR: %s" % (ts(), err.strip()))
        if killed[0]:
            m = "[%s] ERROR: timeout" % ts(); _log(log, m)
            return 124
        return proc.returncode
    except Exception as e:
        m = "[%s] ERROR: %s" % (ts(), e); _log(log, m)
        return 1

def write_csv_row(csv_path, headers, row):
    new = not os.path.exists(csv_path)
    ensure_dir(os.path.dirname(csv_path))
    f = open(csv_path, "ab")
    try:
        w = csv.writer(f)
        if new: w.writerow(headers)
        w.writerow(row)
    finally:
        f.close()

def tg_send(token, chat_id, text):
    if not token or not chat_id or not text: return
    try:
        url = "https://api.telegram.org/bot%s/sendMessage" % token
        data = urllib.urlencode({"chat_id": chat_id, "text": text})
        urllib2.urlopen(urllib2.Request(url, data), timeout=10).read()
    except Exception as e:
        append_controller_error(DEFAULT_LOG_DIR, "[%s] TG ERROR: %s" % (ts(), e))

def tg_send_long(token, chat_id, text, chunk=3500):
    if not token or not chat_id or not text: return
    for i in xrange(0, len(text), chunk):
        tg_send(token, chat_id, text[i:i+chunk])

# ==== Tail helper ====
class LogFollower(object):
    def __init__(self, path, seek_end=True, poll_interval=0.5):
        self.path, self.poll, self.seek_end = path, poll_interval, seek_end
        self._fh=None; self._ino=None; self._stop=False
    def stop(self):
        self._stop=True
        try:
            if self._fh: self._fh.close()
        except:
            pass
    def _open(self):
        while not self._stop:
            try:
                fh=open(self.path,"rb"); st=os.fstat(fh.fileno())
                self._fh, self._ino = fh, st.st_ino
                if self.seek_end: fh.seek(0, os.SEEK_END)
                return
            except IOError:
                time.sleep(self.poll)
    def lines(self):
        self._open()
        while not self._stop:
            line = self._fh.readline()
            if line:
                try:
                    yield line.decode("utf-8","ignore")
                except:
                    yield ""
                continue
            try:
                cur_ino=os.stat(self.path).st_ino
                if cur_ino!=self._ino:
                    try: self._fh.close()
                    except: pass
                    self._open()
            except OSError:
                time.sleep(self.poll); continue
            time.sleep(self.poll)

# ==== Queue integration (optional) ====
def enqueue_request(flag_dir, node, host, comment_cmd, uncomment_cmd, reason_pattern,
                    report_dir, tg_token, tg_chat):
    qdir = os.path.join(flag_dir, "queue")
    ensure_dir(qdir)
    rid = str(uuid.uuid4())[:8]
    tsid = now().strftime("%Y%m%d_%H%M%S")
    rq = {
        "id": rid,
        "ts": tsid,
        "node": node,
        "host": host,
        "comment_cmd": comment_cmd,
        "uncomment_cmd": uncomment_cmd,
        "reason": reason_pattern,
        "report_dir": report_dir,
        "tg_token": tg_token or "",
        "tg_chat": tg_chat or ""
    }
    path = os.path.join(qdir, "rq_%s_%s_%s.json" % (tsid, node, rid))
    try:
        with open(path, "wb") as f:
            f.write(json.dumps(rq))
    except Exception as e:
        append_controller_error(DEFAULT_LOG_DIR, "[%s] QUEUE WRITE ERROR: %s" % (ts(), e))
        return None
    return path

# ==== Controller ====
class PatternController(object):
    HEADERS = ["timestamp","host","node","phase","severity","action","result","note","op_log","logfile","line_snippet"]

    def __init__(self, logs, node, flag_dir, report_dir, log_dir, comment_cmd, uncomment_cmd,
                 ack_timeout_sec, cmd_timeout_sec, cooldown_sec, debounce_sec,
                 rules, tg_token, tg_chat, queue_mode, uncomment_on_fail):
        self.logs, self.node = logs, node
        self.flag_dir, self.report_dir, self.log_dir = flag_dir, report_dir, log_dir
        self.comment_cmd, self.uncomment_cmd = comment_cmd, uncomment_cmd
        self.ack_timeout_sec, self.cmd_timeout_sec = int(ack_timeout_sec), int(cmd_timeout_sec)
        self.cooldown_sec, self.debounce_sec = int(cooldown_sec), int(debounce_sec)
        self.host = socket.gethostname()
        self.rules = [{"rx": re.compile(r["pattern"], re.I|re.U),
                       "severity": r.get("severity","info"),
                       "action": r.get("action","notify")} for r in rules]
        self.tg_token, self.tg_chat = tg_token, tg_chat
        self.queue_mode = bool(queue_mode)
        self.uncomment_on_fail = (str(uncomment_on_fail).lower() in ("1","true","yes","y"))
        self._followers=[]; self._threads=[]; self._stopping=False
        self._last_action_ts=0.0; self._last_match_ts=0.0
        ensure_dir(self.flag_dir); ensure_dir(self.report_dir); ensure_dir(self.log_dir)
        self.csv_path = os.path.join(self.report_dir, "controller_summary.csv")

    def _cooldowns_ok(self):
        t=time.time()
        if (t - self._last_match_ts) < self.debounce_sec:
            return False, "debounced"
        if (t - self._last_action_ts) < self.cooldown_sec:
            return False, "cooldown"
        return True, "ok"

    def _cleanup_flags(self, node):
        # Чистим и restart_*.txt, и done_*.txt для идемпотентности
        paths = [
            os.path.join(self.flag_dir, "restart_%s.txt" % node),
            os.path.join(self.flag_dir, "done_%s.txt" % node),
        ]
        for p in paths:
            try:
                if os.path.exists(p): os.unlink(p)
            except Exception as e:
                append_controller_error(self.log_dir, "[%s] CLEANUP WARN node=%s path=%s err=%s" % (ts(), node, p, e))

    def _signal_restart_and_wait(self, log, severity, matched_log, matched_line, matched_pattern):
        op_id = now().strftime("%Y%m%d_%H%M%S")+"_"+self.node
        op_log_path = os.path.join(self.report_dir, "op_%s.log" % op_id)
        restart_flag = os.path.join(self.flag_dir, "restart_%s.txt" % self.node)
        done_flag    = os.path.join(self.flag_dir, "done_%s.txt" % self.node)

        # Комментирование
        rc = run_cmd(["/bin/sh","-lc", self.comment_cmd], timeout=self.cmd_timeout_sec, log=log)
        write_csv_row(self.csv_path, self.HEADERS,
                      [ts(), self.host, self.node, "comment", severity, "comment_node",
                       ("OK" if rc==0 else "FAIL"), "", op_log_path, matched_log, matched_line.strip()[:400]])
        tg_send_long(self.tg_token, self.tg_chat,
                     u"[%s] %s: COMMENT '%s' → %s" % (ts(), self.host, self.node, ("OK" if rc==0 else "FAIL")))
        append_node_log(self.log_dir, self.node, "[%s] COMMENT node → %s" % (ts(), "OK" if rc==0 else "FAIL"))
        if rc != 0:
            append_controller_error(self.log_dir, "[%s] COMMENT FAIL node=%s" % (ts(), self.node))
            return op_log_path, False, "comment_fail"

        # Подготовка флагов (чистка и создание restart)
        self._cleanup_flags(self.node)
        try:
            f=open(restart_flag,"wb")
            try:
                f.write("request_reboot %s node=%s host=%s reason=%s\n" % (ts(), self.node, self.host, matched_pattern))
            finally:
                f.close()
            write_csv_row(self.csv_path, self.HEADERS,
                          [ts(), self.host, self.node, "signal", severity, "create_restart_flag",
                           "OK", restart_flag, op_log_path, matched_log, matched_line.strip()[:400]])
            tg_send_long(self.tg_token, self.tg_chat, u"[%s] %s: RESTART signal for '%s'" % (ts(), self.host, self.node))
            append_node_log(self.log_dir, self.node, "[%s] SIGNAL restart flag created" % ts())
        except Exception as e:
            append_controller_error(self.log_dir, "[%s] FLAG CREATE FAIL node=%s err=%s" % (ts(), self.node, e))
            return op_log_path, False, "flag_create_fail"

        # Ожидание done
        deadline = time.time() + self.ack_timeout_sec
        got_ack=False; ack_note=""; verify_status="UNKNOWN"
        while time.time() < deadline:
            if os.path.exists(done_flag):
                got_ack=True
                try:
                    with open(done_flag,"rb") as df:
                        ack_note = df.read().strip()
                        s = (ack_note or "").lower()
                        if "verify=ok" in s:   verify_status="OK"
                        elif "verify=fail" in s: verify_status="FAIL"
                except:
                    ack_note="ack"
                break
            time.sleep(2)

        # Решение по раскомментированию
        if got_ack:
            do_uncomment = True
            if verify_status=="FAIL" and not self.uncomment_on_fail:
                do_uncomment = False
            if do_uncomment:
                rc2 = run_cmd(["/bin/sh","-lc", self.uncomment_cmd], timeout=self.cmd_timeout_sec, log=log)
                write_csv_row(self.csv_path, self.HEADERS,
                              [ts(), self.host, self.node, "uncomment", severity, "uncomment_node",
                               ("OK" if rc2==0 else "FAIL"), ack_note, op_log_path, matched_log, matched_line.strip()[:400]])
                tg_send_long(self.tg_token, self.tg_chat,
                             u"[%s] %s: UNCOMMENT '%s' → %s\nACK: %s" %
                             (ts(), self.host, self.node, ("OK" if rc2==0 else "FAIL"), ack_note))
                append_node_log(self.log_dir, self.node,
                                "[%s] UNCOMMENT node → %s; ACK: %s" % (ts(), "OK" if rc2==0 else "FAIL", ack_note))
            else:
                write_csv_row(self.csv_path, self.HEADERS,
                              [ts(), self.host, self.node, "uncomment", severity, "uncomment_node",
                               "SKIP", "verify=%s; %s" % (verify_status, ack_note), op_log_path, matched_log, matched_line.strip()[:400]])
                tg_send_long(self.tg_token, self.tg_chat,
                             u"[%s] %s: verify=%s — node LEFT DISABLED\nACK: %s" %
                             (ts(), self.host, verify_status, ack_note))
                append_node_log(self.log_dir, self.node,
                                "[%s] UNCOMMENT SKIP (verify=%s) ; node left disabled" % (ts(), verify_status))
        else:
            write_csv_row(self.csv_path, self.HEADERS,
                          [ts(), self.host, self.node, "wait_ack", severity, "timeout",
                           "FAIL", "ACK TIMEOUT %ss" % self.ack_timeout_sec, op_log_path, matched_log, matched_line.strip()[:400]])
            tg_send_long(self.tg_token, self.tg_chat, u"[%s] %s: ACK TIMEOUT for '%s'" % (ts(), self.host, self.node))
            append_controller_error(self.log_dir, "[%s] ACK TIMEOUT node=%s" % (ts(), self.node))

        # Финальная чистка флагов
        self._cleanup_flags(self.node)
        write_csv_row(self.csv_path, self.HEADERS,
                      [ts(), self.host, self.node, "cleanup", severity, "remove_flags", "OK", "restart/done removed", op_log_path, matched_log, matched_line.strip()[:400]])
        return op_log_path, got_ack, ack_note or "no-ack"

    def _handle_match(self, matched_log, matched_line, rule):
        self._last_match_ts = time.time()
        can, reason = self._cooldowns_ok()

        op_id = now().strftime("%Y%m%d_%H%M%S")+"_"+self.node
        op_log_path = os.path.join(self.report_dir, "op_%s.log" % op_id)
        log = open(op_log_path, "ab")

        try:
            sev, action = rule["severity"], rule["action"]
            append_node_log(self.log_dir, self.node,
                            "[%s] MATCH sev=%s action=%s file=%s line=%s" %
                            (ts(), sev, action, matched_log, matched_line.strip()[:300]))

            if not can:
                write_csv_row(self.csv_path, self.HEADERS,
                              [ts(), self.host, self.node, "match", sev, "skip", reason, op_log_path, matched_log, matched_line.strip()[:400]])
                append_node_log(self.log_dir, self.node, "[%s] SKIP due to %s" % (ts(), reason))
                return

            self._last_action_ts = time.time()

            if action == "restart":
                # Очередной режим — кладём заявку и выходим
                if self.queue_mode:
                    rq_path = enqueue_request(self.flag_dir, self.node, self.host,
                                              self.comment_cmd, self.uncomment_cmd,
                                              rule["rx"].pattern, self.report_dir,
                                              self.tg_token, self.tg_chat)
                    result = "OK" if rq_path else "FAIL"
                    note = rq_path or "enqueue_failed"
                    write_csv_row(self.csv_path, self.HEADERS,
                                  [ts(), self.host, self.node, "queue", sev, "enqueue", result, note, op_log_path, matched_log, matched_line.strip()[:400]])
                    if rq_path:
                        tg_send_long(self.tg_token, self.tg_chat,
                                     u"[%s] %s: QUEUED restart for '%s' (%s)" % (ts(), self.host, self.node, rq_path))
                        append_node_log(self.log_dir, self.node,
                                        "[%s] QUEUED rq=%s" % (ts(), os.path.basename(rq_path)))
                    else:
                        append_controller_error(self.log_dir, "[%s] QUEUE FAIL node=%s" % (ts(), self.node))
                    return

                # Немедленный режим — старая логика, но с безопасностями
                tg_send_long(self.tg_token, self.tg_chat,
                             u"[%s] %s: MATCH '%s' on %s → RESTART (%s)" %
                             (ts(), self.host, rule["rx"].pattern, matched_log, self.node))
                self._signal_restart_and_wait(log, sev, matched_log, matched_line, rule["rx"].pattern)
                return

            elif action == "notify":
                write_csv_row(self.csv_path, self.HEADERS,
                              [ts(), self.host, self.node, "notify", sev, "notify", "line_only", op_log_path, matched_log, matched_line.strip()[:400]])
                tg_send_long(self.tg_token, self.tg_chat,
                             u"[%s] %s: NOTIFY %s\n%s" %
                             (ts(), self.host, matched_log, matched_line.strip()[:3500]))
            else:
                write_csv_row(self.csv_path, self.HEADERS,
                              [ts(), self.host, self.node, "noop", sev, "none", "rule_action_none", op_log_path, matched_log, matched_line.strip()[:400]])

        except Exception as e:
            append_controller_error(self.log_dir, "[%s] HANDLE ERR node=%s err=%s\n%s" %
                                    (ts(), self.node, e, traceback.format_exc()))
        finally:
            try: log.close()
            except: pass

    def _watch_one(self, logfile):
        follower = LogFollower(logfile, seek_end=True)
        self._followers.append(follower)
        for line in follower.lines():
            if self._stopping: break
            for rule in self.rules:
                try:
                    if rule["rx"].search(line):
                        self._handle_match(logfile, line, rule); break
                except Exception as e:
                    append_controller_error(self.log_dir, "[%s] WATCH ERR log=%s err=%s" % (ts(), logfile, e))

    def start(self):
        for p in self.logs:
            t=threading.Thread(target=self._watch_one, args=(p,))
            t.setDaemon(True); t.start(); self._threads.append(t)
        try:
            while any(t.is_alive() for t in self._threads):
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self._stopping=True
        for f in self._followers:
            try: f.stop()
            except: pass

# ==== CLI ====
def parse_args():
    ap = argparse.ArgumentParser(description="Pattern Controller: tail logs 24/7, classify, queue or immediate comment->restart via flag->wait done->(un)comment, Telegram")
    ap.add_argument("--logs", nargs="+", required=True, help="пути к логам для мониторинга")
    ap.add_argument("--node", required=True, help="уникальное имя ноды")
    ap.add_argument("--flag-dir", default=DEFAULT_FLAG_DIR)
    ap.add_argument("--report-dir", default=DEFAULT_REPORT_DIR)
    ap.add_argument("--log-dir", default=DEFAULT_LOG_DIR)
    ap.add_argument("--comment-cmd", required=True, help="команда комментирования ноды в LB")
    ap.add_argument("--uncomment-cmd", required=True, help="команда снятия комментария")
    ap.add_argument("--ack-timeout-sec", type=int, default=900, help="макс ожидание done_<node>.txt")
    ap.add_argument("--cmd-timeout-sec", type=int, default=60, help="таймаут на comment/uncomment команды")
    ap.add_argument("--cooldown-sec", type=int, default=600, help="пауза между действиями")
    ap.add_argument("--debounce-sec", type=int, default=10, help="игнор повторов вблизи")
    ap.add_argument("--rules-file", help="JSON с правилами (regex/severity/action)")
    ap.add_argument("--tg-token")
    ap.add_argument("--tg-chat")
    ap.add_argument("--queue-mode", action="store_true", help="не рестартовать сразу; класть заявку в signals/queue/")
    ap.add_argument("--uncomment-on-fail", default="no", help="yes/no — раскомментировать ли при verify=FAIL (по умолчанию no)")
    return ap.parse_args()

def main():
    args = parse_args()
    rules = []
    if args.rules_file and os.path.exists(args.rules_file):
        try:
            with open(args.rules_file,"rb") as f:
                rules = json.load(f)
        except:
            rules = []
    if not rules:
        rules = DEFAULT_RULES

    PatternController(
        logs=args.logs, node=args.node,
        flag_dir=args.flag_dir, report_dir=args.report_dir, log_dir=args.log_dir,
        comment_cmd=args.comment_cmd, uncomment_cmd=args.uncomment_cmd,
        ack_timeout_sec=args.ack_timeout_sec, cmd_timeout_sec=args.cmd_timeout_sec,
        cooldown_sec=args.cooldown_sec, debounce_sec=args.debounce_sec,
        rules=rules, tg_token=args.tg_token, tg_chat=args.tg_chat,
        queue_mode=args.queue_mode, uncomment_on_fail=args.uncomment_on_fail
    ).start()

if __name__ == "__main__":
    sys.exit(main())
