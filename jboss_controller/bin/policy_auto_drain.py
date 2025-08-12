#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
policy_auto_drain.py — авто-действия по метрикам/флагам (Py2.7).

Логика (MVP):
  1) Если для server verify=FAIL (из done_<node>.txt) И одновременно рост 5xx за 5 минут > --thr-5xx,
     то кладём заявки в очередь haproxy_ops: drain + weight=0.
  2) Если затем verify=OK держится --heal-min минут и 5xx за 5 минут < --heal-5xx,
     возвращаем weight=1 и enable.

Источники:
  - agg_5m.json из stats_collector_haproxy.py
  - signals/done_<node>.txt (ищем "verify=OK/FAIL")
  - опционально: список допустимых backend-ов (--backend Jboss_client)

Выход:
  - заявки в /signals/haproxy_ops/*.json
  - событие в /signals/events/*.json
"""

import os, sys, time, json, argparse

DEFAULT_ROOT   = os.environ.get("PC_BASE", "/tmp/pattern_controller")
DEFAULT_SIGNALS = os.path.join(DEFAULT_ROOT, "signals")
DEFAULT_REPORT  = os.path.join(DEFAULT_ROOT, "report")
DEFAULT_OPS_Q   = os.path.join(DEFAULT_SIGNALS, "haproxy_ops")
EVENTS_DIR      = os.path.join(DEFAULT_SIGNALS, "events")

def ensure_dir(p):
    if p and not os.path.isdir(p): os.makedirs(p)

def emit_event(text, severity="info", node=None):
    ensure_dir(EVENTS_DIR)
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    path = os.path.join(EVENTS_DIR, "ev_%s_auto_drain_%s.json" % (ts, node or "na"))
    try:
        open(path,"wb").write(json.dumps({"ts": ts, "severity": severity, "source":"auto_drain", "node": node, "text": text}))
    except: pass

def enqueue_op(qdir, op, scope, backend, server, extra=None):
    ensure_dir(qdir)
    rid = ("%08x" % int(time.time()*1000))[-8:]
    tsid = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    payload = {"id": rid, "ts": tsid, "op": op, "scope": scope, "backend": backend, "server": server}
    if extra: payload.update(extra)
    name = "rq_%s_%s_%s_%s.json" % (tsid, backend or "be", server or "srv", rid)
    path = os.path.join(qdir, name)
    try:
        open(path,"wb").write(json.dumps(payload)); return True, path
    except Exception as e:
        return False, str(e)

def read_agg_5m(agg_path):
    try:
        return json.load(open(agg_path,"rb"))
    except:
        return {}

def list_nodes_from_verify(flag_dir):
    nodes=set()
    if not flag_dir or not os.path.isdir(flag_dir): return nodes
    for n in os.listdir(flag_dir):
        if n.startswith("done_") and n.endswith(".txt"):
            nodes.add(n[5:-4])
        if n.startswith("restart_") and n.endswith(".txt"):
            nodes.add(n[8:-4])
    return nodes

def read_verify(flag_dir, node):
    okp = os.path.join(flag_dir, "done_%s.txt" % node)
    if not os.path.exists(okp): return None, None
    try:
        data = open(okp,"rb").read()
        try: txt = data.decode("utf-8","ignore")
        except: txt = data.decode("latin-1","ignore")
        ts = os.path.getmtime(okp)
        s = txt.lower()
        if "verify=ok" in s: return "OK", ts
        if "verify=fail" in s: return "FAIL", ts
        return None, ts
    except:
        return None, None

def main():
    ap = argparse.ArgumentParser(description="Auto drain policy (Py2.7)")
    ap.add_argument("--signals-dir", default=DEFAULT_SIGNALS)
    ap.add_argument("--report-dir", default=DEFAULT_REPORT)
    ap.add_argument("--ops-queue",  default=DEFAULT_OPS_Q)
    ap.add_argument("--backend", default="Jboss_client")
    ap.add_argument("--thr-5xx", type=int, default=20, help="порог 5xx за 5 минут для drain")
    ap.add_argument("--heal-5xx", type=int, default=2, help="порог 5xx для возвращения в пул")
    ap.add_argument("--heal-min", type=int, default=10, help="сколько минут после verify=OK ждать перед возвратом")
    args = ap.parse_args()

    agg5_path = os.path.join(args.report_dir, "metrics", "agg_5m.json")
    agg = read_agg_5m(agg5_path)  # key "backend/server": {sum_5xx, ...}

    flag_dir = args.signals_dir
    nodes = list_nodes_from_verify(flag_dir)

    # состояние: дергаем лёгкую state БД в /report/auto_drain_state.json
    state_path = os.path.join(args.report_dir, "auto_drain_state.json")
    try:
        state = json.load(open(state_path,"rb"))
    except:
        state = {}  # node -> {"drained": bool, "last_ok_ts": epoch}

    changed = False

    for node in nodes:
        verify, vts = read_verify(flag_dir, node)
        key = "%s/%s" % (args.backend, node)
        m = agg.get(key, {})
        sum5 = int(m.get("sum_5xx", 0) or 0)

        st = state.get(node) or {"drained": False, "last_ok_ts": 0}
        drained = bool(st.get("drained"))

        if verify == "FAIL" and sum5 >= args.thr_5xx and not drained:
            # авто-drain
            ok1,_ = enqueue_op(args.ops_queue, "drain",  "runtime", args.backend, node, None)
            ok2,_ = enqueue_op(args.ops_queue, "weight", "runtime", args.backend, node, {"weight": 0})
            emit_event("auto drain %s (5xx=%d/5m, verify=FAIL) → drain + weight=0" % (node, sum5), "critical", node=node)
            st["drained"] = True
            changed = True

        if verify == "OK":
            st["last_ok_ts"] = int(vts or time.time())
            # можно ли вернуть?
            if drained:
                age_min = int((time.time() - st["last_ok_ts"]) / 60)
                if age_min >= args.heal_min and sum5 <= args.heal_5xx:
                    ok1,_ = enqueue_op(args.ops_queue, "weight", "runtime", args.backend, node, {"weight": 1})
                    ok2,_ = enqueue_op(args.ops_queue, "enable", "runtime", args.backend, node, None)
                    emit_event("auto heal %s (verify=OK %dmin, 5xx=%d/5m) → weight=1 + enable" % (node, age_min, sum5), "info", node=node)
                    st["drained"] = False
                    changed = True

        state[node] = st

    if changed:
        tmp = state_path + ".tmp"
        try:
            open(tmp,"wb").write(json.dumps(state, ensure_ascii=False, sort_keys=True, indent=2))
            os.rename(tmp, state_path)
        except: pass

    sys.stdout.write("policy_auto_drain done, nodes=%d\n" % len(nodes))

if __name__ == "__main__":
    main()
