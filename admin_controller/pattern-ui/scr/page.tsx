"use client";
import { useEffect, useMemo, useState } from "react";

export default function Page() {
  // локальные настройки браузера (не секреты)
  const [apiBase, setApiBase] = useState<string>(() => localStorage.getItem("pc.apiBase") || "");
  const [token, setToken] = useState<string>(() => localStorage.getItem("pc.token") || "");
  useEffect(()=>{ if(!apiBase){ fetch("/api/config").then(r=>r.json()).then(j=>setApiBase(j.apiBase)); }},[]);
  useEffect(()=>{ localStorage.setItem("pc.apiBase", apiBase); }, [apiBase]);
  useEffect(()=>{ localStorage.setItem("pc.token", token); }, [token]);
  const hdrs = useMemo(()=> token ? {"X-Auth-Token": token} : {}, [token]);

  // health
  const [health,setHealth]=useState<any>(null);
  const refreshHealth = async () => {
    const r = await fetch(`/api/health`, { headers: hdrs });
    setHealth(await r.json());
  };
  useEffect(()=>{ refreshHealth(); }, []);

  // ops
  const [opsLimit,setOpsLimit]=useState<number>(100);
  const [ops,setOps]=useState<any>(null);
  const refreshOps = async () => {
    const r = await fetch(`/api/ops?limit=${opsLimit}`, { headers: hdrs });
    setOps(await r.json());
  };

  // agg
  const [aggName,setAggName]=useState("agg_5m.json");
  const [agg,setAgg]=useState<any>(null);
  const refreshAgg = async () => {
    const r = await fetch(`/api/metrics/agg?name=${encodeURIComponent(aggName)}`, { headers: hdrs });
    setAgg(await r.json());
  };

  // graphs
  const [graphs,setGraphs]=useState<string[]>([]);
  const [graphSel,setGraphSel]=useState<string>("");
  const [graphData,setGraphData]=useState<any>(null);
  const refreshGraphs = async () => {
    const r = await fetch(`/api/graphs`, { headers: hdrs });
    setGraphs((await r.json()).graphs || []);
  };
  const loadGraph = async (name:string) => {
    setGraphSel(name);
    if(!name){ setGraphData(null); return; }
    const r = await fetch(`/api/graphs/${encodeURIComponent(name)}`, { headers: hdrs });
    setGraphData(await r.json());
  };

  // haproxy actions
  const [be,setBe]=useState("Jboss_client");
  const [srv,setSrv]=useState("");
  const [action,setAction]=useState("drain");
  const [toggleResp,setToggleResp]=useState<any>(null);
  const doToggle = async () => {
    setToggleResp(null);
    const r = await fetch(`/api/haproxy/toggle`, {
      method:"POST", headers:{"Content-Type":"application/json", ...hdrs},
      body: JSON.stringify({action, backend:be, server:srv})
    });
    setToggleResp(await r.json());
  };
  const [retryResp,setRetryResp]=useState<any>(null);
  const doRetry = async () => {
    setRetryResp(null);
    const r = await fetch(`/api/queue/retry`, { method:"POST", headers: hdrs });
    setRetryResp(await r.json());
  };

  return (
    <main className="row" style={{gap:16}}>
      {/* Config */}
      <section className="card">
        <div style={{display:"grid",gap:12, gridTemplateColumns:"repeat(3, minmax(0,1fr))"}}>
          <label> <div className="small">API URL</div>
            <input className="input" placeholder="http://127.0.0.1:35073" value={apiBase} onChange={e=>setApiBase(e.target.value)} />
          </label>
          <label> <div className="small">Auth token (TOGGLE_SECRET)</div>
            <input className="input" placeholder="••••••" value={token} onChange={e=>setToken(e.target.value)} />
          </label>
          <div style={{display:"flex",alignItems:"end",gap:8}}>
            <button className="btn primary" onClick={refreshHealth}>Обновить health</button>
          </div>
        </div>
      </section>

      {/* Row 1 */}
      <section className="row row-3">
        <div className="card">
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center", marginBottom:8}}>
            <h3 style={{margin:0}}>Health</h3>
            <button className="btn primary" onClick={refreshHealth}>Refresh</button>
          </div>
          {health?.error && <div className="small" style={{color:"#dc2626"}}>Ошибка: {String(health.error)}</div>}
          {health && !health.error && (
            <div className="small">
              <div>status: {health.status}</div>
              <div>ts: {health.ts}</div>
              <div style={{marginTop:8}}>
                <div className="small" style={{marginBottom:4}}>Nodes:</div>
                <div>{(health.nodes||[]).map((n:string)=> <span className="badge" key={n}>{n}</span>)}</div>
              </div>
            </div>
          )}
        </div>

        <div className="card">
          <h3 style={{marginTop:0}}>HAProxy — Быстрые действия</h3>
          <div style={{display:"grid",gap:8, gridTemplateColumns:"repeat(3, minmax(0,1fr))"}}>
            <label><div className="small">Backend</div><input className="input" value={be} onChange={e=>setBe(e.target.value)} /></label>
            <label><div className="small">Server</div><input className="input" value={srv} onChange={e=>setSrv(e.target.value)} placeholder="node-a3" /></label>
            <label><div className="small">Action</div>
              <select className="input" value={action} onChange={e=>setAction(e.target.value)}>
                <option value="drain">drain</option>
                <option value="disable">disable</option>
                <option value="enable">enable</option>
              </select>
            </label>
          </div>
          <div style={{marginTop:10, display:"flex", gap:8}}>
            <button className="btn blue" onClick={doToggle}>Выполнить</button>
            <button className="btn" onClick={doRetry}>Retry очередь</button>
          </div>
          {toggleResp && <pre style={{marginTop:10}}>{JSON.stringify(toggleResp, null, 2)}</pre>}
          {retryResp && <pre style={{marginTop:10}}>{JSON.stringify(retryResp, null, 2)}</pre>}
        </div>

        <div className="card">
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center", marginBottom:8}}>
            <h3 style={{margin:0}}>Aggregates</h3>
            <div style={{display:"flex",gap:8}}>
              <input className="input" value={aggName} onChange={e=>setAggName(e.target.value)} />
              <button className="btn primary" onClick={refreshAgg}>Загрузить</button>
            </div>
          </div>
          {agg?.error && <div className="small" style={{color:"#dc2626"}}>Ошибка: {String(agg.error)}</div>}
          {agg && !agg.error && <pre>{JSON.stringify(agg, null, 2)}</pre>}
        </div>
      </section>

      {/* Row 2 */}
      <section className="row row-3">
        <div className="card" style={{gridColumn:"span 2 / span 2"}}>
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center", marginBottom:8}}>
            <h3 style={{margin:0}}>Ops (последние)</h3>
            <div style={{display:"flex",gap:8}}>
              <input className="input" type="number" min={1} value={opsLimit} onChange={e=>setOpsLimit(Number(e.target.value)||1)} />
              <button className="btn primary" onClick={refreshOps}>Обновить</button>
            </div>
          </div>
          {!ops && <div className="small">Нет данных. Нажми «Обновить».</div>}
          {ops?.error && <div className="small" style={{color:"#dc2626"}}>Ошибка: {String(ops.error)}</div>}
          {ops?.by_node && (
            <div style={{display:"grid", gap:12}}>
              {Object.entries(ops.by_node).map(([node, table]: any) => (
                <div key={node} style={{border:"1px solid #e5e7eb", borderRadius:8, overflow:"hidden"}}>
                  <div style={{background:"#f1f5f9", padding:"6px 10px"}} className="small"><b>{node}</b></div>
                  <div style={{maxHeight:300, overflow:"auto"}}>
                    <table style={{width:"100%", fontSize:12}}>
                      <thead style={{position:"sticky", top:0, background:"#f8fafc"}}>
                        <tr>{(table.headers||[]).map((h:string, i:number)=><th key={i} style={{textAlign:"left", padding:"6px 8px", borderBottom:"1px solid #e5e7eb", whiteSpace:"nowrap"}}>{h}</th>)}</tr>
                      </thead>
                      <tbody>
                        {(table.rows||[]).map((r:string[], ri:number)=>(
                          <tr key={ri} style={{background: ri%2? "#fff":"#f8fafc"}}>
                            {r.map((c:string, ci:number)=><td key={ci} style={{padding:"6px 8px", borderBottom:"1px solid #eef2f7", whiteSpace:"nowrap"}}>{c}</td>)}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="card">
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center", marginBottom:8}}>
            <h3 style={{margin:0}}>Graphs</h3>
            <div style={{display:"flex",gap:8}}>
              <button className="btn primary" onClick={refreshGraphs}>Список</button>
              <select className="input" value={graphSel} onChange={e=>loadGraph(e.target.value)}>
                <option value="">— выбрать —</option>
                {graphs.map((g)=> <option key={g} value={g}>{g}</option>)}
              </select>
            </div>
          </div>
          {graphData?.error && <div className="small" style={{color:"#dc2626"}}>Ошибка: {String(graphData.error)}</div>}
          {graphData && !graphData.error && <pre>{JSON.stringify(graphData, null, 2)}</pre>}
          {!graphData && <div className="small">Выбери график из списка.</div>}
        </div>
      </section>
    </main>
  );
}
