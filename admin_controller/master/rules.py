import httpx
from typing import Dict, Any

class RuleEngine:
    """
    Простой rules-движок с гистерезисом:
    - Если status != UP ИЛИ hrsp_5xx > порога N тиков подряд → drain
    - Если затем status == UP И hrsp_5xx <= порога M тиков подряд → enable
    """
    def __init__(self, cfg: Dict[str, Any], token: str):
        self.cfg = cfg
        self.token = token
        # состояние по нодам: (cluster, node_name) -> dict
        self.state: Dict[tuple, Dict[str, Any]] = {}

    async def tick(self):
        rules = self.cfg.get("rules", {})
        if not rules:
            return

        thr5   = int(rules.get("hrsp_5xx_threshold", 5))
        need_b = int(rules.get("bad_intervals_required", 3))
        need_g = int(rules.get("good_intervals_required", 3))

        headers = {"X-Auth-Token": self.token} if self.token else {}

        async with httpx.AsyncClient(timeout=8, verify=False) as client:
            for cid, c in (self.cfg.get("clusters") or {}).items():
                for n in (c.get("nodes") or []):
                    base     = n["agent_base_url"]
                    backend  = n["haproxy_backend"]
                    server   = n["haproxy_server"]
                    key      = (cid, n["name"])
                    st       = self.state.setdefault(key, {"bad": 0, "good": 0, "drained": False})

                    # Получаем nested-метрики от агента
                    try:
                        r = await client.get(f"{base}/haproxy/stat", headers=headers)
                        r.raise_for_status()
                        nested = (r.json() or {}).get("data", {})
                        row = (nested.get(backend, {}) or {}).get(server)
                    except Exception:
                        # Агент недоступен — считаем «плохим» тиком
                        row = None

                    if not row:
                        st["bad"] += 1
                        st["good"] = 0
                        await self._maybe_act(client, headers, n, st, need_b, need_g)
                        continue

                    status   = (row.get("status") or "").upper()
                    hrsp_5xx = int((row.get("hrsp_5xx") or "0") or "0")

                    if status != "UP" or hrsp_5xx > thr5:
                        st["bad"]  += 1
                        st["good"]  = 0
                    else:
                        st["good"] += 1
                        st["bad"]   = 0

                    await self._maybe_act(client, headers, n, st, need_b, need_g)

    async def _maybe_act(self, client: httpx.AsyncClient, headers: Dict[str, str],
                         n: Dict[str, Any], st: Dict[str, Any], need_b: int, need_g: int):
        base    = n["agent_base_url"]
        backend = n["haproxy_backend"]
        server  = n["haproxy_server"]

        if st["bad"] >= need_b and not st["drained"]:
            await client.post(f"{base}/node/{backend}/{server}/drain", headers=headers)
            st["drained"] = True

        elif st["good"] >= need_g and st["drained"]:
            await client.post(f"{base}/node/{backend}/{server}/enable", headers=headers)
            st["drained"] = False
