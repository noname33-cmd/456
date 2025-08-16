export async function POST(req: Request) {
  const body = await req.json().catch(()=> ({}));
  const apiBase = process.env.API_BASE || "http://127.0.0.1:35073";
  const token = process.env.TOGGLE_SECRET || "";
  const r = await fetch(apiBase + "/haproxy/toggle", {
    method: "POST",
    headers: { "Content-Type":"application/json", "X-Auth-Token": token },
    body: JSON.stringify(body || {})
  });
  const text = await r.text();
  const data = text ? JSON.parse(text) : {};
  return Response.json(data, { status: r.status });
}
