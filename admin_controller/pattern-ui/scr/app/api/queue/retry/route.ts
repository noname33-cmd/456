export async function POST() {
  const apiBase = process.env.API_BASE || "http://127.0.0.1:35073";
  const token = process.env.TOGGLE_SECRET || "";
  const r = await fetch(apiBase + "/queue/retry", { method:"POST", headers: { "X-Auth-Token": token } });
  const text = await r.text();
  const data = text ? JSON.parse(text) : {};
  return Response.json(data, { status: r.status });
}
