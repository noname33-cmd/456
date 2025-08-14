export async function GET(req: Request) {
  const u = new URL(req.url);
  const limit = u.searchParams.get("limit") || "100";
  const apiBase = process.env.API_BASE || "http://127.0.0.1:35073";
  const token = process.env.TOGGLE_SECRET || "";
  const r = await fetch(apiBase + `/ops?limit=${encodeURIComponent(limit)}`, { headers: { "X-Auth-Token": token } });
  const text = await r.text();
  const data = text ? JSON.parse(text) : {};
  return Response.json(data, { status: r.status });
}
