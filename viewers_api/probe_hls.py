# probe_hls.py
import asyncio, aiohttp, json, random, string, urllib.parse, sys, argparse

CLIENT_IDS = [
    "kimne78kx3ncx6brgo4mv6wki5h1ko",
    "n8z5w9dwbaj38c6u1hk3w1ys3k4hv9",
    "gp762oj0d8ezg5drgsw9j7f0ga1pno",
]

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/125.0.0.0 Safari/537.36")

def rand_device(n=32):
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(n))

async def fetch(session, url, *, proxy=None, **kwargs):
    async with session.get(url, proxy=proxy, **kwargs) as r:
        text = await r.text()
        return r.status, text

async def post_json(session, url, json_body, *, proxy=None, **kwargs):
    async with session.post(url, json=json_body, proxy=proxy, **kwargs) as r:
        txt = await r.text()
        return r.status, txt

async def get_playback_token(session, channel, x_device_id, oauth=None, proxy=None):
    cid = random.choice(CLIENT_IDS)
    headers = {
        "Client-ID": cid,
        "Content-Type": "application/json",
        "Origin": "https://www.twitch.tv",
        "Referer": f"https://www.twitch.tv/{channel}",
        "X-Device-Id": x_device_id,
        "User-Agent": UA,
    }
    if oauth:
        headers["Authorization"] = f"OAuth {oauth}"

    payload = [{
        "operationName": "PlaybackAccessToken_Template",
        "variables": {
            "isLive": True,
            "login": channel,
            "isVod": False,
            "vodID": "",
            "playerType": "site"
        },
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "0828119ded59d43f0e4c3c0a42878b3b5e0f7aa774e32e1bdc6f8f3f2e0e0eb6"
            }
        }
    }]

    status, txt = await post_json(session, "https://gql.twitch.tv/gql", payload, proxy=proxy, headers=headers)
    if status != 200:
        raise RuntimeError(f"GQL {status}: {txt[:200]}")
    data = json.loads(txt)
    sig = data[0]["data"]["streamPlaybackAccessToken"]["signature"]
    token = data[0]["data"]["streamPlaybackAccessToken"]["value"]
    return sig, token

def build_usher_url(channel, sig, token):
    token_qs = urllib.parse.quote(token, safe="")
    return (
        f"https://usher.ttvnw.net/api/channel/hls/{channel}.m3u8"
        f"?sig={sig}&token={token_qs}"
        f"&allow_source=true&fast_bread=true&p={random.randint(10_000, 99_999_999)}"
    )

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", required=True)
    ap.add_argument("--proxy", help="http://user:pass@host:port or http://host:port")
    ap.add_argument("--oauth", help="OAuth token (optional)")
    ap.add_argument("--timeout", type=int, default=15)
    args = ap.parse_args()

    timeout = aiohttp.ClientTimeout(total=args.timeout)
    x_device_id = rand_device()
    headers = {
        "User-Agent": UA,
        "Origin": "https://www.twitch.tv",
        "Referer": f"https://www.twitch.tv/{args.channel}",
        "X-Device-Id": x_device_id,
        "Accept": "*/*",
    }
    if args.oauth:
        headers["Authorization"] = f"OAuth {args.oauth}"

    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        print(f"[i] Getting token for #{args.channel} (proxy={bool(args.proxy)}) …")
        sig, token = await get_playback_token(session, args.channel, x_device_id, oauth=args.oauth, proxy=args.proxy)
        usher = build_usher_url(args.channel, sig, token)
        print("[i] Usher:", usher)

        print("[i] Fetch master m3u8 …")
        st, master = await fetch(session, usher, proxy=args.proxy)
        if st != 200:
            print(f"[!] Master HTTP {st}:\n{master[:400]}")
            return
        print("[ok] Master loaded")

        # найдём первый медиаплейлист
        media_url = None
        for line in (ln.strip() for ln in master.splitlines() if ln.strip()):
            if line.endswith(".m3u8") and not line.startswith("#"):
                media_url = urllib.parse.urljoin(usher, line)
                break
        if not media_url:
            print("[!] No variant playlist in master")
            return

        print("[i] Fetch variant m3u8 …")
        st, media = await fetch(session, media_url, proxy=args.proxy)
        if st != 200:
            print(f"[!] Variant HTTP {st}:\n{media[:400]}")
            return
        print("[ok] Variant loaded")

        # найдём первый сегмент
        seg_url = None
        for line in (ln.strip() for ln in media.splitlines() if ln and not ln.startswith("#")):
            seg_url = urllib.parse.urljoin(media_url, line)
            break
        if not seg_url:
            print("[!] No segments in variant")
            return

        print("[i] Fetch first segment …")
        async with session.get(seg_url, proxy=args.proxy) as r:
            if r.status != 200:
                print(f"[!] Segment HTTP {r.status}")
                print(await r.text())
                return
            _ = await r.read()
            print("[ok] Segment downloaded")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
