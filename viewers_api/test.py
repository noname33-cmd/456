import asyncio
import aiohttp
import json

NOTION_HOST = "http://45.144.53.236:9001"
NOTION_KEY = "pCQWuf25KmNTY8dSqJUW3nqhz58Zyq4G"

async def main():
    url = f"{NOTION_HOST}/api/v1/solution"
    params = {"apikey": NOTION_KEY, "site": "twitch"}
    async with aiohttp.ClientSession() as s:
        async with s.get(url, params=params) as r:
            txt = await r.text()
            print(f"HTTP {r.status}")
            print(txt)
            try:
                data = json.loads(txt)
                print(json.dumps(data, indent=2, ensure_ascii=False))
            except Exception as e:
                print(f"JSON parse error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
