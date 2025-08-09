# config.py
import json
import os
from dataclasses import dataclass
from typing import List

CONFIG_PATH = "config.json"

@dataclass
class ServerCfg:
    port: str

@dataclass
class CORSCfg:
    allowed_origins: List[str]
    allowed_methods: List[str]

@dataclass
class SalamoonderCfg:
    enable: bool
    apikey: str

@dataclass
class NotionCfg:
    enable: bool
    host: str
    apikey: str

@dataclass
class KasadaCfg:
    salamoonder: SalamoonderCfg
    notion: NotionCfg

@dataclass
class Settings:
    server: ServerCfg
    cors: CORSCfg
    kasada: KasadaCfg

def _load_env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "on")

def _load_env_list(name: str, default: list[str]) -> list[str]:
    v = os.getenv(name)
    if not v:
        return default
    return [x.strip() for x in v.split(",") if x.strip()]

def load_settings(path: str = CONFIG_PATH) -> Settings:
    # читаем json если есть (локальная разработка)
    raw = {}
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

    # env имеет приоритет
    server = ServerCfg(port=os.getenv("PORT", raw.get("server", {}).get("port", "7702")))
    cors = CORSCfg(
        allowed_origins=_load_env_list("CORS_ALLOWED_ORIGINS", raw.get("cors", {}).get("allowed_origins", ["*"])),
        allowed_methods=_load_env_list(
            "CORS_ALLOWED_METHODS",
            raw.get("cors", {}).get("allowed_methods", ["GET", "POST", "PUT", "DELETE", "OPTIONS"])
        ),
    )
    kasada = KasadaCfg(
        salamoonder=SalamoonderCfg(
            enable=_load_env_bool("KASADA_SALAMOONDER_ENABLE", raw.get("kasada", {}).get("salamoonder", {}).get("enable", False)),
            apikey=os.getenv("KASADA_SALAMOONDER_APIKEY", raw.get("kasada", {}).get("salamoonder", {}).get("apikey", "")),
        ),
        notion=NotionCfg(
            enable=_load_env_bool("KASADA_NOTION_ENABLE", raw.get("kasada", {}).get("notion", {}).get("enable", True)),
            host=os.getenv("KASADA_NOTION_HOST", raw.get("kasada", {}).get("notion", {}).get("host", "")),
            apikey=os.getenv("KASADA_NOTION_APIKEY", raw.get("kasada", {}).get("notion", {}).get("apikey", "")),
        ),
    )
    return Settings(server=server, cors=cors, kasada=kasada)

settings = load_settings()
