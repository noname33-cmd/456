# config.py
import json
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

def load_settings(path: str = CONFIG_PATH) -> Settings:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return Settings(
        server=ServerCfg(**raw["server"]),
        cors=CORSCfg(
            allowed_origins=raw["cors"]["allowed_origins"],
            allowed_methods=raw["cors"]["allowed_methods"],
        ),
        kasada=KasadaCfg(
            salamoonder=SalamoonderCfg(**raw["kasada"]["salamoonder"]),
            notion=NotionCfg(**raw["kasada"]["notion"]),
        ),
    )

settings = load_settings()
