# models.py
from pydantic import BaseModel
from typing import List, Optional

class FloatingOnline(BaseModel):
    enable: bool = False
    percent_min_viewers: int = 0
    percent_max_viewers: int = 0
    percent: int = 0
    delay: int = 60

class Raid(BaseModel):
    enable: bool = False
    depth: int = 0
    percent_dropping_in_minute: int = 0
    percent: int = 100
    dropping_raid: bool = False

class CreateTaskRequest(BaseModel):
    channel: str
    number_of_viewers: int
    percent_auth_viewers: int
    time_in_minutes: int
    floating_online: FloatingOnline
    raid: Raid
    # наши плюшки:
    proxies: Optional[List[str]] = None
    tokens: Optional[List[str]] = None

class Task(BaseModel):
    id: str
    channel: str
    status: str
    viewers: int
    # можно добавить channel_id / delay_between_viewers и т.п., если нужно

class TaskResponse(BaseModel):
    task: Optional[Task] = None
    error: Optional[str] = None

class GetAllResponse(BaseModel):
    tasks: List[Task]
    error: Optional[str] = None

class Message(BaseModel):
    message: Optional[str] = None
    error: Optional[str] = None
