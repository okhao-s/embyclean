from typing import List, Optional, Dict, Any
from pydantic import BaseModel

class DeleteRequest(BaseModel):
    ids: List[str]

class IgnoreRequest(BaseModel):
    ids: List[str]
    mode: str

class RefreshRequest(BaseModel):
    ids: List[str]

class TaskReq(BaseModel):
    name: str
    mode: str
    cron: str
    libraries: str
    enabled: bool

class ConfigRequest(BaseModel):
    host: Optional[str] = ""
    user: Optional[str] = ""
    pwd: Optional[str] = ""
    webhook: Optional[str] = ""
    cron_sync: Optional[str] = ""
    prefs: Optional[Dict[str, Any]] = None
