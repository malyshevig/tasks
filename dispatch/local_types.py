from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class Task:
    id: int
    name: str
    status: str
    ts: datetime
    worker_id: Optional[str] = field(default=None)
    ts: Optional[datetime] = field(default=None)
    lines: Optional[int] = field(default=0)
    fail_count: Optional[int] = field(default=0)
    priority: Optional[int] = field(default=0)


