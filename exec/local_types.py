from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class Proc:
    pid: int
    ppid: int
    tag: str
    cmd: str
