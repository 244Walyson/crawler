from datetime import datetime
from pydantic import BaseModel

class RawWebDocument(BaseModel):
    url: str
    timestamp: datetime
    status_code: int
    depth: int
    html: str
