
import datetime
from typing import List
import pydantic

class ErrorMessage(pydantic.BaseModel):
    title:   str
    content: str

class Dump(pydantic.BaseModel):
    title:     str
    timestamp: datetime.datetime
    username:  str
    messages:  List[ErrorMessage]
