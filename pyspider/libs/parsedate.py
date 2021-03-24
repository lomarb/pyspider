from dateutil.parser import parse
from datetime import datetime
from typing import Optional


FMT = "%Y-%m-%d %H:%M:%S"


def parsedate(txt: str) -> Optional[datetime]:
    dt = parse(txt, fuzzy=True)
    if dt:
        return dt
    return None
