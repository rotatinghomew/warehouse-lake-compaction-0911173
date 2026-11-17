from datetime import date

from .parquet import write_rows
from .storage import fingerprint


def prepare(directory,rows):
    cleaned=[]
    for row in rows:
        date.fromisoformat(row['day'])
        if not row['event_id'] or not isinstance(row['seconds'],int) or row['seconds']<0:
            raise ValueError("观看事件不合法")
        cleaned.append({key:row[key] for key in ('event_id','day','seconds')})
    keys=[row['event_id'] for row in cleaned]
    if len(keys)!=len(set(keys)):
        raise ValueError("批次事件重复")
    path=write_rows(directory,cleaned)
    return {'path':path,'checksum':fingerprint(cleaned),'events':cleaned}
