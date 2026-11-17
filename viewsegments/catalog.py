import json
from pathlib import Path

from .append import prepare
from .storage import atomic, connect, fingerprint

DDL="""
CREATE TABLE IF NOT EXISTS uploads(name TEXT PRIMARY KEY,checksum TEXT,path TEXT);
CREATE TABLE IF NOT EXISTS event_keys(event_id TEXT PRIMARY KEY,upload TEXT REFERENCES uploads(name));
CREATE TABLE IF NOT EXISTS snapshots(id INTEGER PRIMARY KEY,files TEXT NOT NULL,checksum TEXT UNIQUE NOT NULL);
"""


class SegmentLake:
    def __init__(self,root):
        self.root=Path(root)
        self.db=connect(root,DDL)

    def close(self):
        self.db.close()

    def append(self,name,rows):
        checksum=fingerprint(rows)
        with atomic(self.db):
            old=self.db.execute('SELECT checksum FROM uploads WHERE name=?',(name,)).fetchone()
            if old:
                if old[0]!=checksum:
                    raise ValueError("上传标识已绑定其他内容")
                return False
            batch=prepare(self.root/'segments',rows)
            self.db.execute('INSERT INTO uploads VALUES(?,?,?)',(name,checksum,batch['path']))
            for row in batch['events']:
                self.db.execute('INSERT INTO event_keys VALUES(?,?)',(row['event_id'],name))
        return True

    def snapshot(self):
        with atomic(self.db):
            files=[row[0] for row in self.db.execute('SELECT path FROM uploads WHERE path IS NOT NULL ORDER BY name')]
            checksum=fingerprint(files)
            old=self.db.execute('SELECT id FROM snapshots WHERE checksum=?',(checksum,)).fetchone()
            if old:
                return old[0]
            cursor=self.db.execute('INSERT INTO snapshots(files,checksum) VALUES(?,?)',(json.dumps(files),checksum))
        return cursor.lastrowid

    def files(self,snapshot):
        row=self.db.execute('SELECT files FROM snapshots WHERE id=?',(snapshot,)).fetchone()
        if not row:
            raise KeyError(snapshot)
        return json.loads(row[0])
