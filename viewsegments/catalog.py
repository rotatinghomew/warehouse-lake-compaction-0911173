import json
from datetime import timedelta
from pathlib import Path

from .append import prepare
from .clock import from_text, to_text, utcnow
from .compact import ACTIVE_STATES
from .errors import SnapshotExpired, SnapshotMissing
from .storage import atomic, connect, fingerprint

# 新快照与续期的默认有效期：调用者拿到的引用在此时长内稳定可读。
DEFAULT_TTL = timedelta(hours=24)

DDL = """
CREATE TABLE IF NOT EXISTS uploads(name TEXT PRIMARY KEY,checksum TEXT,path TEXT);
CREATE TABLE IF NOT EXISTS event_keys(event_id TEXT PRIMARY KEY,upload TEXT REFERENCES uploads(name));
CREATE TABLE IF NOT EXISTS physical_files(
    path TEXT PRIMARY KEY,
    sha256 TEXT,
    row_count INTEGER,
    registered_at TEXT NOT NULL,
    origin TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots(
    id INTEGER PRIMARY KEY,
    files TEXT NOT NULL,
    checksum TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('active','expired'))
);
CREATE TABLE IF NOT EXISTS compactions(
    id INTEGER PRIMARY KEY,
    state TEXT NOT NULL CHECK(state IN ('planning','writing','ready','published','failed')),
    input_paths TEXT NOT NULL,
    output_paths TEXT,
    plan_checksum TEXT NOT NULL,
    content_checksum TEXT,
    fence INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    detail TEXT
);
CREATE TABLE IF NOT EXISTS compaction_lock(
    id INTEGER PRIMARY KEY CHECK(id=1),
    holder TEXT NOT NULL,
    fence INTEGER NOT NULL,
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reclaim_runs(
    id INTEGER PRIMARY KEY,
    ran_at TEXT NOT NULL,
    deleted_files TEXT NOT NULL,
    skipped_unknown_files TEXT NOT NULL,
    bytes_reclaimed INTEGER NOT NULL
);
"""


def _table_columns(database, table):
    return {row[1] for row in database.execute(f"PRAGMA table_info({table})")}


def _has_table(database, table):
    return database.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


class SegmentLake:
    def __init__(self, root, clock=utcnow):
        self.root = Path(root)
        # 所有有效期判断都走这个可注入时钟，测试可以确定性地推动时间。
        self.clock = clock
        self.db = connect(root, DDL)
        self._migrate()

    def close(self):
        self.db.close()

    # ------------------------------------------------------------------ 迁移
    def _migrate(self):
        """把无有效期、无文件登记的旧清单升级到当前结构。"""
        with atomic(self.db):
            self._migrate_snapshots()
            self._migrate_physical_files()
            self.db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_active_snapshot_set "
                "ON snapshots(checksum) WHERE state='active'"
            )

    def _migrate_snapshots(self):
        if not _has_table(self.db, "snapshots"):
            return
        columns = _table_columns(self.db, "snapshots")
        if {"created_at", "expires_at", "state"} <= columns:
            return
        now = self.clock()
        old_rows = self.db.execute("SELECT id,files,checksum FROM snapshots").fetchall()
        self.db.execute("ALTER TABLE snapshots RENAME TO snapshots_legacy")
        self.db.execute(
            "CREATE TABLE snapshots("
            "id INTEGER PRIMARY KEY,files TEXT NOT NULL,checksum TEXT NOT NULL,"
            "created_at TEXT NOT NULL,expires_at TEXT NOT NULL,"
            "state TEXT NOT NULL CHECK(state IN ('active','expired')))"
        )
        for row in old_rows:
            # 旧引用没有约定有效期：迁移时刻起给一个完整默认有效期，
            # 让既有导出仍能在明确窗口内读完，而不是无限占用空间。
            self.db.execute(
                "INSERT INTO snapshots VALUES(?,?,?,?,?,?)",
                (row[0], row[1], row[2], to_text(now),
                 to_text(now + DEFAULT_TTL), "active"),
            )
        self.db.execute("DROP TABLE snapshots_legacy")

    def _migrate_physical_files(self):
        known = {
            row[0] for row in self.db.execute("SELECT path FROM physical_files")
        }
        now = to_text(self.clock())
        paths = {row[0] for row in self.db.execute(
            "SELECT DISTINCT path FROM uploads WHERE path IS NOT NULL")}
        paths.update(
            path
            for row in self.db.execute("SELECT files FROM snapshots")
            for path in json.loads(row[0])
        )
        for path in paths:
            if path in known or not Path(path).is_file():
                continue
            # 旧文件的内容摘要在首次需要时补齐；登记本身先建立引用关系。
            self.db.execute(
                "INSERT OR IGNORE INTO physical_files(path,sha256,row_count,"
                "registered_at,origin) VALUES(?,?,?,?,?)",
                (path, None, None, now, "legacy"),
            )

    # ------------------------------------------------------------------ 追加
    def append(self, name, rows):
        checksum = fingerprint(rows)
        with atomic(self.db):
            old = self.db.execute(
                'SELECT checksum FROM uploads WHERE name=?', (name,)
            ).fetchone()
            if old:
                if old[0] != checksum:
                    raise ValueError("上传标识已绑定其他内容")
                return False
            batch = prepare(self.root / 'segments', rows)
            self._register_file(batch['path'], "upload")
            self.db.execute(
                'INSERT INTO uploads VALUES(?,?,?)',
                (name, checksum, batch['path']),
            )
            for row in batch['events']:
                self.db.execute(
                    'INSERT INTO event_keys VALUES(?,?)', (row['event_id'], name)
                )
        return True

    def _register_file(self, path, origin):
        from .parquet import read_rows
        from .storage import sha256_file
        rows = read_rows(path)
        self.db.execute(
            "INSERT INTO physical_files(path,sha256,row_count,registered_at,origin) "
            "VALUES(?,?,?,?,?)",
            (path, sha256_file(path), len(rows), to_text(self.clock()), origin),
        )
        return rows

    # ------------------------------------------------------------------ 快照
    def snapshot(self, ttl=DEFAULT_TTL):
        """固定当前上传文件集合，并给引用一个明确的有效期。"""
        now = self.clock()
        with atomic(self.db):
            self._expire_due(now)
            files = [row[0] for row in self.db.execute(
                'SELECT DISTINCT path FROM uploads WHERE path IS NOT NULL ORDER BY path')]
            checksum = fingerprint(files)
            old = self.db.execute(
                "SELECT id FROM snapshots WHERE checksum=? AND state='active'",
                (checksum,),
            ).fetchone()
            if old:
                return old[0]
            cursor = self.db.execute(
                "INSERT INTO snapshots(files,checksum,created_at,expires_at,state) "
                "VALUES(?,?,?,?, 'active')",
                (json.dumps(files), checksum, to_text(now), to_text(now + ttl)),
            )
        return cursor.lastrowid

    def renew(self, snapshot, ttl=DEFAULT_TTL):
        """正常续期：从当前时刻重新计算有效期，引用在整个有效期内可读。"""
        now = self.clock()
        with atomic(self.db):
            row = self._snapshot_row(snapshot, now)
            self.db.execute(
                "UPDATE snapshots SET expires_at=?,state='active' WHERE id=?",
                (to_text(now + ttl), snapshot),
            )
        return {"snapshot": snapshot, "expires_at": to_text(now + ttl)}

    def snapshot_status(self, snapshot):
        """返回引用的可观察状态；未知与到期是不同的错误。"""
        now = self.clock()
        with atomic(self.db):
            row = self._snapshot_row(snapshot, now)
            return {
                "snapshot": snapshot,
                "state": row["state"],
                "created_at": row["created_at"],
                "expires_at": row["expires_at"],
                "files": json.loads(row["files"]),
            }

    def _expire_due(self, now):
        self.db.execute(
            "UPDATE snapshots SET state='expired' "
            "WHERE state='active' AND expires_at<=?",
            (to_text(now),),
        )

    def _snapshot_row(self, snapshot, now):
        row = self.db.execute(
            "SELECT * FROM snapshots WHERE id=?", (snapshot,)
        ).fetchone()
        if not row:
            raise SnapshotMissing(snapshot)
        if row["state"] != "active" or from_text(row["expires_at"]) <= now:
            # 惰性把已到期引用标记出来，保证状态可观察、不依赖进程存在。
            if row["state"] == "active":
                self.db.execute(
                    "UPDATE snapshots SET state='expired' WHERE id=?", (snapshot,)
                )
            raise SnapshotExpired(snapshot)
        return row

    def files(self, snapshot):
        with atomic(self.db):
            row = self._snapshot_row(snapshot, self.clock())
            return json.loads(row["files"])

    # ------------------------------------------------------------- 引用核算
    def referenced_files(self, now=None):
        """根据真实的表版本对应关系计算仍被引用的物理文件集合。

        引用来源只有三类：
        1. 当前上传清单（uploads.path，含整理期间新上传的文件）；
        2. 未到期快照固定的文件集合；
        3. 未结束整理任务已经预登记的输出文件。
        不看文件修改时间，也不按目录猜测。
        """
        now = now or self.clock()
        referenced = set()
        referenced.update(
            row[0] for row in self.db.execute(
                "SELECT DISTINCT path FROM uploads WHERE path IS NOT NULL")
        )
        self._expire_due(now)
        referenced.update(
            path
            for row in self.db.execute(
                "SELECT files FROM snapshots WHERE state='active'")
            for path in json.loads(row[0])
        )
        referenced.update(
            path
            for row in self.db.execute(
                "SELECT output_paths FROM compactions "
                "WHERE output_paths IS NOT NULL "
                f"AND state IN ({','.join('?' for _ in ACTIVE_STATES)})",
                ACTIVE_STATES)
            for path in json.loads(row[0])
        )
        return referenced
