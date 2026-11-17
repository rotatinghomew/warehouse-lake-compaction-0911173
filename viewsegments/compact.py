"""在线整理与空间回收。

整理是一个可恢复的状态机：

    planning -> writing -> ready -> published
                 \----------> failed（校验失败或输入已失效）

关键约束：
- 全局单行锁 + fencing 令牌：两个整理者同时碰到同一批文件时只有一个生效，
  持锁者崩溃后锁可被接管，旧持锁者的令牌立即失效，其写入不能再切换清单。
- 输出文件在写之前就登记进 ``physical_files``，崩溃在任何阶段都能根据
  “登记表 vs 真实文件”核对恢复，不依赖文件修改时间。
- 切换清单（uploads 改指向新文件）与新快照发布在同一个 SQLite 事务里，
  业务记录集合通过事件多重集校验，不增不减。
"""
import json
import uuid
from datetime import timedelta
from pathlib import Path

from .clock import from_text, to_text
from .errors import CompactionConflict, CompactionStale
from .parquet import read_rows, write_rows_to
from .storage import atomic, fingerprint, sha256_file

# 锁的心跳超过该时长未更新，才允许另一个整理者接管；正常运行远短于该值。
LOCK_TTL = timedelta(minutes=30)
ACTIVE_STATES = ("planning", "writing", "ready")


def _check_fence(database, fence):
    row = database.execute(
        "SELECT fence FROM compaction_lock WHERE id=1").fetchone()
    if not row or row[0] != fence:
        raise CompactionStale("整理令牌已失效，本次写入不能生效")


def _acquire_lock(database, now, ttl):
    holder = uuid.uuid4().hex
    row = database.execute(
        "SELECT holder,fence,heartbeat_at FROM compaction_lock WHERE id=1"
    ).fetchone()
    if row is not None:
        if from_text(row[2]) + ttl > now:
            raise CompactionConflict("已有整理任务持有有效锁")
        # 接管过期锁：fence 单调递增，旧持锁者之后的任何写入都会被拒。
        fence = row[1] + 1
        database.execute(
            "UPDATE compaction_lock SET holder=?,fence=?,acquired_at=?,"
            "heartbeat_at=? WHERE id=1",
            (holder, fence, to_text(now), to_text(now)),
        )
    else:
        fence = 1
        database.execute(
            "INSERT INTO compaction_lock(id,holder,fence,acquired_at,heartbeat_at) "
            "VALUES(1,?,?,?,?)",
            (holder, fence, to_text(now), to_text(now)),
        )
    return holder, fence


def _release_lock(database, fence):
    _check_fence(database, fence)
    database.execute("DELETE FROM compaction_lock WHERE id=1")


def _heartbeat(database, fence, now):
    _check_fence(database, fence)
    database.execute(
        "UPDATE compaction_lock SET heartbeat_at=? WHERE id=1 AND fence=?",
        (to_text(now), fence),
    )


def _rows_of(path):
    return sorted(read_rows(path), key=lambda row: row["event_id"])


def _plan_groups(database):
    """按业务日期把小文件分组；跨日文件归入混合组。单文件组不整理。"""
    paths = [row[0] for row in database.execute(
        "SELECT DISTINCT path FROM uploads WHERE path IS NOT NULL ORDER BY path")]
    groups = {}
    for path in paths:
        # 登记表中的文件缺失属于存储损坏：显式失败，绝不静默跳过造成记录丢失。
        rows = read_rows(path)
        days = {row["day"] for row in rows}
        key = ("day", days.pop()) if len(days) == 1 else ("mixed",)
        groups.setdefault(key, []).append(path)
    return [sorted(members) for members in groups.values() if len(members) >= 2]


def compact(lake, lock_ttl=LOCK_TTL):
    """在线整理入口；崩溃后重入先恢复未完成任务，再规划新批次。

    返回每个整理任务的最终状态，供调用者解释当前处于哪一步。
    """
    db = lake.db
    now = lake.clock()
    summary = []
    with atomic(db):
        holder, fence = _acquire_lock(db, now, lock_ttl)
    try:
        with atomic(db):
            unfinished = db.execute(
                "SELECT id FROM compactions WHERE state IN "
                f"({','.join('?' for _ in ACTIVE_STATES)}) ORDER BY id",
                ACTIVE_STATES).fetchall()
        for row in unfinished:
            summary.append(_resume(lake, row[0], fence))
        for inputs in _plan_groups(db):
            try:
                summary.append(_run_plan(lake, inputs, fence))
            except (OSError, RuntimeError) as exc:
                # 进程曾在写文件阶段退出：本次重入先把留下的规划行恢复完。
                pending = db.execute(
                    "SELECT id FROM compactions WHERE state IN "
                    f"({','.join('?' for _ in ACTIVE_STATES)}) ORDER BY id",
                    ACTIVE_STATES).fetchall()
                if not pending:
                    raise
                summary.append({"recovered_after_error": str(exc)})
                for row in pending:
                    summary.append(_resume(lake, row[0], fence))
                break
    finally:
        with atomic(db):
            # 锁可能已被其他整理者接管；只在令牌仍有效时释放自己的锁。
            try:
                _release_lock(db, fence)
            except CompactionStale:
                pass
    return {"holder": holder, "tasks": summary}


def _resume(lake, compaction_id, fence):
    """恢复一个在任意阶段退出的整理任务到可解释状态。"""
    db = lake.db
    now = lake.clock()
    with atomic(db):
        _heartbeat(db, fence, now)
        task = db.execute(
            "SELECT * FROM compactions WHERE id=?", (compaction_id,)
        ).fetchone()
        if task["state"] == "published":
            return {"id": compaction_id, "state": "published", "reused": True}
        inputs = json.loads(task["input_paths"])
        output_paths = json.loads(task["output_paths"]) if task["output_paths"] else None
    if not _inputs_intact(db, inputs):
        # 输入已从上传清单消失：说明切换清单的事务已经生效，
        # 只剩任务状态行没来得及更新——核对输出后补记为已发布。
        if output_paths and _outputs_verified(
                lake, output_paths, task["content_checksum"]):
            with atomic(db):
                _check_fence(db, fence)
                db.execute(
                    "UPDATE compactions SET state='published',updated_at=? WHERE id=?",
                    (to_text(now), compaction_id))
            return {"id": compaction_id, "state": "published", "reused": True}
        return _fail(lake, compaction_id, fence, "输入文件已不在上传清单中，任务作废")
    if output_paths is None or not _outputs_verified(lake, output_paths, task["content_checksum"]):
        _write_outputs(lake, compaction_id, inputs, fence)
    return _publish(lake, compaction_id, fence)

def _run_plan(lake, inputs, fence):
    db = lake.db
    now = lake.clock()
    content_checksum = fingerprint(sorted(
        (row for path in inputs for row in _rows_of(path)),
        key=lambda row: row["event_id"]))
    plan_checksum = fingerprint({"inputs": inputs, "content": content_checksum})
    with atomic(db):
        _heartbeat(db, fence, now)
        done = db.execute(
            "SELECT id,state FROM compactions WHERE plan_checksum=? "
            "ORDER BY id DESC", (plan_checksum,)).fetchone()
        if done and done["state"] == "published":
            # 同一批记录的整理已生效：重试直接复用，绝不产生第二份。
            return {"id": done["id"], "state": "published", "reused": True}
        if done:
            return _resume(lake, done["id"], fence)
        cursor = db.execute(
            "INSERT INTO compactions(state,input_paths,output_paths,"
            "plan_checksum,content_checksum,fence,created_at,updated_at,detail) "
            "VALUES('planning',?,NULL,?,?,?,?,?,NULL)",
            (json.dumps(inputs), plan_checksum, content_checksum, fence,
             to_text(now), to_text(now)),
        )
        compaction_id = cursor.lastrowid
        output = _stage_path(lake, compaction_id)
        # 先登记输出身份：即使下一步进程退出，恢复时也知道该核对哪个文件。
        db.execute(
            "INSERT INTO physical_files(path,sha256,row_count,registered_at,origin) "
            "VALUES(?,?,?,?, 'compaction')",
            (output, None, None, to_text(now)),
        )
        db.execute(
            "UPDATE compactions SET output_paths=?,updated_at=? WHERE id=?",
            (json.dumps([output]), to_text(now), compaction_id),
        )
    _write_outputs(lake, compaction_id, inputs, fence)
    return _publish(lake, compaction_id, fence)


def _stage_path(lake, compaction_id):
    directory = lake.root / "compaction" / f"c{compaction_id}"
    return str((directory / (uuid.uuid4().hex + ".parquet")).resolve())


def _write_outputs(lake, compaction_id, inputs, fence):
    """写新文件并按内容校验；写完新文件就退出也能从 ready/writing 恢复。"""
    db = lake.db
    now = lake.clock()
    with atomic(db):
        _heartbeat(db, fence, now)
        task = db.execute(
            "SELECT output_paths,content_checksum FROM compactions WHERE id=?",
            (compaction_id,),
        ).fetchone()
        outputs = json.loads(task["output_paths"])
        db.execute(
            "UPDATE compactions SET state='writing',updated_at=? WHERE id=?",
            (to_text(now), compaction_id),
        )
    rows = sorted(
        (row for path in inputs for row in read_rows(path)),
        key=lambda row: row["event_id"])
    output = outputs[0]
    # 重写前先清掉残留半成品，避免把上次崩溃留下的文件误当成品。
    stale = Path(output)
    if stale.exists():
        stale.unlink()
    try:
        write_rows_to(output, rows)
    except BaseException as exc:
        # 写完新文件前退出：任务保持 planning/writing，重入时按状态恢复。
        with atomic(db):
            _check_fence(db, fence)
            db.execute(
                "UPDATE compactions SET state='planning',updated_at=? WHERE id=?",
                (to_text(now), compaction_id))
        raise RuntimeError(f"整理输出写入中断: {exc}") from exc
    # 回读行序由 Parquet 决定，与写入顺序无关：按事件身份排序后再摘要。
    written = _rows_of(output)
    if fingerprint(written) != task["content_checksum"]:
        _fail(lake, compaction_id, fence, "整理输出与输入的事件集合不一致")
        raise CompactionStale("整理输出校验失败")
    with atomic(db):
        _heartbeat(db, fence, now)
        digest = sha256_file(output)
        db.execute(
            "UPDATE physical_files SET sha256=?,row_count=? WHERE path=?",
            (digest, len(written), output),
        )
        db.execute(
            "UPDATE compactions SET state='ready',updated_at=? WHERE id=?",
            (to_text(now), compaction_id),
        )


def _outputs_verified(lake, outputs, expected_checksum):
    for path in outputs:
        file = Path(path)
        if not file.is_file():
            return False
        if fingerprint(_rows_of(path)) != expected_checksum:
            return False
    return True


def _inputs_intact(database, inputs):
    if not inputs:
        return False
    placeholders = ",".join("?" for _ in inputs)
    found = {row[0] for row in database.execute(
        f"SELECT DISTINCT path FROM uploads WHERE path IN ({placeholders})",
        inputs)}
    return set(inputs) <= found


def _publish(lake, compaction_id, fence):
    """在一个事务里切换清单并发布新快照；中途退出则整体回滚。"""
    db = lake.db
    now = lake.clock()
    with atomic(db):
        _check_fence(db, fence)
        task = db.execute(
            "SELECT * FROM compactions WHERE id=?", (compaction_id,)
        ).fetchone()
        if task["state"] == "published":
            return {"id": compaction_id, "state": "published", "reused": True}
        inputs = json.loads(task["input_paths"])
        outputs = json.loads(task["output_paths"])
        if not _inputs_intact(db, inputs):
            return _fail_locked(db, compaction_id, now, "输入文件已不在上传清单中，任务作废")
        if not _outputs_verified(lake, outputs, task["content_checksum"]):
            return _fail_locked(db, compaction_id, now, "输出文件缺失或校验失败")
        input_events = sorted(
            (row for path in inputs for row in read_rows(path)),
            key=lambda row: row["event_id"])
        output_events = sorted(
            (row for path in outputs for row in read_rows(path)),
            key=lambda row: row["event_id"])
        # 业务记录不能增减：事件多重集必须完全一致。
        if input_events != output_events:
            return _fail_locked(db, compaction_id, now, "事件多重集校验失败")
        placeholders = ",".join("?" for _ in inputs)
        db.execute(
            f"UPDATE uploads SET path=? WHERE path IN ({placeholders})",
            (outputs[0], *inputs),
        )
        _publish_snapshot(lake, now)
        db.execute(
            "UPDATE compactions SET state='published',updated_at=? WHERE id=?",
            (to_text(now), compaction_id),
        )
    return {"id": compaction_id, "state": "published", "reused": False,
            "inputs": inputs, "outputs": outputs}


def _publish_snapshot(lake, now):
    db = lake.db
    files = [row[0] for row in db.execute(
        "SELECT DISTINCT path FROM uploads WHERE path IS NOT NULL ORDER BY path")]
    checksum = fingerprint(files)
    existing = db.execute(
        "SELECT id FROM snapshots WHERE checksum=? AND state='active'",
        (checksum,),
    ).fetchone()
    if existing:
        return existing[0]
    from .catalog import DEFAULT_TTL
    cursor = db.execute(
        "INSERT INTO snapshots(files,checksum,created_at,expires_at,state) "
        "VALUES(?,?,?,?,'active')",
        (json.dumps(files), checksum, to_text(now), to_text(now + DEFAULT_TTL)),
    )
    return cursor.lastrowid


def _fail(lake, compaction_id, fence, detail):
    with atomic(lake.db):
        _check_fence(lake.db, fence)
        return _fail_locked(lake.db, compaction_id, lake.clock(), detail)


def _fail_locked(database, compaction_id, now, detail):
    database.execute(
        "UPDATE compactions SET state='failed',detail=?,updated_at=? WHERE id=?",
        (detail, to_text(now), compaction_id),
    )
    return {"id": compaction_id, "state": "failed", "detail": detail}


def reclaim_space(lake, lock_ttl=LOCK_TTL):
    """按真实引用关系回收空间，绝不按修改时间删目录。

    可删 = 已登记 physical_files 但不在 {上传清单, 未到期快照, 未完成整理输出}
    并集中的文件。磁盘上未登记的文件只报告、不删除。
    """
    db = lake.db
    now = lake.clock()
    with atomic(db):
        holder, fence = _acquire_lock(db, now, lock_ttl)
    try:
        deleted, unknown_seen, bytes_reclaimed = [], [], 0
        with atomic(db):
            _check_fence(db, fence)
            # 引用核算与删除在同一个立即事务里：决策依据的是该事务视图下
            # 真实的表版本对应关系，持锁期间的任何变化都会被序列化。
            referenced = lake.referenced_files(now)
            registered = [row[0] for row in db.execute(
                "SELECT path FROM physical_files")]
            for path in registered:
                if path in referenced:
                    continue
                file = Path(path)
                if file.is_file():
                    # 再核对一次内容身份，防止登记表之外的替换。
                    digest_row = db.execute(
                        "SELECT sha256 FROM physical_files WHERE path=?",
                        (path,)).fetchone()
                    if digest_row[0] and sha256_file(path) != digest_row[0]:
                        continue
                    bytes_reclaimed += file.stat().st_size
                    file.unlink()
                db.execute("DELETE FROM physical_files WHERE path=?", (path,))
                deleted.append(path)
            known = set(registered)
            for file in lake.root.rglob("*.parquet"):
                resolved = str(file.resolve())
                if resolved not in known and resolved not in referenced:
                    unknown_seen.append(resolved)
            db.execute(
                "INSERT INTO reclaim_runs(ran_at,deleted_files,"
                "skipped_unknown_files,bytes_reclaimed) VALUES(?,?,?,?)",
                (to_text(now), json.dumps(deleted),
                 json.dumps(unknown_seen), bytes_reclaimed),
            )
    finally:
        with atomic(db):
            try:
                _release_lock(db, fence)
            except CompactionStale:
                pass
    return {"holder": holder, "deleted": deleted,
            "skipped_unknown_files": unknown_seen,
            "bytes_reclaimed": bytes_reclaimed}
