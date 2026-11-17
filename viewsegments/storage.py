import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest()


def sha256_file(path, chunk_size=1024 * 1024):
    """物理文件内容摘要；空间回收依据登记的内容身份，而不是修改时间。"""
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def connect(root, schema):
    directory = Path(root)
    directory.mkdir(parents=True, exist_ok=True)
    database = sqlite3.connect(directory / "catalog.sqlite3", timeout=10)
    database.row_factory = sqlite3.Row
    database.execute("PRAGMA foreign_keys=ON")
    database.executescript(schema)
    return database


@contextmanager
def atomic(database):
    """开启立即事务；若调用方已持有事务，则复用它（保存点语义由外层负责）。"""
    in_transaction = database.in_transaction
    if not in_transaction:
        database.execute("BEGIN IMMEDIATE")
    try:
        yield database
        if not in_transaction:
            database.commit()
    except BaseException:
        if not in_transaction:
            database.rollback()
        raise


def records(database, sql, parameters=()):
    return [dict(row) for row in database.execute(sql, parameters)]


def require_text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name}需要非空字符串")
    return value
