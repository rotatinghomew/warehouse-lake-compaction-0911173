import json
import os
import tempfile
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def write_rows(root, rows):
    """按既有命名规则在目录下生成一个新的不可变 Parquet 文件。"""
    rows = list(rows)
    if not rows:
        return None
    directory = Path(root)
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / (uuid.uuid4().hex + ".parquet")
    return write_rows_to(destination, rows)


def write_rows_to(destination, rows):
    """把行写到显式指定的目标路径；目标存在即视为不可变文件冲突。

    整理流程用它把结果写到预登记路径，保证“清单里登记的文件身份”和
    磁盘上的真实文件可以逐一核对，而不是靠目录扫描猜测。
    """
    rows = list(rows)
    if not rows:
        raise ValueError("空批次不产生 Parquet 文件")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".pending")
    database = duckdb.connect(config={"threads": 1})
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", encoding="utf-8") as source:
            json.dump(rows, source, ensure_ascii=False)
            source.flush()
            database.execute("CREATE TABLE payload AS SELECT * FROM read_json_auto("
                             + literal(source.name) + ", format='array')")
            database.execute("COPY payload TO " + literal(temporary) + " (FORMAT PARQUET)")
        os.replace(temporary, destination)
    finally:
        database.close()
        if temporary.exists():
            temporary.unlink()
    return str(destination.resolve())


def read_rows(path):
    return query([path])


def query(paths, sql="SELECT * FROM payload", parameters=()):
    paths = list(paths)
    if not paths:
        return []
    if any(not Path(path).is_file() for path in paths):
        raise FileNotFoundError("数据文件不存在")
    database = duckdb.connect(config={"threads": 1})
    try:
        database.read_parquet(paths).create_view("payload")
        result = database.execute(sql, parameters)
        names = [column[0] for column in result.description]
        return [normalize(dict(zip(names, row))) for row in result.fetchall()]
    finally:
        database.close()


def normalize(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {name: normalize(item) for name, item in value.items()}
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value
