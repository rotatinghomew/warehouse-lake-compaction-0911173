"""统一的时间来源。

有效期判断一律使用调用者可观察的 UTC 时刻，不依赖进程是否还活着。
生产代码使用 ``utcnow``；测试可以注入固定时钟来推动时间。
"""
from datetime import datetime, timezone


def utcnow():
    return datetime.now(timezone.utc)


def to_text(moment):
    return moment.isoformat()


def from_text(value):
    return datetime.fromisoformat(value)
