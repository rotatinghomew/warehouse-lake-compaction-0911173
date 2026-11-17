"""调用者可区分的错误类型。

到期失效与“数据为空”是不同的结果：空集合正常返回 ``[]``，
引用过期或不存在则抛出这里定义的异常。
"""


class SnapshotExpired(KeyError):
    """快照引用已超过明确的有效期，其文件不再保证可读。"""


class SnapshotMissing(KeyError):
    """引用的快照版本在清单中不存在。"""


class CompactionConflict(RuntimeError):
    """已有另一个整理者持有有效锁，本次整理不能生效。"""


class CompactionStale(RuntimeError):
    """本次整理的 fencing 令牌已失效（锁被接管或释放），写入必须中止。"""
