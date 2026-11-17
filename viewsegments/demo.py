from datetime import timedelta

from .catalog import SegmentLake
from .compact import compact, reclaim_space
from .errors import SnapshotExpired
from .query import detail, duration


class DemoClock:
    """演示用可推动时钟：有效期判断只依赖可观察的当前时刻。"""

    def __init__(self):
        from datetime import datetime, timezone
        self.now = datetime(2026, 11, 17, 9, 0, tzinfo=timezone.utc)

    def __call__(self):
        return self.now

    def advance(self, **delta):
        self.now += timedelta(**delta)


def run(root):
    clock = DemoClock()
    lake = SegmentLake(root, clock=clock)
    try:
        # 开学活动后同一日期积累了多个上传小文件。
        for index in range(4):
            lake.append(
                f"开学活动上传{index}",
                [{"event_id": f"E{index}", "day": "2026-11-16",
                  "seconds": 30 + index * 10}],
            )
        nightly = lake.snapshot(ttl=timedelta(hours=12))
        before_files = lake.files(nightly)

        # 在线整理：不暂停写入，布局变化但业务记录不增不减。
        compaction = compact(lake)
        current = lake.snapshot()
        after_files = lake.files(current)

        # 夜间导出持有的旧快照在有效期内仍可完整读完，即使物理布局已变。
        old_rows = detail(lake, nightly)
        lake.renew(nightly, ttl=timedelta(hours=6))
        clock.advance(hours=5)
        renewed_rows = detail(lake, nightly)

        # 整理期间持续入湖的新上传不被盖掉。
        lake.append("活动后补传", [
            {"event_id": "Enew", "day": "2026-11-16", "seconds": 90}])
        compact(lake)
        grown = lake.snapshot()

        # 旧引用未到期前，回收不能动它们引用的文件。
        early_reclaim = reclaim_space(lake)

        # 引用到期后是明确的失效结果，与空数据不同；空间随后可回收。
        clock.advance(hours=8)
        expiry_result = "expired"
        try:
            detail(lake, nightly)
        except SnapshotExpired:
            expiry_result = "SnapshotExpired（引用已过有效期）"
        reclamation = reclaim_space(lake)

        return {
            "整理前文件数": len(before_files),
            "整理任务": compaction["tasks"],
            "整理后当前版本文件数": len(after_files),
            "旧快照整理后可读行数": len(old_rows),
            "续期后可读行数": len(renewed_rows),
            "补传并入后记录数": len(detail(lake, grown)),
            "当前日期汇总": duration(lake, grown),
            "到期前回收文件数": len(early_reclaim["deleted"]),
            "过期引用读取结果": expiry_result,
            "到期后回收": {
                "删除文件数": len(reclamation["deleted"]),
                "回收字节": reclamation["bytes_reclaimed"],
                "未登记文件只报告不删除": reclamation["skipped_unknown_files"],
            },
        }
    finally:
        lake.close()
