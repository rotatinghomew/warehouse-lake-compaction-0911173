from .catalog import SegmentLake
from .query import detail, duration


def run(root):
    lake=SegmentLake(root)
    try:
        lake.append('上传A',[{'event_id':'E1','day':'2026-11-01','seconds':120}])
        first=lake.snapshot()
        lake.append('上传B',[{'event_id':'E2','day':'2026-11-01','seconds':60}])
        second=lake.snapshot()
        return {'早期快照':detail(lake,first),'当前日期汇总':duration(lake,second),'文件数':len(lake.files(second))}
    finally:
        lake.close()
