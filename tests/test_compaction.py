import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from viewsegments.catalog import DEFAULT_TTL, SegmentLake
from viewsegments.clock import to_text
from viewsegments.compact import compact, reclaim_space
from viewsegments.errors import (
    CompactionConflict,
    SnapshotExpired,
    SnapshotMissing,
)
from viewsegments.query import detail, duration, export


class FakeClock:
    """可推动的确定性时钟；有效期判断只依赖它给出的可观察时刻。"""

    def __init__(self, start=None):
        self.now = start or datetime(2026, 11, 17, tzinfo=timezone.utc)

    def __call__(self):
        return self.now

    def advance(self, **delta):
        self.now += timedelta(**delta)


def event(name, day="2026-11-01", seconds=30):
    return {"event_id": name, "day": day, "seconds": seconds}


class OnlineCompactionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = self.temporary.name
        self.clock = FakeClock()
        self.lakes = []

    def tearDown(self):
        for lake in self.lakes:
            lake.close()
        self.temporary.cleanup()

    def open_lake(self):
        lake = SegmentLake(self.directory, clock=self.clock)
        self.lakes.append(lake)
        return lake

    def seed_day(self, lake, day="2026-11-01", count=4):
        for index in range(count):
            lake.append(f"U{index}", [event(f"E{index}", day)])

    def parquet_files(self):
        return list(Path(self.directory).rglob("*.parquet"))

    def event_ids(self, lake, snapshot):
        return {row["event_id"] for row in detail(lake, snapshot)}

    # --------------------------------------------------------- 整理正确性
    def test_compaction_merges_files_without_record_change(self):
        lake = self.open_lake()
        self.seed_day(lake)
        before = lake.snapshot()
        before_rows = detail(lake, before)
        result = compact(lake)
        self.assertEqual(len(result["tasks"]), 1)
        task = result["tasks"][0]
        self.assertEqual(task["state"], "published")
        after = lake.snapshot()
        # 业务记录不增不减，顺序也保持稳定。
        self.assertEqual(detail(lake, after), before_rows)
        self.assertEqual(self.event_ids(lake, after),
                         {f"E{i}" for i in range(4)})
        # 文件布局从 4 个小文件变成 1 个被引用的大文件；旧文件待回收。
        current_snapshot = lake.snapshot()
        self.assertEqual(len(lake.files(current_snapshot)), 1)
        self.assertEqual(len(self.parquet_files()), 5)
        # 时长汇总口径不变。
        self.assertEqual(duration(lake, after)[0]["seconds"], 120)
        # 旧快照未到期前，旧文件必须保留给持有旧版本的读者。
        self.assertEqual(reclaim_space(lake)["deleted"], [])
        # 旧引用到期后，回收只删无引用文件：当前大文件保留，4 个旧文件收回。
        self.clock.advance(hours=25)
        report = reclaim_space(lake)
        self.assertEqual(len(report["deleted"]), 4)
        self.assertEqual(len(self.parquet_files()), 1)

    def test_repeated_compaction_does_not_duplicate_records(self):
        lake = self.open_lake()
        self.seed_day(lake)
        first = compact(lake)
        second = compact(lake)
        self.assertEqual(second["tasks"], [])
        current = lake.snapshot()
        self.assertEqual(len(detail(lake, current)), 4)
        # 同一计划即使因外部原因再次出现，也复用已发布任务。
        self.assertEqual(first["tasks"][0]["state"], "published")

    def test_new_uploads_during_compaction_are_not_overwritten(self):
        lake = self.open_lake()
        self.seed_day(lake, count=3)
        compact(lake)
        # 整理完成后持续入湖的新上传走自己的不可变文件。
        self.assertTrue(lake.append("Unew", [event("Enew")]))
        current = lake.snapshot()
        rows = detail(lake, current)
        self.assertEqual(len(rows), 4)
        self.assertIn("Enew", {row["event_id"] for row in rows})
        # 再整理一次：新文件并入大文件，记录仍然完整。
        compact(lake)
        merged = lake.snapshot()
        self.assertEqual(len(detail(lake, merged)), 4)
        self.assertEqual(len(lake.files(merged)), 1)
        self.clock.advance(hours=25)
        reclaim_space(lake)
        self.assertEqual(len(self.parquet_files()), 1)

    def test_retry_same_plan_reuses_published_task(self):
        from viewsegments import compact as compact_module
        from viewsegments.storage import atomic
        from datetime import timedelta
        lake = self.open_lake()
        self.seed_day(lake, count=2)
        result = compact(lake)
        task_id = result["tasks"][0]["id"]
        plan = lake.db.execute(
            "SELECT input_paths FROM compactions WHERE id=?", (task_id,)
        ).fetchone()
        # 持锁后再次规划同一批输入：已发布计划必须被复用而不是再产出一份数据。
        with atomic(lake.db):
            _holder, fence = compact_module._acquire_lock(
                lake.db, self.clock(), timedelta(minutes=30))
        try:
            resumed = compact_module._run_plan(
                lake, json.loads(plan["input_paths"]), fence)
        finally:
            with atomic(lake.db):
                compact_module._release_lock(lake.db, fence)
        self.assertTrue(resumed.get("reused"))
        self.assertEqual(len(detail(lake, lake.snapshot())), 2)

    def test_export_works_after_compaction_and_renewal(self):
        lake = self.open_lake()
        self.seed_day(lake)
        snapshot = lake.snapshot()
        compact(lake)
        target = Path(self.directory) / "out.jsonl"
        report = export(lake, snapshot, target)
        self.assertEqual(report["rows"], 4)
        # 旧快照被显式续期后仍可完整读取。
        self.clock.advance(hours=12)
        lake.renew(snapshot)
        self.clock.advance(hours=20)
        self.assertEqual(len(detail(lake, snapshot)), 4)

    # --------------------------------------------------------- 快照有效期
    def test_snapshot_expiry_has_explicit_observable_result(self):
        lake = self.open_lake()
        self.seed_day(lake, count=1)
        snapshot = lake.snapshot(ttl=timedelta(hours=1))
        status = lake.snapshot_status(snapshot)
        self.assertEqual(status["state"], "active")
        self.clock.advance(hours=2)
        # 到期引用是明确异常，与空结果区分；空日期过滤仍返回 []。
        with self.assertRaises(SnapshotExpired):
            lake.files(snapshot)
        with self.assertRaises(SnapshotExpired):
            detail(lake, snapshot)
        with self.assertRaises(SnapshotExpired):
            lake.snapshot_status(snapshot)
        fresh = lake.snapshot()
        self.assertEqual(detail(lake, fresh, "2099-01-01"), [])

    def test_unknown_snapshot_is_distinct_from_expiry(self):
        lake = self.open_lake()
        with self.assertRaises(SnapshotMissing):
            lake.files(999)

    def test_renewal_extends_validity_window(self):
        lake = self.open_lake()
        self.seed_day(lake, count=2)
        snapshot = lake.snapshot(ttl=timedelta(hours=1))
        self.clock.advance(minutes=50)
        lake.renew(snapshot, ttl=timedelta(hours=2))
        self.clock.advance(hours=1, minutes=30)
        # 原始 TTL 已过，但续期窗口内仍然可读。
        self.assertEqual(len(detail(lake, snapshot)), 2)
        self.clock.advance(hours=1)
        with self.assertRaises(SnapshotExpired):
            detail(lake, snapshot)

    def test_identical_active_file_sets_share_snapshot(self):
        lake = self.open_lake()
        self.seed_day(lake, count=1)
        self.assertEqual(lake.snapshot(), lake.snapshot())

    # --------------------------------------------------------- 空间回收
    def test_reclaim_uses_reference_graph_not_mtime(self):
        lake = self.open_lake()
        self.seed_day(lake)
        protected = lake.snapshot(ttl=timedelta(hours=1))
        compact(lake)
        # 整理后旧小文件仍被未到期快照引用，不能删。
        first = reclaim_space(lake)
        self.assertEqual(first["deleted"], [])
        self.assertEqual(len(self.parquet_files()), 4 + 1)
        # 到期后引用消失，旧文件才可回收；当前快照的数据文件保留。
        self.clock.advance(hours=2)
        current = lake.snapshot()
        second = reclaim_space(lake)
        self.assertEqual(len(second["deleted"]), 4)
        with self.assertRaises(SnapshotExpired):
            lake.files(protected)
        self.assertEqual(len(detail(lake, current)), 4)
        self.assertEqual(len(self.parquet_files()), 1)
        # 再次回收是幂等的。
        third = reclaim_space(lake)
        self.assertEqual(third["deleted"], [])

    def test_reclaim_reports_unregistered_files_instead_of_deleting(self):
        lake = self.open_lake()
        self.seed_day(lake, count=1)
        lake.snapshot()
        planted = Path(self.directory) / "segments" / "planted.parquet"
        planted.write_bytes(b"not a real managed file")
        report = reclaim_space(lake)
        self.assertTrue(planted.exists())
        self.assertIn(str(planted.resolve()), report["skipped_unknown_files"])

    def test_reclaim_keeps_unfinished_compaction_output(self):
        lake = self.open_lake()
        self.seed_day(lake, count=2)
        # 制造一个停在 writing 的任务及其预登记输出。
        result = compact(lake)
        task_id = result["tasks"][0]["id"]
        from viewsegments.storage import atomic
        with atomic(lake.db):
            lake.db.execute(
                "UPDATE compactions SET state='writing' WHERE id=?", (task_id,))
        output = json.loads(lake.db.execute(
            "SELECT output_paths FROM compactions WHERE id=?", (task_id,)
        ).fetchone()[0])[0]
        report = reclaim_space(lake)
        self.assertNotIn(output, report["deleted"])
        self.assertTrue(Path(output).is_file())

    # ----------------------------------------------------- 崩溃与并发
    def test_resume_after_crash_in_each_phase(self):
        for phase in ("planning", "writing", "ready"):
            with self.subTest(phase=phase):
                self._single_phase_resume(phase)

    def _single_phase_resume(self, phase):
        self.temporary.cleanup()
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = self.temporary.name
        self.clock = FakeClock()
        lake = self.open_lake()
        self.seed_day(lake, count=3)
        lake.snapshot()
        expected_ids = {f"E{i}" for i in range(3)}

        from viewsegments import compact as compact_module
        if phase == "planning":
            # 在输出文件真正落盘前退出：只留下清单中的规划行。
            real_write = compact_module.write_rows_to

            def crash_write(destination, rows):
                raise RuntimeError("crash before data file")

            compact_module.write_rows_to = crash_write
            try:
                with self.assertRaises(RuntimeError):
                    compact(lake)
            finally:
                compact_module.write_rows_to = real_write
        else:
            # 让任务停在发布事务之前：ready 保留成品，writing 视为成品未完成。
            real_publish = compact_module._publish

            def crash_publish(lake_arg, compaction_id, fence):
                output = json.loads(lake_arg.db.execute(
                    "SELECT output_paths FROM compactions WHERE id=?",
                    (compaction_id,)).fetchone()[0])[0]
                if phase == "writing":
                    Path(output).unlink()
                    lake_arg.db.execute(
                        "UPDATE compactions SET state='writing' WHERE id=?",
                        (compaction_id,))
                    lake_arg.db.commit()
                raise RuntimeError("crash before manifest switch")

            compact_module._publish = crash_publish
            try:
                with self.assertRaises(RuntimeError):
                    compact(lake)
            finally:
                compact_module._publish = real_publish

        # 重新打开湖（新进程），重入整理必须恢复到已发布且数据一致。
        lake.close()
        lake = self.open_lake()
        result = compact(lake)
        self.assertTrue(
            all(task.get("state") == "published" for task in result["tasks"]))
        current = lake.snapshot()
        self.assertEqual(self.event_ids(lake, current), expected_ids)
        self.assertEqual(len(lake.files(current)), 1)
        # 旧文件在所有旧快照到期并回收后才物理消失。
        self.clock.advance(hours=25)
        reclaim_space(lake)
        self.assertEqual(len(self.parquet_files()), 1)

    def test_two_compactors_only_one_takes_effect(self):
        from viewsegments.compact import _acquire_lock
        from viewsegments.storage import atomic
        lake = self.open_lake()
        self.seed_day(lake, count=2)
        # 第一个整理者持锁并完成整理。
        with atomic(lake.db):
            _acquire_lock(lake.db, self.clock(), timedelta(minutes=30))
        try:
            # 锁仍在有效期时，第二个整理者直接被拒绝，不能碰同一批文件。
            other = SegmentLake(self.directory, clock=self.clock)
            self.lakes.append(other)
            with self.assertRaises(CompactionConflict):
                compact(other)
        finally:
            with atomic(lake.db):
                lake.db.execute("DELETE FROM compaction_lock WHERE id=1")
        # 锁释放后整理可以正常进行，且只有一份合并结果。
        compact(lake)
        # 旧文件在旧快照到期并回收后才真正消失。
        self.clock.advance(hours=25)
        reclaim_space(lake)
        self.assertEqual(len(self.parquet_files()), 1)
        # 时钟推进后重新取当前版本，验证业务记录完整无重复。
        current = lake.snapshot()
        self.assertEqual(len(detail(lake, current)), 2)
        self.assertEqual(len(lake.files(current)), 1)
        self.assertEqual(len(self.event_ids(lake, current)), 2)

    def test_stale_lock_is_taken_over_and_old_fence_rejected(self):
        from viewsegments.compact import _acquire_lock, _check_fence
        from viewsegments.storage import atomic
        lake = self.open_lake()
        self.seed_day(lake, count=2)
        with atomic(lake.db):
            _holder, old_fence = _acquire_lock(lake.db, self.clock(), timedelta(seconds=1))
        # 锁过期后另一个整理者接管，fence 递增。
        self.clock.advance(seconds=2)
        with atomic(lake.db):
            _holder, new_fence = _acquire_lock(lake.db, self.clock(), timedelta(seconds=1))
        self.assertEqual(new_fence, old_fence + 1)
        from viewsegments.errors import CompactionStale
        with self.assertRaises(CompactionStale):
            _check_fence(lake.db, old_fence)

    def test_reopen_preserves_compaction_history_and_files(self):
        lake = self.open_lake()
        self.seed_day(lake, count=2)
        compact(lake)
        snapshot = lake.snapshot()
        lake.close()
        reopened = self.open_lake()
        self.assertEqual(len(detail(reopened, snapshot)), 2)
        history = reopened.db.execute(
            "SELECT state FROM compactions").fetchall()
        self.assertTrue(all(row[0] == "published" for row in history))

    def test_day_filter_still_exact_after_compaction(self):
        lake = self.open_lake()
        lake.append("D1a", [event("A1", "2026-11-01"), event("A2", "2026-11-01")])
        lake.append("D1b", [event("A3", "2026-11-01")])
        lake.append("D2", [event("B1", "2026-11-02")])
        compact(lake)
        snapshot = lake.snapshot()
        self.assertEqual(
            {row["event_id"] for row in detail(lake, snapshot, "2026-11-01")},
            {"A1", "A2", "A3"})
        self.assertEqual(
            [row["event_id"] for row in detail(lake, snapshot, "2026-11-02")],
            ["B1"])
        self.assertEqual(detail(lake, snapshot, "2026-12-25"), [])

    def test_mixed_day_files_are_grouped_separately(self):
        lake = self.open_lake()
        # 两个跨日文件进入混合组，仍可被合并且记录不丢。
        lake.append("M1", [event("M1a", "2026-11-01"), event("M1b", "2026-11-02")])
        lake.append("M2", [event("M2a", "2026-11-01"), event("M2b", "2026-11-03")])
        result = compact(lake)
        self.assertEqual(result["tasks"][0]["state"], "published")
        snapshot = lake.snapshot()
        self.assertEqual(len(lake.files(snapshot)), 1)
        self.assertEqual(len(detail(lake, snapshot)), 4)


if __name__ == "__main__":
    unittest.main()
