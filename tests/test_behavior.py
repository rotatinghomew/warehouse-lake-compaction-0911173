import tempfile
import unittest


class WorkspaceTest(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.directory=self.temporary.name
        self.resources=[]

    def open(self, factory):
        resource=factory(self.directory)
        self.resources.append(resource)
        return resource

    def tearDown(self):
        for resource in reversed(self.resources):
            resource.close()
        self.temporary.cleanup()

from viewsegments.catalog import SegmentLake
from viewsegments.query import detail, duration, export


def event(name='E1',day='2026-11-01'):
    return {'event_id':name,'day':day,'seconds':30}


class Behavior(WorkspaceTest):
    def test_upload_identity(self):
        lake=self.open(SegmentLake)
        self.assertTrue(lake.append('U1',[event()]))
        self.assertFalse(lake.append('U1',[event()]))
        with self.assertRaises(ValueError):
            lake.append('U1',[event('E2')])

    def test_snapshots_preserve_files(self):
        lake=self.open(SegmentLake)
        lake.append('U1',[event()])
        old=lake.snapshot()
        lake.append('U2',[event('E2')])
        current=lake.snapshot()
        self.assertEqual(len(detail(lake,old)),1)
        self.assertEqual(len(detail(lake,current)),2)
        self.assertEqual(lake.snapshot(),current)
        self.assertEqual(duration(lake,current)[0]['seconds'],60)

    def test_key_conflict_and_day_filter(self):
        import sqlite3
        lake=self.open(SegmentLake)
        lake.append('U1',[event()])
        with self.assertRaises(sqlite3.IntegrityError):
            lake.append('U2',[event('E2'),event()])
        self.assertEqual(len(detail(lake,lake.snapshot())),1)
        self.assertEqual(detail(lake,lake.snapshot(),'2026-11-02'),[])

    def test_reopen_and_export(self):
        from pathlib import Path
        lake=self.open(SegmentLake)
        lake.append('U1',[event()])
        snapshot=lake.snapshot()
        reopened=self.open(SegmentLake)
        output=Path(self.directory)/'export.jsonl'
        self.assertEqual(export(reopened,snapshot,output)['rows'],1)
        self.assertIn('E1',output.read_text())
        with self.assertRaises(KeyError):
            detail(reopened,999)
