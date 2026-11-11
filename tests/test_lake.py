import unittest
from lake.catalog import Catalog, Conflict
from lake.segments import SegmentStore, read_rows
from lake import cdc, compact, evidence


class LakeTest(unittest.TestCase):
    def test_regression(self):
        result = evidence.regression()
        expected = [{"key": "A", "day": "2026-10-02", "amount": 15},
                    {"key": "C", "day": "2026-10-02", "amount": 7}]
        self.assertEqual(result["before"]["rows"], expected)
        self.assertEqual(result["after"]["rows"], expected)
        self.assertEqual(result["before"]["checkpoint"], 6)
        self.assertEqual(result["after"]["checkpoint"], 6)
        self.assertEqual(result["before"]["segments"], 4)
        self.assertEqual(result["after"]["segments"], 2)
        self.assertEqual(result["pinned_v1"],
                         [{"key": "A", "day": "2026-10-01", "amount": 10},
                          {"key": "B", "day": "2026-10-01", "amount": 20}])

    def test_conflicts_do_not_publish(self):
        traces = evidence.interleavings()
        for trace in traces.values():
            self.assertEqual(trace["error"], "HEAD_CONFLICT")
            self.assertEqual(trace["before"], trace["rejected"])
            self.assertEqual(trace["retried"]["checkpoint"], 4)
            self.assertEqual(trace["retried"]["rows"],
                             [{"key": "A", "day": "2026-10-02", "amount": 12}])
        self.assertEqual(traces["stale_writer"]["before"]["checkpoint"], 2)

    def test_receipt_is_bound_to_request(self):
        result = evidence.receipt()
        self.assertEqual(result["receipt_version"], 1)
        self.assertEqual(result["head"]["version"], 2)
        self.assertEqual(result["head"]["checkpoint"], 4)
        self.assertEqual(result["changed_request"], "TOKEN_REUSE")

    def test_projection_preserves_global_key_semantics(self):
        result = evidence.projection()
        self.assertEqual(result["full_manifest"], [])
        self.assertEqual(result["projected_manifest"],
                         [{"key": "B", "day": "2026-10-01", "amount": 20}])

    def test_source_gap_does_not_publish(self):
        catalog, store = Catalog(), SegmentStore()
        with self.assertRaisesRegex(ValueError, "NONCONTIGUOUS_SOURCE"):
            cdc.ingest(catalog, store, evidence.sample()[1:2], "gap")
        self.assertEqual(catalog.snapshot().version, 0)
        self.assertEqual(catalog.snapshot().checkpoint, 0)

    def test_overlapping_compactors(self):
        catalog, store = Catalog(), SegmentStore()
        cdc.ingest(catalog, store, evidence.sample()[:2], "source-1")
        first = compact.plan(catalog, store, "compact-1")
        second = compact.plan(catalog, store, "compact-2")
        compact.publish(catalog, first)
        with self.assertRaises(Conflict):
            compact.publish(catalog, second)
        self.assertEqual(catalog.snapshot().version, 2)
        self.assertEqual(catalog.snapshot().checkpoint, 2)

    def test_checkpoint_regression_is_rejected(self):
        catalog, store = Catalog(), SegmentStore()
        cdc.ingest(catalog, store, evidence.sample()[:2], "source-1")
        with self.assertRaisesRegex(ValueError, "CHECKPOINT_REGRESSION"):
            catalog.publish(1, catalog.snapshot().segments, 1, "bad")
        self.assertEqual(catalog.snapshot().checkpoint, 2)


if __name__ == "__main__":
    unittest.main()
