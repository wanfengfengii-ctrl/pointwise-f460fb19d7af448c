import argparse
import json
from pathlib import Path
from .catalog import Catalog, Conflict, Snapshot
from .segments import SegmentStore, read_rows
from . import cdc, compact


def sample():
    return json.loads((Path(__file__).resolve().parents[1] / "data" / "cdc.json").read_text())


def state(catalog, store):
    current = catalog.snapshot()
    return {"version": current.version, "checkpoint": current.checkpoint,
            "segments": len(current.segments), "rows": read_rows(store, current)}


def regression():
    catalog, store = Catalog(), SegmentStore()
    events = sample()
    first = cdc.ingest(catalog, store, events[:2], "source-1")
    cdc.ingest(catalog, store, events[2:4], "source-2")
    cdc.ingest(catalog, store, events[4:], "source-3")
    before = state(catalog, store)
    compact.publish(catalog, compact.plan(catalog, store, "compact-1"))
    return {"before": before, "after": state(catalog, store),
            "pinned_v1": read_rows(store, first)}


def interleavings():
    events = sample()
    catalog, store = Catalog(), SegmentStore()
    cdc.ingest(catalog, store, events[:2], "source-1")
    pending = compact.plan(catalog, store, "compact-stale")
    cdc.ingest(catalog, store, events[2:4], "source-2")
    before = state(catalog, store)
    try:
        compact.publish(catalog, pending)
    except Conflict as exc:
        stale_compactor = str(exc)
    else:
        raise AssertionError("stale compactor was accepted")
    rejected = state(catalog, store)
    compact.publish(catalog, compact.plan(catalog, store, "compact-fresh"))
    compact_result = {"error": stale_compactor, "before": before,
                      "rejected": rejected, "retried": state(catalog, store)}

    catalog, store = Catalog(), SegmentStore()
    cdc.ingest(catalog, store, events[:2], "source-1")
    pending = cdc.prepare(catalog, store, events[2:4], "source-2")
    compact.publish(catalog, compact.plan(catalog, store, "compact-1"))
    before = state(catalog, store)
    try:
        cdc.commit(catalog, pending)
    except Conflict as exc:
        stale_writer = str(exc)
    else:
        raise AssertionError("stale writer was accepted")
    rejected = state(catalog, store)
    cdc.ingest(catalog, store, events[2:4], "source-2")
    return {"stale_compactor": compact_result,
            "stale_writer": {"error": stale_writer, "before": before,
                             "rejected": rejected, "retried": state(catalog, store)}}


def receipt():
    catalog, store = Catalog(), SegmentStore()
    events = sample()
    pending = cdc.prepare(catalog, store, events[:2], "source-1")
    cdc.commit(catalog, pending)
    cdc.ingest(catalog, store, events[2:4], "source-2")
    replayed = cdc.commit(catalog, pending)
    try:
        catalog.publish(pending.base, pending.manifest, pending.checkpoint + 1,
                        pending.token)
    except ValueError as exc:
        mismatch = str(exc)
    else:
        raise AssertionError("token rebound")
    return {"receipt_version": replayed.version, "head": state(catalog, store),
            "changed_request": mismatch}


def projection():
    catalog, store = Catalog(), SegmentStore()
    upsert = store.put([{"seq": 1, "key": "B", "op": "upsert",
                         "day": "2026-10-01", "amount": 20}])
    delete = store.put([{"seq": 2, "key": "B", "op": "delete",
                         "day": "2026-10-02"}])
    head = catalog.publish(0, (upsert, delete), 2, "fixture")
    return {"full_manifest": read_rows(store, head),
            "projected_manifest": read_rows(store, Snapshot(1, (upsert,), 2)),
            "selected_events": store.get(delete)}


def run_all():
    return {"regression": regression(), "interleavings": interleavings(),
            "receipt": receipt(), "projection": projection()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", choices=("all", "regression", "interleavings",
                                             "receipt", "projection"))
    args = parser.parse_args()
    result = run_all() if args.scenario == "all" else globals()[args.scenario]()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
