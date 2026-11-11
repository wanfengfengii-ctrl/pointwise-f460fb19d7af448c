from dataclasses import dataclass
from .segments import write_partitioned


@dataclass(frozen=True)
class PreparedBatch:
    base: int
    manifest: tuple
    checkpoint: int
    token: str


def prepare(catalog, store, events, token):
    base = catalog.snapshot()
    if not events:
        raise ValueError("EMPTY_BATCH")
    expected = list(range(base.checkpoint + 1, base.checkpoint + 1 + len(events)))
    if [event["seq"] for event in events] != expected:
        raise ValueError("NONCONTIGUOUS_SOURCE")
    for event in events:
        if event["op"] not in ("upsert", "delete"):
            raise ValueError("UNKNOWN_OPERATION")
        if event["op"] == "upsert" and not isinstance(event.get("amount"), int):
            raise ValueError("BAD_AMOUNT")
        if not isinstance(event.get("key"), str) or not isinstance(event.get("day"), str):
            raise ValueError("BAD_KEY_OR_PARTITION")
    added = write_partitioned(store, events)
    return PreparedBatch(base.version, base.segments + added, events[-1]["seq"], token)


def commit(catalog, prepared):
    return catalog.publish(prepared.base, prepared.manifest,
                           prepared.checkpoint, prepared.token)


def ingest(catalog, store, events, token):
    return commit(catalog, prepare(catalog, store, events, token))
