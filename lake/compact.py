from dataclasses import dataclass
from .segments import winners, write_partitioned


@dataclass(frozen=True)
class Plan:
    base: int
    inputs: tuple
    outputs: tuple
    checkpoint: int
    token: str


def plan(catalog, store, token):
    """Offline compaction: all current segments, latest event including tombstones."""
    base = catalog.snapshot()
    outputs = write_partitioned(store, winners(store, base.segments))
    return Plan(base.version, base.segments, outputs, base.checkpoint, token)


def publish(catalog, prepared):
    return catalog.publish(prepared.base, prepared.outputs,
                           prepared.checkpoint, prepared.token)
