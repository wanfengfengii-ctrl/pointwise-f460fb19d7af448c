# CDC manifest lake reference repository

Python 3.9+; only json, hashlib, sqlite3, dataclasses, pathlib, argparse and unittest.
No package installation, network, Docker or background service is needed.

This repository is an executable local reference for a manifest-driven CDC lake.
The SQL catalog stores immutable snapshot manifests, an atomic head/checkpoint and
request-bound publication receipts. SegmentStore is a separate immutable
content-addressed SQLite blob adapter, not a production object-store benchmark.
Both adapters accept a database path; demonstrations use two independent in-memory
SQLite databases so every command is deterministic and leaves no persistent files.
There is no transaction spanning segment creation and catalog publication.

Input: data/cdc.json is a JSON array of ordered source records. seq is a globally
unique, contiguous positive source position. key is a global primary key; day is a
physical routing partition, not part of key identity. upsert includes integer
amount; delete has no amount. A key may move between days. Source batches are
processed in order by one logical ingest writer. Deletes and upserts for a key are
ordered by seq, including across days. A delete may be routed to another day than
an older upsert.

Entry points:
- python3 -m lake.evidence all: prints the four deterministic JSON evidence traces.
- python3 -m lake.evidence regression: three ingests, offline compaction and a
  pinned historical read.
- python3 -m lake.evidence interleavings: prepare/commit interleavings of ingest
  and offline compaction, reporting rejection and full recomputation.
- python3 -m lake.evidence receipt: retries an already committed request after
  head advanced, then tries to reuse its token for a different request.
- python3 -m lake.evidence projection: reads two explicitly supplied manifests.
- python3 -m lake.review --baseline: regression assertions and JSON evidence.
- python3 -m lake.review: same assertions plus discussion-artifact integrity checks.

read_rows returns sorted live rows with key/day/amount; tombstones are internal.
After all six source events the live rows are A/2026-10-02/15 and
C/2026-10-02/7, checkpoint 6. Offline compaction changes version 3 to 4 and
segment count 4 to 2, retaining the B tombstone. Version 1 still reads A=10/B=20.

Current compaction is deliberately offline: plan folds all segments from a
snapshot, keeping winning delete records, and publish replaces the whole manifest
only if the catalog head has that exact base version. Concurrent publication
raises HEAD_CONFLICT. Catalog rejects lower checkpoints but cannot distinguish
an ingest caller from a maintenance caller. A receipt is bound to the exact
(base, manifest, checkpoint), not a logical job name alone. Failed CAS creates
neither snapshot nor receipt; independently written immutable segments may remain.

No vacuum, TTL, object listing consistency model, schema evolution, multi-source
ordering or external table-format library is implemented. Historical snapshots
and their objects are retained indefinitely. Production constraints for the
online-compaction discussion are specified in the task, not implemented here.
