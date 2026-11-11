import hashlib
import json
import sqlite3


class SegmentStore:
    """Immutable, content-addressed adapter; independent of Catalog transactions."""

    def __init__(self, database=":memory:"):
        self.db = sqlite3.connect(database, isolation_level=None)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS objects(id TEXT PRIMARY KEY, body BLOB NOT NULL)")

    def put(self, events):
        body = json.dumps(events, sort_keys=True, separators=(",", ":")).encode()
        object_id = hashlib.sha256(body).hexdigest()
        self.db.execute("INSERT OR IGNORE INTO objects VALUES(?, ?)",
                        (object_id, body))
        return object_id

    def get(self, object_id):
        row = self.db.execute("SELECT body FROM objects WHERE id=?",
                              (object_id,)).fetchone()
        if row is None:
            raise KeyError(object_id)
        body = row[0]
        if hashlib.sha256(body).hexdigest() != object_id:
            raise ValueError("CORRUPT_SEGMENT")
        return json.loads(body)


def write_partitioned(store, events):
    groups = {}
    for event in events:
        groups.setdefault(event["day"], []).append(event)
    return tuple(store.put(sorted(groups[day], key=lambda e: (e["seq"], e["key"])))
                 for day in sorted(groups))


def winners(store, segments):
    latest = {}
    seen_sequences = {}
    for object_id in segments:
        for event in store.get(object_id):
            previous = seen_sequences.setdefault(event["seq"], event)
            if previous != event:
                raise ValueError("SOURCE_SEQUENCE_COLLISION")
            key = event["key"]
            if key not in latest or event["seq"] > latest[key]["seq"]:
                latest[key] = event
    return [latest[key] for key in sorted(latest)]


def read_rows(store, snapshot):
    return [{"key": event["key"], "day": event["day"], "amount": event["amount"]}
            for event in winners(store, snapshot.segments)
            if event["op"] == "upsert"]
