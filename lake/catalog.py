import hashlib
import json
import sqlite3
from dataclasses import dataclass


class Conflict(RuntimeError):
    pass


@dataclass(frozen=True)
class Snapshot:
    version: int
    segments: tuple
    checkpoint: int


class Catalog:
    def __init__(self, database=":memory:"):
        self.db = sqlite3.connect(database, isolation_level=None)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS snapshots(
                version INTEGER PRIMARY KEY, segments TEXT NOT NULL,
                checkpoint INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS head(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                version INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS receipts(
                token TEXT PRIMARY KEY, signature TEXT NOT NULL,
                version INTEGER NOT NULL);
            INSERT OR IGNORE INTO snapshots VALUES(0, '[]', 0);
            INSERT OR IGNORE INTO head VALUES(1, 0);
        """)

    def snapshot(self, version=None):
        if version is None:
            version = self.db.execute(
                "SELECT version FROM head WHERE singleton=1").fetchone()[0]
        row = self.db.execute(
            "SELECT segments, checkpoint FROM snapshots WHERE version=?",
            (version,)).fetchone()
        if row is None:
            raise KeyError(version)
        return Snapshot(version, tuple(json.loads(row[0])), row[1])

    def publish(self, base, segments, checkpoint, token):
        if len(set(segments)) != len(segments):
            raise ValueError("DUPLICATE_SEGMENT")
        request = json.dumps([base, list(segments), checkpoint],
                             separators=(",", ":")).encode()
        signature = hashlib.sha256(request).hexdigest()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            receipt = self.db.execute(
                "SELECT signature, version FROM receipts WHERE token=?",
                (token,)).fetchone()
            if receipt:
                if receipt[0] != signature:
                    raise ValueError("TOKEN_REUSE")
                result = self.snapshot(receipt[1])
            else:
                current = self.snapshot()
                if current.version != base:
                    raise Conflict("HEAD_CONFLICT")
                if checkpoint < current.checkpoint:
                    raise ValueError("CHECKPOINT_REGRESSION")
                version = current.version + 1
                self.db.execute("INSERT INTO snapshots VALUES(?, ?, ?)",
                                (version, json.dumps(list(segments)), checkpoint))
                self.db.execute("UPDATE head SET version=? WHERE singleton=1",
                                (version,))
                self.db.execute("INSERT INTO receipts VALUES(?, ?, ?)",
                                (token, signature, version))
                result = self.snapshot(version)
            self.db.execute("COMMIT")
            return result
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
