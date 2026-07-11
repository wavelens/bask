# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""A custom checkpoint dataset backed by a SQL database.

`bask.data.Dataset` writes Parquet shards to a directory; here we instead implement the
dataset protocol against a sqlite table -- the same shape a Postgres connector would take --
so materialized checkpoints are rows in a database and provenance coverage compacts
superseded shards in place. The pipeline is `feed -> save -> edit -> resave`: the resave
re-derives the same source rows, so only the final shards stay live.
"""
import json
import sqlite3
import tempfile

import bask
from bask.data import coverage_rows, coverage_to_bytes
from bask.tasks import Checkpoint


class SqlDataset:
    """A dataset whose shards live as rows in a sqlite `entry` table: `status`/`coverage`
    are the checkpoint index the protocol reserves, `payload`/`seq`/`live` the shards."""

    def __init__(self, path: str):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS entry (name TEXT, key TEXT, status INT, coverage BLOB, "
            "payload BLOB, seq INT, live INT DEFAULT 0, PRIMARY KEY (name, key));"
            "CREATE TABLE IF NOT EXISTS extent (source TEXT PRIMARY KEY, coverage BLOB);"
        )
        self.db.commit()

    # -- store side: the checkpoint index --
    def statuses(self):
        return list(self.db.execute("SELECT name, key, status FROM entry"))

    def covered(self) -> bytes:
        rows: set[int] = set()
        for (cov,) in self.db.execute("SELECT coverage FROM entry"):
            rows.update(coverage_rows(cov))
        return coverage_to_bytes(rows)

    def extents(self):
        return list(self.db.execute("SELECT source, coverage FROM extent"))

    def stored_items(self, name):
        return self._live(name)

    def commit(self, name, key, payload, coverage):
        self.db.execute(
            "INSERT OR REPLACE INTO entry (name, key, status, coverage, payload, seq, live) "
            "VALUES (?, ?, 0, ?, NULL, NULL, 0)",
            (name, key, coverage),
        )
        self.db.commit()

    def consume(self, name, key):
        self.db.execute("UPDATE entry SET status = 1 WHERE name = ? AND key = ?", (name, key))
        self.db.commit()

    def record_extent(self, source, coverage):
        self.db.execute("INSERT OR REPLACE INTO extent (source, coverage) VALUES (?, ?)", (source, coverage))
        self.db.commit()

    # -- dataset side: the shards --
    def put(self, name, key, payload, coverage):
        seq = self.db.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM entry").fetchone()[0]
        self.db.execute(
            "INSERT OR IGNORE INTO entry (name, key, status, coverage, live) VALUES (?, ?, 0, ?, 0)",
            (name, key, coverage),
        )
        self.db.execute(
            "UPDATE entry SET payload = ?, seq = ?, live = 1 WHERE name = ? AND key = ?",
            (payload, seq, name, key),
        )
        self._supersede()
        self.db.commit()

    def stored(self, name):
        return self._live(name)

    def read(self):
        return self._live(None)

    def _live(self, name):
        sql = "SELECT key, payload, coverage FROM entry WHERE live = 1 AND payload IS NOT NULL"
        args = () if name is None else (name,)
        if name is not None:
            sql += " AND name = ?"
        return list(self.db.execute(sql, args))

    def _supersede(self):
        # A live shard whose rows a strictly-newer save re-derived is retired.
        shards = list(self.db.execute("SELECT name, key, seq, coverage FROM entry WHERE payload IS NOT NULL"))
        for name, key, seq, coverage in shards:
            rows = set(coverage_rows(coverage))
            newer = {r for _, _, s, cov in shards if s > seq for r in coverage_rows(cov)}
            if rows and rows <= newer:
                self.db.execute("UPDATE entry SET live = 0, payload = NULL WHERE name = ? AND key = ?", (name, key))


class Feed:
    pass


class Item:
    def __init__(self, i: int, value: int):
        self.i = i
        self.value = value


class Row(Checkpoint):
    """A checkpoint keyed by `id`, encoded to JSON so its shard is portable."""

    def __init__(self, id: str, value: int):
        self.id = id
        self.value = value

    def key(self) -> str:
        return self.id

    def encode(self) -> bytes:
        return json.dumps({"id": self.id, "value": self.value}).encode()

    @classmethod
    def decode(cls, data: bytes):
        row = json.loads(data)
        return cls(row["id"], row["value"])


class Saved(Row):
    pass


class Resaved(Row):
    pass


def main():
    path = tempfile.mktemp(suffix=".sqlite")
    data = SqlDataset(path)
    engine = bask.Engine(concurrency=1, dataset=data)

    @engine.worker(Feed)
    def read(_feed, ctx):
        for i in range(6):
            ctx.emit_keyed(i, Item(i, i))

    @engine.worker(Item)
    def fold(item, ctx):
        ctx.emit(Saved(str(item.i), item.value))

    @engine.worker(Saved)
    def edit(saved, ctx):
        ctx.emit(Resaved(saved.id, saved.value * 10))

    engine.source(Feed(), "feed")
    report = engine.run()

    live = sorted((json.loads(p)["id"], json.loads(p)["value"]) for _, p, _ in data.read())
    print(f"processed {report.processed} tasks into {path}")
    print(f"live shards after save -> edit -> resave: {len(live)}")
    for id, value in live:
        print(f"  {id} = {value}")


if __name__ == "__main__":
    main()
