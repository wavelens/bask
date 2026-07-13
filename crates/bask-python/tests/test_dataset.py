# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""A custom `Dataset` (a sqlite table) materializes checkpoints as shards, supersedes the
shards a resave re-derives, reads the live snapshot back, and prunes a covered source on a
later run. The built-in Parquet `Dataset` does the same over a directory (needs pyarrow)."""
import json
import sqlite3
import tempfile

import pytest

import bask
from bask import Worker
from bask.data import coverage_rows, coverage_to_bytes
from bask.tasks import Batch, Checkpoint


class SqlDataset:
    """A dataset whose shards are rows in a sqlite table; the full dataset protocol."""

    def __init__(self, path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS entry (name TEXT, key TEXT, status INT, coverage BLOB, "
            "payload BLOB, seq INT, live INT DEFAULT 0, PRIMARY KEY (name, key));"
            "CREATE TABLE IF NOT EXISTS extent (source TEXT PRIMARY KEY, coverage BLOB);"
        )
        self.db.commit()

    def statuses(self):
        return list(self.db.execute("SELECT name, key, status FROM entry"))

    def covered(self):
        rows = set()
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
        shards = list(self.db.execute("SELECT name, key, seq, coverage FROM entry WHERE payload IS NOT NULL"))
        for name, key, seq, cov in shards:
            rows = set(coverage_rows(cov))
            newer = {r for _, _, s, c in shards if s > seq for r in coverage_rows(c)}
            if rows and rows <= newer:
                self.db.execute("UPDATE entry SET live = 0, payload = NULL WHERE name = ? AND key = ?", (name, key))


class Feed:
    pass


class Item:
    def __init__(self, i):
        self.i = i


class Row(Checkpoint):
    def __init__(self, id, value):
        self.id = id
        self.value = value

    def key(self):
        return self.id

    def encode(self):
        return json.dumps({"id": self.id, "value": self.value}).encode()

    @classmethod
    def decode(cls, data):
        row = json.loads(data)
        return cls(row["id"], row["value"])


class Saved(Row):
    pass


class Resaved(Row):
    pass


def _build(data, reads):
    engine = bask.Engine(concurrency=1, dataset=data)

    @engine.worker(Feed)
    class Read(Worker):
        def process(self, _feed, ctx):
            reads.append(1)
            for i in range(6):
                ctx.emit_keyed(i, Item(i))

    @engine.worker(Item)
    class Fold(Worker):
        def process(self, item, ctx):
            ctx.emit(Saved(str(item.i), item.i))

    @engine.worker(Saved)
    class Edit(Worker):
        def process(self, saved, ctx):
            ctx.emit(Resaved(saved.id, saved.value * 10))

    engine.source(Feed(), "feed")
    return engine


def test_custom_dataset_supersedes_and_reads_back():
    data = SqlDataset(tempfile.mktemp(suffix=".sqlite"))
    report = _build(data, []).run()
    assert report.failed == 0
    live = sorted((json.loads(p)["id"], json.loads(p)["value"]) for _, p, _ in data.read())
    assert live == [("0", 0), ("1", 10), ("2", 20), ("3", 30), ("4", 40), ("5", 50)]
    assert data.stored("Saved") == [], "every Saved shard superseded by its Resaved"


def test_custom_dataset_resume_prunes_source():
    path = tempfile.mktemp(suffix=".sqlite")
    reads = []
    _build(SqlDataset(path), reads).run()
    assert reads == [1]
    _build(SqlDataset(path), reads).run()
    assert reads == [1], "source fully covered, so it is skipped whole on resume"


class Piece(Batch):
    pass


class Groups:
    pass


def test_builtin_parquet_dataset(tmp_path):
    pa = pytest.importorskip("pyarrow")
    from bask.data import BatchCheckpoint, Dataset

    class Chunk(BatchCheckpoint):
        def key(self):
            return str(self.batch.column(0)[0].as_py())

    data = Dataset(str(tmp_path / "out"))

    def build(reads):
        engine = bask.Engine(concurrency=1, dataset=data)
        engine.row_batch(Groups, Chunk, rows=2)

        @engine.worker(Feed)
        class Read(Worker):
            def process(self, _feed, ctx):
                reads.append(1)
                for i in range(5):
                    ctx.emit_keyed(i, Piece(pa.record_batch({"n": [i]})))

        @engine.worker(Piece)
        class Fold(Worker):
            def process(self, piece, ctx):
                ctx.route(Groups, piece.batch)

        engine.source(Feed(), "feed")
        return engine

    reads = []
    build(reads).run()
    assert reads == [1]
    assert data.to_pyarrow().num_rows == 5, "every row materialized into a Parquet shard"

    # Reopen and rerun: the source is fully covered, so it is skipped whole.
    data = Dataset(str(tmp_path / "out"))
    build(reads).run()
    assert reads == [1], "source pruned on resume"
    assert data.to_pyarrow().num_rows == 5
