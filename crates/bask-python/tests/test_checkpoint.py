# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Checkpoint behavior through the Python bindings: dispatch-level skip, source pruning,
process-later reseeding (pickle round-trip), and key-only side effects."""
import os
import tempfile

from bask import Engine, Worker
from bask.tasks import Checkpoint


class Feed:
    pass


class Row:
    def __init__(self, id: int):
        self.id = id


class Saved(Checkpoint):
    def __init__(self, id: int):
        self.id = id

    def key(self) -> str:
        return str(self.id)


class Twice:
    pass


class Dup(Checkpoint):
    def key(self) -> str:
        return "dup"


class Ping(Checkpoint):
    KEY_ONLY = True

    def __init__(self, id: int):
        self.id = id

    def key(self) -> str:
        return str(self.id)


class Fan(Worker):
    def process(self, _task, ctx):
        ctx.emit(Dup())


class Reader(Worker):
    def __init__(self, reads):
        self.reads = reads

    def process(self, _feed, ctx):
        self.reads.append(1)
        for i in range(4):
            ctx.emit_keyed(i, Row(i))


class Convert(Worker):
    def process(self, row, ctx):
        ctx.emit(Saved(row.id))


class Consume(Worker):
    def __init__(self, consumed):
        self.consumed = consumed

    def process(self, saved, ctx):
        self.consumed.append(saved.id)


class Send(Worker):
    def __init__(self, sends):
        self.sends = sends

    def process(self, ping, ctx):
        self.sends.append(ping.id)


def _store() -> str:
    return os.path.join(tempfile.mkdtemp(), "bask.sqlite")


def test_duplicate_key_is_skipped():
    engine = Engine(concurrency=1, store=_store())
    engine.register(Twice, Fan())
    engine.seed(Twice())
    engine.seed(Twice())
    report = engine.run()
    assert report.skipped == 1


def test_source_prune_and_process_later():
    store = _store()
    reads = []
    consumed = []

    def build(consumer):
        engine = Engine(concurrency=1, store=store)
        engine.register(Feed, Reader(reads))
        engine.register(Row, Convert())
        if consumer:
            engine.register(Saved, Consume(consumed))
        engine.source(Feed(), "feed")
        return engine

    # Run 1 stores four Saved with no consumer; run 2 registers one and reseeds them.
    build(consumer=False).run()
    assert reads == [1], "run 1 read the source once"
    build(consumer=True).run()

    assert reads == [1], "run 2 pruned the source, no re-read"
    assert sorted(consumed) == [0, 1, 2, 3], "stored items reseeded and consumed once"


def test_key_only_side_effect_runs_once():
    store = _store()
    sends = []

    def build():
        engine = Engine(concurrency=1, store=store)
        engine.register(Ping, Send(sends))
        engine.seed(Ping(7))
        return engine

    build().run()
    build().run()
    assert sends == [7]


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
