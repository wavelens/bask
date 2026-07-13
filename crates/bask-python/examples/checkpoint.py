# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Opt-in durability from Python: a source reads rows and a worker "fetches" each into a
`Saved` checkpoint. The two runs share one store, so the second finds the source fully
covered and skips it whole (no re-read), the same as the Rust engine.
"""
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
    def __init__(self, id: int, body: str):
        self.id = id
        self.body = body

    def key(self) -> str:
        return str(self.id)


store = os.path.join(tempfile.mkdtemp(), "bask.sqlite")


def build() -> Engine:
    engine = Engine(concurrency=1, store=store)

    @engine.worker(Feed)
    class Read(Worker):
        def process(self, feed, ctx):
            print("  reading source")
            for i in range(8):
                ctx.emit_keyed(i, Row(i))

    @engine.worker(Row)
    class Fetch(Worker):
        def process(self, row, ctx):
            ctx.emit(Saved(row.id, f"row-{row.id}"))

    engine.source(Feed(), "feed")
    return engine


for run in (1, 2):
    report = build().run()
    print(f"run {run}: processed={report.processed} skipped={report.skipped}")
