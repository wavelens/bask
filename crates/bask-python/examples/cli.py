# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""A bask script that is also a program. Run it with no arguments for live progress, or:

    python cli.py list-tasks              # list the checkpoints and their stored status
    python cli.py --tasks=Saved           # run only up to the Saved checkpoint boundary
    python cli.py --json --store=out.db   # newline-delimited JSON progress, custom store

`engine.cli()` forwards argv into the same Rust frontend the binary uses, so the CLI, live
renderer, and exit codes are defined once in Rust.
"""
import json

import bask
from bask import Worker
from bask.tasks import Checkpoint


class Feed:
    pass


class Line:
    def __init__(self, i: int):
        self.i = i


# A JSON-encoded checkpoint payload; a plain mixin, so only the concrete Saved/Resaved
# subclasses register as checkpoints (an intermediate Checkpoint base would list as a task).
class RowData:
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


class Saved(RowData, Checkpoint):
    pass


class Resaved(RowData, Checkpoint):
    pass


engine = bask.Engine(concurrency=4)


@engine.worker(Feed)
class Read(Worker):
    def process(self, _feed, ctx):
        for i in range(20):
            ctx.emit_keyed(i, Line(i))


@engine.worker(Line)
class Convert(Worker):
    def process(self, line, ctx):
        ctx.emit(Saved(f"row-{line.i}", line.i))


@engine.worker(Saved)
class Edit(Worker):
    def process(self, saved, ctx):
        ctx.emit(Resaved(saved.id, saved.value * 10))


engine.source(Feed(), "feed")


if __name__ == "__main__":
    engine.cli()
