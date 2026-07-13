# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Call both Rust predefined stages from Python over pyarrow data: a 250-row batch is
split into 32-row pieces by the chunker, then re-aggregated into groups of at least 100
rows by the row_batch router (the trailing group flushes at end-of-run).
"""
import pyarrow as pa

from bask import Engine, Worker
from bask.tasks import Batch


class Whole(Batch):
    pass


class Piece(Batch):
    pass


class Groups:  # marker: the aggregator's route key
    pass


class Group(Batch):
    pass


engine = Engine(concurrency=1)
engine.chunker(Whole, Piece, rows=32)
engine.row_batch(Groups, Group, rows=100)

seen = []


@engine.worker(Piece)
class Feed(Worker):
    def process(self, piece, ctx):
        ctx.route(Groups, piece.batch)


@engine.worker(Group)
class Handle(Worker):
    def process(self, group, ctx):
        seen.append(group.batch.num_rows)


engine.seed(Whole(pa.record_batch({"n": list(range(250))})))
report = engine.run()

print("group row counts:", seen)
print("groups:", report.output(Groups))
print("stats:", report)
