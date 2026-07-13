# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Call the Rust chunker from Python over pyarrow data: a 10-row record batch is split
into pieces of at most 3 rows by bask_tasks::chunk, each emitted back as a pyarrow batch.
"""
import pyarrow as pa

from bask import Engine, Worker
from bask.tasks import Batch


class Whole(Batch):
    pass


class Piece(Batch):
    pass


engine = Engine(concurrency=1)
engine.chunker(Whole, Piece, rows=3)

seen = []


@engine.worker(Piece)
class Handle(Worker):
    def process(self, piece, ctx):
        seen.append(piece.batch.num_rows)


engine.seed(Whole(pa.record_batch({"n": list(range(10))})))
report = engine.run()

print("piece row counts:", seen)
print("total rows:", sum(seen))
print("stats:", report)
