# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""The `row_batch` router unions the source rows folded into each group (full groups and
the flushed remainder), so a group-derived checkpoint covers every row and a later run
prunes the source. Requires pyarrow."""
import os
import tempfile

import pyarrow as pa

from bask import Engine
from bask.tasks import Batch, Checkpoint


class Feed:
    pass


class Piece(Batch):
    pass


class Groups:  # the aggregator's route key
    pass


class Group(Batch, Checkpoint):
    def key(self) -> str:
        return str(self.batch.column(0)[0].as_py())


def _build(store, reads):
    engine = Engine(concurrency=1, store=store)
    engine.row_batch(Groups, Group, rows=2)

    @engine.worker(Feed)
    def read(_feed, ctx):
        reads.append(1)
        for i in range(5):
            ctx.emit_keyed(i, Piece(pa.record_batch({"n": [i]})))

    @engine.worker(Piece)
    def fold(piece, ctx):
        ctx.route(Groups, piece.batch)

    engine.source(Feed(), "feed")
    return engine


def test_row_batch_unions_coverage():
    store = os.path.join(tempfile.mkdtemp(), "bask.sqlite")
    reads = []

    # Five rows -> two full groups {0,1},{2,3} plus a flushed remainder {4}: their union
    # is every row, so run 2 finds the source fully covered and skips it.
    _build(store, reads).run()
    assert reads == [1]
    _build(store, reads).run()
    assert reads == [1], "source pruned; row_batch covered every row across its groups"


if __name__ == "__main__":
    test_row_batch_unions_coverage()
    print("ok  test_row_batch_unions_coverage")
