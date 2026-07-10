# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Router as a batcher: a stream of readings is folded into fixed-size batches, each
full batch is emitted downstream, and the trailing partial batch flushes at end-of-run.
A second router reduces the per-batch sums into a grand total.

A Python router implements route(self, value, out) and, for batching, flush(self, out):
it folds state and may out.emit(task) to route, filter, or batch.
"""
from bask import Engine


class Reading:
    def __init__(self, n):
        self.n = n


class Batch:
    def __init__(self, values):
        self.values = values


engine = Engine(concurrency=1)


@engine.router
class Batcher:
    def __init__(self):
        self.buf = []

    def route(self, n, out):
        self.buf.append(n)
        if len(self.buf) >= 4:
            out.emit(Batch(self.buf))
            self.buf = []

    def flush(self, out):
        if self.buf:
            out.emit(Batch(self.buf))
            self.buf = []

    def finalize(self):
        return None


@engine.router
class Total:
    def __init__(self):
        self.total = 0

    def route(self, n, out):
        self.total += n

    def finalize(self):
        return self.total


@engine.worker(Reading)
def ingest(reading, ctx):
    ctx.route(Batcher, reading.n)


@engine.worker(Batch)
def process(batch, ctx):
    total = sum(batch.values)
    print(f"batch of {len(batch.values)} sums to {total}")
    ctx.route(Total, total)


for i in range(1, 11):
    engine.seed(Reading(i))
report = engine.run()

print("grand total =", report.output(Total))
print("stats:", report)
