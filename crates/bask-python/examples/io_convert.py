# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Convert a CSV file to JSONL by streaming it through the engine: a reader worker
emits row-batches and a writer worker appends them, so the file is never fully resident.

The Rust IO plane (Source/Sink registries) is not yet exposed to Python, so this mirrors
`examples/io_convert.rs` with plain workers doing the file IO.

Run with: python examples/io_convert.py
"""
import csv
import json
import os
import tempfile

from bask import Engine, Worker


class ReadCsv:
    def __init__(self, path, out, batch_rows=2):
        self.path = path
        self.out = out
        self.batch_rows = batch_rows


class WriteBatch:
    def __init__(self, out, rows):
        self.out = out
        self.rows = rows


engine = Engine()


@engine.worker(ReadCsv)
class Reader(Worker):
    def process(self, task, ctx):
        batch = []
        with open(task.path, newline="") as handle:
            for row in csv.DictReader(handle):
                batch.append(row)
                if len(batch) >= task.batch_rows:
                    ctx.emit(WriteBatch(task.out, batch))
                    batch = []
        if batch:
            ctx.emit(WriteBatch(task.out, batch))


# concurrency=1 so appends from concurrent batches never interleave.
@engine.worker(WriteBatch, concurrency=1)
class Writer(Worker):
    def process(self, task, ctx):
        with open(task.out, "a") as handle:
            for row in task.rows:
                handle.write(json.dumps(row) + "\n")


workdir = tempfile.mkdtemp(prefix="bask_io_convert_")
csv_path = os.path.join(workdir, "in.csv")
jsonl_path = os.path.join(workdir, "out.jsonl")
with open(csv_path, "w", newline="") as handle:
    handle.write("id,name\n1,alice\n2,bob\n3,carol\n")

engine.seed(ReadCsv(csv_path, jsonl_path))
report = engine.run()

with open(jsonl_path) as handle:
    rows = sum(1 for _ in handle)
print(f"csv -> jsonl: {rows} rows across {report.processed} tasks, {report.failed} failed")
print("stats:", report)
