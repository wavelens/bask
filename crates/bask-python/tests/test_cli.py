# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""`engine.cli(argv)` forwards into the Rust CLI frontend: `list-tasks` prints the
checkpoints, `--tasks=NAME` runs only up to that boundary, a later `--tasks` on the
downstream checkpoint resumes without re-reading the source, and an unknown task exits 2.
The whole CLI is Rust, so this mirrors the binary."""
import json

import pytest

import bask
from bask.tasks import Checkpoint


class Feed:
    pass


class Line:
    def __init__(self, i):
        self.i = i


class RowData:
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


class CliSaved(RowData, Checkpoint):
    pass


class CliResaved(RowData, Checkpoint):
    pass


def _engine(reads):
    engine = bask.Engine(concurrency=2)

    @engine.worker(Feed)
    def read(_feed, ctx):
        reads.append(1)
        for i in range(6):
            ctx.emit_keyed(i, Line(i))

    @engine.worker(Line)
    def convert(line, ctx):
        ctx.emit(CliSaved(f"row-{line.i}", line.i))

    @engine.worker(CliSaved)
    def edit(saved, ctx):
        ctx.emit(CliResaved(saved.id, saved.value * 10))

    engine.source(Feed(), "feed")
    return engine


def _cli(engine, args):
    with pytest.raises(SystemExit) as exit:
        engine.cli(["prog", *args])
    return exit.value.code


def test_list_tasks_and_terminal_selection(tmp_path, capfd):
    store = str(tmp_path / "bask.sqlite")
    reads = []
    engine = _engine(reads)

    assert _cli(engine, ["list-tasks", "--store", store]) == 0
    out = capfd.readouterr().out
    assert "CliSaved" in out and "CliResaved" in out

    # Run up to CliSaved: it materializes but its worker (edit) never runs.
    assert _cli(engine, ["--tasks=CliSaved", "--no-live", "--store", store]) == 0
    assert reads == [1]

    # Select the downstream checkpoint: resume from the stored CliSaved, source skipped whole.
    assert _cli(engine, ["--tasks=CliResaved", "--no-live", "--store", store]) == 0
    assert reads == [1], "source not re-read on resume"


def test_unknown_task_exits_2(tmp_path):
    assert _cli(_engine([]), ["--tasks=Nope", "--store", str(tmp_path / "s.db")]) == 2
