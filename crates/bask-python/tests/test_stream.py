# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""The streaming handoff: a terminal collected type drains through Engine.stream()."""
import bask
from bask import Worker


class Feed:
    pass


class Sample:
    def __init__(self, i):
        self.i = i


def _engine(n, concurrency=2):
    engine = bask.Engine(concurrency=concurrency)

    @engine.worker(Feed)
    class Emit(Worker):
        def process(self, _feed, ctx):
            for i in range(n):
                ctx.emit(Sample(i))

    engine.collect(Sample)
    engine.seed(Feed())
    return engine


def test_stream_drains_all_samples():
    got = []
    with _engine(100).stream() as handle:
        for sample in handle:
            got.append(sample.i)
    assert sorted(got) == list(range(100))


def test_stream_report_after_drain():
    handle = _engine(10).stream()
    drained = list(handle)
    assert len(drained) == 10
    assert handle.report["processed"] >= 10


def test_stream_backpressure_small_capacity():
    got = [sample.i for sample in _engine(200).stream(capacity=1)]
    assert sorted(got) == list(range(200))


def test_stream_close_midway_does_not_hang():
    handle = _engine(1000).stream(capacity=1)
    first = next(handle)
    handle.close()
    assert first.i in range(1000)


def test_stream_requires_collect():
    engine = bask.Engine(concurrency=1)
    engine.seed(Feed())
    try:
        engine.stream()
        assert False, "expected RuntimeError"
    except RuntimeError:
        pass


def test_collect_and_worker_overlap_rejected():
    engine = bask.Engine(concurrency=1)

    @engine.worker(Sample)
    class Handle(Worker):
        def process(self, sample, ctx):
            pass

    engine.collect(Sample)
    engine.seed(Feed())
    try:
        engine.stream()
        assert False, "expected ValueError for collect+worker overlap"
    except ValueError:
        pass
