# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""emit_policy through the high-level Engine: a disallowed emit fails terminally, an
allowed one runs clean."""
from bask import Engine, Worker


class Job:
    pass


class Forbidden:
    def __init__(self, id: int):
        self.id = id


class Emits(Worker):
    def process(self, _job, ctx):
        ctx.emit(Forbidden(1))


class Sink(Worker):
    def process(self, _forbidden, ctx):
        pass


def test_disallowed_emit_fails_terminally():
    engine = Engine(concurrency=1)
    engine.register(Job, Emits())
    engine.emit_policy(Job, allows=[])
    engine.seed(Job())
    report = engine.run()

    assert report.failed == 1
    assert len(report.failures) == 1
    assert "may not emit" in report.failures[0]["error"]


def test_allowed_emit_runs_clean():
    engine = Engine(concurrency=1)
    engine.register(Job, Emits())
    engine.register(Forbidden, Sink())
    engine.emit_policy(Job, allows=[Forbidden])
    engine.seed(Job())
    report = engine.run()

    assert report.failed == 0
    assert report.failures == []


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
