# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Live dashboard: a fan-out crawl -> render pipeline with two worker types and
two instances each. Run in a terminal to watch queue depth and per-type
concurrency update in place."""
import time

from bask import Engine, Worker

MAX_DEPTH = 3
FANOUT = 4


class Page:
    def __init__(self, id, depth):
        self.id = id
        self.depth = depth


class Render:
    pass


engine = Engine(concurrency=6, sample_interval_ms=120)


class Crawler(Worker):
    def process(self, page, ctx):
        time.sleep(0.03)  # simulate fetching
        ctx.emit(Render())
        if page.depth < MAX_DEPTH:
            for i in range(FANOUT):
                ctx.emit(Page(page.id * FANOUT + i, page.depth + 1))


class Renderer(Worker):
    def process(self, render, ctx):
        time.sleep(0.05)  # simulate rendering
        ctx.route(Rendered, 1)


engine.register(Page, Crawler(), label="crawler-1", concurrency=2)
engine.register(Page, Crawler(), label="crawler-2", concurrency=2)
engine.register(Render, Renderer(), label="renderer-1", concurrency=2)
engine.register(Render, Renderer(), label="renderer-2", concurrency=2)


@engine.router
class Rendered:
    def __init__(self):
        self.n = 0

    def route(self, value, out):
        self.n += value

    def finalize(self):
        return self.n


engine.seed(Page(1, 0))
report = engine.run(live=True)
print("rendered", report.output(Rendered), "pages")
