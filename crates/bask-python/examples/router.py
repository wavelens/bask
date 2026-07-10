# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Router as a routing/filtering stage: a stream of numbers is classified so evens are
routed on as `Even` tasks and odds are filtered out, then a second router sums the evens.

A Python router implements route(self, value, out): it folds `value` into its own state
and may `out.emit(task)` to route, filter (emit nothing), or fan out.
"""
from bask import Engine


class Number:
    def __init__(self, n):
        self.n = n


class Even:
    def __init__(self, n):
        self.n = n


engine = Engine()


@engine.router
class Classify:
    def __init__(self):
        self.seen = 0

    def route(self, n, out):
        self.seen += 1
        if n % 2 == 0:
            out.emit(Even(n))  # route evens on, filter odds

    def finalize(self):
        return self.seen


@engine.router
class SumEven:
    def __init__(self):
        self.total = 0

    def route(self, n, out):
        self.total += n

    def finalize(self):
        return self.total


@engine.worker(Number)
def feed(number, ctx):
    ctx.route(Classify, number.n)


@engine.worker(Even)
def collect(even, ctx):
    ctx.route(SumEven, even.n)


for n in range(10):
    engine.seed(Number(n))
report = engine.run()

print(f"classified {report.output(Classify)} numbers")
print(f"sum of evens = {report.output(SumEven)}")
print("stats:", report)
