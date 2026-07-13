# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Dedup + instance-aware retry. Raw links (with duplicates) are deduped into one
Fetch per distinct URL; two proxy instances fetch each with failover, and a host
no proxy can serve exhausts its retries and lands in the failure list."""
from bask import Engine, Retry, Worker


class Link:
    def __init__(self, url):
        self.url = url


class Fetch:
    def __init__(self, url):
        self.url = url


engine = Engine(concurrency=1, retry=Retry(max_attempts=3, avoid_failed=True, backoff_ms=10))


@engine.dedup
class SeenUrls:
    pass


@engine.worker(Link)
class Dedupe(Worker):
    def process(self, link, ctx):
        if ctx.first_seen(SeenUrls, link.url):
            ctx.emit(Fetch(link.url))


class Proxy(Worker):
    def __init__(self, name, blocks):
        self.name = name
        self.blocks = blocks

    def process(self, task, ctx):
        if any(task.url.endswith(suffix) for suffix in self.blocks):
            raise RuntimeError(f"{self.name} cannot reach {task.url}")
        ctx.route(Served, (task.url, self.name))


engine.register(Fetch, Proxy("eu", (".ru", ".onion")), label="eu")
engine.register(Fetch, Proxy("us", (".cn", ".onion")), label="us")


@engine.router
class Served:
    def __init__(self):
        self.hits = []

    def route(self, hit, out):
        self.hits.append(hit)

    def finalize(self):
        return sorted(self.hits)


links = ["a.com", "b.ru", "a.com", "c.cn", "d.onion", "b.ru"]
for url in links:
    engine.seed(Link(url))

report = engine.run()

print(f"seeded {len(links)} links, {report.unique(SeenUrls)} unique urls")
print("served:")
for url, proxy in report.output(Served):
    print(f"  {url:8} via {proxy}")
print("\nfailed:")
for failure in report.failures:
    print(f"  after {failure['attempts']} attempts: {failure['error']}")
print("\nstats:", report)
