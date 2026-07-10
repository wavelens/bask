# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Resource-attribute-aware retry. Two GPU instances self-describe with a `gpu` attribute
and draw from a shared `gpu` pool; a job too large for the a100 raises different_attr("gpu")
and is retried on the h100, while a job with no data raises Fatal and lands in the sink.
"""
from bask import Engine, Retry, Fatal, different_attr


class TrainJob:
    def __init__(self, id, size_gb):
        self.id = id
        self.size_gb = size_gb


class Trainer:
    def __init__(self, kind, vram_gb):
        self.kind = kind
        self.vram_gb = vram_gb

    def process(self, job, ctx):
        if job.size_gb == 0:
            raise Fatal(f"job {job.id} has no data")
        if job.size_gb > self.vram_gb:
            raise different_attr("gpu", f"job {job.id} needs {job.size_gb}GB, {self.kind} has {self.vram_gb}GB")
        ctx.route(Placed, (job.id, self.kind))


dead = []


def on_dead(letter):
    dead.append(letter["task"].id)
    print(f"dead-letter: {letter['error']} ({letter['attempts']} attempts)")


engine = Engine(
    concurrency=1,
    retry=Retry(max_attempts=2, jitter=0.2),
    resources={"gpu": 2},
    dead_letter=on_dead,
)


@engine.router
class Placed:
    def __init__(self):
        self.hits = []

    def route(self, hit, out):
        self.hits.append(hit)

    def finalize(self):
        return sorted(self.hits)


engine.register(TrainJob, Trainer("a100", 40), label="a100", attrs={"gpu": "a100"}, requires=["gpu"])
engine.register(TrainJob, Trainer("h100", 80), label="h100", attrs={"gpu": "h100"}, requires=["gpu"])

for job_id, size_gb in [(1, 20), (2, 60), (3, 0), (4, 75), (5, 35)]:
    engine.seed(TrainJob(job_id, size_gb))
report = engine.run()

print("\nplacements:")
for job_id, gpu in report.output(Placed):
    print(f"  job {job_id} -> {gpu}")
print("\ndead-lettered jobs:", dead)
print("stats:", report)
