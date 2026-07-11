# Bask

> **B**uild T**ask**s

```mermaid
flowchart TD
    seed([Seed tasks]) --> Q[[Task Queue · bounded async]]
    Q -->|pull by type| R{Router}
    R -->|select instance · attribute-aware| WG
    subgraph WG [Worker group · per task type]
        direction LR
        WA[instance · gpu a100]
        WB[instance · gpu h100]
    end
    WG -->|emit new tasks| Q
    WG -->|route| A[Routing plane · fold · route · filter · batch]
    WG -. on error · retry same/other/attr .-> Q
    WG -. exhausted or fatal .-> DL([Dead-letter sink])
    Q -. backpressure .-> WG
    R -. queue empty and all idle .-> Z{{Quiescence}}
    A --> F[finalize]
    Z --> F
    F --> OUT([RunReport · outputs])
```

## Python

```python
from bask import Engine


class Document:
    def __init__(self, text):
        self.text = text


class Word:
    def __init__(self, value):
        self.value = value


engine = Engine()


@engine.worker(Document)
def split(doc, ctx):
    for word in doc.text.split():
        ctx.emit(Word(word.lower()))


@engine.worker(Word)
def count(word, ctx):
    ctx.route(WordCount, word.value)


# A router folds a value into state and may out.emit(task) to route, filter, or batch.
@engine.router
class WordCount:
    def __init__(self):
        self.counts = {}

    def route(self, word, out):
        self.counts[word] = self.counts.get(word, 0) + 1

    def finalize(self):
        return self.counts


engine.seed(Document("the quick brown fox the fox"))
report = engine.run()
print(report.output(WordCount))
```

## Rust

```rust
use std::collections::HashMap;
use bask::prelude::*;

struct Document { text: String }
struct Word(String);

struct Split;
#[async_trait]
impl Worker for Split {
    type Task = Document;
    async fn process(&self, doc: &Document, ctx: &Context) -> anyhow::Result<()> {
        for word in doc.text.split_whitespace() {
            ctx.emit(Word(word.to_lowercase())).await?;
        }
        Ok(())
    }
}

struct Count;
#[async_trait]
impl Worker for Count {
    type Task = Word;
    async fn process(&self, word: &Word, ctx: &Context) -> anyhow::Result<()> {
        ctx.route::<WordCount>(word.0.clone()).await?;
        Ok(())
    }
}

// A Router folds a task stream into state and may emit, route, filter, or batch
// derived tasks. Emit nothing (as here) and it is a pure reducer.
struct WordCount;
impl Router for WordCount {
    type Input = String;
    type State = HashMap<String, u64>;
    type Output = HashMap<String, u64>;
    fn route(state: &mut Self::State, word: String, _out: &mut Emit) {
        *state.entry(word).or_default() += 1;
    }
    fn merge(left: &mut Self::State, right: Self::State) {
        for (word, n) in right { *left.entry(word).or_default() += n; }
    }
    fn finalize(state: Self::State) -> Self::Output { state }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let report = Engine::builder()
        .worker(Split)
        .worker(Count)
        .router::<WordCount>()
        .seed(Document { text: "the quick brown fox the fox".into() })
        .run()
        .await?;
    println!("{:?}", report.output::<WordCount>().unwrap());
    Ok(())
}
```

## Resource-aware retry

Instances self-describe with attributes and may draw from named resource pools; a worker
steers its own retry by attaching a hint to the error, and terminally failed tasks go to a
dead-letter sink. Defaults need no configuration.

```rust
Engine::builder()
    .resource("gpu", 2) // a pool of 2 permits shared across instances
    .worker_cfg(Trainer { vram_gb: 40 }, WorkerCfg::new().attr("gpu", "a100").requires("gpu"))
    .worker_cfg(Trainer { vram_gb: 80 }, WorkerCfg::new().attr("gpu", "h100").requires("gpu"))
    .retry(RetryPolicy::new().max_attempts(2).jitter(0.2))
    .dead_letter(|dl| eprintln!("dropped {}: {}", dl.task_type, dl.error))
    .seed(TrainJob { size_gb: 60 })
    .run()
    .await?;

// inside Trainer::process, when the job will not fit this gpu:
return Err(anyhow::anyhow!("out of memory")).retry_on(RetryOn::DifferentAttr("gpu".into()));
```

The retry lands least-loaded on an instance whose `gpu` attribute differs from the one that
failed (here the h100). Hints are `SameInstance`, `DifferentInstance`, `DifferentAttr`,
`AnyWith(predicate)`, and `Fatal`; a policy attaches per worker with `WorkerCfg::retry`. In
Python, raise `different_attr("gpu")` or `Fatal(...)` and pass `resources=` and `dead_letter=`
to `Engine`.

## IO plane

`Source` and `Sink` are generic over the item type and selected by file extension or
URI scheme from a registry, so adding a format is one trait impl plus a registration,
never a core change. A `SourceWorker` streams a source into the pipeline under the same
backpressure as any worker; a `SinkWorker` drains items out and finalizes on shutdown.

```rust
use arrow::record_batch::RecordBatch;
use bask::io::{Read, SinkRegistry, SinkWorker, SourceRegistry, SourceWorker};
use bask::prelude::*;

// csv -> parquet: the format on each side is chosen from its extension.
let sinks = SinkRegistry::<RecordBatch>::formats();
Engine::builder()
    .worker(SourceWorker::new(SourceRegistry::<RecordBatch>::formats()))
    .worker_cfg(SinkWorker::open(&sinks, "out.parquet")?, WorkerCfg::new().concurrency(1))
    .seed(Read::<RecordBatch>::new("in.csv"))
    .run()
    .await?;
```

Built-ins are feature-gated: `formats` (arrow, parquet, csv, jsonl and a row-rotating
sink), `download` (resumable HTTP fetch), `object-store` (S3/GCS/Azure), `postgres`
(`COPY` bulk-load). The byte plane (`SourceRegistry::<Bytes>::blobs()`) reads a file or
directory tree and writes blobs back out, and composes the network sources when their
features are on.

## Predefined tasks

`bask::tasks` ships reusable stages built on the engine: `Chunker::<N>` splits a record
batch into fixed-row pieces, and `RowBatch::<N>` is a router that re-aggregates a batch
stream into groups of at least `N` rows, flushing the trailing group at end-of-run.

```rust
use bask::tasks::{Chunker, RowBatch};

Engine::builder()
    .worker(Chunker::<32>)          // split each Whole into 32-row Pieces
    .router::<RowBatch<100>>()      // re-batch Pieces into >=100-row groups
    // ...feed pieces with ctx.route::<RowBatch<100>>(piece) and consume the groups
    .run()
    .await?;
```

From Python the same Rust splitter runs over pyarrow batches; subclass `bask.tasks.Batch`
for the routing types and wire it with `engine.chunker`:

```python
import pyarrow as pa
from bask import Engine
from bask.tasks import Batch

class Whole(Batch): pass
class Piece(Batch): pass

engine = Engine()
engine.chunker(Whole, Piece, rows=8192)   # Rust bask_tasks::chunk over pyarrow data

@engine.worker(Piece)
def handle(piece, ctx):
    ...  # piece.batch is a pyarrow RecordBatch of at most 8192 rows

engine.seed(Whole(pa.record_batch({"n": list(range(1_000_000))})))
engine.run()
```

`engine.row_batch(key, group_cls, rows)` registers the row-count aggregator the same way:
feed it with `ctx.route(key, batch)` and it emits `group_cls(batch)` per group.

## Checkpoints

Durability is opt-in and defined on the task: mark a type a checkpoint and it becomes a
restore point. On arrival its payload is materialized to a store (default `bask.sqlite`,
created lazily) and the source rows it covers are recorded, so a re-run skips finished
items, prunes a fully-covered source (no re-read), and reseeds anything not yet consumed.
A pipeline with no checkpoint type is byte-for-byte the in-memory engine.

```rust
use bask::prelude::*;
use bask::Checkpoint;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Checkpoint)]  // `#[checkpoint(key_only)]` stores just the key
struct Saved { #[key] id: String, body: String }

Engine::builder()
    .worker(Fetch)
    .source("feed", Feed)               // its rows are stamped with ctx.emit_keyed(ordinal, ..)
    .run()                              // .store(..) optional; defaults to ./bask.sqlite
    .await?;
```

Provenance rides the data: a source stamps each row, workers inherit their parent's rows,
and routers union the rows folded since their last emit, so a checkpoint traces back to
exactly the source rows it covers. From Python, subclass `bask.tasks.Checkpoint`:

```python
from bask import Engine
from bask.tasks import Checkpoint

class Saved(Checkpoint):
    def __init__(self, id, body): self.id, self.body = id, body
    def key(self): return str(self.id)

engine = Engine(store="bask.sqlite")     # default when a checkpoint participates
engine.source(Feed(), "feed")            # a source worker calls ctx.emit_keyed(ordinal, task)
```

## Crates

You depend only on `bask`; it re-exports the engine at the crate root and the rest behind
features. The internals are separate crates so the engine stays dependency-light.

| crate          | contents                                          | reached via                     |
|----------------|---------------------------------------------------|---------------------------------|
| `bask-core`    | engine: workers, routers, scheduler, retry        | `bask` root and `bask::prelude` |
| `bask-macros`  | `#[derive(Checkpoint)]`                            | `bask::Checkpoint`              |
| `bask-io`      | pluggable source/sink IO plane                    | `bask::io` (feature `io`)       |
| `bask-formats` | Arrow/Parquet/CSV/JSONL and record IO             | `bask::formats` (feature `formats`) |
| `bask-tasks`   | predefined workers and routers (chunk, row-batch) | `bask::tasks` (feature `formats`) |

## Acknowledgements

Developed by Wavelens GmbH. Support us by contributing.
