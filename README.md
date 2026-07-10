# Bask

> **B**uild T**ask**s

```mermaid
flowchart TD
    seed([Seed tasks]) --> Q[[Task Queue · bounded async]]
    Q -->|pull by type| R{Router}
    R -->|select instance · skip failed| WG
    subgraph WG [Worker group · per task type]
        direction LR
        WA[instance · proxy A]
        WB[instance · proxy B]
    end
    WG -->|emit new tasks| Q
    WG -->|contribute| A[Aggregation plane · sharded reducers]
    WG -. on error · retry on another instance .-> Q
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
    ctx.aggregate(WordCount, word.value)


@engine.aggregator
class WordCount:
    def __init__(self):
        self.counts = {}

    def fold(self, word):
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
        ctx.aggregate::<WordCount>(word.0.clone());
        Ok(())
    }
}

struct WordCount;
impl Aggregator for WordCount {
    type Input = String;
    type State = HashMap<String, u64>;
    type Output = HashMap<String, u64>;
    fn fold(state: &mut Self::State, word: String) { *state.entry(word).or_default() += 1; }
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
        .aggregator::<WordCount>()
        .seed(Document { text: "the quick brown fox the fox".into() })
        .run()
        .await?;
    println!("{:?}", report.output::<WordCount>().unwrap());
    Ok(())
}
```

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

## Acknowledgements

Developed by Wavelens GmbH. Support us by contributing.
