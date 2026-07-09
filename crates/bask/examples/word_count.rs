/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Canonical map -> aggregate pipeline: split documents into words (emit), count
//! words in a separate aggregation plane.
use std::collections::HashMap;

use bask::prelude::*;

struct Document {
    text: String,
}
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
    fn fold(state: &mut Self::State, word: String) {
        *state.entry(word).or_default() += 1;
    }
    fn merge(left: &mut Self::State, right: Self::State) {
        for (word, n) in right {
            *left.entry(word).or_default() += n;
        }
    }
    fn finalize(state: Self::State) -> Self::Output {
        state
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let docs = ["the quick brown fox", "the lazy dog and the fox"];

    let mut builder = Engine::builder().worker(Split).worker(Count).aggregator::<WordCount>();
    for text in docs {
        builder = builder.seed(Document { text: text.to_string() });
    }
    let report = builder.run().await?;

    let counts = report.output::<WordCount>().unwrap();
    let mut sorted: Vec<_> = counts.iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
    for (word, n) in sorted {
        println!("{n:>3}  {word}");
    }
    println!("stats: {:?}", report.stats);
    Ok(())
}
