# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Canonical map -> aggregate pipeline: split documents into words (emit),
count words in a separate aggregation plane."""
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


for text in ["the quick brown fox", "the lazy dog and the fox"]:
    engine.seed(Document(text))

report = engine.run()

for word, n in sorted(report.output(WordCount).items(), key=lambda kv: (-kv[1], kv[0])):
    print(f"{n:>3}  {word}")
print("stats:", report)
