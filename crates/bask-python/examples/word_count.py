# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
#
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Canonical map -> reduce pipeline: split documents into words (emit), count words
in a separate routing plane."""
from bask import Engine, Worker


class Document:
    def __init__(self, text):
        self.text = text


class Word:
    def __init__(self, value):
        self.value = value


engine = Engine()


@engine.worker(Document)
class Split(Worker):
    def process(self, doc, ctx):
        for word in doc.text.split():
            ctx.emit(Word(word.lower()))


@engine.worker(Word)
class Count(Worker):
    def process(self, word, ctx):
        ctx.route(WordCount, word.value)


@engine.router
class WordCount:
    def __init__(self):
        self.counts = {}

    def route(self, word, out):
        self.counts[word] = self.counts.get(word, 0) + 1

    def finalize(self):
        return self.counts


for text in ["the quick brown fox", "the lazy dog and the fox"]:
    engine.seed(Document(text))

report = engine.run()

for word, n in sorted(report.output(WordCount).items(), key=lambda kv: (-kv[1], kv[0])):
    print(f"{n:>3}  {word}")
print("stats:", report)
