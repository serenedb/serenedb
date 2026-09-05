---
title: "copy_from"
sidebar_label: Copy From
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# copy_from

The `copy_from` template derives a new dictionary from an existing one named by `FROM`, inheriting its template and all of its options, and lets you override just the ones you want to change. This avoids repeating a long definition when you need close variants — for instance copying an English `text` dictionary and overriding only `STEMMING` or the stop-word list.

Any option accepted by the source dictionary's template can be given here to override the inherited value; everything you do not mention is carried over unchanged. Because the source's template is inherited too, prefixed options reach into composed sources — `STEP2_CASE` overrides a [`pipeline`](./pipeline/index.md) step, `TOKENIZER_STOPWORDS` overrides a [`minhash`](./minhash/index.md) nested analyzer.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `FROM` | string | **required** | Name of the source dictionary to copy |
| *any option* | — | — | Override any option accepted by the source's template; unmentioned options are inherited |

## Tokenization

A copy behaves exactly like its source except where you override. Starting from an English `text` dictionary that lowercases and stems — so `running flies` indexes as `{run,fli}` — a copy that overrides only `STEMMING = false` keeps the inherited locale and lower-casing but emits the full words `{running,flies}`. The same input through the two dictionaries shows precisely what the override changed and what it left alone.

| Input | Dictionary | Tokens |
|---|---|---|
| `running flies` | source `text` (`CASE = 'lower'`, `STEMMING = true`) | `{run,fli}` |
| `running flies` | copy overriding `STEMMING = false` | `{running,flies}` |

Define a stemming English dictionary, then copy it and turn stemming off:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/copy-from/example_001" />

<SqlLogicTest id="sql/statements/create_text_search_dictionary/copy-from/example_002" />

The source stems each word to its root:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/copy-from/example_003" />

The copy inherits locale and case but keeps full words, because only `STEMMING` was overridden:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/copy-from/example_004" />

## Examples

### Override a pipeline step option

A prefixed option overrides the matching step of a copied `pipeline`, leaving the other steps intact:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/copy-from/example_005" />

### Extend a pipeline with additional steps

Naming a step number beyond the source's last step appends a new step to the copied pipeline:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/copy-from/example_006" />

### Override a nested minhash analyzer option

A `TOKENIZER_`-prefixed option reaches into the nested analyzer of a copied `minhash` dictionary:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/copy-from/example_007" />

## See also

- [`pipeline`](./pipeline/index.md) — composed source whose steps can be overridden
- [`minhash`](./minhash/index.md) — composed source whose nested analyzer can be overridden
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
