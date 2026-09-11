---
title: "copy_from"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# copy_from

The `copy_from` template derives a new dictionary from an existing one named by `FROM`, inheriting its template and all of its options, and lets you override just the ones you want to change. This avoids repeating a long definition when you need close variants — for instance copying an English `text` dictionary and overriding only `STEMMING` or the stop-word list.

Any option accepted by the source dictionary's template can be given here to override the inherited value; everything you do not mention is carried over unchanged. An option that is required when its template is used directly — `DELIMITER` for [`delimiter`](./delimiter.md), `PATTERN` for [`pattern`](./pattern.md), `EXPRESSION` for [`sql`](./sql.md) — therefore need not be repeated, because the inherited value stands. An option the source's template does not accept is rejected when the copy is created: a name that belongs to another template fails with `option "<name>" is not applicable in this context`, and any other name fails as an unrecognized option.

Because the source's template is inherited too, prefixed options reach into composed sources: `STEP⟨N⟩_*` overrides the Nth step of a copied [`pipeline`](./pipeline/index.md), `TOKENIZER⟨N⟩_*` the Nth member of a copied [`union`](./union.md), and `TOKENIZER_*` the nested analyzer of a copied [`wildcard`](./wildcard.md) or [`shingle`](./shingle.md). Naming a numbered child's own template is the exception to inheritance: a `STEP⟨N⟩_TEMPLATE` or `TOKENIZER⟨N⟩_TEMPLATE` at a position the source already fills replaces that child rather than adjusting it, so the child is built from the options you give and its template's defaults. A single nested analyzer behaves differently: repeating `TOKENIZER_TEMPLATE` with the template the source's child already uses keeps that child's inherited options, and only a different template starts it from defaults. A child whose own template is `copy_from` is built from the dictionary its own `FROM` names rather than from defaults.

A copy cannot change the source's template — the template of the dictionary named by `FROM` is what gets built. `FROM` takes a schema-qualified name; an unqualified one resolves against the current schema. If no such dictionary exists, the statement fails with `text search dictionary "<name>" does not exist`. The source is read while the copy is created and the resulting configuration is stored in the new dictionary, so nothing links the two afterwards.

`copy_from` is available at every level, not only at the root. A [`pipeline`](./pipeline/index.md) step or a [`union`](./union.md) member can name it as `STEP⟨N⟩_TEMPLATE = 'copy_from'` with `STEP⟨N⟩_FROM`, or `TOKENIZER⟨N⟩_TEMPLATE = 'copy_from'` with `TOKENIZER⟨N⟩_FROM`, and the nested analyzer of a [`wildcard`](./wildcard.md) or [`shingle`](./shingle.md) as `TOKENIZER_TEMPLATE = 'copy_from'` with `TOKENIZER_FROM`. Options spelled at that same prefix override what the nested copy inherits, exactly as they do at the root.

Index features are not inherited. `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` are read only from the copying statement's own root options, so a copy of a dictionary created with `FREQUENCY = true` records no features unless you repeat them, and a prefixed spelling such as `STEP1_FREQUENCY` is not recognized. Which of the four [feature flags](./index.md#feature-flags) are accepted is decided by the source's template, not by `copy_from`.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `FROM` | string | **required** | Source dictionary to copy, optionally schema-qualified as `schema.name` |
| *any option of the source's template* | — | — | Overrides the inherited value; unmentioned options are inherited |

## Tokenization

A copy tokenizes exactly like its source except where you override. Starting from an English `text` dictionary that lowercases and stems — so `running flies` indexes as `{run,fli}` — a copy that overrides only `STEMMING = false` keeps the inherited locale and lower-casing but emits the full words `{running,flies}`. The same input through the two dictionaries shows precisely what the override changed and what it left alone.

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

Steps stay dense: the first number with neither an inherited step nor a `STEP⟨N⟩_TEMPLATE` of its own ends the pipeline. Naming the step number just past the source's last step therefore appends a new step to the copy:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/copy-from/example_006" />

## See also

- [`pipeline`](./pipeline/index.md) — composed source whose steps can be overridden
- [`union`](./union.md) — composed source whose members can be overridden
- [`wildcard`](./wildcard.md) — composed source whose nested analyzer can be overridden
- [`shingle`](./shingle.md) — composed source whose nested analyzer can be overridden
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
