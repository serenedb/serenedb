---
title: "collation"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# collation

The `collation` template converts the input into a single locale-aware collation key for the configured `LOCALE`, rather than into search tokens. A collation key is a transformed form of the string whose byte order matches the locale's sorting rules, so comparing or ordering the keys yields linguistically correct results — for example placing `ä` where the locale expects it relative to `a` and `z`.

Use this template when you need locale-correct sorting or equality over an indexed column. It produces one token per value and is not intended for free-text matching.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `LOCALE` | string | **required** | ICU locale whose collation rules produce the key |

`LOCALE` has no usable default. Omitting it, or passing an empty string, leaves the locale unset and `CREATE` fails with `collation: invalid locale`. A string ICU cannot parse at all fails earlier with `Invalid locale "<value>" for option "locale"`, and a locale ICU parses but cannot open a collator for fails with `collation: failed to create collator for the locale`. Missing collation data is not such a failure: ICU falls back to the closest available collator — the root rules in the worst case — so the locale is accepted and its keys follow that fallback. Collation keywords are part of the locale name: `de@collation=phonebook`, `de_phonebook` and `de__phonebook` all select the German phonebook tailoring, and longer spellings such as `de_DE.UTF-8@collation=phonebook` and `de_DE.utf-8@phonebook` are accepted too.

`LOCALE` is the only option this template takes. There is no strength, case or accent setting, and any other option — `CASE`, `ACCENT`, `MINGRAM` — fails with `option "<name>" is not applicable in this context`. Inside a [`pipeline`](./pipeline/index.md) the option is spelled `STEP⟨N⟩_LOCALE`, and because the tokens are `BLOB`s a `collation` step can only be the last one. The [feature flags](./index.md#feature-flags) are dictionary-level options, and all four — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — are accepted here, as long as their dependencies hold: `OFFSET` requires `POSITION`, and `POSITION` and `NORM` require `FREQUENCY`.

## Tokenization

`collation` emits exactly one token per value: the ICU sort key for that value under the locale's collator, with the trailing `NUL` byte removed. The token type is `BLOB`, so `ts_lexize` on a collation dictionary returns `BLOB[]` with a single element, and the dictionary name has to be a constant — `ts_lexize` refuses a non-constant name for a dictionary that produces `BLOB` terms. The bytes are opaque — not human-readable, and not reversible, so the original text does not survive — but comparing two keys byte-wise reproduces the locale's sort order. Under `en_US.UTF-8` the key for `apple` is less than the key for `banana`, just as the words are.

| Input | LOCALE | Output |
|---|---|---|
| `apple` | `en_US.UTF-8` | sort key (sorts before `banana`) |
| `banana` | `en_US.UTF-8` | sort key (sorts after `apple`) |

The locale's tailoring is honored, collation keywords included: keys built under `sv` or under `de@collation=phonebook` order differently from keys built under `en`, so a key is only meaningful next to keys built with the same locale. Because the collator is left at the locale's default settings, strings that differ only in case or only in accents produce different keys.

Offsets always cover the whole value: start `0`, end the value's length in bytes. Positions are implicit, so the single token of a scalar value sits at the first position. An empty value still yields one token — the locale's key for the empty string — while a `NULL` value yields none. Malformed UTF-8 is not rejected: illegal sequences are replaced with `U+FFFD` before collation.

A value is dropped, with no token emitted for it, when its sort key would reach the 32768-byte limit; the server logs `Collated token exceeds maximum allowed length of 32768 bytes`. Other values in the same batch are unaffected.

## Examples

The example below builds a collation dictionary and confirms that the key for `apple` orders before the key for `banana`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/collation/example_001" />

## See also

- [keyword](./keyword.md) — keeps the literal value as one token for exact matching
- [norm](./norm.md) — normalizes a value for case- or accent-insensitive equality
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
