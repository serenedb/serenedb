---
title: "collate_tokens"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# collate_tokens

The `collate_tokens` template converts the input into a single locale-aware collation key for the configured `LOCALE`, rather than into search tokens. A collation key is a transformed form of the string whose byte order matches the locale's sorting rules, so comparing or ordering the keys yields linguistically correct results — for example placing `ä` where the locale expects it relative to `a` and `z`.

Use this template when you need locale-correct sorting or equality over an indexed column. It produces one token per value and is not intended for free-text matching.

**As a function:** `collate_tokens(value, locale := '')` — the value first, then the options in the order below. See [tokenizer functions](./index.md) for how a value, a list and a chain of calls behave.

<SqlLogicTest id="sql/functions/search/tokenizers/collate_tokens/function_form" />

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `LOCALE` | string | **required** | Locale whose collation rules produce the key (e.g., `'en_US.UTF-8'`, `'sv'`, `'zh_TW'`) |

`LOCALE` has no usable default. Omitting it, or passing an empty string, leaves the locale unset and `CREATE` fails with `collate_tokens: invalid locale`. A value that is not a valid locale fails with `Invalid locale "<value>" for option "locale"` and names the reason in the error detail — see [locales](./split_text_icu.md#locales) for what is accepted. The locale picks one of the collations of the [`COLLATE` clause](../../../expressions/collations/index.md): its language, script and region are shortened step by step until they name locale data — `de_DE.UTF-8` collates like `de`, `zh_Hant_TW` and `zh_TW` like `zh_tw` (stroke order), `sr_BA` like `sr_ba` — and a language without a tailoring of its own collates with the root rules. Alternative collation types and collation settings are not available: a locale with a `collation`, `colStrength` or other `col…` keyword (`de@collation=phonebook`) and the locales whose tailoring has no collation here (`sr_Latn`, `bs_Cyrl`, `ff_Adlm`, `kk_Arab`, `en_US_POSIX`) fail with `collate_tokens: there is no collation for locale "<name>"`.

`LOCALE` is the only option this template takes. There is no strength, case or accent setting, and any other option — `CASE`, `ACCENT`, `MIN_GRAM` — fails with `collate_tokens(): unknown option "<name>"`. Because the tokens are `BLOB`s, a `collate_tokens` stage can only be the last one of a [`pipeline`](../../../statements/create_text_search_dictionary/pipeline/index.md). The [feature flags](../../../statements/create_text_search_dictionary/index.md#feature-flags) are dictionary-level options, and all four — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — are accepted here, as long as their dependencies hold: `OFFSET` requires `POSITION`, and `POSITION` and `NORM` require `FREQUENCY`.

## Tokenization

`collate_tokens` emits exactly one token per value: the sort key for that value under the locale's collation, with the trailing `NUL` byte removed. The token type is `BLOB`, so `ts_lexize` on a collation dictionary returns `BLOB[]` with a single element, and the dictionary name has to be a constant — `ts_lexize` refuses a non-constant name for a dictionary that produces `BLOB` terms. The bytes are opaque — not human-readable, and not reversible, so the original text does not survive — but comparing two keys byte-wise reproduces the locale's sort order. Under `en_US.UTF-8` the key for `apple` is less than the key for `banana`, just as the words are.

| Input | LOCALE | Output |
|---|---|---|
| `apple` | `en_US.UTF-8` | sort key (sorts before `banana`) |
| `banana` | `en_US.UTF-8` | sort key (sorts after `apple`) |

The locale's tailoring is honored: keys built under `sv` order `å` after `z`, keys built under `en` before it, so a key is only meaningful next to keys built with the same locale. Keys order the same way as the collation of the `COLLATE` clause with the same name, and as the ICU collator of the locale did; their bytes are not the bytes ICU produced, so an index built with an earlier version has to be rebuilt. Because the collator is left at the locale's default settings, strings that differ only in case or only in accents produce different keys.

Offsets always cover the whole value: start `0`, end the value's length in bytes. Positions are implicit, so the single token of a scalar value sits at the first position. An empty value still yields one token — the locale's key for the empty string — while a `NULL` value yields none. Malformed UTF-8 is not rejected: illegal bytes are collated as `U+FFFD`.

A value is dropped, with no token emitted for it, when its sort key would reach the 32768-byte limit; the server logs `Collated token exceeds maximum allowed length of 32768 bytes`. Other values in the same batch are unaffected.

## Examples

The example below builds a collation dictionary and confirms that the key for `apple` orders before the key for `banana`:

<SqlLogicTest id="sql/functions/search/tokenizers/collate_tokens/example_001" />

## See also

- [keyword](../../../statements/create_text_search_dictionary/keyword.md) — keeps the literal value as one token for exact matching
- [`normalize_tokens`](./normalize_tokens.md) — normalizes a value for case- or accent-insensitive equality
- [CREATE TEXT SEARCH DICTIONARY](../../../statements/create_text_search_dictionary/index.md)
