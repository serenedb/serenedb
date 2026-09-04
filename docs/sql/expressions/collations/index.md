---
title: Collations
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

Collations provide rules for how text should be sorted or compared in the execution engine. Collations are useful for localization, as the rules for how text should be ordered are different for different languages or for different countries. These orderings are often incompatible with one another. For example, in English the letter `y` comes between `x` and `z`. However, in Lithuanian the letter `y` comes between the `i` and `j`. For that reason, different collations are supported. The user must choose which collation they want to use when performing sorting and comparison operations.

By default, the `BINARY` collation is used. That means that strings are ordered and compared based only on their binary contents. This makes sense for standard ASCII characters (i.e., the letters A-Z and numbers 0-9), but generally does not make much sense for special unicode characters. It is, however, by far the fastest method of performing ordering and comparisons. Hence it is recommended to stick with the `BINARY` collation unless required otherwise.

<DocCallout type="tip">
The `BINARY` collation is also available under the aliases `C` and `POSIX`.
</DocCallout>

## Using Collations

SereneDB ships with three built-in, region-independent collations: `NOCASE`, `NOACCENT` and `NFC`. The `NOCASE` collation compares characters as equal regardless of their casing. The `NOACCENT` collation compares characters as equal regardless of their accents. The `NFC` collation performs NFC-normalized comparisons, see [Unicode normalization](https://en.wikipedia.org/wiki/Unicode_equivalence#Normalization) for more information. In addition to these three built-ins, SereneDB also includes the region- and language-specific (ICU) collations described in [ICU Collations](#icu-collations) below.

The available collations can be listed with `PRAGMA collations`. The query below filters to a stable subset to show both the built-ins and a few ICU locales:

<SqlLogicTest id="sql/expressions/collations/index/example_015" />

<SqlLogicTest id="sql/expressions/collations/index/example_001" />

<SqlLogicTest id="sql/expressions/collations/index/example_002" />

<SqlLogicTest id="sql/expressions/collations/index/example_003" />

<SqlLogicTest id="sql/expressions/collations/index/example_004" />

Collations can be combined by chaining them using the dot operator. Note, however, that not all collations can be combined together. In general, the `NOCASE` collation can be combined with any other collator, but most other collations cannot be combined.

<SqlLogicTest id="sql/expressions/collations/index/example_005" />

<SqlLogicTest id="sql/expressions/collations/index/example_006" />

<SqlLogicTest id="sql/expressions/collations/index/example_007" />

## Default Collations

The collations we have seen so far have all been specified _per expression_. It is also possible to specify a default collator, either on the global database level or on a base table column. The `PRAGMA` `default_collation` can be used to specify the global default collator. This is the collator that will be used if no other one is specified.

<SqlLogicTest id="sql/expressions/collations/index/example_008" />

Collations can also be specified per-column when creating a table. When that column is then used in a comparison, the per-column collation is used to perform that comparison.

<SqlLogicTest id="sql/expressions/collations/index/example_009" />

<SqlLogicTest id="sql/expressions/collations/index/example_010" />

Be careful here, however, as different collations cannot be combined. This can be problematic when you want to compare columns that have a different collation specified.

<SqlLogicTest id="sql/expressions/collations/index/example_011" />

<SqlLogicTest id="sql/expressions/collations/index/example_012" />

<SqlLogicTest id="sql/expressions/collations/index/example_013" />

We need to manually overwrite the collation:

<SqlLogicTest id="sql/expressions/collations/index/example_014" />

## ICU Collations

The collations we have seen so far are not region-dependent and do not follow any specific regional rules. SereneDB also includes region- and language-specific collations powered by [ICU](https://icu.unicode.org/). These follow the ordering and comparison rules of a specific language or region.

ICU collations are named by their locale, for example `de` (German), `fr` (French) and `ja` (Japanese). Region-qualified locales are also available, such as `de_at` (German as used in Austria). The full set of available locales can be inspected with `PRAGMA collations` as shown above.

For example, the German collation orders the umlaut `ä` next to `a` rather than after `z`:

<SqlLogicTest id="sql/expressions/collations/index/example_016" />

Like the built-in collations, ICU collations can be applied per expression with the `COLLATE` operator:

<SqlLogicTest id="sql/expressions/collations/index/example_017" />

They can equally be used per column when creating a table (`s VARCHAR COLLATE DE`) and as the global `default_collation`.
