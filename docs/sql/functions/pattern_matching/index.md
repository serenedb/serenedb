---
title: Pattern Matching
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

There are four separate approaches to pattern matching provided by SereneDB:
the traditional SQL [`LIKE` operator](#like),
the more recent [`SIMILAR TO` operator](#similar-to) (added in SQL:1999),
a [`GLOB` operator](#glob),
and POSIX-style [regular expressions](#regular-expressions).

## `LIKE`

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

The `LIKE` expression returns `true` if the string matches the supplied pattern. (As expected, the `NOT LIKE` expression returns `false` if `LIKE` returns `true`, and vice versa. An equivalent expression is `NOT (string LIKE pattern)`.)

If pattern does not contain percent signs or underscores, then the pattern only represents the string itself; in that case `LIKE` acts like the equals operator. An underscore (`_`) in pattern stands for (matches) any single character; a percent sign (`%`) matches any sequence of zero or more characters.

`LIKE` pattern matching always covers the entire string. Therefore, if it's desired to match a sequence anywhere within a string, the pattern must start and end with a percent sign.

Some examples:

<SqlLogicTest id="sql/functions/pattern_matching/index/example_001" />

The keyword `ILIKE` can be used instead of `LIKE` to make the match case-insensitive according to the active locale:

<SqlLogicTest id="sql/functions/pattern_matching/index/example_002" />

<SqlLogicTest id="sql/functions/pattern_matching/index/example_003" />

To search within a string for a character that is a wildcard (`%` or `_`), the pattern must use an `ESCAPE` clause and an escape character to indicate the wildcard should be treated as a literal character instead of a wildcard. See an example below.

Additionally, the function `like_escape` has the same functionality as a `LIKE` expression with an `ESCAPE` clause, but using function syntax. See the [Text Functions page](../../../sql/functions/text.md) for details.

Search for strings with 'a' then a literal percent sign then 'c':

<SqlLogicTest id="sql/functions/pattern_matching/index/example_004" />

Case-insensitive `ILIKE` with `ESCAPE`:

<SqlLogicTest id="sql/functions/pattern_matching/index/example_005" />

There are also alternative characters that can be used as keywords in place of `LIKE` expressions. These enhance PostgreSQL compatibility.

<div class="monospace_table"></div>

| PostgreSQL-style | `LIKE`-style |
| :--------------- | :----------- |
| `~~`             | `LIKE`       |
| `!~~`            | `NOT LIKE`   |
| `~~*`            | `ILIKE`      |
| `!~~*`           | `NOT ILIKE`  |

## `SIMILAR TO`

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />

The `SIMILAR TO` operator returns true or false depending on whether its pattern matches the given string. It is similar to `LIKE`, except that it interprets the pattern using a [regular expression](../../../sql/functions/regular_expressions.md). Like `LIKE`, the `SIMILAR TO` operator succeeds only if its pattern matches the entire string; this is unlike common regular expression behavior where the pattern can match any part of the string.

A regular expression is a character sequence that is an abbreviated definition of a set of strings (a regular set). A string is said to match a regular expression if it is a member of the regular set described by the regular expression. As with `LIKE`, pattern characters match string characters exactly unless they are special characters in the regular expression language — but regular expressions use different special characters than `LIKE` does.

Some examples:

<SqlLogicTest id="sql/functions/pattern_matching/index/example_006" />

<DocCallout type="tip">

SereneDB's POSIX regular-expression match operators (`~`, `~*`, `!~`, `!~*`) follow PostgreSQL's partial-match semantics.

</DocCallout>

## Globbing

SereneDB supports file name expansion, also known as globbing, for discovering files.
SereneDB's glob syntax uses the question mark (`?`) wildcard to match any single character and the asterisk (`*`) to match zero or more characters.
In addition, you can use the bracket syntax (`[...]`) to match any single character contained within the brackets, or within the character range specified by the brackets.
An exclamation mark (`!`) may be used inside the first bracket to search for a character that is not contained within the brackets.
To learn more, visit the [“glob (programming)” Wikipedia page](https://en.wikipedia.org/wiki/Glob_%28programming%29).

### `GLOB`

<RailroadDiagram source={RailroadSource} production="rrdiagram3" />

The `GLOB` operator returns `true` or `false` if the string matches the `GLOB` pattern. The `GLOB` operator is most commonly used when searching for filenames that follow a specific pattern (for example a specific file extension).

Some examples:

<SqlLogicTest id="sql/functions/pattern_matching/index/example_007" />

The bracket syntax is case-sensitive:

<SqlLogicTest id="sql/functions/pattern_matching/index/example_008" />

The `!` applies to all characters within the brackets:

<SqlLogicTest id="sql/functions/pattern_matching/index/example_009" />

To negate a GLOB operator, negate the entire expression:

<SqlLogicTest id="sql/functions/pattern_matching/index/example_010" />

Three tildes (`~~~`) may also be used in place of the `GLOB` keyword.

| GLOB-style | Symbolic-style |
| :--------- | :------------- |
| `GLOB`     | `~~~`          |

### Glob Function to Find Filenames

The glob pattern matching syntax can also be used to search for filenames using the `glob` table function.
It accepts one parameter: the path to search (which may include glob patterns).

Search the current directory for all files:

<SqlLogicTest id="sql/functions/pattern_matching/index/example_011" />

### Globbing Semantics

SereneDB's globbing implementation follows the semantics of [Python's `glob`](https://docs.python.org/3/library/glob.html) and not the `glob` used in the shell.
A notable difference is the behavior of the `**/` construct: `**/⟨filename⟩`{:.language-sql .highlight} will not return a file with `⟨filename⟩`{:.language-sql .highlight} in top-level directory.
For example, with a `README.md` file present in the directory, the following query finds it:

<SqlLogicTest id="sql/functions/pattern_matching/index/example_012" />

However, the following query returns an empty result:

<SqlLogicTest id="sql/functions/pattern_matching/index/example_013" />

Meanwhile, the globbing of Bash, Zsh, etc. finds the file using the same syntax:

```batch
ls **/README.md
```

```text
README.md
```

## Regular Expressions

SereneDB's regular expression support is documented on the [Regular Expressions page](../../../sql/functions/regular_expressions.md).
SereneDB supports some PostgreSQL-style operators for regular expression matching:

| PostgreSQL-style | Equivalent expression                                                                     |
| :--------------- | :---------------------------------------------------------------------------------------- |
| `~`              | [`regexp_full_match`](../../../sql/functions/text.md#regexp_full_matchstring-regex-col2)       |
| `!~`             | `NOT` [`regexp_full_match`](../../../sql/functions/text.md#regexp_full_matchstring-regex-col2) |
| `~*`             | (not supported)                                                                           |
| `!~*`            | (not supported)                                                                           |
