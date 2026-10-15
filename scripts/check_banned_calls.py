#!/usr/bin/env python3
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fix_enum_trailing_comma import blanked  # noqa: E402

RANGES_WITH_ABSL = (
    "adjacent_find", "all_of", "any_of", "binary_search", "contains",
    "contains_subrange", "copy", "copy_backward", "copy_if", "copy_n", "count",
    "count_if", "distance", "equal", "equal_range", "fill", "fill_n", "find",
    "find_end", "find_first_of", "find_if", "find_if_not", "for_each",
    "generate", "generate_n", "includes", "inplace_merge", "is_heap",
    "is_heap_until", "is_partitioned", "is_permutation", "is_sorted",
    "is_sorted_until", "lexicographical_compare", "lower_bound", "make_heap",
    "max_element", "merge", "min_element", "minmax_element", "mismatch", "move",
    "move_backward", "next_permutation", "none_of", "nth_element",
    "partial_sort", "partial_sort_copy", "partition", "partition_copy",
    "partition_point", "pop_heap", "prev_permutation", "push_heap",
    "remove_copy", "remove_copy_if", "replace", "replace_copy",
    "replace_copy_if", "replace_if", "reverse", "reverse_copy", "rotate",
    "rotate_copy", "sample", "search", "search_n", "set_difference",
    "set_intersection", "set_symmetric_difference", "set_union", "shuffle",
    "sort", "sort_heap", "stable_partition", "stable_sort", "swap_ranges",
    "transform", "unique_copy", "upper_bound",
)

BANNED = [
    (
        re.compile(r"\bstd::ranges::(" + "|".join(RANGES_WITH_ABSL) + r")\s*\("),
        "absl::c_{name} (a projection becomes a lambda)",
    ),
    (
        re.compile(r"\bstd::(unordered_(?:map|set|multimap|multiset))\b"),
        "irs::containers::FlatHashMap, FlatHashSet, NodeHashMap or NodeHashSet",
    ),
    (
        re.compile(r"(?<![\w:])(?:std::)?(v?(?:f|s|sn)?printf)\s*\("),
        "absl::StrFormat, absl::StrAppendFormat, absl::FPrintF or absl::SNPrintF",
    ),
    (
        re.compile(r"\bstd::(v?format(?:_to(?:_n)?)?|print(?:ln)?)\s*\("),
        "absl::StrFormat or absl::StrAppendFormat (fmt only where neither fits)",
    ),
    (
        re.compile(
            r"(?<![\w:])(?:std::)?(sto(?:i|l|ll|ul|ull|f|d|ld)|from_chars"
            r"|ato(?:i|l|ll|f)|strto(?:l|ll|ul|ull|f|d|ld))\s*\("
        ),
        "fast_float::from_chars",
    ),
]

EXCEPTIONS = {
    "iresearch/utils/crash_handler.cpp": {"snprintf"},
}


def check_file(path: str) -> list[str]:
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            content = f.read()
    except OSError as e:
        return [f"cannot read: {e}"]

    sh = blanked(content)
    allowed = EXCEPTIONS.get(path, set())
    errors = []
    for regex, instead in BANNED:
        for m in regex.finditer(sh):
            if m.group(1) in allowed:
                continue
            line = sh.count("\n", 0, m.start()) + 1
            errors.append(
                f"line {line}: use {instead.format(name=m.group(1))} "
                f"instead of {m.group(1)}"
            )
    return errors


failed = 0
for path in sys.argv[1:]:
    errors = check_file(path)
    for e in errors:
        print(f"{path}: {e}", file=sys.stderr)
    if errors:
        failed += 1

sys.exit(1 if failed else 0)
