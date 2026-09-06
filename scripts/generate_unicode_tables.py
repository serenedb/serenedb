#!/usr/bin/env python3
"""Generate UAX#29 WordBreak property and simple case mapping tables.

Emits two checked-in headers consumed by the segmentation tokenizer:

  libs/iresearch/include/iresearch/analysis/text/words/tables.hpp
    WbProp enum (19 classes), Extended_Pictographic flag, and a shift-8
    two-level lookup table (kWbStage1 u16 block ids over cp>>8, kWbStage2
    deduplicated 256-byte blocks). WbLookup(cp) is two dependent loads.

  libs/iresearch/include/iresearch/utils/utf8_case_tables.hpp
    Sorted {cp, to} pairs for simple (1:1, context-free) lower/upper case
    mappings from UnicodeData.txt fields 13/12, plus the generator-verified
    kSimpleCaseMaxUtf8Growth bound (UTF-8 byte growth per mapped codepoint).

Inputs are UCD data files (not vendored; fetch or point at a checkout):
  WordBreakProperty.txt (auxiliary/), emoji-data.txt (emoji/), UnicodeData.txt

Usage:
  python3 scripts/generate_unicode_tables.py --ucd-dir <dir> [--repo-root <dir>]
  python3 scripts/generate_unicode_tables.py --download [--repo-root <dir>]

The class/DFA design is derived from turbopuffer/alyze (MIT) and the table
compression recipe follows StringZilla's utf8_wordbreaks generator (Apache-2.0);
see the attribution note in the emitted headers.
"""

import argparse
import os
import re
import sys
import urllib.request

UCD_VERSION_FALLBACK = "17.0.0"
UCD_BASE = "https://www.unicode.org/Public/{v}/ucd"

WB_PROPS = {
    "Other": 0,
    "CR": 1,
    "LF": 2,
    "Newline": 3,
    "Extend": 4,
    "ZWJ": 5,
    "Format": 6,
    "Regional_Indicator": 7,
    "Katakana": 8,
    "Hebrew_Letter": 9,
    "ALetter": 10,
    "Single_Quote": 11,
    "Double_Quote": 12,
    "MidNumLet": 13,
    "MidLetter": 14,
    "MidNum": 15,
    "Numeric": 16,
    "ExtendNumLet": 17,
    "WSegSpace": 18,
}

WB_ENUM_NAMES = [
    "kOther", "kCR", "kLF", "kNewline", "kExtend", "kZWJ", "kFormat", "kRI",
    "kKatakana", "kHebrew", "kALetter", "kSingleQuote", "kDoubleQuote",
    "kMidNumLet", "kMidLetter", "kMidNum", "kNumeric", "kExtendNumLet",
    "kWSegSpace",
]

EXT_PICT_FLAG = 0x80
NUM_CODEPOINTS = 0x110000

RANGE_RE = re.compile(r"^([0-9A-F]+)(?:\.\.([0-9A-F]+))?\s*;\s*([\w]+)")

LICENSE_HEADER = """\
////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////
"""


def parse_ucd_version(path):
    with open(path, encoding="utf-8") as f:
        first = f.readline()
    m = re.search(r"-(\d+\.\d+\.\d+)\.txt", first)
    return m.group(1) if m else UCD_VERSION_FALLBACK


def parse_ranges(path, wanted):
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            m = RANGE_RE.match(line)
            if not m:
                continue
            prop = m.group(3)
            if prop not in wanted:
                continue
            lo = int(m.group(1), 16)
            hi = int(m.group(2), 16) if m.group(2) else lo
            out.append((lo, hi, prop))
    return out


def build_wb_props(ucd_dir):
    props = bytearray(NUM_CODEPOINTS)
    for lo, hi, prop in parse_ranges(
            os.path.join(ucd_dir, "WordBreakProperty.txt"), WB_PROPS):
        for cp in range(lo, hi + 1):
            props[cp] = WB_PROPS[prop]
    for lo, hi, _ in parse_ranges(
            os.path.join(ucd_dir, "emoji-data.txt"),
            {"Extended_Pictographic"}):
        for cp in range(lo, hi + 1):
            props[cp] |= EXT_PICT_FLAG
    return props


def dedup_blocks(props):
    stage1 = []
    stage2 = []
    seen = {}
    for block_index in range(NUM_CODEPOINTS >> 8):
        block = bytes(props[block_index << 8:(block_index + 1) << 8])
        if block not in seen:
            seen[block] = len(stage2)
            stage2.append(block)
        stage1.append(seen[block])
    assert len(stage2) < 0xFFFF, "stage1 must fit uint16_t"
    return stage1, stage2


def parse_simple_case(ucd_dir):
    lower = []
    upper = []
    with open(os.path.join(ucd_dir, "UnicodeData.txt"), encoding="utf-8") as f:
        for line in f:
            fields = line.rstrip("\n").split(";")
            if len(fields) < 15:
                continue
            cp = int(fields[0], 16)
            name = fields[1]
            if name.endswith("First>") or name.endswith("Last>"):
                assert not fields[12] and not fields[13], \
                    "range rows must not carry case mappings"
                continue
            if fields[12]:
                to = int(fields[12], 16)
                if to != cp:
                    upper.append((cp, to))
            if fields[13]:
                to = int(fields[13], 16)
                if to != cp:
                    lower.append((cp, to))
    lower.sort()
    upper.sort()
    return lower, upper


def utf8_len(cp):
    if cp < 0x80:
        return 1
    if cp < 0x800:
        return 2
    if cp < 0x10000:
        return 3
    return 4


def max_growth(*tables):
    growth = 0
    for table in tables:
        for cp, to in table:
            growth = max(growth, utf8_len(to) - utf8_len(cp))
    return growth


def fmt_rows(values, fmt, per_line, indent="  "):
    lines = []
    for i in range(0, len(values), per_line):
        lines.append(indent + " ".join(fmt(v) for v in values[i:i + per_line]))
    return "\n".join(lines)


def provenance(ucd_version):
    return (
        "// Generated by scripts/generate_unicode_tables.py from UCD "
        f"{ucd_version}. DO NOT EDIT.\n"
        "// Class set and table recipe derived from turbopuffer/alyze (MIT)\n"
        "// and StringZilla utf8_wordbreaks (Apache-2.0).\n")


def emit_word_break_tables(path, stage1, stage2, ucd_version):
    flat2 = [b for block in stage2 for b in block]
    enum_body = fmt_rows(
        [f"{name} = {i}," for i, name in enumerate(WB_ENUM_NAMES)],
        lambda v: v, 4)
    out = [LICENSE_HEADER]
    out.append(provenance(ucd_version))
    out.append("#pragma once\n")
    out.append("#include <absl/base/optimization.h>\n")
    out.append("#include <array>\n#include <cstdint>\n")
    out.append('#include "basics/shared.hpp"\n')
    out.append("namespace irs::analysis::words {\n")
    out.append(f"enum WbProp : uint8_t {{\n{enum_body}\n}};\n")
    out.append(f"inline constexpr uint8_t kWbPropMask = 0x1F;\n"
               f"inline constexpr uint8_t kWbExtPictFlag = 0x80;\n"
               f"inline constexpr size_t kWbPropCount = {len(WB_ENUM_NAMES)};\n")
    out.append("// clang-format off")
    out.append(
        f"ABSL_CACHELINE_ALIGNED inline constexpr std::array<uint16_t, "
        f"{len(stage1)}> kWbStage1{{{{")
    out.append(fmt_rows(stage1, lambda v: f"{v},", 16))
    out.append("}};\n")
    out.append(
        f"ABSL_CACHELINE_ALIGNED inline constexpr std::array<uint8_t, "
        f"{len(flat2)}> kWbStage2{{{{")
    out.append(fmt_rows(flat2, lambda v: f"0x{v:02x},", 12))
    out.append("}};")
    out.append("// clang-format on\n")
    out.append(
        "IRS_FORCE_INLINE constexpr uint8_t WbLookup(uint32_t cp) noexcept {\n"
        "  return kWbStage2[size_t{kWbStage1[cp >> 8]} * 256 + (cp & 0xFF)];\n"
        "}\n")
    out.append("}  // namespace irs::analysis::words")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    return len(stage1) * 2 + len(flat2)


def emit_case_tables(path, lower, upper, growth, ucd_version):
    out = [LICENSE_HEADER]
    out.append(provenance(ucd_version))
    out.append("#pragma once\n")
    out.append("#include <absl/base/optimization.h>\n")
    out.append("#include <array>\n#include <cstdint>\n")
    out.append('#include "basics/shared.hpp"\n')
    out.append("namespace irs::utf8_utils {\n")
    out.append(
        "struct CaseMap {\n"
        "  uint32_t cp;\n"
        "  uint32_t to;\n\n"
        "  IRS_FORCE_INLINE bool operator==(CaseMap other) const noexcept {\n"
        "    return cp == other.cp;\n"
        "  }\n\n"
        "  IRS_FORCE_INLINE auto operator<=>(CaseMap other) const noexcept {\n"
        "    return cp <=> other.cp;\n"
        "  }\n"
        "};\n")
    out.append(
        f"inline constexpr uint32_t kSimpleCaseMaxUtf8Growth = {growth};\n")
    out.append("// clang-format off")
    for name, table in (("kSimpleLowerTable", lower),
                        ("kSimpleUpperTable", upper)):
        out.append(
            f"ABSL_CACHELINE_ALIGNED inline constexpr std::array<CaseMap, "
            f"{len(table)}> {name}{{{{CaseMap")
        out.append(fmt_rows(
            table, lambda v: f"{{0x{v[0]:x}, 0x{v[1]:x}}},", 4))
        out.append("}};\n")
    out.append("// clang-format on\n")
    out.append("}  // namespace irs::utf8_utils")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")


def download_ucd(dest):
    os.makedirs(dest, exist_ok=True)
    base = UCD_BASE.format(v=UCD_VERSION_FALLBACK)
    for sub, name in (("auxiliary", "WordBreakProperty.txt"),
                      ("emoji", "emoji-data.txt"), ("", "UnicodeData.txt")):
        url = "/".join(p for p in (base, sub, name) if p)
        path = os.path.join(dest, name)
        if not os.path.exists(path):
            print(f"fetching {url}")
            urllib.request.urlretrieve(url, path)
    return dest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ucd-dir")
    parser.add_argument("--download", action="store_true")
    parser.add_argument(
        "--repo-root",
        default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    args = parser.parse_args()

    if not args.ucd_dir and not args.download:
        parser.error("pass --ucd-dir or --download")
    ucd_dir = args.ucd_dir or download_ucd(
        os.path.join(args.repo_root, "build", "ucd"))

    ucd_version = parse_ucd_version(
        os.path.join(ucd_dir, "WordBreakProperty.txt"))
    utils_dir = os.path.join(args.repo_root, "libs", "iresearch", "include",
                             "iresearch", "utils")

    props = build_wb_props(ucd_dir)
    stage1, stage2 = dedup_blocks(props)
    words_dir = os.path.join(args.repo_root, "libs", "iresearch", "include",
                             "iresearch", "analysis", "text", "words")
    os.makedirs(words_dir, exist_ok=True)
    wb_bytes = emit_word_break_tables(
        os.path.join(words_dir, "tables.hpp"), stage1, stage2, ucd_version)
    print(f"words/tables.hpp: stage1 {len(stage1)} x u16 + "
          f"stage2 {len(stage2)} blocks = {wb_bytes} bytes total")

    lower, upper = parse_simple_case(ucd_dir)
    growth = max_growth(lower, upper)
    assert growth <= 1, f"unexpected UTF-8 case growth {growth}"
    emit_case_tables(
        os.path.join(utils_dir, "utf8_case_tables.hpp"), lower, upper, growth,
        ucd_version)
    print(f"utf8_case_tables.hpp: lower {len(lower)} + upper {len(upper)} "
          f"entries, max utf8 growth {growth}")


if __name__ == "__main__":
    sys.exit(main())
