////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/container/inlined_vector.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <span>

#include "iresearch/types.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/type_info.hpp"

namespace irs {

// Represents metadata associated with the term
struct TermMeta : Attribute {
  static constexpr std::string_view type_name() noexcept { return "term_meta"; }

  void clear() noexcept {
    docs_count = 0;
    freq = 0;
  }

  // How many documents a particular term contains
  uint32_t docs_count = 0;

  // How many times a particular term occur in documents
  uint32_t freq = 0;
};

// One row group a term occurs in, as the term's record spells it. `rg` indexes
// row groups within the segment; everything else is measured from the term's
// own anchors, which sit once in the cookie -- a run carries its extent, never
// a file position. Doc ids are row-group local, so a consumer converts to a
// rowid exactly once, at its output boundary.
struct TermRowGroup {
  uint32_t rg;
  uint32_t docs_count;
  uint32_t freq;
  // `.doc` / `.pos` / `.pay` bytes the term's earlier runs hold. A plane the
  // field's features exclude never advances, so its offset is zero throughout.
  uint32_t doc_offset;
  uint32_t pos_offset;
  uint32_t pay_offset;
  // The one word the record inlines for this run: the row-group-local id of a
  // df == 1 run, which writes no `.doc` bytes at all, or -- past one postings
  // block -- where the run's skip data starts, measured from its own `.doc`
  // start. The run's df says which; nothing else fits in it, so they share it.
  uint32_t inlined;
  // Slot of the run's first position inside the block at `pos_offset`.
  uint8_t pos_slot;
};

// A term of many row groups keeps one of these per run, inline while it has
// few, so its width is what a query pays per term.
static_assert(sizeof(TermRowGroup) <= 32);

// A term's record, decoded once: the field-wide stats, the term's anchor in
// each stream, and its posting lists -- one per row group it occurs in,
// ascending by `rg`. That list is the term's postings handle: there is no
// whole-term posting list once a field is partitioned, and no term-level score
// bound either, since the bounds a scored walk reads sit at the head of every
// run in `.doc`.
//
// One row group is the overwhelmingly common case (every term of an
// unpartitioned field, and the long tail of a partitioned one), so it needs no
// allocation.
struct TermCookie {
  // Across every row group: what a term-level statistics collector reads.
  TermMeta stats;
  // Where the term's first run sits in each stream. For a payload field
  // `pay_start` is a lane ordinal rather than a byte position -- its runs are
  // lanes of one stream, placed by the df sums the record spells.
  uint64_t doc_start{};
  uint64_t pos_start{};
  uint64_t pay_start{};
  absl::InlinedVector<TermRowGroup, 1> rgs;

  std::span<const TermRowGroup> RowGroups() const noexcept { return rgs; }

  // The term's run inside `rg`, or null when it has none there.
  const TermRowGroup* Find(uint32_t rg) const noexcept {
    const auto it = std::lower_bound(
      rgs.begin(), rgs.end(), rg,
      [](const TermRowGroup& group, uint32_t key) { return group.rg < key; });
    if (it == rgs.end() || it->rg != rg) {
      return nullptr;
    }
    return &*it;
  }

  void clear() noexcept {
    stats.clear();
    doc_start = pos_start = pay_start = 0;
    rgs.clear();
  }
};

// One posting list as the postings machinery addresses it: absolute stream
// positions, resolved where an iterator is made. Nothing stores one -- the
// stored currency is `TermRowGroup`.
struct TermMetaImpl : TermMeta {
  TermMetaImpl() noexcept : e_skip_start{0} {}

  TermMetaImpl(const TermCookie& term, const TermRowGroup& run) noexcept
    : doc_start{term.doc_start + run.doc_offset},
      pos_start{term.pos_start + run.pos_offset},
      pay_start{term.pay_start + run.pay_offset},
      pos_offset{run.pos_slot},
      // The two readings of this union share their low word, so one store
      // spells whichever of them the run's df selects.
      e_skip_start{run.inlined} {
    docs_count = run.docs_count;
    freq = run.freq;
  }

  uint64_t doc_start = 0;  // where this term's postings start in the .doc file
  uint64_t pos_start = 0;  // where this term's postings start in the .pos file
  uint64_t pay_start = 0;  // where this term's postings start in the .pay file
  // Slot of the first position inside the block at `pos_start`, so it is
  // bounded by the position block size.
  uint8_t pos_offset = 0;
  union {
    uint64_t e_skip_start;  // pointer where skip data starts (after doc_start)
    doc_id_t e_single_doc;  // singleton document id delta
  };
};

}  // namespace irs
