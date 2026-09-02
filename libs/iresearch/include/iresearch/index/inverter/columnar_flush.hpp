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

#pragma once

#include <cstring>
#include <optional>

#include "iresearch/index/inverter/fields_inverter.hpp"

namespace irs {

// Flush-time scatter scratch: owned by the writer and reused across the
// fields of one segment flush; Release() drops everything at flush end
// (cross-flush retention measured null while pinning 20-76MB per pooled
// writer).
struct ScatterScratch {
  struct RankedTerm {
    uint64_t key;
    uint32_t id;
  };

  explicit ScatterScratch(IResourceManager& rm)
    : blocks{ManagedTypedAllocator<duckdb::AllocatedData>{rm}},
      cursors{ManagedTypedAllocator<uint32_t>{rm}},
      bounds{ManagedTypedAllocator<uint32_t>{rm}},
      ranked{ManagedTypedAllocator<RankedTerm>{rm}},
      ranked_alt{ManagedTypedAllocator<RankedTerm>{rm}},
      radix_counts{ManagedTypedAllocator<uint32_t>{rm}},
      term_starts{ManagedTypedAllocator<uint32_t>{rm}},
      docs{ManagedTypedAllocator<uint32_t*>{rm}},
      pos{ManagedTypedAllocator<uint32_t*>{rm}},
      offs_start{ManagedTypedAllocator<uint32_t*>{rm}},
      offs_end{ManagedTypedAllocator<uint32_t*>{rm}} {}

  // Scratch reuse pays within one flush (across fields); across flushes the
  // realloc cost is unmeasurable while retention is 20-76MB per pooled
  // writer, so Flush releases everything at its end.
  void Release() noexcept {
    const auto release = [](auto& v) {
      v = std::remove_reference_t<decltype(v)>{v.get_allocator()};
    };
    release(blocks);
    release(cursors);
    release(bounds);
    release(ranked);
    release(ranked_alt);
    release(radix_counts);
    release(term_starts);
    release(docs);
    release(pos);
    release(offs_start);
    release(offs_end);
  }

  ManagedVector<duckdb::AllocatedData> blocks;
  ManagedVector<uint32_t> cursors;
  ManagedVector<uint32_t> bounds;
  ManagedVector<RankedTerm> ranked;
  ManagedVector<RankedTerm> ranked_alt;
  ManagedVector<uint32_t> radix_counts;
  ManagedVector<uint32_t> term_starts;
  ManagedVector<uint32_t*> docs;
  ManagedVector<uint32_t*> pos;
  ManagedVector<uint32_t*> offs_start;
  ManagedVector<uint32_t*> offs_end;
};

// Two-level view over fixed-size scatter blocks: consumers are doc-at-a-time
// iterators, so term-major output never needs to be contiguous.
class ScatterView {
 public:
  static constexpr size_t kBlockShift = 16;
  static constexpr size_t kBlockValues = size_t{1} << kBlockShift;
  static constexpr size_t kBlockMask = kBlockValues - 1;

  explicit ScatterView(uint32_t* const* blocks) noexcept : _blocks{blocks} {}

  uint32_t operator[](uint64_t i) const noexcept {
    return _blocks[i >> kBlockShift][i & kBlockMask];
  }

 private:
  uint32_t* const* _blocks = nullptr;
};

// Flush-time scratch, reused across fields of one segment flush. The scatter
// turns the doc-major occurrence log into term-major {doc, pos, offs} regions
// with one stable counting-sort pass; docs within a term's region are
// non-decreasing by construction, freq(term, doc) = run length of equal doc.
class ScatteredField : util::Noncopyable {
 public:
  ScatteredField(InverterMemory& mem, ScatterScratch& scratch) noexcept
    : _mem{&mem}, _s{&scratch} {}

  void Reset(const FieldInverter& field);

  const FieldInverter& Field() const noexcept { return *_field; }
  size_t TermCount() const noexcept {
    if (_identity) {
      return _field->InlineDocs().size();
    }
    return _s->term_starts.empty() ? _s->ranked.size()
                                   : _s->term_starts.size() - 1;
  }

  bytes_view TermAt(size_t rank) const noexcept {
    const auto entries = _field->Dictionary().Entries();
    if (_identity) {
      return AsBytesView(entries[rank].term);
    }
    return AsBytesView(entries[_s->ranked[RankAt(rank)].id].term);
  }

  // Region bounds are materialized per rank: [bounds[rank], bounds[rank+1]).
  // Append-only (unique) dictionaries can hold duplicate entries; they rank
  // adjacent (id order = doc order), so term_starts folds each group into
  // one emitted term spanning the group's contiguous regions. An all-inline
  // field (empty log: every occurrence is a dictionary-captured first
  // occurrence, the PK shape) materializes nothing -- bounds are the rank
  // identity and docs are gathered straight from the dictionary's inline
  // capture, so blocks, cursors and the scatter pass never run.
  uint64_t TermBegin(size_t rank) const noexcept {
    const auto r = RankAt(rank);
    return _all_inline ? r : _s->bounds[r];
  }
  uint64_t TermEnd(size_t rank) const noexcept {
    const auto r = RankAt(rank + 1);
    return _all_inline ? r : _s->bounds[r];
  }

  bool AllInline() const noexcept { return _all_inline; }

  // Raw block arrays behind the scatter, for span-at-a-time consumers. A
  // lane is assigned only when the field's log layout carries it (the
  // scatter's own dispatch), so an absent feature reads as nullptr here --
  // consumers copy these blindly instead of re-deriving feature gates.
  uint32_t* const* DocBlocks() const noexcept {
    SDB_ASSERT(!_all_inline);
    return _s->docs.data();
  }

  // All-inline shape: docs live in the dictionary's inline capture behind
  // the rank permutation; gather [begin, end) of them for a span consumer.
  void GatherInlineDocs(uint64_t begin, uint64_t end,
                        uint32_t* out) const noexcept {
    SDB_ASSERT(_all_inline);
    const auto docs = _field->InlineDocs();
    if (_identity) {
      std::memcpy(out, docs.data() + begin, (end - begin) * sizeof(doc_id_t));
      return;
    }
    const auto* ranked = _s->ranked.data();
    for (uint64_t k = begin; k != end; ++k) {
      *out++ = docs[ranked[k].id];
    }
  }
  uint32_t* const* PosBlocks() const noexcept {
    return _s->pos.empty() ? nullptr : _s->pos.data();
  }
  uint32_t* const* OffsStartBlocks() const noexcept {
    return _s->offs_start.empty() ? nullptr : _s->offs_start.data();
  }
  uint32_t* const* OffsEndBlocks() const noexcept {
    return _s->offs_end.empty() ? nullptr : _s->offs_end.data();
  }

 private:
  size_t RankAt(size_t rank) const noexcept {
    return _s->term_starts.empty() ? rank : _s->term_starts[rank];
  }

  void BuildHistogram(const LogColumn& term_ids, size_t vocab);
  bool InlineFillSorted() const noexcept;
  std::optional<size_t> RankLiveTerms(
    std::span<const TermDictionary::Entry> entries);
  void RekeyPastSharedPrefix(std::span<const TermDictionary::Entry> entries,
                             uint64_t min_key, uint64_t max_key,
                             bool& key_sorted);
  void FoldDuplicateTerms(std::span<const TermDictionary::Entry> entries,
                          std::optional<size_t> first_dup_hint);
  uint64_t PrefixSums();
  // Fixed-size blocks from a grow-only pool, reused across fields; a column
  // is a flat array of block pointers, written and read via ScatterView's
  // power-of-2 indexing.
  uint32_t** AssignBlocks(ManagedVector<uint32_t*>& col, size_t nblocks,
                          size_t& next);
  void RadixSortByKey();

  template<typename Log>
  void Scatter(const Log& log, uint64_t nocc);

  InverterMemory* _mem;
  ScatterScratch* _s;
  const FieldInverter* _field = nullptr;
  bool _all_inline = false;
  bool _identity = false;
};

}  // namespace irs
