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

#include <algorithm>
#include <limits>
#include <span>

#include "iresearch/index/inverter/fields_inverter.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs {

struct ScatterScratch {
  static constexpr size_t kBlockShift = 16;
  static constexpr size_t kBlockValues = size_t{1} << kBlockShift;
  static constexpr size_t kBlockMask = kBlockValues - 1;

  struct RankedTerm {
    uint64_t key;
    uint32_t id;
  };

  explicit ScatterScratch(MemoryBudget* budget = nullptr)
    : cursors{TrackedAllocator<uint32_t>{budget}},
      bounds{TrackedAllocator<uint32_t>{budget}},
      ranked{TrackedAllocator<RankedTerm>{budget}},
      ranked_alt{TrackedAllocator<RankedTerm>{budget}} {}

  static size_t Bytes(size_t terms, size_t blocks) noexcept;

  void Reserve(size_t terms, size_t blocks, duckdb::Allocator& allocator);

  std::vector<duckdb::AllocatedData> blocks;
  TrackedVector<uint32_t> cursors;
  TrackedVector<uint32_t> bounds;
  TrackedVector<RankedTerm> ranked;
  TrackedVector<RankedTerm> ranked_alt;
  std::vector<uint32_t> radix_counts;
  std::vector<uint32_t*> docs;
  std::vector<uint32_t*> pos;
  std::vector<uint32_t*> offs_start;
  std::vector<uint32_t*> offs_end;
};

class ScatteredField : util::Noncopyable {
 public:
  ScatteredField(InverterMemory& mem, ScatterScratch& scratch) noexcept
    : _mem{&mem}, _s{&scratch} {}

  static size_t Blocks(const FieldInverter& field, uint64_t nocc) noexcept;

  void Reset(const FieldInverter& field);

  const FieldInverter& Field() const noexcept { return *_field; }
  size_t TermCount() const noexcept { return _s->ranked.size(); }

  const duckdb::string_t& TermAt(size_t rank) const noexcept {
    const auto entries = _field->Dictionary().Entries();
    return entries[_s->ranked[rank].id];
  }

  uint64_t TermBegin(size_t rank) const noexcept { return _s->bounds[rank]; }
  uint64_t TermEnd(size_t rank) const noexcept { return _s->bounds[rank + 1]; }

  uint32_t KeyWidth() const noexcept { return _width == kNoWidth ? 0 : _width; }
  uint64_t TermBytes() const noexcept { return _term_bytes; }

  uint32_t* const* DocBlocks() const noexcept {
    return _s->docs.empty() ? nullptr : _s->docs.data();
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
  static constexpr uint32_t kNoWidth = std::numeric_limits<uint32_t>::max();

  void ResetSizes() noexcept {
    _width = kNoWidth;
    _term_bytes = 0;
  }
  void NoteSize(uint32_t size) noexcept {
    _width = _width == kNoWidth || _width == size ? size : 0;
    _term_bytes += size;
  }

  void BuildHistogram(const PostingLogBase& log, size_t vocab);
  void RankLiveTerms(std::span<const duckdb::string_t> entries);
  void RekeyPastSharedPrefix(std::span<const duckdb::string_t> entries,
                             uint64_t min_key, uint64_t max_key);
  uint64_t PrefixSums();
  uint32_t** AssignBlocks(std::vector<uint32_t*>& col, size_t nblocks,
                          size_t& next);
  void RadixSortByKey();

  template<bool kRefs, typename Log>
  void Scatter(const Log& log, uint64_t nocc);

  template<typename Log>
  bool ScatterEntry(const Log& log, const LogEntry& entry, uint32_t ntokens,
                    doc_id_t doc, uint32_t** docs, uint32_t** positions,
                    uint32_t** offs_start, uint32_t** offs_end);

  InverterMemory* _mem;
  ScatterScratch* _s;
  const FieldInverter* _field = nullptr;
  uint64_t _term_bytes = 0;
  uint32_t _width = kNoWidth;
};

}  // namespace irs
