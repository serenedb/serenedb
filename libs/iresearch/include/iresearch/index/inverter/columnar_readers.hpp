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

#include "iresearch/formats/formats.hpp"
#include "iresearch/index/inverter/columnar_flush.hpp"
#include "iresearch/index/iterators.hpp"

namespace irs {

// Write-side reader: terms and postings flow exclusively through the
// batched pull -- each term as an in-place TermPostings view, contiguous
// for a term inside one scatter block, blocked otherwise. The classic
// per-term iterator surface exists only as pure-virtual obligations and
// refuses use.
class ColumnarTermIterator final : public TermOnlyIterator {
 public:
  explicit ColumnarTermIterator(IResourceManager& rm)
    : _gather{ManagedTypedAllocator<uint32_t>{rm}} {}

  void Reset(const ScatteredField& scattered) {
    _scattered = &scattered;
    _rank = 0;
    if (scattered.AllInline()) {
      _docs = nullptr;
      _pos = nullptr;
      _offs_start = nullptr;
      _offs_end = nullptr;
      return;
    }
    _docs = scattered.DocBlocks();
    _pos = scattered.PosBlocks();
    _offs_start = scattered.OffsStartBlocks();
    _offs_end = scattered.OffsEndBlocks();
  }

  bytes_view value() const noexcept final {
    SDB_ENSURE(false, "columnar terms are batch-only");
    return {};
  }

  bool next() final {
    SDB_ENSURE(false, "columnar terms are batch-only");
    return false;
  }

  DocIterator::ptr postings(IndexFeatures /*features*/) const final {
    SDB_ENSURE(false, "columnar postings are span-only");
    return {};
  }

  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

  size_t NextTermsWithPostings(std::span<bytes_view> terms,
                               std::span<TermPostings> postings,
                               IndexFeatures /*features*/) final {
    const auto total = _scattered->TermCount();
    SDB_ASSERT(_rank <= total);
    size_t n = std::min({terms.size(), postings.size(), total - _rank});
    if (n == 0) {
      return 0;
    }
    const size_t first = _rank;
    if (_scattered->AllInline()) {
      // PK shape: doc ids live in the dictionary's inline capture behind
      // the rank permutation, so each term is one contiguous slice of a
      // single per-batch gather (positions never exist in this shape).
      // The batch extends while it fits the gather budget; a single
      // over-budget term gathers whole.
      const auto base = _scattered->TermBegin(first);
      size_t used = 1;
      while (used < n &&
             _scattered->TermEnd(first + used) - base <= kGatherBudget) {
        ++used;
      }
      n = used;
      const auto batch_end = _scattered->TermEnd(first + n - 1);
      _gather.resize(batch_end - base);
      _scattered->GatherInlineDocs(base, batch_end, _gather.data());
      auto begin = base;
      for (size_t i = 0; i < n; ++i) {
        const auto end = _scattered->TermEnd(first + i);
        postings[i] = {.span = {.docs = _gather.data() + (begin - base),
                                .pos = nullptr,
                                .offs_start = nullptr,
                                .offs_end = nullptr,
                                .count = static_cast<size_t>(end - begin)}};
        terms[i] = _scattered->TermAt(first + i);
        begin = end;
      }
    } else {
      // consecutive terms share their boundary (TermEnd(i) is
      // TermBegin(i+1)), so one bound is read per term. A term inside one
      // scatter block is a contiguous span read in place; a block-crossing
      // term hands out the blocked row-range view instead.
      auto begin = _scattered->TermBegin(first);
      for (size_t i = 0; i < n; ++i) {
        const auto end = _scattered->TermEnd(first + i);
        if (end <= BlockEnd(begin)) [[likely]] {
          postings[i] = {.span = BlockSpan(begin, end)};
        } else {
          postings[i] = {
            .span = {},
            .docs_blocks = _docs,
            .pos_blocks = _pos,
            .offs_start_blocks = _offs_start,
            .offs_end_blocks = _offs_end,
            .begin = begin,
            .end = end,
            .block_shift = ScatterView::kBlockShift,
          };
        }
        terms[i] = _scattered->TermAt(first + i);
        begin = end;
      }
    }
    _rank += n;
    return n;
  }

  const FieldMeta& Meta() const noexcept { return _scattered->Field().Meta(); }

 private:
  static constexpr uint64_t kGatherBudget = 4096;

  static uint64_t BlockEnd(uint64_t row) noexcept {
    return (row & ~uint64_t{ScatterView::kBlockMask}) +
           ScatterView::kBlockValues;
  }

  PostingsSpan BlockSpan(uint64_t begin, uint64_t end) const noexcept {
    const auto block = begin >> ScatterView::kBlockShift;
    const auto off = begin & ScatterView::kBlockMask;
    return {
      .docs = _docs[block] + off,
      .pos = _pos ? _pos[block] + off : nullptr,
      .offs_start = _offs_start ? _offs_start[block] + off : nullptr,
      .offs_end = _offs_end ? _offs_end[block] + off : nullptr,
      .count = static_cast<size_t>(end - begin),
    };
  }

  const ScatteredField* _scattered = nullptr;
  uint32_t* const* _docs = nullptr;
  uint32_t* const* _pos = nullptr;
  uint32_t* const* _offs_start = nullptr;
  uint32_t* const* _offs_end = nullptr;
  ManagedVector<uint32_t> _gather;
  size_t _rank = 0;
};

class ColumnarTermReader final : public BasicTermReader,
                                 private util::Noncopyable {
 public:
  explicit ColumnarTermReader(IResourceManager& rm) : _it{rm} {}

  void Reset(const ScatteredField& scattered) {
    _it.Reset(scattered);
    _min = _max = {};
    if (const auto nterms = scattered.TermCount()) {
      _min = scattered.TermAt(0);
      _max = scattered.TermAt(nterms - 1);
    }
  }

  bytes_view(min)() const noexcept final { return _min; }
  bytes_view(max)() const noexcept final { return _max; }
  const FieldMeta& Meta() const noexcept { return _it.Meta(); }
  field_id id() const noexcept final { return Meta().id; }
  FieldProperties properties() const noexcept final { return Meta(); }

  irs::TermOnlyIterator::ptr iterator() const noexcept final {
    return memory::to_managed<irs::TermOnlyIterator>(_it);
  }

 private:
  mutable ColumnarTermIterator _it;
  bytes_view _min{};
  bytes_view _max{};
};

}  // namespace irs
