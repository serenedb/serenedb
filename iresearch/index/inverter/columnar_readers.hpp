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

#include "iresearch/analysis/text/term_view.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/inverter/columnar_flush.hpp"
#include "iresearch/index/iterators.hpp"

namespace irs {

class ColumnarTermIterator final : public TermOnlyIterator {
 public:
  void Reset(const ScatteredField& scattered) {
    _scattered = &scattered;
    _rank = 0;
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

  TermPostings::ptr postings(IndexFeatures) const final {
    SDB_ENSURE(false, "columnar postings are span-only");
    return {};
  }

  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

  size_t NextTermsWithPostings(std::span<bytes_view> terms,
                               std::span<PostingRows> postings,
                               IndexFeatures) final {
    const auto total = _scattered->TermCount();
    SDB_ASSERT(_rank <= total);
    const size_t n = std::min({terms.size(), postings.size(), total - _rank});
    if (n == 0) {
      return 0;
    }
    const size_t first = _rank;
    if (_scattered->AllInline()) {
      const auto* docs = _scattered->RankedInlineDocs();
      auto begin = _scattered->TermBegin(first);
      for (size_t i = 0; i < n; ++i) {
        const auto end = _scattered->TermEnd(first + i);
        postings[i] = {.span = {.docs = docs + begin,
                                .pos = nullptr,
                                .offs_start = nullptr,
                                .offs_end = nullptr,
                                .count = static_cast<size_t>(end - begin)}};
        terms[i] = AsBytesView(_scattered->TermAt(first + i));
        begin = end;
      }
    } else {
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
            .block_shift = ScatterScratch::kBlockShift,
          };
        }
        terms[i] = AsBytesView(_scattered->TermAt(first + i));
        begin = end;
      }
    }
    _rank += n;
    return n;
  }

  const FieldMeta& Meta() const noexcept { return _scattered->Field().Meta(); }

 private:
  static uint64_t BlockEnd(uint64_t row) noexcept {
    return (row & ~uint64_t{ScatterScratch::kBlockMask}) +
           ScatterScratch::kBlockValues;
  }

  PostingsSpan BlockSpan(uint64_t begin, uint64_t end) const noexcept {
    const auto block = begin >> ScatterScratch::kBlockShift;
    const auto off = begin & ScatterScratch::kBlockMask;
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
  size_t _rank = 0;
};

class ColumnarTermReader final : public BasicTermReader,
                                 private util::Noncopyable {
 public:
  void Reset(const ScatteredField& scattered) {
    _it.Reset(scattered);
    _min = _max = {};
    if (const auto nterms = scattered.TermCount()) {
      _min = AsBytesView(scattered.TermAt(0));
      _max = AsBytesView(scattered.TermAt(nterms - 1));
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
