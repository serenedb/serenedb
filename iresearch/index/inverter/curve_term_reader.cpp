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

#include "iresearch/index/inverter/curve_term_reader.hpp"

#include <absl/algorithm/container.h>

#include <iterator>

#include "iresearch/utils/space_filling_curve.hpp"

namespace irs {
namespace {

inline constexpr uint8_t kNoLevel = std::numeric_limits<uint8_t>::max();
inline constexpr auto kAncestor = static_cast<byte_type>(curve::kAncestorTerm);
inline constexpr auto kLeaf = static_cast<byte_type>(curve::kLeafTerm);
inline constexpr size_t kLevelByte = 2;

bool IsSingle(const ScatteredField& scattered, size_t rank) noexcept {
  return scattered.TermEnd(rank) - scattered.TermBegin(rank) == 1;
}

doc_id_t SingleDoc(const ScatteredField& scattered, size_t rank) noexcept {
  const auto row = scattered.TermBegin(rank);
  if (scattered.AllInline()) {
    return scattered.RankedInlineDocs()[row];
  }
  return scattered.DocBlocks()[row >> ScatterScratch::kBlockShift]
                              [row & ScatterScratch::kBlockMask];
}

}  // namespace

bytes_view CurveTermIterator::Term(const Entry& entry) const noexcept {
  const auto term = AsBytesView(_scattered->TermAt(entry.rank));
  if (entry.renamed == kOriginal) {
    return term;
  }
  return {_renamed + entry.renamed, term.size()};
}

PostingRows CurveTermIterator::Postings(uint32_t rank) const noexcept {
  const auto begin = _scattered->TermBegin(rank);
  const auto end = _scattered->TermEnd(rank);
  const auto count = static_cast<size_t>(end - begin);
  if (_scattered->AllInline()) {
    return {
      .span =
        {
          .docs = _scattered->RankedInlineDocs() + begin,
          .pos = nullptr,
          .offs_start = nullptr,
          .offs_end = nullptr,
          .count = count,
        },
    };
  }
  const auto* docs = _scattered->DocBlocks();
  const auto* pos = _scattered->PosBlocks();
  const auto* offs_start = _scattered->OffsStartBlocks();
  const auto* offs_end = _scattered->OffsEndBlocks();
  const auto block_end = (begin & ~uint64_t{ScatterScratch::kBlockMask}) +
                         ScatterScratch::kBlockValues;
  if (end <= block_end) {
    const auto block = begin >> ScatterScratch::kBlockShift;
    const auto off = begin & ScatterScratch::kBlockMask;
    return {
      .span =
        {
          .docs = docs[block] + off,
          .pos = pos ? pos[block] + off : nullptr,
          .offs_start = offs_start ? offs_start[block] + off : nullptr,
          .offs_end = offs_end ? offs_end[block] + off : nullptr,
          .count = count,
        },
    };
  }
  return {
    .span = {},
    .docs_blocks = docs,
    .pos_blocks = pos,
    .offs_start_blocks = offs_start,
    .offs_end_blocks = offs_end,
    .begin = begin,
    .end = end,
    .block_shift = ScatterScratch::kBlockShift,
  };
}

size_t CurveTermIterator::NextTermsWithPostings(std::span<bytes_view> terms,
                                                std::span<PostingRows> postings,
                                                IndexFeatures) {
  const size_t n =
    std::min({terms.size(), postings.size(), _order.size() - _next});
  for (size_t i = 0; i < n; ++i) {
    const auto& entry = _order[_next + i];
    terms[i] = Term(entry);
    postings[i] = Postings(entry.rank);
  }
  _next += n;
  return n;
}

void CurveTermReader::Reset(const ScatteredField& scattered) {
  const auto count = scattered.TermCount();
  _lone.assign(scattered.Field().LastDoc() + 1, kNoLevel);
  for (size_t rank = 0; rank < count; ++rank) {
    const auto term = AsBytesView(scattered.TermAt(rank));
    if (term.size() < curve::kTermHeader || term[0] != kAncestor ||
        !IsSingle(scattered, rank)) {
      continue;
    }
    auto& lone = _lone[SingleDoc(scattered, rank)];
    lone = std::min<uint8_t>(lone, term[kLevelByte]);
  }

  _kept.clear();
  _moved.clear();
  _renamed.clear();
  for (size_t rank = 0; rank < count; ++rank) {
    const auto term = AsBytesView(scattered.TermAt(rank));
    if (term.size() >= curve::kTermHeader && IsSingle(scattered, rank)) {
      const auto lone = _lone[SingleDoc(scattered, rank)];
      if (lone != kNoLevel && term[0] == kAncestor &&
          term[kLevelByte] >= lone) {
        continue;
      }
      if (lone != kNoLevel && term[0] == kLeaf) {
        _moved.push_back({static_cast<uint32_t>(rank),
                          static_cast<uint32_t>(_renamed.size())});
        _renamed.insert(_renamed.end(), term.begin(), term.end());
        _renamed[_moved.back().renamed + kLevelByte] = lone;
        continue;
      }
    }
    _kept.push_back(
      {static_cast<uint32_t>(rank), CurveTermIterator::kOriginal});
  }

  absl::c_stable_sort(_moved, [&](const auto& lhs, const auto& rhs) {
    return _renamed[lhs.renamed + kLevelByte] <
           _renamed[rhs.renamed + kLevelByte];
  });
  _it.Reset(scattered, {}, _renamed.data());
  _order.clear();
  _order.reserve(_kept.size() + _moved.size());
  absl::c_merge(_kept, _moved, std::back_inserter(_order),
                [&](const auto& lhs, const auto& rhs) {
                  return _it.Term(lhs) < _it.Term(rhs);
                });
  _it.Reset(scattered, _order, _renamed.data());
  _min = _max = {};
  if (!_order.empty()) {
    _min = _it.Term(_order.front());
    _max = _it.Term(_order.back());
  }
}

}  // namespace irs
