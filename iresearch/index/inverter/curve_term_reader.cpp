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

#include <bit>

namespace irs {
namespace {

inline constexpr auto kAncestor = static_cast<byte_type>(curve::kAncestorTerm);
inline constexpr auto kLeaf = static_cast<byte_type>(curve::kLeafTerm);
inline constexpr size_t kLevelByte = 2;
inline constexpr uint32_t kNoRank = std::numeric_limits<uint32_t>::max();

doc_id_t DocAt(const ScatteredField& scattered, uint64_t row) noexcept {
  if (scattered.AllInline()) {
    return scattered.RankedInlineDocs()[row];
  }
  return scattered.DocBlocks()[row >> ScatterScratch::kBlockShift]
                              [row & ScatterScratch::kBlockMask];
}

uint16_t SharedBits(bytes_view lhs, bytes_view rhs, uint32_t bits) noexcept {
  for (size_t i = curve::kTermHeader; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      const auto shared =
        (i - curve::kTermHeader) * 8 +
        std::countl_zero(static_cast<uint8_t>(lhs[i] ^ rhs[i]));
      return static_cast<uint16_t>(std::min<size_t>(bits, shared));
    }
  }
  return static_cast<uint16_t>(bits);
}

}  // namespace

bytes_view CurveTermIterator::Term(const Entry& entry) const noexcept {
  const auto term = AsBytesView(_scattered->TermAt(entry.rank));
  if (entry.renamed == kOriginal) {
    return term;
  }
  return {_bytes + entry.renamed, term.size()};
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
  const size_t n = std::min({terms.size(), postings.size(), Size() - _next});
  for (size_t i = 0; i < n; ++i, ++_next) {
    if (_next < _generated.size()) {
      const auto& term = _generated[_next];
      terms[i] = Term(term);
      postings[i] = {
        .span =
          {
            .docs = _docs + term.docs,
            .pos = nullptr,
            .offs_start = nullptr,
            .offs_end = nullptr,
            .count = term.count,
          },
      };
      continue;
    }
    const auto& entry = _order[_next - _generated.size()];
    terms[i] = Term(entry);
    postings[i] = Postings(entry.rank);
  }
  return n;
}

void CurveTermReader::Split(const Group& group, uint32_t bits) {
  const auto first = _next_groups.size();
  auto begin = group.begin;
  for (auto rank = group.begin + 1; rank <= group.end; ++rank) {
    if (rank != group.end && _shared[rank] >= bits) {
      continue;
    }
    const auto count = _before[rank] - _before[begin];
    if (count >= 2) {
      _next_groups.push_back({begin, rank, 0, count});
    }
    begin = rank;
  }
  const auto last = _next_groups.size();
  if (first == last) {
    return;
  }
  if (last - first == 1 && _next_groups[first].count == group.count) {
    _next_groups[first].docs = group.docs;
    return;
  }
  _fill.clear();
  for (auto i = first; i < last; ++i) {
    _next_groups[i].docs = static_cast<uint32_t>(_docs.size());
    _fill.push_back(_next_groups[i].docs);
    _docs.resize(_docs.size() + _next_groups[i].count);
  }
  const auto children = std::span{_next_groups}.subspan(first, last - first);
  for (uint32_t i = 0; i < group.count; ++i) {
    const auto doc = _docs[group.docs + i];
    const auto rank = _rank_of[doc];
    const auto child = absl::c_upper_bound(
      children, rank,
      [](uint32_t value, const Group& next) { return value < next.begin; });
    if (child == children.begin() || rank >= std::prev(child)->end) {
      continue;
    }
    _docs[_fill[std::prev(child) - children.begin()]++] = doc;
  }
}

void CurveTermReader::Reset(const ScatteredField& scattered,
                            const curve::Options& options) {
  const auto count = static_cast<uint32_t>(scattered.TermCount());
  const auto dimensions = options.dimensions;
  const auto bits = dimensions * options.max_level;

  _shared.assign(count + 1, 0);
  _before.resize(count + 1);
  _rank_of.assign(scattered.Field().LastDoc() + 1, kNoRank);
  uint32_t docs = 0;
  for (uint32_t rank = 0; rank < count; ++rank) {
    const auto term = AsBytesView(scattered.TermAt(rank));
    SDB_ASSERT(term.size() == curve::TermSize(dimensions, options.max_level) &&
               term[0] == kLeaf && term[kLevelByte] == options.max_level);
    if (rank != 0) {
      _shared[rank] =
        SharedBits(AsBytesView(scattered.TermAt(rank - 1)), term, bits);
    }
    _before[rank] = docs;
    for (auto row = scattered.TermBegin(rank); row != scattered.TermEnd(rank);
         ++row) {
      _rank_of[DocAt(scattered, row)] = rank;
    }
    docs += static_cast<uint32_t>(scattered.TermEnd(rank) -
                                  scattered.TermBegin(rank));
  }
  _before[count] = docs;

  _generated.clear();
  _bytes.clear();
  _docs.clear();
  _groups.clear();
  if (docs >= 2) {
    _groups.push_back({0, count, 0, docs});
    for (doc_id_t doc = 0; doc < _rank_of.size(); ++doc) {
      if (_rank_of[doc] != kNoRank) {
        _docs.push_back(doc);
      }
    }
  }
  for (uint32_t level = 0; level < options.max_level && !_groups.empty();
       level += options.level_step) {
    const auto size = curve::TermSize(dimensions, level);
    const auto level_bits = dimensions * level;
    for (const auto& group : _groups) {
      const auto code = AsBytesView(scattered.TermAt(group.begin));
      const auto term = static_cast<uint32_t>(_bytes.size());
      _bytes.push_back(kAncestor);
      _bytes.push_back(static_cast<byte_type>(dimensions));
      _bytes.push_back(static_cast<byte_type>(level));
      _bytes.insert(_bytes.end(), code.begin() + curve::kTermHeader,
                    code.begin() + size);
      if (level_bits % 8) {
        _bytes.back() &= static_cast<byte_type>(0xff << (8 - level_bits % 8));
      }
      _generated.push_back({term, size, group.docs, group.count});
    }
    const auto next = level + options.level_step;
    if (next >= options.max_level) {
      break;
    }
    _next_groups.clear();
    for (const auto& group : _groups) {
      Split(group, dimensions * next);
    }
    std::swap(_groups, _next_groups);
  }

  _moved.clear();
  _order.clear();
  for (uint32_t rank = 0; rank < count; ++rank) {
    if (_before[rank + 1] - _before[rank] != 1) {
      _order.push_back({rank, CurveTermIterator::kOriginal});
      continue;
    }
    int32_t shared = -1;
    if (rank != 0) {
      shared = _shared[rank];
    }
    if (rank + 1 != count) {
      shared = std::max<int32_t>(shared, _shared[rank + 1]);
    }
    const auto alone =
      shared < 0 ? 0 : static_cast<uint32_t>(shared) / dimensions + 1;
    const auto lone = (alone + options.level_step - 1) / options.level_step *
                      options.level_step;
    if (lone >= options.max_level) {
      _order.push_back({rank, CurveTermIterator::kOriginal});
      continue;
    }
    const auto term = AsBytesView(scattered.TermAt(rank));
    _moved.push_back({rank, static_cast<uint32_t>(_bytes.size())});
    _bytes.insert(_bytes.end(), term.begin(), term.end());
    _bytes[_moved.back().renamed + kLevelByte] = static_cast<byte_type>(lone);
  }
  absl::c_stable_sort(_moved, [&](const auto& lhs, const auto& rhs) {
    return _bytes[lhs.renamed + kLevelByte] < _bytes[rhs.renamed + kLevelByte];
  });
  _moved.insert(_moved.end(), _order.begin(), _order.end());
  std::swap(_moved, _order);

  _it.Reset(scattered, _generated, _order, _bytes.data(), _docs.data());
  _min = _max = {};
  if (_it.Size() != 0) {
    _min = _it.TermAt(0);
    _max = _it.TermAt(_it.Size() - 1);
  }
}

}  // namespace irs
