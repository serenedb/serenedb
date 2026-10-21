////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/index/iterators.hpp"

#include <absl/algorithm/container.h>

#include <utility>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/singleton.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

// A term whose posting list holds nothing
struct EmptyTermPostings : TermPostings {
  uint32_t NextDocs(doc_id_t* /*docs*/, uint32_t* /*freqs*/) noexcept final {
    return 0;
  }
};

EmptyTermPostings gEmptyTermPostings;

// Represents an iterator without terms
struct EmptySeekTermIterator : SeekTermIterator {
  bytes_view value() const noexcept final { return {}; }

  TermPostings::ptr postings(IndexFeatures /*features*/) const noexcept final {
    return TermPostings::empty();
  }

  bool next() noexcept final { return false; }

  Attribute* GetMutable(TypeInfo::type_id /*type*/) noexcept final {
    return nullptr;
  }

  SeekResult seek_ge(bytes_view /*value*/) noexcept final {
    return SeekResult::End;
  }

  bool seek(bytes_view /*value*/) noexcept final { return false; }

  const PostingMeta& cookie() const noexcept final { return kNoPosting; }
};

EmptySeekTermIterator gEmptySeekIterator;

uint32_t DescendingKey(score_t score) noexcept {
  const auto bits = std::bit_cast<uint32_t>(score + 0.0f);
  const auto sign = static_cast<uint32_t>(static_cast<int32_t>(bits) >> 31);
  return ~(bits ^ (sign | 0x80000000U));
}

bool Better(const ScoreDoc& l, const ScoreDoc& r) noexcept {
  if (l.score != r.score) {
    return l.score > r.score;
  }
  return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
}

}  // namespace

void SortHits(std::span<ScoreDoc> hits) {
  const auto n = hits.size();
  std::vector<uint64_t> keys(n);
  for (size_t i = 0; i != n; ++i) {
    keys[i] = uint64_t{DescendingKey(hits[i].score)} << 32 | i;
  }
  absl::c_sort(keys);
  std::vector<ScoreDoc> sorted(n);
  for (size_t i = 0; i != n; ++i) {
    sorted[i] = hits[static_cast<uint32_t>(keys[i])];
  }
  absl::c_copy(sorted, hits.begin());
  for (size_t i = 0; i != n;) {
    auto j = i + 1;
    while (j != n && hits[j].score == hits[i].score) {
      ++j;
    }
    if (j - i > 1) {
      auto ties = hits.subspan(i, j - i);
      absl::c_sort(ties, Better);
    }
    i = j;
  }
}

SeekTermIterator::ptr SeekTermIterator::empty() noexcept {
  return memory::to_managed<SeekTermIterator>(gEmptySeekIterator);
}

TermPostings::ptr TermPostings::empty() noexcept {
  return memory::to_managed<TermPostings>(gEmptyTermPostings);
}

}  // namespace irs
