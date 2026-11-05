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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <numeric>
#include <span>
#include <vector>

#include "iresearch/search/detail/bitset_build.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

inline constexpr uint64_t kSortedDocsPerWord = 8;
inline constexpr size_t kRadixDocs = 2048;

inline bool SparseFold(uint64_t docs, doc_id_t docs_count) noexcept {
  return docs * kSortedDocsPerWord <= SegmentWords(docs_count);
}

struct AppendDocs {
  static constexpr bool kOrdered = true;

  std::vector<doc_id_t>& docs;
  std::vector<uint32_t>& starts;

  void Term() { starts.push_back(static_cast<uint32_t>(docs.size())); }

  IRS_FORCE_INLINE void Run(uint64_t prev, uint32_t len) {
    const auto size = docs.size();
    docs.resize(size + len);
    std::iota(docs.begin() + static_cast<ptrdiff_t>(size), docs.end(),
              static_cast<doc_id_t>(prev + 1));
  }

  IRS_FORCE_INLINE void Bitset(uint64_t prev,
                               const UnalignedWord* IRS_RESTRICT src,
                               uint32_t n, uint64_t) {
    for (uint32_t w = 0; w != n; ++w) {
      const auto base = prev + 1 + uint64_t{w} * BitsetStorage::kBits;
      for (uint64_t word = src[w]; word != 0; word &= word - 1) {
        docs.push_back(static_cast<doc_id_t>(
          base + static_cast<uint64_t>(std::countr_zero(word))));
      }
    }
  }

  IRS_FORCE_INLINE void Doc(size_t doc) {
    docs.push_back(static_cast<doc_id_t>(doc));
  }

  IRS_FORCE_INLINE void Finish(uint32_t) noexcept {}
};

struct DocsRun {
  doc_id_t first;
  uint32_t begin;
  uint32_t end;
};

inline bool ConcatRuns(std::vector<doc_id_t>& docs,
                       std::span<const uint32_t> starts) {
  std::vector<DocsRun> runs;
  runs.reserve(starts.size());
  const auto size = static_cast<uint32_t>(docs.size());
  for (size_t i = 0; i != starts.size(); ++i) {
    const auto begin = starts[i];
    const auto end = i + 1 != starts.size() ? starts[i + 1] : size;
    if (begin != end) {
      runs.push_back({docs[begin], begin, end});
    }
  }
  if (runs.size() < 2) {
    return false;
  }
  absl::c_sort(runs, [](const DocsRun& lhs, const DocsRun& rhs) {
    return lhs.first < rhs.first;
  });
  for (size_t i = 1; i != runs.size(); ++i) {
    if (docs[runs[i - 1].end - 1] >= runs[i].first) {
      return false;
    }
  }
  std::vector<doc_id_t> ordered;
  ordered.reserve(docs.size());
  for (const auto& run : runs) {
    ordered.insert(ordered.end(), docs.begin() + run.begin,
                   docs.begin() + run.end);
  }
  docs.swap(ordered);
  return true;
}

inline void SortDocs(std::vector<doc_id_t>& docs,
                     std::span<const uint32_t> starts) {
  if (absl::c_is_sorted(docs) || ConcatRuns(docs, starts)) {
    return;
  }
  if (docs.size() < kRadixDocs) {
    absl::c_sort(docs);
    return;
  }
  constexpr uint32_t kDigitBits = 11;
  constexpr uint32_t kBuckets = uint32_t{1} << kDigitBits;
  std::vector<doc_id_t> other(docs.size());
  std::array<uint32_t, kBuckets> counts;
  for (uint32_t shift = 0; shift < 32; shift += kDigitBits) {
    counts.fill(0);
    for (const auto doc : docs) {
      ++counts[(doc >> shift) & (kBuckets - 1)];
    }
    if (counts[(docs.front() >> shift) & (kBuckets - 1)] == docs.size()) {
      continue;
    }
    uint32_t sum = 0;
    for (auto& count : counts) {
      const auto bucket = count;
      count = sum;
      sum += bucket;
    }
    for (const auto doc : docs) {
      other[counts[(doc >> shift) & (kBuckets - 1)]++] = doc;
    }
    docs.swap(other);
  }
}

template<typename Term>
std::vector<doc_id_t> SortedDocsOf(std::span<const Term> terms,
                                   const TermReader* field,
                                   const IndexInput& doc) {
  std::vector<doc_id_t> docs;
  docs.reserve(SumDocs(terms) + 1);
  std::vector<uint32_t> starts;
  starts.reserve(terms.size());
  ResolveInput(doc, [&]<typename Input> {
    PostingReader<Input> reader{doc};
    AppendDocs sink{docs, starts};
    ReadTerms(terms, field, reader, sink);
  });
  SortDocs(docs, starts);
  docs.erase(std::ranges::unique(docs).begin(), docs.end());
  docs.push_back(doc_limits::eof());
  return docs;
}

}  // namespace irs::detail
