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

#include "iresearch/search/detail/bitset_build.hpp"

namespace irs::detail {

void FillPostings(BitsetBuckets& buckets, const duckdb::InputFile& doc,
                  doc_id_t docs_count, doc_id_t min, doc_id_t max,
                  uint64_t* IRS_RESTRICT words) {
  SDB_ASSERT(!buckets.must.empty() || !buckets.fills.empty());
  SDB_ASSERT(buckets.fills.empty() || buckets.must.size() <= 1);
  SDB_ASSERT(min < max);
  constexpr auto kBits = BitsetStorage::kBits;
  const auto count =
    static_cast<uint32_t>((uint64_t{max - min} + kBits - 1) / kBits);
  const auto seed = buckets.Seed(docs_count);
  if (buckets.next.empty()) {
    auto terms = buckets.must_not.size();
    for (const auto& clause : buckets.must) {
      terms += clause.size();
    }
    buckets.next.assign(terms, doc_limits::invalid());
  }
  const auto next_of = [&](size_t clause) {
    auto* next = buckets.next.data();
    for (size_t i = 0; i != clause; ++i) {
      next += buckets.must[i].size();
    }
    return next;
  };

  Resolve(doc, [&]<typename Input> {
    PostingReader<Input> reader{doc};
    if (!buckets.must.empty()) {
      OrBits or_seed{.words = words, .base = min};
      FillTerms(std::span<const PostingClause>{buckets.must[seed]},
                next_of(seed), reader, min, max, nullptr, or_seed);
    }
    for (auto& node : buckets.fills) {
      SDB_ASSERT(node);
      ReadFill(*node, min, max, nullptr, words);
    }

    auto& scratch = buckets.scratch;
    const auto open_scratch = [&]() -> uint64_t* {
      if (scratch.WordCount() < count) {
        scratch = BitsetStorage{max - min};
      } else {
        std::fill_n(scratch.Words(), count, uint64_t{0});
      }
      return scratch.Words();
    };
    auto& live = buckets.live;
    const auto gate = [&] {
      live.Reset(words, count, min);
      return &live;
    };

    for (size_t i = 0, n = buckets.must.size(); i != n; ++i) {
      if (i == seed) {
        continue;
      }
      const std::span<const PostingClause> clause{buckets.must[i]};
      SDB_ASSERT(!clause.empty());
      const auto* const alive = gate();
      if (AppliedInPlace(clause, docs_count)) {
        RetainBits retain{.words = words, .base = min};
        FillTerms(clause, next_of(i), reader, min, max, alive, retain);
        retain.Finish(count);
        continue;
      }
      auto* const other = open_scratch();
      OrBits or_clause{.words = other, .base = min};
      FillTerms(clause, next_of(i), reader, min, max, alive, or_clause);
      for (uint32_t w = 0; w != count; ++w) {
        words[w] &= other[w];
      }
    }

    if (!buckets.must_not.empty()) {
      ClearBits clear{.words = words, .base = min};
      FillTerms(std::span<const PostingClause>{buckets.must_not},
                next_of(buckets.must.size()), reader, min, max, gate(), clear);
    }
    if (!buckets.exclude_fills.empty()) {
      const auto* const alive = gate();
      auto* const other = open_scratch();
      for (auto& node : buckets.exclude_fills) {
        SDB_ASSERT(node);
        ReadFill(*node, min, max, alive, other);
      }
      for (uint32_t w = 0; w != count; ++w) {
        words[w] &= ~other[w];
      }
    }
  });
}

}  // namespace irs::detail
