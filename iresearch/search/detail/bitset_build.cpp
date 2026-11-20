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

void FillBitset(BitsetBuckets& buckets, const duckdb::InputFile& doc,
                doc_id_t docs_count, BitsetStorage& bits) {
  auto* const words = bits.Words();
  const auto seed = buckets.Seed(docs_count);
  Resolve(doc, [&]<typename Input> {
    PostingReader<Input> reader{doc};
    if (!buckets.must.empty()) {
      OrBits or_seed{words};
      ReadTerms(std::span<const PostingClause>{buckets.must[seed]}, nullptr,
                reader, or_seed);
    }
    for (auto& node : buckets.fills) {
      SDB_ASSERT(node);
      ReadFill(*node, bits.End(), words);
    }

    std::optional<BitsetStorage> scratch;
    const auto open_scratch = [&]() -> uint64_t* {
      if (!scratch) {
        scratch.emplace(docs_count);
      } else {
        std::fill_n(scratch->Words(), scratch->Alloc(), uint64_t{0});
      }
      return scratch->Words();
    };

    for (size_t i = 0, n = buckets.must.size(); i != n; ++i) {
      if (i == seed) {
        continue;
      }
      const std::span<const PostingClause> clause{buckets.must[i]};
      SDB_ASSERT(!clause.empty());
      if (AppliedInPlace(clause, docs_count)) {
        RetainBits retain{words};
        ReadTerms(clause, nullptr, reader, retain);
        retain.Finish(bits.WordCount());
        continue;
      }
      auto* const other = open_scratch();
      OrBits or_clause{other};
      ReadTerms(clause, nullptr, reader, or_clause);
      for (uint32_t w = 0, count = bits.WordCount(); w != count; ++w) {
        words[w] &= other[w];
      }
    }

    if (!buckets.must_not.empty()) {
      ClearBits clear{words};
      ReadTerms(std::span<const PostingClause>{buckets.must_not}, nullptr,
                reader, clear);
    }
    if (!buckets.exclude_fills.empty()) {
      auto* const other = open_scratch();
      for (auto& node : buckets.exclude_fills) {
        SDB_ASSERT(node);
        ReadFill(*node, bits.End(), other);
      }
      for (uint32_t w = 0, count = bits.WordCount(); w != count; ++w) {
        words[w] &= ~other[w];
      }
    }
  });
}

}  // namespace irs::detail
