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
#include <cstdlib>
#include <duckdb/common/allocator.hpp>
#include <stdexcept>
#include <utility>
#include <vector>

#include "basics/shared.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

inline uint64_t ExecuteTopK(const DirectoryReader& reader, const Filter& filter,
                            const Scorer& scorer, size_t k, bool score_prune,
                            std::span<ScoreDoc> hits) {
  SDB_ASSERT(k == hits.size());

  auto& allocator = duckdb::Allocator::DefaultAllocator();
  StatsArena stats_arena{allocator};
  PreparedCollector collector_tree{filter, scorer, stats_arena, 1};
  std::vector<QueryBuilder::ptr> queries;
  queries.reserve(reader.size());
  for (auto& segment : reader) {
    queries.emplace_back(
      filter.PrepareSegment(segment, {.collector = collector_tree.Get()}));
  }
  collector_tree.Finish();

  score_t score_threshold = std::numeric_limits<score_t>::lowest();
  LoserScoreCollector collector{score_threshold, hits};
  ColumnArgsFetcher fetcher;
  uint32_t seg_idx = 0;
  for ([[maybe_unused]] auto& segment : reader) {
    fetcher.Clear();
    auto& query = queries[seg_idx];
    collector.SetSegment(seg_idx++);
    if (!query) {
      continue;
    }
    auto plan = top::MakeRoot(*query, {
                                        .scorer = scorer,
                                        .fetcher = fetcher,
                                        .prune = score_prune,
                                        .k = static_cast<uint32_t>(k),
                                      });
    if (!plan) {
      continue;
    }
    plan->Run(collector);
  }

  std::sort(
    hits.data(), hits.data() + collector.AcceptedCount(),
    [](const ScoreDoc& l, const ScoreDoc& r) { return l.score > r.score; });
  return collector.TotalMatches();
}

}  // namespace irs
