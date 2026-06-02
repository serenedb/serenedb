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
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

constexpr size_t BlockSize(size_t k) noexcept { return 2 * k; }

inline uint64_t ExecuteTopKWithCount(const DirectoryReader& reader,
                                     const Filter& filter, const Scorer& scorer,
                                     size_t k, std::span<ScoreDoc> hits) {
  SDB_ASSERT(BlockSize(k) == hits.size());

  auto prepare_collector = filter.MakeCollector(&scorer);
  std::vector<QueryBuilder::ptr> queries;
  queries.reserve(reader.size());
  for (auto& segment : reader) {
    queries.emplace_back(
      filter.PrepareSegment(segment, {.collector = prepare_collector.get()}));
  }
  const auto stats = prepare_collector->Finish(IResourceManager::gNoop);

  score_t score_threshold = std::numeric_limits<score_t>::min();
  NthPartitionScoreCollector collector{score_threshold, k, hits};
  ColumnArgsFetcher fetcher;
  uint32_t seg_idx = 0;
  for (auto& segment : reader) {
    fetcher.Clear();
    auto& query = queries[seg_idx];
    collector.SetSegment(seg_idx++);

    if (!query) {
      continue;
    }

    auto it = query->Execute({.stats = &stats});

    auto score_func = it->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    it->Collect(score_func, fetcher, collector);
  }

  const auto count = collector.Finalize();

  return count;
}

inline uint64_t ExecuteTopK(const DirectoryReader& reader, const Filter& filter,
                            const Scorer& scorer, size_t k, WandContext wand,
                            std::span<ScoreDoc> hits) {
  SDB_ASSERT(BlockSize(k) == hits.size());

  auto prepare_collector = filter.MakeCollector(&scorer);
  std::vector<QueryBuilder::ptr> queries;
  queries.reserve(reader.size());
  for (auto& segment : reader) {
    queries.emplace_back(
      filter.PrepareSegment(segment, {.collector = prepare_collector.get()}));
  }
  const auto stats = prepare_collector->Finish(IResourceManager::gNoop);

  score_t score_threshold = std::numeric_limits<score_t>::min();
  NthPartitionScoreCollector collector{score_threshold, k, hits};
  ColumnArgsFetcher fetcher;
  uint32_t seg_idx = 0;
  for (auto& segment : reader) {
    fetcher.Clear();
    auto& query = queries[seg_idx];
    collector.SetSegment(seg_idx++);

    if (!query) {
      continue;
    }

    auto it = query->Execute({.stats = &stats, .wand = wand});

    auto score_func = it->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    if (auto* score_threshold = irs::GetMutable<ScoreThresholdAttr>(it.get())) {
      collector.SetScoreThreshold(score_threshold->value);
    }

    it->Collect(score_func, fetcher, collector);

    collector.SetScoreThreshold(score_threshold);
  }

  const auto count = collector.Finalize();

  return count;
}

}  // namespace irs
