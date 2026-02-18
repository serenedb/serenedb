#pragma once

#include <algorithm>
#include <iresearch/index/index_reader_options.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <utility>

#include "basics/shared.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/types.hpp"

namespace irs {

constexpr size_t BlockSize(size_t k) noexcept { return 2 * k; }

inline uint64_t ExecuteTopKWithCount(const DirectoryReader& reader,
                                     const Filter& filter, const Scorer& scorer,
                                     size_t k, std::span<ScoreDoc> hits) {
  SDB_ASSERT(BlockSize(k) == hits.size());

  auto prepared = filter.prepare({
    .index = reader,
    .scorer = &scorer,
  });

  score_t score_threshold = std::numeric_limits<score_t>::min();
  NthPartitionScoreCollector collector{score_threshold, k, hits};
  ColumnArgsFetcher fetcher;
  for (auto& segment : reader) {
    fetcher.Clear();

    auto it = prepared->execute({
      .segment = segment,
      .scorer = &scorer,
    });

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

  auto prepared = filter.prepare({
    .index = reader,
    .scorer = &scorer,

  });

  score_t score_threshold = std::numeric_limits<score_t>::min();
  NthPartitionScoreCollector collector{score_threshold, k, hits};
  ColumnArgsFetcher fetcher;
  for (auto& segment : reader) {
    fetcher.Clear();

    auto it = prepared->execute({
      .segment = segment,
      .scorer = &scorer,
      .wand = wand,
    });

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
