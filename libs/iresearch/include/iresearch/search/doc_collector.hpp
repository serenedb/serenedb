#pragma once

#include <algorithm>
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

constexpr size_t BlockSize(size_t k) noexcept { return 2 * k + kScoreBlock; }

template<size_t K = std::dynamic_extent>
size_t ExecuteTopKWithCount(const DirectoryReader& reader, const Filter& filter,
                            const Scorers& scorers, size_t k,
                            std::span<std::pair<doc_id_t, score_t>, K> hits) {
  SDB_ASSERT(BlockSize(k) <= hits.size());

  auto prepared = filter.prepare({
    .index = reader,
    .scorers = scorers,
  });

  size_t count = 0;
  size_t offset = 0;
  score_t min_threshold = 0;
  const size_t max_size = 2 * k;
  auto pivot = hits.begin() + k;

  auto cmp = [](const auto& lhs, const auto& rhs) noexcept {
    return std::get<score_t>(lhs) > std::get<score_t>(rhs);
  };
  auto repivot = [&] noexcept {
    std::nth_element(hits.begin(), pivot, hits.begin() + offset, cmp);
    min_threshold = pivot->second;
  };

  ColumnCollector columns;
  for (auto& segment : reader) {
    columns.Clear();

    auto it = prepared->execute({
      .segment = segment,
      .scorers = scorers,
    });

    auto scorer = it->PrepareScore({
      .scorer = scorers.buckets().front().bucket,
      .segment = &segment,
      .collector = &columns,
    });

    while (true) {
      const auto [accepted, total] = it->Collect(scorer, columns, hits.subspan(offset), min_threshold);
      if (accepted == 0) {
        break;
      }
      count += total;
      offset += accepted;

      if (offset >= max_size) {
        repivot();
        offset = k;
      }
    }
  }

  if (offset > k) {
    repivot();
  }

  std::sort(hits.begin(), pivot, cmp);

  return count;
}

}  // namespace irs
