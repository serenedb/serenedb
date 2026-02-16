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

constexpr size_t BlockSize(size_t k) noexcept { return 2 * k + kScoreBlock; }

template<size_t K>
size_t ExecuteTopKWithCount(const DirectoryReader& reader, const Filter& filter,
                            const Scorer& scorer, size_t k,
                            std::span<std::pair<doc_id_t, score_t>, K> hits) {
  SDB_ASSERT(BlockSize(k) <= hits.size());

  auto prepared = filter.prepare({
    .index = reader,
    .scorer = &scorer,
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

  std::array<doc_id_t, kScoreBlock> docs;
  std::array<score_t, kScoreBlock> scores;
  auto copy = [&](size_t n) IRS_FORCE_INLINE noexcept {
    if (min_threshold > 0) [[unlikely]] {
      for (size_t i = 0; i < n; ++i) {
        if (scores[i] > min_threshold) {
          hits[offset++] = {docs[i], scores[i]};
        }
      }
    } else {
      for (size_t i = 0; i < n; ++i) {
        hits[offset++] = {docs[i], scores[i]};
      }
    }
  };

  ColumnCollector columns;
  for (auto& segment : reader) {
    columns.Clear();

    auto it = prepared->execute({
      .segment = segment,
      .scorer = &scorer,
    });

    auto score_func = it->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .collector = &columns,
    });

    while (true) {
      const uint32_t n = it->Collect(score_func, columns, docs, scores);
      if (n == 0) {
        break;
      }
      count += n;

      if (n == kScoreBlock) [[likely]] {
        copy(kScoreBlock);
      } else {
        copy(n);
      }

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

template<size_t K>
size_t ExecuteTopK(const DirectoryReader& reader, const Filter& filter,
                   const Scorer& scorer, size_t k, WandContext wand,
                   std::span<std::pair<doc_id_t, score_t>, K> hits) {
  SDB_ASSERT(BlockSize(k) <= hits.size());

  auto prepared = filter.prepare({
    .index = reader,
    .scorer = &scorer,
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

  ScoreThresholdAttr* threshold_attr = nullptr;

  std::array<doc_id_t, kScoreBlock> docs;
  std::array<score_t, kScoreBlock> scores;
  auto copy = [&](size_t n) IRS_FORCE_INLINE noexcept {
    if (min_threshold > 0) [[likely]] {
      for (size_t i = 0; i < n; ++i) {
        if (scores[i] > min_threshold) {
          hits[offset++] = {docs[i], scores[i]};
        }
      }
    } else {
      for (size_t i = 0; i < n; ++i) {
        hits[offset++] = {docs[i], scores[i]};
      }
    }
  };

  ColumnCollector columns;
  for (auto& segment : reader) {
    columns.Clear();

    auto it = prepared->execute({
      .segment = segment,
      .scorer = &scorer,
      .wand = wand,
    });

    auto score_func = it->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .collector = &columns,
    });

    threshold_attr = irs::GetMutable<ScoreThresholdAttr>(it.get());
    if (threshold_attr && min_threshold > 0) {
      threshold_attr->value = min_threshold;
    }

    while (true) {
      const uint32_t n = it->Collect(score_func, columns, docs, scores);
      if (n == 0) {
        break;
      }
      count += n;

      if (n == kScoreBlock) [[likely]] {
        copy(kScoreBlock);
      } else {
        copy(n);
      }

      if (offset >= max_size) {
        repivot();
        offset = k;
        if (threshold_attr) {
          threshold_attr->value = min_threshold;
        }
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
