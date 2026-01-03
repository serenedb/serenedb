#pragma once

#include <miniselect/median_of_ninthers.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/column_collector.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <utility>

#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/types.hpp"

namespace irs {

constexpr size_t BlockSize(size_t k) noexcept {
  auto ceil = [](double v) {
    const auto i = static_cast<size_t>(v);
    return v > i ? i + 1 : i;
  };

  // TODO(gnusi): adjust min size
  return std::max(2 * kScoreBlock, ceil(2. * k / kScoreBlock) * kScoreBlock);
}

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
  auto end = hits.end();

  auto cmp = [](const auto& lhs, const auto& rhs) noexcept {
    return std::get<score_t>(lhs) > std::get<score_t>(rhs);
  };
  auto repivot = [&] noexcept {
    std::nth_element(hits.begin(), pivot, end, cmp);
    min_threshold = pivot->second;
  };

  ColumnCollector columns;
  for (auto& segment : reader) {
    columns.Clear();

    auto it = prepared->execute({
      .segment = segment,
      .scorers = scorers,
    });

    const auto* scorer = &it->PrepareScore({
      .scorer = scorers.buckets().front().bucket,
      .segment = &segment,
      .collector = &columns,
    });

    while (true) {
      const uint32_t n = it->Collect(*scorer, columns, offset, hits);
      if (n == 0) {
        break;
      }
      count += n;

      for (size_t i = offset, end = i + n; i < end; ++i) {
        if (hits[i].second > min_threshold) {
          hits[offset++] = hits[i];
        }
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

template<size_t K = std::dynamic_extent>
size_t ExecuteTopK(const DirectoryReader& reader, const Filter& filter,
                   const Scorers& scorers, const WandContext& wand, size_t k,
                   std::span<std::pair<score_t, doc_id_t>, K> results) {
  SDB_ASSERT(k * 2 <= results.size());

  auto prepared = filter.prepare({
    .index = reader,
    .scorers = scorers,
  });

  size_t count = 0;
  float_t min_threshold = 0;
  auto begin = results.begin();
  auto pivot = begin + k;
  auto end = results.end();

  ColumnCollector columns;
  for (auto& segment : reader) {
    columns.Clear();

    auto docs = prepared->execute({
      .segment = segment,
      .scorers = scorers,
      .wand = wand,
    });
    const auto* score = irs::get<ScoreAttr>(*docs);
    if (score) {
      score->Min(min_threshold);
    }

    float_t score_value;
    doc_id_t doc;
    while (!doc_limits::eof(doc = docs->advance())) {
      docs->CollectData(0);
      columns.Collect({&doc, 1});
      ++count;

      score->Score(&score_value, 1);

      if (score_value <= min_threshold) {
        continue;
      }

      *begin = {score_value, doc};
      ++begin;

      if (begin == end) {
        std::nth_element(results.begin(), pivot, end,
                         [](const auto& lhs, const auto& rhs) noexcept {
                           return lhs.first > rhs.first;
                         });
        begin = pivot;
        min_threshold = begin->first;
        score->Min(min_threshold);
      }
    }
  }

  if (begin > pivot) {
    std::nth_element(results.begin(), pivot, end,
                     [](const auto& lhs, const auto& rhs) noexcept {
                       return lhs.first > rhs.first;
                     });
    begin = pivot;
  }
  std::sort(results.begin(), begin,
            [](const auto& lhs, const auto& rhs) noexcept {
              return lhs.first > rhs.first;
            });

  return count;
}

template<size_t Extent = std::dynamic_extent>
size_t ExecuteTopKHeap(
  const DirectoryReader& reader, const Filter& filter, const Scorers& scorers,
  const WandContext& wand, size_t k,
  std::span<std::pair<score_t, doc_id_t>, Extent> results) {
  SDB_ASSERT(k >= results.size());

  auto prepared = filter.prepare({
    .index = reader,
    .scorers = scorers,
  });

  size_t count = 0;
  auto begin = results.begin();
  auto end = results.end();

  for (auto left = k; auto& segment : reader) {
    auto docs = prepared->execute(irs::ExecutionContext{
      .segment = segment, .scorers = scorers, .wand = wand});
    const auto* score = irs::get<irs::ScoreAttr>(*docs);

    if (!left && score) {
      score->Min(results.front().first);
    }

    float_t score_value;
    doc_id_t doc;
    while (!doc_limits::eof(doc = docs->advance())) {
      ++count;

      score->Score(&score_value, 1);

      if (begin != end) {
        *begin = {score_value, doc};
        ++begin;

        if (begin == end) {
          absl::c_make_heap(results,
                            [](const auto& lhs, const auto& rhs) noexcept {
                              return lhs.first > rhs.first;
                            });

          score->Min(results.front().first);
        }
      } else if (results.front().first < score_value) {
        absl::c_pop_heap(results,
                         [](const auto& lhs, const auto& rhs) noexcept {
                           return lhs.first > rhs.first;
                         });

        auto& [score, doc_id] = results.back();
        score = score_value;
        doc_id = doc;

        absl::c_push_heap(
          results, [](const std::pair<float_t, irs::doc_id_t>& lhs,
                      const std::pair<float_t, irs::doc_id_t>& rhs) noexcept {
            return lhs.first > rhs.first;
          });

        score->Min(results.front().first);
      }
    }
  }

  absl::c_sort(results, [](const auto& lhs, const auto& rhs) noexcept {
    return lhs.first > rhs.first;
  });

  return count;
}

}  // namespace irs
