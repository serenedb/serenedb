#pragma once

#include <cmath>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/column_collector.hpp>
#include <utility>

#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/types.hpp"

namespace irs {

template<size_t K = std::dynamic_extent>
size_t ExecuteCountTopK(const DirectoryReader& reader, const Filter& filter,
                        const Scorers& scorers, size_t k,
                        std::span<score_t, K> scores,
                        std::span<doc_id_t, K> docs) {
  SDB_ASSERT(k * 2 <= docs.size());

  auto prepared = filter.prepare({
    .index = reader,
    .scorers = scorers,
  });

  size_t block_count = 0;
  auto hits = std::views::zip(docs, scores);
  auto begin = hits.begin();
  auto pivot = begin + k;
  auto end = hits.end();

  auto cmp = [](const auto& lhs, const auto& rhs) noexcept {
    return std::get<score_t>(lhs) > std::get<score_t>(rhs);
  };
  auto repivot = [&] noexcept {
    std::nth_element(hits.begin(), pivot, end, cmp);
  };

  ColumnCollector columns;
  for (auto& segment : reader) {
    columns.Clear();

    auto it = prepared->execute({
      .segment = segment,
      .scorers = scorers,
      .collector = &columns,
    });
    const auto* score = irs::get<ScoreAttr>(*docs);

    doc_id_t doc;
    while (!doc_limits::eof(doc = it->advance())) {
      columns.Collect(doc);
      score->Collect();

      *begin++ = doc;

      if (begin == end) {
        score->Score(scores.data());
        repivot();
        begin = pivot;
        ++block_count;
      }
    }
  }

  const size_t count =
    (block_count ? k * (1 + block_count) : 0) + (begin - hits.begin());

  if (begin > pivot) {
    repivot();
    begin = pivot;
  }

  std::sort(hits.begin(), begin, cmp);

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
      .collector = &columns,
      .wand = wand,
    });
    const auto* score = irs::get<ScoreAttr>(*docs);
    if (score) {
      score->Min(min_threshold);
    }

    float_t score_value;
    doc_id_t doc;
    while (!doc_limits::eof(doc = docs->advance())) {
      columns.Collect(doc);
      ++count;

      (*score)(&score_value);

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

      (*score)(&score_value);

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
