#pragma once

#include <cmath>

#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/types.hpp"

namespace irs {

template<size_t Extent = std::dynamic_extent>
size_t ExecuteTopK(const DirectoryReader& reader, const Filter& filter,
                   const Scorers& scorers, const WandContext& wand, size_t k,
                   std::span<std::pair<score_t, doc_id_t>, Extent> results) {
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

  for (auto& segment : reader) {
    auto docs = prepared->execute({
      .segment = segment,
      .scorers = scorers,
      .wand = wand,
    });
    const auto* doc = irs::get<DocAttr>(*docs);
    const auto* score = irs::get<ScoreAttr>(*docs);
    if (score) {
      score->Min(min_threshold);
    }

    for (float_t score_value; docs->next();) {
      ++count;

      (*score)(&score_value);

      if (score_value <= min_threshold) {
        continue;
      }

      *begin = {score_value, doc->value};
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
    const auto* doc = irs::get<irs::DocAttr>(*docs);
    const auto* score = irs::get<irs::ScoreAttr>(*docs);
    auto* threshold = irs::GetMutable<irs::ScoreAttr>(docs.get());
    SDB_ASSERT(threshold);

    if (!left && threshold) {
      threshold->Min(results.front().first);
    }

    for (float_t score_value; docs->next();) {
      ++count;

      (*score)(&score_value);

      if (begin != end) {
        *begin = {score_value, doc->value};
        ++begin;

        if (begin == end) {
          absl::c_make_heap(results,
                            [](const auto& lhs, const auto& rhs) noexcept {
                              return lhs.first > rhs.first;
                            });

          threshold->Min(results.front().first);
        }
      } else if (results.front().first < score_value) {
        absl::c_pop_heap(results,
                         [](const auto& lhs, const auto& rhs) noexcept {
                           return lhs.first > rhs.first;
                         });

        auto& [score, doc_id] = results.back();
        score = score_value;
        doc_id = doc->value;

        absl::c_push_heap(
          results, [](const std::pair<float_t, irs::doc_id_t>& lhs,
                      const std::pair<float_t, irs::doc_id_t>& rhs) noexcept {
            return lhs.first > rhs.first;
          });

        threshold->Min(results.front().first);
      }
    }
  }

  absl::c_sort(results, [](const auto& lhs, const auto& rhs) noexcept {
    return lhs.first > rhs.first;
  });

  return count;
}

}  // namespace irs
