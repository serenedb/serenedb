#pragma once

#include <iresearch/search/score.hpp>
#include <vector>

#include "iresearch/index/directory_reader.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/types.hpp"

namespace irs {

size_t ExecuteTopK(const DirectoryReader& reader, const Filter& filter,
                   const Scorers& scorers, size_t k,
                   std::vector<std::pair<score_t, doc_id_t>>& results);

template<size_t K>
size_t ExecuteTopKOptimized(
  const DirectoryReader& reader, const Filter& filter, const Scorers& scorers,
  size_t k, std::span<std::pair<score_t, doc_id_t>, 2 * K> results) {
  static_assert(K > 0);

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
    auto docs = prepared->execute(
      irs::ExecutionContext{.segment = segment, .scorers = scorers});
    const auto* doc = irs::get<DocAttr>(*docs);
    const auto* score = irs::get<ScoreAttr>(*docs);

    for (float_t score_value; docs->next();) {
      ++count;

      (*score)(&score_value);

      if (score_value <= min_threshold) {
        continue;
      }

      *begin = {score_value, doc->value};
      ++begin;

      if (begin == end) {
        absl::c_nth_element(results, pivot,
                            [](const auto& lhs, const auto& rhs) noexcept {
                              return lhs.first > rhs.first;
                            });
        begin = pivot;
        min_threshold = begin->first;
      }
    }
  }

  if (begin > pivot) {
    absl::c_nth_element(results, pivot,
                        [](const auto& lhs, const auto& rhs) noexcept {
                          return lhs.first > rhs.first;
                        });
    begin = pivot;
  }
  std::sort(results.begin(), pivot,
            [](const auto& lhs, const auto& rhs) noexcept {
              return lhs.first > rhs.first;
            });

  return count;
}

}  // namespace irs
