#include "iresearch/search/doc_collector.hpp"

#include "iresearch/index/directory_reader.hpp"
#include "iresearch/search/score.hpp"

namespace irs {

size_t ExecuteTopK(const DirectoryReader& reader, const Filter& filter,
                   const Scorers& scorers, size_t k,
                   std::vector<std::pair<score_t, doc_id_t>>& results) {
  if (!k) [[unlikely]] {
    return 0;
  }

  results.resize(2 * k);

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
  results.erase(begin, end);
  absl::c_sort(results, [](const auto& lhs, const auto& rhs) noexcept {
    return lhs.first > rhs.first;
  });

  return count;
}

}  // namespace irs
