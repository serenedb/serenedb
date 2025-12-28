#include "iresearch/search/doc_collector.hpp"

#include "iresearch/index/directory_reader.hpp"
#include "iresearch/search/score.hpp"

namespace irs {

size_t ExecuteTopK(const DirectoryReader& reader, const Filter& filter,
                   const Scorers& scorers, size_t k,
                   std::vector<std::pair<score_t, doc_id_t>>& results) {
  auto prepared = filter.prepare({
    .index = reader,
    .scorers = scorers,
  });

  size_t left = 2 * k;
  results.reserve(left);

  size_t count = 0;
  float_t min_threshold = 0;
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

      if (left) {
        results.emplace_back(score_value, doc->value);

        if (0 == --left) {
          absl::c_nth_element(results, results.begin() + k,
                              [](const auto& lhs, const auto& rhs) noexcept {
                                return lhs.first > rhs.first;
                              });
          min_threshold = results[k - 1].first;
          results.erase(results.begin() + k, results.end());
          left = k;
        }
      }
    }
  }

  if (left < k) {
    SDB_ASSERT(results.size() > k);
    absl::c_nth_element(results, results.begin() + k,
                        [](const auto& lhs, const auto& rhs) noexcept {
                          return lhs.first > rhs.first;
                        });
    results.erase(results.begin() + k, results.end());
  }
  absl::c_sort(results, [](const auto& lhs, const auto& rhs) noexcept {
    return lhs.first > rhs.first;
  });

  return count;
}

}  // namespace irs
