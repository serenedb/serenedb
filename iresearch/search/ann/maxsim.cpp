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

#include "iresearch/search/ann/maxsim.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <limits>

#include "iresearch/search/ann/batch.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::ann {

void MaxSimCandidates::Search(const AnnIndex& ann, const AnnEntries& entries,
                              const SubReader& segment,
                              const PrepareContext& ctx,
                              const VectorFilterOptions& opts, uint32_t effort,
                              std::span<const float> vectors,
                              std::span<const uint32_t> group_ends) {
  const uint32_t d = ann.Dim();
  SDB_ASSERT(d != 0 && vectors.size() % d == 0);
  SDB_ASSERT(opts.maxsim_depth != 0);
  const auto count = static_cast<uint32_t>(vectors.size() / d);
  SDB_ASSERT(group_ends.empty() || group_ends.back() == count);
  _floors.clear();
  _collectors.clear();
  _collectors.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    _collectors.emplace_back(_floors.emplace_back(opts.maxsim_depth),
                             opts.maxsim_depth);
  }
  if (count != 0) {
    ann.SearchBatch(segment, ctx, opts, QueryBatch{.vectors = vectors, .d = d},
                    effort, _collectors);
  }
  _bounds.clear();
  _group_first.assign(1, 0);
  uint32_t first = 0;
  for (const auto end : group_ends) {
    _excess.clear();
    score_t base = 0;
    for (auto v = first; v < end; ++v) {
      auto& collector = _collectors[v];
      const auto hits = collector.Flush();
      if (hits.empty()) {
        continue;
      }
      score_t lowest = std::numeric_limits<score_t>::max();
      _rows.clear();
      for (const auto& hit : hits) {
        lowest = std::min(lowest, hit.score);
        _rows.emplace_back(entries.Row(hit.doc - doc_limits::min()), hit.score);
      }
      const score_t imputed = std::max(collector.Floor(), lowest);
      base += imputed;
      absl::c_sort(_rows, [](const RowScore& l, const RowScore& r) {
        return l.row < r.row;
      });
      for (size_t i = 0; i < _rows.size();) {
        const auto row = _rows[i].row;
        auto best = _rows[i].score;
        while (++i < _rows.size() && _rows[i].row == row) {
          best = std::max(best, _rows[i].score);
        }
        _excess[row] += std::max(score_t{0}, best - imputed);
      }
    }
    const auto from = _bounds.size();
    for (const auto& [row, excess] : _excess) {
      _bounds.push_back(
        {.doc = row + doc_limits::min(), .upper = base + excess});
    }
    absl::c_sort(
      std::span{_bounds}.subspan(from),
      [](const MaxSimBound& l, const MaxSimBound& r) { return l.doc < r.doc; });
    _group_first.push_back(_bounds.size());
    first = end;
  }
}

}  // namespace irs::ann
