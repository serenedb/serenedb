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

#pragma once

#include <absl/base/optimization.h>

#include <tuple>
#include <utility>

#include "iresearch/search/score_function.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Include, typename Excludes, typename Table>
class SparseExclusion : public Root {
 public:
  static constexpr uint32_t kFill = Include::kFill;

  template<typename IncludeArgs, typename ExcludesArgs>
  SparseExclusion(Table table, std::piecewise_construct_t,
                  IncludeArgs&& include, ExcludesArgs&& excludes)
    : _include{std::make_from_tuple<Include>(
        std::forward<IncludeArgs>(include))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _admit{table} {}

  SparseExclusion(SparseExclusion&&) = delete;
  SparseExclusion& operator=(SparseExclusion&&) = delete;

  Include& Include_() noexcept { return _include; }

  void Run(LoserScoreCollector& collector) final {
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kFill];
    ABSL_CACHELINE_ALIGNED score_t scores[kFill];

    for (;;) {
      const auto len = _include.Fill(docs, scores);
      if (len == 0) {
        break;
      }
      uint32_t kept = 0;
      for (uint32_t i = 0; i != len; ++i) {
        const auto doc = docs[i];
        if (_excludes.Probe(doc) == doc) {
          continue;
        }
        docs[kept] = doc;
        scores[kept] = scores[i];
        ++kept;
      }
      if (kept != 0) {
        _admit.AddDocs(collector, docs, kept, scores);
      }
    }
    _admit.Flush(collector);
  }

 private:
  Include _include;
  Excludes _excludes;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
