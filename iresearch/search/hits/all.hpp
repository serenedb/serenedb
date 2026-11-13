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

#include <algorithm>
#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/hits/root.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::hits {

class All : public Root {
 public:
  All(ColumnArgsFetcher& fetcher, doc_id_t count, score_t score = 0) noexcept
    : _end{doc_limits::min() + count},
      _score{ScoreFunction::Constant(score)},
      _fetcher{fetcher} {}

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* IRS_RESTRICT out,
               score_t* IRS_RESTRICT scores) final {
    const auto stop = std::min(max, _end);
    const auto n = min < stop ? static_cast<uint32_t>(stop - min) : 0;
    for (uint32_t i = 0; i != n; ++i) {
      out[i] = min + i;
    }
    static_assert(kPostingBlock > kScoreBlock);
    uint32_t offset = 0;
    for (; offset + kPostingBlock <= n; offset += kPostingBlock) {
      FetchAndScorePostingBlock(_fetcher, _score, out + offset, scores + offset,
                                doc_limits::invalid());
    }
    if (offset != n) {
      FetchAndScore(_fetcher, _score, out + offset, scores + offset,
                    n - offset);
    }
    return n;
  }

 private:
  doc_id_t _end;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
};

}  // namespace irs::hits
