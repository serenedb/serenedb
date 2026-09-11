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

#include <utility>

#include "iresearch/search/common/plain_scored.hpp"
#include "iresearch/search/common/posting_count_scored.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/fill/posting_scored.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs::search {

template<typename Result, typename Make>
Result ResolveFillScored(const IndexInput& doc, bool per_doc,
                         ScoreMergeType merge, Make&& make) {
  return ResolveInput(doc, [&]<typename Input> -> Result {
    using Plain = PlainFillScored<Input>;
    const auto with = [&]<ScoreMergeType M> -> Result {
      if (per_doc) {
        return make.template operator()<PostingFillScored<Input, M>, Plain>();
      }
      return make.template operator()<ConstFillScored<Input, M>, Plain>();
    };
    if (merge == ScoreMergeType::Max) {
      return with.template operator()<ScoreMergeType::Max>();
    }
    SDB_ASSERT(merge == ScoreMergeType::Sum);
    return with.template operator()<ScoreMergeType::Sum>();
  });
}

template<typename Result, typename Make>
Result ResolveCountScored(const IndexInput& doc, bool per_doc,
                          ScoreMergeType merge, Make&& make) {
  return ResolveInput(doc, [&]<typename Input> -> Result {
    using Plain = PlainCountScored<Input>;
    const auto with = [&]<ScoreMergeType M> -> Result {
      if (per_doc) {
        return make.template operator()<PostingCountScored<Input, M>, Plain>();
      }
      return make.template operator()<ConstCountScored<Input, M>, Plain>();
    };
    if (merge == ScoreMergeType::Max) {
      return with.template operator()<ScoreMergeType::Max>();
    }
    SDB_ASSERT(merge == ScoreMergeType::Sum);
    return with.template operator()<ScoreMergeType::Sum>();
  });
}

}  // namespace irs::search
