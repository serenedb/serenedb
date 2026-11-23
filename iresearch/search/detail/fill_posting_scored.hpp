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

#include "iresearch/search/detail/plain_scored.hpp"
#include "iresearch/search/detail/posting_window_scored.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/scorers/scorer.hpp"

namespace irs::detail {

template<typename Result, typename P = NoPins, typename Make>
Result ResolveFillScored(const duckdb::InputFile& doc, bool per_doc,
                         ScoreMergeType merge, Make&& make) {
  return Resolve<P>(doc, [&]<typename Input> -> Result {
    using Plain = PlainFillScored<Input>;
    return Resolve<P>(merge, [&]<ScoreMergeType M> -> Result {
      return Resolve<PerDocAxis, P>(per_doc, [&]<bool PerDoc> -> Result {
        using Leaf = std::conditional_t<PerDoc, PostingFillScored<Input, M>,
                                        ConstFillScored<Input, M>>;
        return make.template operator()<Leaf, Plain>();
      });
    });
  });
}

template<typename Result, typename P = NoPins, typename Make>
Result ResolveCountScored(const duckdb::InputFile& doc, bool per_doc,
                          ScoreMergeType merge, Make&& make) {
  return Resolve<P>(doc, [&]<typename Input> -> Result {
    using Plain = PlainCountScored<Input>;
    return Resolve<P>(merge, [&]<ScoreMergeType M> -> Result {
      return Resolve<PerDocAxis, P>(per_doc, [&]<bool PerDoc> -> Result {
        using Leaf = std::conditional_t<PerDoc, PostingCountScored<Input, M>,
                                        ConstCountScored<Input, M>>;
        return make.template operator()<Leaf, Plain>();
      });
    });
  });
}

}  // namespace irs::detail
