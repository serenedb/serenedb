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

#include <cstdint>
#include <type_traits>

#include "iresearch/types.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

template<typename Excludes>
IRS_FORCE_INLINE bool IsExcluded(Excludes& excludes, doc_id_t doc) {
  if constexpr (requires { excludes.Test(doc); }) {
    return excludes.Test(doc);
  } else {
    return excludes.Probe(doc) == doc;
  }
}

template<typename Excludes>
inline constexpr bool kSkipsExcluded =
  requires { requires Excludes::kSkipsSpans; };

template<typename Excludes, typename Lead>
IRS_FORCE_INLINE bool SkipSpan(Excludes& excludes, Lead& lead, doc_id_t& doc) {
  if constexpr (kSkipsExcluded<Excludes>) {
    if (const auto live = excludes.NextLive(doc); live != doc) {
      doc = lead.Seek(live);
      return true;
    }
  }
  return false;
}

template<typename Excludes>
IRS_FORCE_INLINE bool Kept(Excludes& excludes, doc_id_t doc) {
  if constexpr (kSkipsExcluded<Excludes> ||
                std::is_same_v<Excludes, utils::Empty>) {
    return true;
  } else {
    return !IsExcluded(excludes, doc);
  }
}

template<typename Excludes, typename Lead>
IRS_FORCE_INLINE bool SkipExcluded(Excludes& excludes, Lead& lead,
                                   doc_id_t& doc) {
  if (SkipSpan(excludes, lead, doc)) {
    return true;
  }
  if (!Kept(excludes, doc)) {
    doc = lead.Next();
    return true;
  }
  return false;
}

template<typename Excludes>
IRS_FORCE_INLINE uint32_t ExcludeBlock(Excludes& excludes,
                                       doc_id_t* IRS_RESTRICT docs,
                                       score_t* IRS_RESTRICT scores,
                                       uint32_t len) {
  if (len == 0 || excludes.Probe(docs[0]) > docs[len - 1]) {
    return len;
  }
  if constexpr (requires { excludes.FilterBlock(docs, scores, len); }) {
    return excludes.FilterBlock(docs, scores, len);
  } else {
    uint32_t kept = 0;
    for (uint32_t i = 0; i != len; ++i) {
      const auto doc = docs[i];
      docs[kept] = doc;
      scores[kept] = scores[i];
      kept += static_cast<uint32_t>(!IsExcluded(excludes, doc));
    }
    return kept;
  }
}

}  // namespace irs::detail
