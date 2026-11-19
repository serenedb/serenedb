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

#include "iresearch/search/detail/phrase_match.hpp"
#include "iresearch/utils/dispatch.hpp"

namespace irs::detail {

template<PhraseMatch M>
struct PhraseKind {
  static constexpr bool kConstant = false;
};

enum class NGramMatch : uint8_t {
  Threshold,
  All,
};

template<NGramMatch M>
struct NGramKind {
  static constexpr bool kConstant = false;
};

struct NGramMatchAxis : EnumAxis<NGramMatch> {
  template<typename Query>
  static NGramMatch Of(const Query& query) noexcept {
    return query.Every() ? NGramMatch::All : NGramMatch::Threshold;
  }
};

struct TokenPhraseKind {
  static constexpr bool kConstant = false;
};

struct GeoKind {
  static constexpr bool kConstant = true;
};

struct WildcardNGramKind {
  static constexpr bool kConstant = true;
};

template<typename Family, typename Ctx>
consteval bool ScoredPlan() {
  if constexpr (requires { typename Family::ScoredContext; }) {
    return std::is_same_v<Ctx, typename Family::ScoredContext>;
  } else {
    return false;
  }
}

template<typename Family, typename Ctx>
inline constexpr bool kScoredPlan = ScoredPlan<Family, Ctx>();

template<typename Family, typename Kind, typename Query>
struct DocsMaker {
  static typename Family::Result Make(const Query& query,
                                      const typename Family::DocsContext& ctx);
};

template<typename Family, typename Kind, typename Query>
struct ScoredMaker {
  static typename Family::Result Make(
    const Query& query, const typename Family::ScoredContext& ctx);
};

template<typename Family, typename Kind, typename Query, typename Ctx>
typename Family::Result MakeKind(const Query& query, const Ctx& ctx) {
  if constexpr (!kScoredPlan<Family, Ctx>) {
    return DocsMaker<Family, Kind, Query>::Make(query, ctx);
  } else {
    if constexpr (!Kind::kConstant && requires { Family::DocsOf(ctx); }) {
      if (!query.Stats().stats) {
        return DocsMaker<Family, Kind, Query>::Make(query, Family::DocsOf(ctx));
      }
    }
    return ScoredMaker<Family, Kind, Query>::Make(query, ctx);
  }
}

}  // namespace irs::detail
