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

#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/plan.hpp"

namespace irs::detail {

template<typename Query, typename Ctx>
ScoreMergeType MergeOf(const Query& query, const Ctx& ctx) noexcept {
  if constexpr (requires { ctx.merge; }) {
    return ctx.merge;
  } else if constexpr (requires { query.MergeType(); }) {
    return query.MergeType();
  } else {
    return ScoreMergeType::Noop;
  }
}

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const TermQuery& query, const Ctx& ctx);

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const MultiTermQuery& query, const Ctx& ctx);

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const FixedPhraseQuery& query,
                                  const Ctx& ctx);

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const VariadicPhraseQuery& query,
                                  const Ctx& ctx);

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const NGramSimilarityQuery& query,
                                  const Ctx& ctx);

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const AllQuery& query, const Ctx& ctx);

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const WildcardNGramQuery& query,
                                  const Ctx& ctx);

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const ApproxQuery& query, const Ctx& ctx);

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const TokenPhraseQuery& query,
                                  const Ctx& ctx);

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const ByNestedQuery& query, const Ctx& ctx);

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const RangeVectorQuery& query,
                                  const Ctx& ctx);

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const BooleanQuery& query, const Ctx& ctx);

template<typename Family, typename Parser, typename Acceptor, typename Ctx>
typename Family::Result MakeQuery(const GeoQuery<Parser, Acceptor>& query,
                                  const Ctx& ctx);

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const HnswQuery&, const Ctx&) {
  return {};
}

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const KnnVectorQuery&, const Ctx&) {
  return {};
}

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const ExactVectorQuery&, const Ctx&) {
  return {};
}

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const EmptyQueryBuilder&, const Ctx&) {
  return {};
}

template<typename Family, typename Query, typename Ctx>
typename Family::Result Plan(const Query& query, const Ctx& ctx) {
  if constexpr (requires { Family::Make(query, ctx); }) {
    return Family::Make(query, ctx);
  } else {
    return MakeQuery<Family>(query, ctx);
  }
}

}  // namespace irs::detail
