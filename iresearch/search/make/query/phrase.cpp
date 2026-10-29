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

#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/make_query.hpp"
#include "iresearch/search/detail/phrase_match.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/query_builder_impl.tpp"

namespace irs::detail {

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const FixedPhraseQuery& query,
                                  const Ctx& ctx) {
  return ResolveMatch(query, [&]<PhraseMatch M> {
    return MakeKind<Family, PhraseKind<M>>(query, ctx);
  });
}

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const VariadicPhraseQuery& query,
                                  const Ctx& ctx) {
  return ResolveMatch(query, [&]<PhraseMatch M> {
    return MakeKind<Family, PhraseKind<M>>(query, ctx);
  });
}

}  // namespace irs::detail
namespace irs {

template class QueryBuilderImpl<FixedPhraseQuery, PhraseQuery>;
template class QueryBuilderImpl<VariadicPhraseQuery, PhraseQuery>;

}  // namespace irs
