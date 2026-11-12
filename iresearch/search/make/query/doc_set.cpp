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

#include <type_traits>

#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/make_query.hpp"
#include "iresearch/search/detail/sorted_docs.hpp"
#include "iresearch/search/docs/boolean_bitset.hpp"
#include "iresearch/search/fill/bitset_docs.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/lead/bitset_docs.hpp"
#include "iresearch/search/probe/bitset_docs.hpp"
#include "iresearch/search/queries/doc_set_query.hpp"
#include "iresearch/search/queries/query_builder_impl.tpp"
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::detail {

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const DocSetQuery& query, const Ctx& ctx) {
  using Result = typename Family::Result;
  using Node = std::conditional_t<std::is_same_v<Result, ProbeNode::ptr>,
                                  probe::BitsetDocs, lead::BitsetDocs>;
  if (query.Sparse()) {
    if constexpr (kScoredPlan<Family, Ctx>) {
      return Family::template Constant<SortedDocs>(
        ctx, AllDocsScoreOf(query, Family::Scoring(ctx)), query.Docs());
    } else {
      return Family::template Docs<SortedDocs>(ctx, query.Docs());
    }
  }
  if constexpr (kScoredPlan<Family, Ctx>) {
    return Family::template Constant<Node>(
      ctx, AllDocsScoreOf(query, Family::Scoring(ctx)), query.Share());
  } else if constexpr (std::is_same_v<Result, FillNode::ptr>) {
    return memory::make_managed<fill::Impl<fill::BitsetDocs>>(query.Share());
  } else if constexpr (std::is_same_v<Result, docs::Root::ptr>) {
    return memory::make_managed<docs::BooleanBitset>(query.Share());
  } else {
    return Family::template Docs<Node>(ctx, query.Share());
  }
}

}  // namespace irs::detail
namespace irs {

template class QueryBuilderImpl<DocSetQuery>;

}  // namespace irs
