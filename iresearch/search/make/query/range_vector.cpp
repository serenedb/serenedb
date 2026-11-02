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
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/vector_of.hpp"
#include "iresearch/search/queries/query_builder_impl.tpp"
#include "iresearch/search/queries/vector_similarity_query.hpp"

namespace irs::detail {

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const RangeVectorQuery& query,
                                  const Ctx& ctx) {
  return ResolveBool(query.Inclusive(), [&]<bool Inclusive>() {
    if constexpr (kScoredPlan<Family, Ctx>) {
      return MakeVectorScored<Family, RadiusGate<Inclusive>>(query, ctx,
                                                             query.Threshold());
    } else {
      return MakeVectorDocs<Family, RadiusGate<Inclusive>>(query, ctx,
                                                           query.Threshold());
    }
  });
}

}  // namespace irs::detail
namespace irs {

template class QueryBuilderImpl<RangeVectorQuery>;

}  // namespace irs
