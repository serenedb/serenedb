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
#include <span>

#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/docs/empty.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/search/states/term_state.hpp"

namespace irs::docs {

using search::LeadNode;

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx = {});

Root::ptr Make(const TermQuery& query, const Context& ctx);
Root::ptr Make(const MultiTermQuery& query, const Context& ctx);
Root::ptr Make(const FixedPhraseQuery& query, const Context& ctx);
Root::ptr Make(const VariadicPhraseQuery& query, const Context& ctx);
Root::ptr Make(const NGramSimilarityQuery& query, const Context& ctx);
Root::ptr Make(const AllQuery& query, const Context& ctx);
Root::ptr Make(const WildcardNgramQuery& query, const Context& ctx);
Root::ptr Make(const ByNestedQuery& query, const Context& ctx);
Root::ptr Make(const RangeVectorQuery& query, const Context& ctx);

inline Root::ptr Make(const EmptyQueryBuilder&, const Context& ctx) {
  return memory::make_managed<Empty>();
}

inline Root::ptr Make(const KnnVectorQuery&, const Context& ctx) { return {}; }
Root::ptr Make(const BooleanQuery& query, const Context& ctx);
template<typename Parser, typename Acceptor>
Root::ptr Make(const GeoQuery<Parser, Acceptor>& query, const Context& ctx);

Root::ptr MakeConjunction(std::span<const search::PostingClause> terms,
                          std::span<const QueryBuilder::ptr> filters,
                          const SubReader& segment, const Context& ctx);
Root::ptr MakeDisjunction(std::span<const search::PostingClause> terms,
                          std::span<const QueryBuilder::ptr> filters,
                          const SubReader& segment, const Context& ctx);
Root::ptr MakeThreshold(std::span<const search::PostingClause> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        const SubReader& segment, uint32_t min_match,
                        const Context& ctx);
Root::ptr MakeRequired(const BooleanQuery& query, const SubReader& segment,
                       const Context& ctx);
Root::ptr MakeExclusion(const BooleanQuery& query, const SubReader& segment,
                        const Context& ctx);

Root::ptr MakePosting(const search::PostingClause& posting,
                      const SubReader& segment, const Context& ctx);

Root::ptr MakeSinglePosting(doc_id_t doc, const Context& ctx);

Root::ptr MakeAll(doc_id_t docs_count, const Context& ctx);

}  // namespace irs::docs
