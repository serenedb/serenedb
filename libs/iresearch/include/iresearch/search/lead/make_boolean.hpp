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
#include <tuple>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/boolean_groups.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/lead/boolean_window.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"

namespace irs::lead {

Node::ptr MakeConjunctionDocs(std::span<const PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment);
Node::ptr MakeDisjunctionDocs(std::span<const PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment);
Node::ptr MakeThresholdDocs(std::span<const PostingClause> terms,
                            std::span<const QueryBuilder::ptr> filters,
                            const SubReader& segment, uint32_t min_match);
Node::ptr MakeExclusionDocs(std::span<const PostingClause> must,
                            std::span<const QueryBuilder::ptr> must_filters,
                            std::span<const PostingClause> should,
                            std::span<const QueryBuilder::ptr> should_filters,
                            uint32_t min_should_match,
                            std::span<const PostingClause> excludes,
                            std::span<const QueryBuilder::ptr> exclude_filters,
                            const SubReader& segment);

Node::ptr MakeBitsetDocs(const search::BooleanGroups& groups,
                         const SubReader& segment);

Node::ptr MakeWindowDisjunctionDocs(std::span<const PostingClause> terms,
                                    const IndexInput* doc,
                                    std::vector<FillNode::ptr>& rest);
Node::ptr MakeWindowConjunctionDocs(std::span<const PostingClause> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    const SubReader& segment);
Node::ptr MakeWindowExclusionDocs(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates);

Node::ptr MakeSparseConjunctionDocs(std::span<const PostingClause> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    const SubReader& segment);
Node::ptr MakeSparseConjunctionWithDocs(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters, const SubReader& segment,
  ProbeNode::ptr other);
Node::ptr MakeSparseExclusionDocs(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates);
Node::ptr MakeSparseExclusionOfDocs(
  Node::ptr include, std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates);

template<typename Term>
Node::ptr MakeWindowDisjunctionOfTermsDocs(std::span<const Term> terms,
                                           const TermReader* field,
                                           const IndexInput& doc) {
  SDB_ASSERT(terms.size() > 1);
  return ResolveInput(doc, [&]<typename Input> -> Node::ptr {
    using Leaf = PostingFill<Input>;
    using Optional = search::OrGroup<fill::SetLeaves<Leaf>>;
    using Node =
      BooleanWindow<utils::Empty, utils::Empty, Optional, utils::Empty>;
    return memory::make_managed<Impl<Node>>(
      std::piecewise_construct, std::forward_as_tuple(),
      std::forward_as_tuple(),
      std::forward_as_tuple(
        terms.size(),
        [&](Leaf& leaf, size_t i) {
          const auto& own = FieldOf(terms[i], field);
          const auto& meta = CookieOf(terms[i]);
          SDB_ASSERT(meta.docs_count != 0);
          leaf.Prepare(meta, doc, meta.docs_count != 1 && search::BoundsOf(own),
                       meta.docs_count != 1 && search::FreqOf(own));
        }),
      std::forward_as_tuple());
  });
}

template<typename Term>
Node::ptr MakeDisjunctionOfTermsDocs(std::span<const Term> terms,
                                     const TermReader* field,
                                     const IndexInput& doc,
                                     doc_id_t docs_count) {
  SDB_ASSERT(terms.size() > 1);
  if (search::TakeBitset<Node::ptr>(terms, doc, docs_count)) {
    return search::MakeBitsetOf<Node::ptr>(terms, field, doc, docs_count,
                                           nullptr);
  }
  return MakeWindowDisjunctionOfTermsDocs<Term>(terms, field, doc);
}

}  // namespace irs::lead
