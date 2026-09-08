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

#include <tuple>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/window_of.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/lead/make_boolean.hpp"

namespace irs::lead {
namespace {

template<typename Lead, typename Others, typename Optional, typename Excludes,
         typename... Args>
Node::ptr MakeWindow(Args&&... args) {
  using Node = BooleanWindow<Lead, Others, Optional, Excludes>;
  return memory::make_managed<Impl<Node>>(std::piecewise_construct,
                                          std::forward<Args>(args)...);
}

template<typename Excludes, typename ExcludesArgs>
Node::ptr MakeWindowOfTerms(std::span<const PostingClause> terms,
                            const IndexInput& doc, ExcludesArgs&& excludes) {
  SDB_ASSERT(!terms.empty());
  return ResolveInput(doc, [&]<typename Input> -> Node::ptr {
    using Leaf = PostingFill<Input>;
    const auto& own = FieldOf(terms.front(), nullptr);
    const auto& front = CookieOf(terms.front());
    const auto lead = std::forward_as_tuple(
      front, doc, front.docs_count != 1 && search::BoundsOf(own),
      front.docs_count != 1 && search::FreqOf(own));
    if (terms.size() == 1) {
      return MakeWindow<Leaf, utils::Empty, utils::Empty, Excludes>(
        lead, std::forward_as_tuple(), std::forward_as_tuple(),
        std::forward<ExcludesArgs>(excludes));
    }
    using Others = fill::AndLeaves<Leaf>;
    return MakeWindow<Leaf, Others, utils::Empty, Excludes>(
      lead,
      std::forward_as_tuple(
        terms.size() - 1,
        [&](Leaf& leaf, size_t i) {
          const auto& other = FieldOf(terms[i + 1], nullptr);
          const auto& meta = CookieOf(terms[i + 1]);
          leaf.Prepare(meta, doc,
                       meta.docs_count != 1 && search::BoundsOf(other),
                       meta.docs_count != 1 && search::FreqOf(other));
        }),
      std::forward_as_tuple(), std::forward<ExcludesArgs>(excludes));
  });
}

}  // namespace

Node::ptr MakeWindowDisjunctionDocs(std::span<const PostingClause> terms,
                                    const IndexInput* doc,
                                    std::vector<FillNode::ptr>& rest) {
  return BuildDense<Node::ptr>(
    terms, nullptr, doc, rest, [&]<typename Set>(auto&&... args) -> Node::ptr {
      return MakeWindow<utils::Empty, utils::Empty, search::OrGroup<Set>,
                        utils::Empty>(
        std::forward_as_tuple(), std::forward_as_tuple(),
        std::forward_as_tuple(std::forward<decltype(args)>(args)...),
        std::forward_as_tuple());
    });
}

Node::ptr MakeWindowConjunctionDocs(std::span<const PostingClause> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    const SubReader& segment) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const IndexInput* doc = nullptr;
  if (!search::WindowTerms(terms, filters, nullptr, doc)) {
    return {};
  }
  if (!search::DenseConjunction(terms,
                                static_cast<doc_id_t>(segment.docs_count()))) {
    return {};
  }
  return MakeWindowOfTerms<utils::Empty>(terms, *doc, std::forward_as_tuple());
}

Node::ptr MakeWindowExclusionDocs(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates) {
  SDB_ASSERT(!must.empty() || !must_filters.empty());
  const IndexInput* doc = nullptr;
  if (!search::WindowTerms(must, must_filters, nullptr, doc)) {
    return {};
  }
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if (must.size() >= 2 && !search::DenseConjunction(must, docs_count)) {
    return {};
  }
  return search::BuildExcludeSide<Node::ptr>(
    excludes, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
      return MakeWindowOfTerms<fill::ProbedAndNot<Exclude>>(
        must, *doc,
        std::forward_as_tuple(std::piecewise_construct,
                              std::forward<decltype(exclude)>(exclude)));
    });
}

}  // namespace irs::lead
