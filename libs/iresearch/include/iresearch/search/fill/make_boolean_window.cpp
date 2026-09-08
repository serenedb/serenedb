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

#include <limits>
#include <tuple>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/window_of.hpp"
#include "iresearch/search/fill/all_docs.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/fill/make_boolean.hpp"

namespace irs::fill {
namespace {

template<typename Lead, typename Others, typename Optional, typename Excludes,
         typename... Args>
Node::ptr MakeWindow(Args&&... args) {
  using Node = BooleanWindow<Lead, Others, Optional, Excludes>;
  return memory::make_managed<Impl<Node>>(std::piecewise_construct,
                                          std::forward<Args>(args)...);
}

template<typename Excludes, typename ExcludesArgs>
Node::ptr MakeWindowOfTerms(std::span<const search::PostingClause> terms,
                            const IndexInput& doc, ExcludesArgs&& excludes) {
  SDB_ASSERT(terms.size() >= 2);
  return ResolveInput(doc, [&]<typename Input> -> Node::ptr {
    using Leaf = PostingFill<Input>;
    using Others = AndLeaves<Leaf>;
    const auto& own = search::FieldOf(terms.front(), nullptr);
    const auto& front = search::CookieOf(terms.front());
    return MakeWindow<Leaf, Others, utils::Empty, Excludes>(
      std::forward_as_tuple(front, doc,
                            front.docs_count != 1 && search::BoundsOf(own),
                            front.docs_count != 1 && search::FreqOf(own)),
      std::forward_as_tuple(
        terms.size() - 1,
        [&](Leaf& leaf, size_t i) {
          const auto& other = search::FieldOf(terms[i + 1], nullptr);
          const auto& meta = search::CookieOf(terms[i + 1]);
          leaf.Prepare(meta, doc,
                       meta.docs_count != 1 && search::BoundsOf(other),
                       meta.docs_count != 1 && search::FreqOf(other));
        }),
      std::forward_as_tuple(), std::forward<ExcludesArgs>(excludes));
  });
}

Node::ptr MakeWindowNegation(
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters,
  const SubReader& segment) {
  SDB_ASSERT(!exclude_terms.empty() || !exclude_filters.empty());
  std::vector<Node::ptr> nodes;
  nodes.reserve(exclude_terms.size() + exclude_filters.size());
  const auto take = [&](Node::ptr node) {
    if (!node) {
      return false;
    }
    nodes.emplace_back(std::move(node));
    return true;
  };
  if (!search::VisitOrderedOf(
        exclude_terms, exclude_filters, false, 0,
        std::numeric_limits<size_t>::max(),
        [&](const search::PostingClause& term) {
          return take(FillOf(term, nullptr, segment));
        },
        [&](const QueryBuilder& child) {
          return take(child.PlanFill({}, ScoreMergeType::Noop));
        })) {
    return {};
  }
  using Excludes = FilledAndNot<SetLeaves<Erased>>;
  return MakeWindow<AllDocs, utils::Empty, utils::Empty, Excludes>(
    std::forward_as_tuple(segment), std::forward_as_tuple(),
    std::forward_as_tuple(),
    std::forward_as_tuple(
      std::piecewise_construct,
      std::forward_as_tuple(nodes.size(), [&](Erased& leaf, size_t i) {
        leaf = Erased{std::move(nodes[i])};
      })));
}

}  // namespace

Node::ptr MakeWindowDisjunctionDocs(
  std::span<const search::PostingClause> terms, const IndexInput* doc,
  std::vector<Node::ptr>& rest) {
  return BuildDense<Node::ptr>(
    terms, nullptr, doc, rest, [&]<typename Set>(auto&&... args) -> Node::ptr {
      return MakeWindow<utils::Empty, utils::Empty, search::OrGroup<Set>,
                        utils::Empty>(
        std::forward_as_tuple(), std::forward_as_tuple(),
        std::forward_as_tuple(std::forward<decltype(args)>(args)...),
        std::forward_as_tuple());
    });
}

Node::ptr MakeWindowConjunctionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment) {
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
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates) {
  if (terms.empty() && filters.empty()) {
    return MakeWindowNegation(exclude_terms, exclude_filters, segment);
  }
  if (terms.size() + filters.size() == 1) {
    if (search::HeadIsTerm(terms, filters)) {
      return {};
    }
    auto node = filters.front()->PlanFill({}, ScoreMergeType::Noop);
    if (!node) {
      return {};
    }
    return search::BuildExcludeSide<Node::ptr>(
      exclude_terms, exclude_filters, nullptr, segment, candidates,
      [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
        return MakeWindow<Erased, utils::Empty, utils::Empty,
                          ProbedAndNot<Exclude>>(
          std::forward_as_tuple(std::move(node)), std::forward_as_tuple(),
          std::forward_as_tuple(),
          std::forward_as_tuple(std::piecewise_construct,
                                std::forward<decltype(exclude)>(exclude)));
      });
  }
  const IndexInput* doc = nullptr;
  if (!search::WindowTerms(terms, filters, nullptr, doc)) {
    return {};
  }
  if (!search::DenseConjunction(terms,
                                static_cast<doc_id_t>(segment.docs_count()))) {
    return {};
  }
  return search::BuildExcludeSide<Node::ptr>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
      return MakeWindowOfTerms<ProbedAndNot<Exclude>>(
        terms, *doc,
        std::forward_as_tuple(std::piecewise_construct,
                              std::forward<decltype(exclude)>(exclude)));
    });
}

}  // namespace irs::fill
