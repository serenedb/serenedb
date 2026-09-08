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
#include "iresearch/search/count/make_boolean.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/leaves.hpp"

namespace irs::count {
namespace {

template<typename Lead, typename Others, typename Optional, typename Excludes,
         typename... Args>
Root::ptr MakeWindow(const Context& ctx, Args&&... args) {
  return MakeShape<BooleanWindow, Lead, Others, Optional, Excludes>(
    ctx, std::piecewise_construct, std::forward<Args>(args)...);
}

template<typename Excludes, typename ExcludesArgs>
Root::ptr MakeWindowOfTerms(std::span<const search::PostingClause> terms,
                            const IndexInput& doc, ExcludesArgs&& excludes,
                            const Context& ctx) {
  SDB_ASSERT(terms.size() >= 2);
  return ResolveInput(doc, [&]<typename Input> -> Root::ptr {
    using Leaf = PostingFill<Input>;
    using Others = fill::AndLeaves<Leaf>;
    const auto& own = search::FieldOf(terms.front(), nullptr);
    const auto& front = search::CookieOf(terms.front());
    return MakeWindow<Leaf, Others, utils::Empty, Excludes>(
      ctx,
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

Root::ptr MakeWindowOfNodes(std::span<const search::PostingClause> terms,
                            std::span<const QueryBuilder::ptr> filters,
                            const SubReader& segment, const Context& ctx) {
  std::vector<FillNode::ptr> nodes;
  nodes.reserve(terms.size() + filters.size());
  const auto take = [&](FillNode::ptr node) {
    if (!node) {
      return false;
    }
    nodes.emplace_back(std::move(node));
    return true;
  };
  if (!search::VisitOrderedOf(
        terms, filters, true, 0, std::numeric_limits<size_t>::max(),
        [&](const search::PostingClause& term) {
          return take(FillOf(term, nullptr, segment));
        },
        [&](const QueryBuilder& child) {
          return take(child.PlanFill({}, ScoreMergeType::Noop));
        })) {
    return {};
  }
  using Others = fill::AndLeaves<fill::Erased>;
  return MakeWindow<fill::Erased, Others, utils::Empty, utils::Empty>(
    ctx, std::forward_as_tuple(std::move(nodes.front())),
    std::forward_as_tuple(nodes.size() - 1,
                          [&](fill::Erased& leaf, size_t i) {
                            leaf = fill::Erased{std::move(nodes[i + 1])};
                          }),
    std::forward_as_tuple(), std::forward_as_tuple());
}

}  // namespace

Root::ptr MakeWindowDisjunction(std::span<const search::PostingClause> terms,
                                const IndexInput* doc,
                                std::vector<FillNode::ptr>& rest,
                                const Context& ctx) {
  return BuildDense<Root::ptr>(
    terms, nullptr, doc, rest, [&]<typename Set>(auto&&... args) -> Root::ptr {
      return MakeWindow<utils::Empty, utils::Empty, search::OrGroup<Set>,
                        utils::Empty>(
        ctx, std::forward_as_tuple(), std::forward_as_tuple(),
        std::forward_as_tuple(std::forward<decltype(args)>(args)...),
        std::forward_as_tuple());
    });
}

Root::ptr MakeWindowConjunction(std::span<const search::PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                const SubReader& segment, const Context& ctx) {
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if (!filters.empty()) {
    if (search::HeadEstimate(terms, filters) <
        docs_count / search::kDensityThresholdInverse) {
      return {};
    }
    return MakeWindowOfNodes(terms, filters, segment, ctx);
  }
  const IndexInput* doc = nullptr;
  if (!search::WindowTerms(terms, filters, nullptr, doc)) {
    return {};
  }
  if (!search::DenseConjunction(terms, docs_count)) {
    return {};
  }
  return MakeWindowOfTerms<utils::Empty>(terms, *doc, std::forward_as_tuple(),
                                         ctx);
}

Root::ptr MakeWindowExclusion(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  if (terms.size() + filters.size() == 1) {
    if (search::HeadIsTerm(terms, filters)) {
      return {};
    }
    auto node = filters.front()->PlanFill({}, ScoreMergeType::Noop);
    if (!node) {
      return {};
    }
    return search::BuildExcludeSide<Root::ptr>(
      exclude_terms, exclude_filters, nullptr, segment, candidates,
      [&]<typename Exclude>(auto&& exclude) -> Root::ptr {
        return MakeWindow<fill::Erased, utils::Empty, utils::Empty,
                          fill::ProbedAndNot<Exclude>>(
          ctx, std::forward_as_tuple(std::move(node)), std::forward_as_tuple(),
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
  return search::BuildExcludeSide<Root::ptr>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Root::ptr {
      return MakeWindowOfTerms<fill::ProbedAndNot<Exclude>>(
        terms, *doc,
        std::forward_as_tuple(std::piecewise_construct,
                              std::forward<decltype(exclude)>(exclude)),
        ctx);
    });
}

}  // namespace irs::count
