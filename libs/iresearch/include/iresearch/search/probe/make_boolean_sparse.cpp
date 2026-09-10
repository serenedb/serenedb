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

#include <algorithm>
#include <span>
#include <tuple>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/probe_leaves.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/probe/all_docs.hpp"
#include "iresearch/search/probe/boolean_sparse.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/plan.hpp"

namespace irs::probe {
namespace {

template<typename Musts, typename Optional, typename Excludes, typename... Args>
Node::ptr MakeSparse(Args&&... args) {
  using Node = BooleanSparse<Musts, Optional, Excludes>;
  return memory::make_managed<Impl<Node>>(std::piecewise_construct,
                                          std::forward<Args>(args)...);
}

template<typename Make>
Node::ptr BuildMusts(std::span<const search::PostingClause> terms,
                     std::span<const QueryBuilder::ptr> filters,
                     const SubReader& segment, uint64_t interrogations,
                     Make&& make) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  if (terms.size() + filters.size() == 1) {
    if (filters.empty()) {
      return ResolvePostingDocs<Node::ptr>(
        terms.front(), [&]<typename Leaf>(auto&&... args) -> Node::ptr {
          return make.template operator()<Leaf>(
            std::forward_as_tuple(std::forward<decltype(args)>(args)...));
        });
    }
    auto node = filters.front()->PlanProbe({}, interrogations);
    if (!node) {
      return {};
    }
    return make.template operator()<Erased>(
      std::forward_as_tuple(std::move(node)));
  }
  return search::BuildProbeLeaves<Node::ptr>(
    terms, filters, nullptr, segment, interrogations,
    search::ProbeOrder::Narrowest,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return search::ResolveArity<search::kRunArity, search::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          return make.template operator()<AndLeaves<Leaf, N>>(
            std::forward_as_tuple(size, std::forward<decltype(init)>(init)));
        });
    });
}

}  // namespace

Node::ptr MakeSparseConjunctionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  uint64_t interrogations) {
  const auto size = terms.size() + filters.size();
  if (size == 0) {
    return MakeAllDocs(segment);
  }
  if (size == 1) {
    return filters.empty() ? MakePostingDocs(terms.front(), segment)
                           : filters.front()->PlanProbe({}, interrogations);
  }
  return BuildMusts(terms, filters, segment, interrogations,
                    [&]<typename Musts>(auto&& musts) -> Node::ptr {
                      return MakeSparse<Musts, utils::Empty, utils::Empty>(
                        std::forward<decltype(musts)>(musts),
                        std::forward_as_tuple(), std::forward_as_tuple());
                    });
}

Node::ptr MakeSparseConjunctionWithDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  uint64_t interrogations, Node::ptr other) {
  SDB_ASSERT(other);
  SDB_ASSERT(!terms.empty() || !filters.empty());
  return BuildMusts(terms, filters, segment, interrogations,
                    [&]<typename Musts>(auto&& musts) -> Node::ptr {
                      return MakeSparse<Musts, Erased, utils::Empty>(
                        std::forward<decltype(musts)>(musts),
                        std::forward_as_tuple(std::move(other)),
                        std::forward_as_tuple());
                    });
}

Node::ptr MakeSparseThresholdDocs(std::span<const search::PostingClause> terms,
                                  std::span<const QueryBuilder::ptr> filters,
                                  const SubReader& segment, uint32_t min_match,
                                  uint64_t interrogations) {
  SDB_ASSERT(min_match > 1);
  SDB_ASSERT(terms.size() + filters.size() >= min_match);
  return search::BuildProbeLeaves<Node::ptr>(
    terms, filters, nullptr, segment, interrogations,
    search::ProbeOrder::Densest,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return search::ResolveArity<search::kRunArity, search::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          return MakeSparse<utils::Empty, ThresholdLeaves<Leaf, N>,
                            utils::Empty>(
            std::forward_as_tuple(),
            std::forward_as_tuple(size, std::forward<decltype(init)>(init),
                                  min_match),
            std::forward_as_tuple());
        });
    });
}

Node::ptr MakeSparseExclusionDocs(
  std::span<const search::PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const search::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters, uint32_t min_should_match,
  std::span<const search::PostingClause> exclude,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t interrogations) {
  SDB_ASSERT(!exclude.empty() || !exclude_filters.empty());
  const bool no_must = must.empty() && must_filters.empty();
  const uint64_t docs_count = segment.docs_count();
  uint64_t lead = search::IncludeCandidates(must, must_filters, segment);
  if (no_must && min_should_match != 0) {
    lead = std::min(docs_count,
                    search::LeadCandidates(should, should_filters,
                                           static_cast<doc_id_t>(docs_count)));
  }
  const auto reach = std::max<uint64_t>(
    1, docs_count == 0 ? 0 : interrogations * lead / docs_count);
  Node::ptr optional;
  if (min_should_match != 0) {
    optional = BuildOptionalProbe(should, should_filters, min_should_match,
                                  segment, no_must ? interrogations : reach);
    if (!optional) {
      return {};
    }
  }
  const auto excluded = [&]<typename Musts, typename Optional>(
                          auto&& musts, auto&& optional_args) -> Node::ptr {
    return search::BuildExcludeSide<Node::ptr>(
      exclude, exclude_filters, nullptr, segment, reach, lead,
      [&]<typename Exclude>(auto&& excludes) -> Node::ptr {
        return MakeSparse<Musts, Optional, Exclude>(
          std::forward<decltype(musts)>(musts),
          std::forward<decltype(optional_args)>(optional_args),
          std::forward<decltype(excludes)>(excludes));
      });
  };
  if (no_must) {
    if (!optional) {
      return excluded.template operator()<AllDocs, utils::Empty>(
        std::forward_as_tuple(segment), std::forward_as_tuple());
    }
    return excluded.template operator()<utils::Empty, Erased>(
      std::forward_as_tuple(), std::forward_as_tuple(std::move(optional)));
  }
  return BuildMusts(
    must, must_filters, segment, interrogations,
    [&]<typename Musts>(auto&& musts) -> Node::ptr {
      if (!optional) {
        return excluded.template operator()<Musts, utils::Empty>(
          std::forward<decltype(musts)>(musts), std::forward_as_tuple());
      }
      return excluded.template operator()<Musts, Erased>(
        std::forward<decltype(musts)>(musts),
        std::forward_as_tuple(std::move(optional)));
    });
}

}  // namespace irs::probe
