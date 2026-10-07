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

#include <span>
#include <tuple>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/probe_leaves.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/probe/all_docs.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/make_boolean_sparse_impl.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::probe {

Node::ptr MakeSparseConjunctionDocs(
  std::span<const detail::PostingClause> terms,
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
  std::span<const detail::PostingClause> terms,
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

Node::ptr MakeSparseThresholdDocs(std::span<const detail::PostingClause> terms,
                                  std::span<const QueryBuilder::ptr> filters,
                                  const SubReader& segment, uint32_t min_match,
                                  uint64_t interrogations) {
  SDB_ASSERT(min_match > 1);
  SDB_ASSERT(terms.size() + filters.size() >= min_match);
  return detail::BuildProbeLeaves<Node::ptr>(
    terms, filters, nullptr, segment, interrogations,
    detail::ProbeOrder::Densest,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return detail::ResolveArity<detail::kRunArity, detail::kRunFloor>(
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

}  // namespace irs::probe
