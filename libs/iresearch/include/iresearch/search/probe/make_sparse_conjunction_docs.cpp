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
#include <type_traits>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/probe_leaves.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/sparse_conjunction_docs.hpp"

namespace irs::probe {

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
  return search::BuildProbeLeaves<Node::ptr>(
    terms, filters, nullptr, segment, interrogations,
    search::ProbeOrder::Narrowest,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return search::ResolveArity<search::kRunArity, search::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          using Node = SparseConjunctionDocs<Leaf, N>;
          return memory::make_managed<Impl<Node>>(
            size, std::forward<decltype(init)>(init));
        });
    });
}

Node::ptr MakeSparseConjunctionWithDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  uint64_t interrogations, Node::ptr other) {
  auto required =
    MakeSparseConjunctionDocs(terms, filters, segment, interrogations);
  if (!required || !other) {
    return {};
  }
  using Node = BothLeaves<Erased, Erased>;
  return memory::make_managed<Impl<Node>>(Erased{std::move(required)},
                                          Erased{std::move(other)});
}

}  // namespace irs::probe
