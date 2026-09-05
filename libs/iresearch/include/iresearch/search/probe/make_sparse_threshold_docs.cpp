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
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/sparse_threshold_docs.hpp"

namespace irs::probe {

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
          using Node = SparseThresholdDocs<Leaf, N>;
          return memory::make_managed<Impl<Node>>(
            size, std::forward<decltype(init)>(init), min_match);
        });
    });
}

}  // namespace irs::probe
