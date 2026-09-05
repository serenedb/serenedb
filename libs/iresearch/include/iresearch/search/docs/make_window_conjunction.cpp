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
#include <span>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/window_of.hpp"
#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/docs/window.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/leaves.hpp"

namespace irs::docs {
namespace {

Root::ptr MakeWindowOfNodes(std::span<const search::PostingClause> terms,
                            std::span<const QueryBuilder::ptr> filters,
                            const SubReader& segment, const Context& ctx) {
  std::vector<search::FillNode::ptr> nodes;
  nodes.reserve(terms.size() + filters.size());
  const auto take = [&](search::FillNode::ptr node) {
    if (!node) {
      return false;
    }
    nodes.emplace_back(std::move(node));
    return true;
  };
  if (!search::VisitOrderedOf(
        terms, filters, true, 0, std::numeric_limits<size_t>::max(),
        [&](const search::PostingClause& term) {
          return take(search::FillOf(term, nullptr, segment));
        },
        [&](const QueryBuilder& child) {
          return take(child.PlanFill({}, ScoreMergeType::Noop));
        })) {
    return {};
  }
  using Others = fill::AndLeaves<fill::Erased>;
  return MakeShape<Window, fill::Erased, Others, utils::Empty>(
    ctx, std::piecewise_construct,
    std::forward_as_tuple(std::move(nodes.front())),
    std::forward_as_tuple(nodes.size() - 1,
                          [&](fill::Erased& leaf, size_t i) {
                            leaf = fill::Erased{std::move(nodes[i + 1])};
                          }),
    std::forward_as_tuple());
}

}  // namespace

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
  return MakeWindowOfTerms<utils::Empty>(terms, nullptr, *doc,
                                         std::forward_as_tuple(), ctx);
}

}  // namespace irs::docs
