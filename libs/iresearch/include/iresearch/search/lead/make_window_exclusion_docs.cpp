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

#include <cstdint>
#include <span>
#include <type_traits>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/window_of.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/lead/window_exclusion_docs.hpp"

namespace irs::lead {

Node::ptr MakeWindowExclusionDocs(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause>, std::span<const QueryBuilder::ptr>,
  uint32_t min_should_match, std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters,
  const SubReader& segment) {
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  if (min_should_match != 0) {
    return {};
  }
  if (must.empty() && must_filters.empty()) {
    return {};
  }
  const IndexInput* doc = nullptr;
  if (!search::WindowTerms(must, must_filters, nullptr, doc)) {
    return {};
  }
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if (must.size() >= 2 && !search::DenseConjunction(must, docs_count)) {
    return {};
  }
  const auto candidates = IncludeCandidates(must, must_filters, segment);
  return ResolveInput(*doc, [&]<typename Input> -> Node::ptr {
    using Leaf = PostingFill<Input>;
    const auto& front_own = FieldOf(must.front(), nullptr);
    const auto& front = CookieOf(must.front());
    const auto front_bounds =
      front.docs_count != 1 && search::BoundsOf(front_own);
    const auto front_freq = front.docs_count != 1 && search::FreqOf(front_own);
    const auto others = [&](Leaf& leaf, size_t i) {
      const auto& term = must[i + 1];
      const auto& own = FieldOf(term, nullptr);
      const auto& meta = CookieOf(term);
      leaf.Prepare(meta, *doc, meta.docs_count != 1 && search::BoundsOf(own),
                   meta.docs_count != 1 && search::FreqOf(own));
    };
    return search::BuildExcludeSide<Node::ptr>(
      excludes, exclude_filters, nullptr, segment, candidates,
      [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
        using Excludes = fill::ProbedAndNot<Exclude>;
        const auto lead =
          std::forward_as_tuple(front, *doc, front_bounds, front_freq);
        if (must.size() == 1) {
          using Node = WindowExclusionDocs<Leaf, utils::Empty, Excludes>;
          return memory::make_managed<Impl<Node>>(
            std::piecewise_construct, lead, std::forward_as_tuple(),
            std::forward_as_tuple(std::piecewise_construct,
                                  std::forward<decltype(exclude)>(exclude)));
        }
        using Others = fill::AndLeaves<Leaf>;
        using Node = WindowExclusionDocs<Leaf, Others, Excludes>;
        return memory::make_managed<Impl<Node>>(
          std::piecewise_construct, lead,
          std::forward_as_tuple(must.size() - 1, others),
          std::forward_as_tuple(std::piecewise_construct,
                                std::forward<decltype(exclude)>(exclude)));
      });
  });
}

}  // namespace irs::lead
