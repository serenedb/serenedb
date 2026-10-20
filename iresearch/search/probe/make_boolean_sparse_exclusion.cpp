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
#include <cstdint>
#include <span>
#include <tuple>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/exclusion_of.hpp"
#include "iresearch/search/detail/with_mask.hpp"
#include "iresearch/search/probe/all_docs.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/make_boolean_sparse_impl.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::probe {

Node::ptr MakeSparseExclusionDocs(
  std::span<const detail::PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const detail::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters, uint32_t min_should_match,
  std::span<const detail::PostingClause> exclude,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t interrogations) {
  SDB_ASSERT(!exclude.empty() || !exclude_filters.empty());
  const bool no_must = must.empty() && must_filters.empty();
  const uint64_t docs_count = segment.docs_count();
  uint64_t lead = detail::IncludeCandidates(must, must_filters, segment);
  if (no_must && min_should_match != 0) {
    lead = std::min(docs_count,
                    detail::LeadCandidates(should, should_filters,
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
    return detail::BuildExcludeSide<Node::ptr>(
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
      if (detail::OnlyMask(exclude, exclude_filters)) {
        return MakeLiveDocs(segment);
      }
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
