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

#pragma once

#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/probe/sparse_disjunction_docs.hpp"

namespace irs::search {

template<typename Term>
bool ExcludeTerms(std::span<const Term> terms,
                  std::span<const QueryBuilder::ptr> filters,
                  const TermReader* field, const IndexInput*& doc) noexcept {
  if (!filters.empty() || terms.empty()) {
    return false;
  }
  doc = DocOf(FieldOf(terms.front(), field));
  return doc != nullptr;
}

template<typename Result, typename Input, typename Term, typename Make>
Result BuildExcludeSideOf(std::span<const Term> metas,
                          std::span<const QueryBuilder::ptr> filters,
                          const TermReader* field, const SubReader& segment,
                          uint64_t candidates, Make&& make) {
  SDB_ASSERT(!metas.empty() || !filters.empty());
  const IndexInput* doc = nullptr;
  if (ExcludeTerms(metas, filters, field, doc)) {
    const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
    if (TakeProbeBitset(metas, *doc, docs_count, candidates)) {
      auto buckets = DisjunctionBuckets(metas, field);
      return make.template operator()<probe::BitsetDocs>(
        std::forward_as_tuple(BuildBitset(buckets, *doc, docs_count)));
    }
    const auto concrete = [&]<typename In> -> Result {
      using Probe = PostingProbe<In>;
      if (metas.size() == 1) {
        const auto& own = FieldOf(metas.front(), field);
        return make.template operator()<Probe>(std::forward_as_tuple(
          CookieOf(metas.front()), *DocOf(own), LayoutOf(own), BoundsOf(own)));
      }
      return make.template operator()<probe::SparseDisjunctionDocs<Probe>>(
        std::forward_as_tuple(metas.size(), [&](Probe& probe, size_t i) {
          const auto& own = FieldOf(metas[i], field);
          probe.Prepare(CookieOf(metas[i]), *DocOf(own), LayoutOf(own),
                        BoundsOf(own));
        }));
    };
    if constexpr (std::is_void_v<Input>) {
      return ResolveInput(*doc, concrete);
    } else {
      return concrete.template operator()<Input>();
    }
  }
  std::vector<probe::Erased> probes;
  probes.reserve(metas.size() + filters.size());
  const auto ask = [&](const PostingClause& posting,
                       const QueryBuilder* child) noexcept {
    auto node = ProbeOf(posting, child, segment, candidates);
    if (!node) {
      return false;
    }
    probes.emplace_back(std::move(node));
    return true;
  };
  if (!VisitOrderedOf(
        metas, filters, false, 0, std::numeric_limits<size_t>::max(),
        [&](const Term& term) { return ask(ClauseOf(term, field), nullptr); },
        [&](const QueryBuilder& child) {
          return ask(PostingClause{TermState{nullptr, PostingMeta{}}}, &child);
        })) {
    return {};
  }
  if (probes.size() == 1) {
    return make.template operator()<probe::Erased>(
      std::forward_as_tuple(std::move(probes.front())));
  }
  using Exclude = probe::SparseDisjunctionDocs<probe::Erased>;
  return make.template operator()<Exclude>(std::forward_as_tuple(
    probes.size(),
    [&](probe::Erased& leaf, size_t i) { leaf = std::move(probes[i]); }));
}

template<typename Result, typename Term, typename Make>
Result BuildExcludeSide(std::span<const Term> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        const TermReader* field, const SubReader& segment,
                        uint64_t candidates, Make&& make) {
  return BuildExcludeSideOf<Result, void, Term>(
    terms, filters, field, segment, candidates, std::forward<Make>(make));
}

}  // namespace irs::search
