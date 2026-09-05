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

#include "basics/down_cast.h"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/fill/all_docs.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/all_docs.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"
#include "iresearch/search/lead/wildcard_ngram_slots_docs.hpp"
#include "iresearch/search/lead/window_disjunction_docs.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/probe/all_docs.hpp"
#include "iresearch/search/probe/sparse_disjunction_docs.hpp"
#include "iresearch/search/probe/two_phase_docs.hpp"
#include "iresearch/search/probe/wildcard_ngram_slots_docs.hpp"
#include "iresearch/search/term_query.hpp"
#include "iresearch/search/wildcard_ngram_filter.hpp"

namespace irs::search {

inline const TermState& AsTerm(const QueryBuilder& query) noexcept {
  SDB_ASSERT(query.Kind() == QueryKind::Term);
  return sdb::basics::downCast<TermQuery>(query).State();
}

inline const MultiTermState& AsTerms(const QueryBuilder& query) noexcept {
  SDB_ASSERT(query.Kind() == QueryKind::Terms);
  return sdb::basics::downCast<MultiTermQuery>(query).State();
}

template<template<typename> class Impl, typename Result, typename... Prefix>
Result MakeWildcardNgram(const WildcardNgramQuery& query,
                         uint64_t interrogations, Prefix&&... prefix) {
  constexpr bool kProbed = std::is_same_v<Result, ProbeNode::ptr>;
  SDB_ASSERT(query.Kind() != QueryKind::Empty);
  const auto recipe = query.MakeRecipe();
  const auto& ngrams = query.NGrams();
  const auto kind = ngrams.Kind();
  SDB_ASSERT(kind != QueryKind::Empty);

  if (kind == QueryKind::All) {
    const auto& segment = query.Segment();
    if constexpr (kProbed) {
      using Slots = probe::WildcardNgramSlotsDocs<probe::AllDocs>;
      return memory::make_managed<Impl<probe::TwoPhaseDocs<Slots>>>(
        std::forward<Prefix>(prefix)..., std::piecewise_construct,
        std::forward_as_tuple(segment), recipe);
    } else {
      using Slots = lead::WildcardNgramSlotsDocs<lead::AllDocs>;
      return memory::make_managed<Impl<lead::TwoPhaseDocs<Slots>>>(
        std::forward<Prefix>(prefix)..., std::piecewise_construct,
        std::forward_as_tuple(segment), recipe);
    }
  }

  const auto erased = [&]() -> Result {
    if constexpr (kProbed) {
      auto node = ngrams.PlanProbe({}, interrogations);
      if (!node) {
        return {};
      }
      using Slots = probe::WildcardNgramSlotsDocs<probe::Erased>;
      return memory::make_managed<Impl<probe::TwoPhaseDocs<Slots>>>(
        std::forward<Prefix>(prefix)..., std::piecewise_construct,
        std::forward_as_tuple(std::move(node)), recipe);
    } else {
      auto node = ngrams.PlanLead({});
      if (!node) {
        return {};
      }
      using Slots = lead::WildcardNgramSlotsDocs<lead::Erased>;
      return memory::make_managed<Impl<lead::TwoPhaseDocs<Slots>>>(
        std::forward<Prefix>(prefix)..., std::piecewise_construct,
        std::forward_as_tuple(std::move(node)), recipe);
    }
  };

  if (kind != QueryKind::Term && kind != QueryKind::Terms) {
    return erased();
  }

  const auto* const field =
    kind == QueryKind::Term ? AsTerm(ngrams).reader : AsTerms(ngrams).Reader();
  if (field == nullptr || DocOf(*field) == nullptr) {
    return erased();
  }
  const auto make = [&]<typename Approx>(auto&&... args) -> Result {
    const auto one =
      std::forward_as_tuple(std::forward<decltype(args)>(args)...);
    if constexpr (kProbed) {
      using Slots = probe::WildcardNgramSlotsDocs<Approx>;
      return memory::make_managed<Impl<probe::TwoPhaseDocs<Slots>>>(
        std::forward<Prefix>(prefix)..., std::piecewise_construct, one, recipe);
    } else {
      using Slots = lead::WildcardNgramSlotsDocs<Approx>;
      return memory::make_managed<Impl<lead::TwoPhaseDocs<Slots>>>(
        std::forward<Prefix>(prefix)..., std::piecewise_construct, one, recipe);
    }
  };

  const auto* single =
    kind == QueryKind::Term ? &AsTerm(ngrams).cookie : nullptr;

  if (single != nullptr) {
    return ResolveInput(*DocOf(*field), [&]<typename Input> -> Result {
      if constexpr (kProbed) {
        using Leaf = PostingProbe<Input>;
        return make.template operator()<Leaf>(
          *single, *DocOf(*field), LayoutOf(*field), BoundsOf(*field));
      } else {
        using Leaf = PostingLead<Input>;
        return make.template operator()<Leaf>(
          *single, *DocOf(*field), LayoutOf(*field), BoundsOf(*field));
      }
    });
  }

  const auto& terms = AsTerms(ngrams).Terms();
  SDB_ASSERT(terms.size() > 1);
  if constexpr (kProbed) {
    return ResolveInput(*DocOf(*field), [&]<typename Input> -> Result {
      using Leaf = PostingProbe<Input>;
      return make.template operator()<probe::SparseDisjunctionDocs<Leaf>>(
        terms.size(), [&](Leaf& leaf, size_t i) {
          leaf.Prepare(terms[i].cookie, *DocOf(*field), LayoutOf(*field),
                       BoundsOf(*field));
        });
    });
  } else {
    return ResolveInput(*DocOf(*field), [&]<typename Input> -> Result {
      using Leaf = PostingFill<Input>;
      using Node = lead::WindowDisjunctionDocs<fill::SetLeaves<Leaf>>;
      return make.template operator()<Node>(
        std::piecewise_construct,
        std::forward_as_tuple(terms.size(), [&](Leaf& leaf, size_t i) {
          leaf.Prepare(terms[i].cookie, *DocOf(*field), BoundsOf(*field),
                       FreqOf(*field));
        }));
    });
  }
}

}  // namespace irs::search
