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

#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/wildcard_ngram_slots.hpp"
#include "iresearch/search/fill/all_docs.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/filters/wildcard_ngram_filter.hpp"
#include "iresearch/search/lead/all_docs.hpp"
#include "iresearch/search/lead/boolean_window.hpp"
#include "iresearch/search/lead/two_phase.hpp"
#include "iresearch/search/probe/all_docs.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/two_phase.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::detail {

inline const TermState& AsTerm(const QueryBuilder& query) noexcept {
  SDB_ASSERT(query.Kind() == QueryKind::Term);
  return irs::utils::downCast<TermQuery>(query).State();
}

inline const MultiTermState& AsTerms(const QueryBuilder& query) noexcept {
  SDB_ASSERT(query.Kind() == QueryKind::Terms);
  return irs::utils::downCast<MultiTermQuery>(query).State();
}

template<typename Result, bool Scored, typename F>
Result Build(WildcardNGramKind, const WildcardNGramQuery& query,
             uint64_t interrogations, F&& f) {
  static_assert(!Scored);
  constexpr bool kProbed = std::is_same_v<Result, ProbeNode::ptr>;
  SDB_ASSERT(query.Kind() != QueryKind::Empty);
  const auto& ngrams = query.NGrams();
  const auto kind = ngrams.Kind();
  SDB_ASSERT(kind != QueryKind::Empty);

  const auto recipe = query.MakeRecipe();
  const auto make = [&]<typename Approx>(auto&&... args) -> Result {
    return f.template operator()<WildcardNGramSlots<Approx>>(
      std::piecewise_construct,
      std::forward_as_tuple(std::forward<decltype(args)>(args)...), recipe);
  };

  if (kind == QueryKind::All) {
    using All = std::conditional_t<kProbed, probe::AllDocs, lead::AllDocs>;
    return make.template operator()<All>(query.Segment());
  }

  const auto erased = [&]() -> Result {
    if constexpr (kProbed) {
      auto node = ngrams.PlanProbe({}, interrogations);
      if (!node) {
        return {};
      }
      return make.template operator()<probe::Erased>(std::move(node));
    } else {
      auto node = ngrams.PlanLead({});
      if (!node) {
        return {};
      }
      return make.template operator()<lead::Erased>(std::move(node));
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
      return make.template operator()<probe::OrLeaves<Leaf>>(
        terms.size(), [&](Leaf& leaf, size_t i) {
          leaf.Prepare(terms[i].cookie, *DocOf(*field), LayoutOf(*field),
                       BoundsOf(*field));
        });
    });
  } else {
    return ResolveInput(*DocOf(*field), [&]<typename Input> -> Result {
      using Leaf = PostingFill<Input>;
      using Optional = OrGroup<fill::SetLeaves<Leaf>>;
      using Node =
        lead::BooleanWindow<utils::Empty, utils::Empty, Optional, utils::Empty>;
      return make.template operator()<Node>(
        std::piecewise_construct, std::forward_as_tuple(),
        std::forward_as_tuple(),
        std::forward_as_tuple(terms.size(),
                              [&](Leaf& leaf, size_t i) {
                                leaf.Prepare(terms[i].cookie, *DocOf(*field),
                                             LayoutOf(*field), BoundsOf(*field),
                                             FreqOf(*field));
                              }),
        std::forward_as_tuple());
    });
  }
}

}  // namespace irs::detail
