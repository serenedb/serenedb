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

#include <absl/functional/overload.h>

#include <tuple>
#include <utility>
#include <variant>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/nested_slots.hpp"
#include "iresearch/search/queries/nested_query.hpp"

namespace irs::detail {

template<bool None, typename Visitor>
auto ResolveRule(const SubReader& segment,
                 const ByNestedOptions::MatchType& match, Visitor&& visitor) {
  return std::visit(absl::Overload{[&](Match v) {
                                     if constexpr (None) {
                                       if (v == kMatchNone) {
                                         return visitor(NoneRule{});
                                       }
                                     } else {
                                       SDB_ASSERT(v != kMatchNone);
                                     }
                                     if (v == kMatchAny) {
                                       return visitor(AnyRule{});
                                     }
                                     if (v.IsMinMatch()) {
                                       return visitor(MinRule{v.min});
                                     }
                                     return visitor(RangeRule{v});
                                   },
                                   [&](const MatchProvider& v) {
                                     auto pred = v(segment);
                                     if (!pred) {
                                       return visitor(PredRule{
                                         SubReader::empty().docs_iterator()});
                                     }
                                     return visitor(PredRule{std::move(pred)});
                                   }},
                    match);
}

inline lead::Node::ptr ChildDocs(const ByNestedQuery& query) {
  auto child = query.Child().PlanLead({});
  if (!child && QueryBuilder::IsEmpty(query.Child())) {
    return SubReader::empty().docs_iterator();
  }
  return child;
}

template<typename Family, typename Make>
typename Family::Result PlanNestedDocs(const ByNestedQuery& query,
                                       Make&& make) {
  auto child = ChildDocs(query);
  if (!child) {
    return {};
  }
  auto parent = query.Parents();
  return ResolveRule<true>(
    query.Segment(), query.MatchKind(),
    [&]<typename Rule>(Rule rule) -> typename Family::Result {
      using Node =
        typename Family::template TwoPhaseDocs<NestedSlots<PlainChild, Rule>>;
      return make.template operator()<Node>(
        std::piecewise_construct, std::move(parent),
        std::forward_as_tuple(std::move(child)), std::move(rule));
    });
}

}  // namespace irs::detail
