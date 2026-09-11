////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <compare>
#include <functional>
#include <variant>

#include "iresearch/search/filter.hpp"
#include "iresearch/search/lead/node.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class ByNestedFilter;

struct Match {
  constexpr explicit Match(doc_id_t value) noexcept
    : Match{value, doc_limits::eof()} {}

  constexpr Match(doc_id_t min, doc_id_t max) noexcept : min(min), max(max) {}

  constexpr bool operator==(const Match&) const noexcept = default;
  constexpr auto operator<=>(const Match&) const noexcept = default;

  constexpr bool IsMinMatch() const noexcept {
    return !doc_limits::eof(min) && doc_limits::eof(max);
  }

  doc_id_t min;
  doc_id_t max;
};

static constexpr Match kMatchNone{0, 0};
static constexpr Match kMatchAny{1};

struct ParentDocs : lead::Node {
  using ptr = memory::managed_ptr<ParentDocs>;

  virtual doc_id_t Prev() const noexcept = 0;
};

using ParentProvider = std::function<ParentDocs::ptr(const SubReader&)>;

using MatchProvider = std::function<lead::Node::ptr(const SubReader&)>;

struct ByNestedOptions {
  using FilterType = ByNestedFilter;

  using MatchType = std::variant<Match, MatchProvider>;

  ParentProvider parent;

  Filter::ptr child;

  MatchType match{kMatchAny};

  ScoreMergeType merge_type{ScoreMergeType::Sum};

  bool operator==(const ByNestedOptions& rhs) const noexcept {
    auto equal = [](const Filter* lhs, const Filter* rhs) noexcept {
      return ((!lhs && !rhs) || (lhs && rhs && *lhs == *rhs));
    };

    return match.index() == rhs.match.index() &&
           std::visit(
             [&]<typename T>(const T& v) {
               if constexpr (std::is_same_v<T, Match>) {
                 return v == std::get<T>(rhs.match);
               }
               return true;
             },
             match) &&
           merge_type == rhs.merge_type && equal(child.get(), rhs.child.get());
  }
};

class ByNestedFilter final : public FilterWithOptions<ByNestedOptions> {
 public:
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollectorImpl(const Scorer* scorer,
                                          StatsArena& stats,
                                          uint32_t threads) const final;
};

}  // namespace irs
