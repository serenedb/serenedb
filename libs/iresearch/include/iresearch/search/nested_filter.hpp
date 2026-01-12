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
#include <variant>

#include "iresearch/search/filter.hpp"
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

using DocIteratorProvider = std::function<DocIterator::ptr(const SubReader&)>;

// Options for ByNestedFilter filter
struct ByNestedOptions {
  using FilterType = ByNestedFilter;

  using MatchType = std::variant<Match, DocIteratorProvider>;

  // Parent filter.
  DocIteratorProvider parent;

  // Child filter.
  Filter::ptr child;

  // Match type: range or predicate
  MatchType match{kMatchAny};

  // Score merge type.
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

// Filter is capable of finding parents by the corresponding child filter.
class ByNestedFilter final : public FilterWithOptions<ByNestedOptions> {
 public:
  Query::ptr prepare(const PrepareContext& ctx) const final;
};

}  // namespace irs
