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

#include <algorithm>
#include <array>
#include <cstdint>
#include <span>
#include <tuple>
#include <vector>

#include "basics/system-compiler.h"
#include "iresearch/search/filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

enum class Occur : uint8_t {
  Must,
  Should,
  MustNot,
};

inline constexpr size_t kNumOccur = 3;

inline constexpr size_t OccurIndex(Occur occur) noexcept {
  return static_cast<size_t>(occur);
}

inline constexpr std::array kAllOccur{Occur::Must, Occur::Should,
                                      Occur::MustNot};

inline void MergeBoost(score_t& boost, score_t other,
                       ScoreMergeType type) noexcept {
  switch (type) {
    case ScoreMergeType::Sum:
      boost += other;
      break;
    case ScoreMergeType::Max:
      boost = std::max(boost, other);
      break;
    case ScoreMergeType::Noop:
      break;
  }
}

struct TermClause {
  field_id field = field_limits::invalid();
  const Scorer* scorer = nullptr;
  bstring term;
  score_t boost = kNoBoost;

  bool operator==(const TermClause& rhs) const noexcept = default;
};

struct TermPostingLess {
  bool operator()(const TermClause& lhs, const TermClause& rhs) const noexcept {
    return std::tie(lhs.field, lhs.term) < std::tie(rhs.field, rhs.term);
  }
};

struct TermClauseLess {
  bool operator()(const TermClause& lhs, const TermClause& rhs) const noexcept {
    return std::tie(lhs.field, lhs.term, lhs.scorer) <
           std::tie(rhs.field, rhs.term, rhs.scorer);
  }
};

struct FilterClause {
  Filter::ptr filter;
  Occur occur = Occur::Must;
};

struct Clauses {
  std::vector<TermClause> terms;
  std::vector<Filter::ptr> filters;

  size_t size() const noexcept { return terms.size() + filters.size(); }
  bool empty() const noexcept { return terms.empty() && filters.empty(); }
};

class BooleanFilter final : public FilterWithType<BooleanFilter> {
 public:
  auto& Bucket(this auto& self, Occur occur) noexcept {
    switch (occur) {
      case Occur::Must:
        return self._must;
      case Occur::Should:
        return self._should;
      case Occur::MustNot:
        return self._must_not;
    }
    SDB_UNREACHABLE();
  }

  std::span<const TermClause> Terms(Occur occur) const noexcept {
    return Bucket(occur).terms;
  }

  std::span<const Filter::ptr> Filters(Occur occur) const noexcept {
    return Bucket(occur).filters;
  }

  size_t Size(Occur occur) const noexcept { return Bucket(occur).size(); }

  uint32_t MinShouldMatch() const noexcept { return _min_should_match; }

  ScoreMergeType MergeType() const noexcept { return _merge_type; }

  void Add(TermClause clause, Occur occur);

  void Add(Filter::ptr filter, Occur occur);

  void Add(FilterClause clause) { Add(std::move(clause.filter), clause.occur); }

  bool Transparent() const noexcept {
    return Size(Occur::Should) == 0 && GetScorer() == nullptr &&
           GetBoost() == kNoBoost;
  }

  void SpliceInto(BooleanFilter& to) {
    SDB_ASSERT(Transparent());
    for (const auto occur : kAllOccur) {
      auto& bucket = Bucket(occur);
      for (auto& term : bucket.terms) {
        to.Add(std::move(term), occur);
      }
      for (auto& child : bucket.filters) {
        to.Add(std::move(child), occur);
      }
      bucket.terms.clear();
      bucket.filters.clear();
    }
  }

  void SetMinShouldMatch(uint32_t value) noexcept;

  void SetMergeType(ScoreMergeType value) noexcept { _merge_type = value; }

  bool Valid() const noexcept;

  void VisitChildren(ChildVisitor visit) final {
    for (const auto occur : kAllOccur) {
      for (auto& child : Bucket(occur).filters) {
        visit(child);
      }
    }
  }

  TermPredicate::ptr CompileTermPredicate() const final;

  TermIterator::ptr CompileTermIterator(const TermReader& reader) const final;

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

 protected:
  PrepareCollector::ptr MakeCollectorImpl(const Scorer* scorer,
                                          StatsArena& stats,
                                          uint32_t threads) const final;

  bool equals(const Filter& rhs) const noexcept final;

 private:
  Clauses _must;
  Clauses _should;
  Clauses _must_not;
  uint32_t _min_should_match = 0;
  ScoreMergeType _merge_type = ScoreMergeType::Sum;
};

}  // namespace irs
