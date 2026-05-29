////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#include <optional>
#include <vector>

#include "basics/shared.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

class TermCollectors {
 public:
  TermCollectors(const Scorer* scorer, size_t size)
    : _collectors(scorer ? size : 0), _scorer{scorer} {}

  TermCollectors(TermCollectors&&) = default;
  TermCollectors& operator=(TermCollectors&&) = default;

  size_t Size() const noexcept { return _collectors.size(); }
  bool Empty() const noexcept { return _collectors.empty(); }

  size_t PushBack() {
    if (!_scorer) {
      return 0;
    }
    _collectors.emplace_back();
    return _collectors.size() - 1;
  }

  void Collect(size_t term_idx, const AttributeProvider& attrs) {
    if (_scorer) {
      SDB_ASSERT(term_idx < _collectors.size());
      _collectors[term_idx].Collect(attrs);
    }
  }

  void Finish(byte_type* stats_buf, size_t term_idx,
              const FieldCollector::Data* field_data) const {
    if (_scorer) {
      SDB_ASSERT(field_data);
      SDB_ASSERT(term_idx < _collectors.size());
      _scorer->collect(stats_buf, field_data, &_collectors[term_idx]);
    }
  }

 private:
  std::vector<TermCollector> _collectors;
  const Scorer* _scorer{};
};

static_assert(std::is_nothrow_move_constructible_v<TermCollectors>);
static_assert(std::is_nothrow_move_assignable_v<TermCollectors>);

// Bundles the FieldCollector and TermCollectors that filters collect scoring
// statistics with as a pair, so callers don't thread the field data into
// Finish by hand.
class StatsCollectors {
 public:
  StatsCollectors(const Scorer* scorer, size_t size)
    : _field{scorer}, _terms{scorer, size} {}

  StatsCollectors(StatsCollectors&&) = default;
  StatsCollectors& operator=(StatsCollectors&&) = default;

  void CollectField(const TermReader& field) noexcept { _field.Collect(field); }

  void CollectTerm(size_t term_idx, const AttributeProvider& attrs) {
    _terms.Collect(term_idx, attrs);
  }

  void Finish(byte_type* stats_buf, size_t term_idx) const {
    _terms.Finish(stats_buf, term_idx, _field.Get());
  }

  size_t Size() const noexcept { return _terms.Size(); }
  bool Empty() const noexcept { return _terms.Empty(); }
  size_t PushBack() { return _terms.PushBack(); }

  FieldCollector& Field() noexcept { return _field; }
  TermCollectors& Terms() noexcept { return _terms; }

 private:
  FieldCollector _field;
  TermCollectors _terms;
};

static_assert(std::is_nothrow_move_constructible_v<StatsCollectors>);
static_assert(std::is_nothrow_move_assignable_v<StatsCollectors>);

}  // namespace irs
