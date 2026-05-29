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

class CollectorBase {
 public:
  CollectorBase(const Scorer* scorer) noexcept : _scorer{scorer} {}

 protected:
  void Finish(byte_type* stats_buf, const TermCollector* collector,
              const FieldCollector::Data* field_data) const {
    SDB_ASSERT(_scorer);
    SDB_ASSERT(field_data);
    SDB_ASSERT(collector);
    _scorer->collect(stats_buf, field_data, collector);
  }

  bool HasScorer() const noexcept { return _scorer != nullptr; }

 private:
  const Scorer* _scorer = nullptr;
};

class FlatTermBuffer {
 public:
  FlatTermBuffer(size_t size, bool has_scorer)
    : _has_scorer{has_scorer}, _collectors{has_scorer ? size : 0} {}

  size_t Size() const noexcept { return _collectors.size(); }
  bool Empty() const noexcept { return _collectors.empty(); }

  size_t PushBack() {
    if (!_has_scorer) {
      return 0;
    }
    _collectors.emplace_back();
    return _collectors.size() - 1;
  }

  void Collect(size_t term_idx, const AttributeProvider& attrs) {
    if (_has_scorer) {
      SDB_ASSERT(term_idx < _collectors.size());
      _collectors[term_idx].Collect(attrs);
    }
  }

  const TermCollector& Get(size_t term_idx) const {
    SDB_ASSERT(_has_scorer);
    SDB_ASSERT(term_idx < _collectors.size());
    return _collectors[term_idx];
  }

  TermCollector& Get(size_t term_idx) {
    SDB_ASSERT(_has_scorer);
    SDB_ASSERT(term_idx < _collectors.size());
    return _collectors[term_idx];
  }

 private:
  bool _has_scorer;
  std::vector<TermCollector> _collectors;
};

class TermCollectorsFlat : public CollectorBase, public FlatTermBuffer {
 public:
  TermCollectorsFlat(const Scorer* scorer, size_t size)
    : CollectorBase{scorer}, FlatTermBuffer{size, scorer != nullptr} {}

  size_t PushBack() {
    if (!HasScorer()) {
      return 0;
    }
    FlatTermBuffer::PushBack();
    return Size() - 1;
  }

  void Collect(size_t term_idx, const AttributeProvider& attrs) {
    if (HasScorer()) {
      SDB_ASSERT(term_idx < Size());
      Get(term_idx).Collect(attrs);
    }
  }

  void Finish(byte_type* stats_buf, size_t term_idx,
              const FieldCollector::Data* field_data) const {
    if (HasScorer()) {
      SDB_ASSERT(term_idx < Size());
      CollectorBase::Finish(stats_buf, &Get(term_idx), field_data);
    }
  }
};

class TermCollectorsVariadic : public CollectorBase {
 public:
  TermCollectorsVariadic(const Scorer* scorer, size_t phrase_size)
    : CollectorBase{scorer},
      _collectors(phrase_size, FlatTermBuffer{0, scorer != nullptr}) {}

  size_t Size() const noexcept { return _collectors.size(); }

  FlatTermBuffer& GetCollector(size_t idx) {
    SDB_ASSERT(idx < _collectors.size());
    return _collectors[idx];
  }

  void Finish(byte_type* stats_buf, size_t part_idx,
              const FieldCollector::Data* field_data) const {
    if (!HasScorer()) {
      return;
    }
    SDB_ASSERT(part_idx < _collectors.size());
    const auto& part = _collectors[part_idx];
    for (size_t i = 0, size = part.Size(); i < size; ++i) {
      CollectorBase::Finish(stats_buf, &part.Get(i), field_data);
    }
  }

 private:
  std::vector<FlatTermBuffer> _collectors;
};

static_assert(std::is_nothrow_move_constructible_v<TermCollectorsFlat>);
static_assert(std::is_nothrow_move_assignable_v<TermCollectorsFlat>);
static_assert(std::is_nothrow_move_constructible_v<TermCollectorsVariadic>);
static_assert(std::is_nothrow_move_assignable_v<TermCollectorsVariadic>);

}  // namespace irs
