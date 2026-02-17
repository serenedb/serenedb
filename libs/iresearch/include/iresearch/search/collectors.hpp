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

#include <vector>

#include "basics/shared.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

// A convinience base class for collector wrappers
template<typename Wrapper, typename Collector>
class CollectorWrapper {
 public:
  using collector_ptr = typename Collector::ptr;
  using element_type = typename collector_ptr::element_type;
  using pointer = typename collector_ptr::pointer;

  pointer get() const noexcept { return _collector.get(); }
  pointer operator->() const noexcept { return get(); }
  element_type& operator*() const noexcept { return *_collector; }
  explicit operator bool() const noexcept {
    return static_cast<bool>(_collector);
  }

 protected:
  CollectorWrapper() noexcept : _collector(&Wrapper::noop()) {}

  ~CollectorWrapper() { reset(nullptr); }

  explicit CollectorWrapper(pointer collector) noexcept
    : _collector(!collector ? &Wrapper::noop() : collector) {
    SDB_ASSERT(_collector);
  }

  CollectorWrapper(CollectorWrapper&& rhs) noexcept
    : _collector(std::move(rhs._collector)) {
    rhs._collector.reset(&Wrapper::noop());
    SDB_ASSERT(_collector);
  }

  CollectorWrapper& operator=(pointer collector) noexcept {
    if (!collector) {
      collector = &Wrapper::noop();
    }

    if (_collector.get() != collector) {
      reset(collector);
    }

    SDB_ASSERT(_collector);
    return *this;
  }

  CollectorWrapper& operator=(CollectorWrapper&& rhs) noexcept {
    if (this != &rhs) {
      reset(rhs._collector.release());
      rhs._collector.reset(&Wrapper::noop());
    }
    SDB_ASSERT(_collector);
    return *this;
  }

 private:
  void reset(pointer collector) noexcept {
    if (_collector.get() == &Wrapper::noop()) {
      _collector.release();
    }
    _collector.reset(collector);
  }

  collector_ptr _collector;
};

// A convinience base class for collectors
template<typename Collector>
class CollectorsBase {
 public:
  using iterator_type = typename std::vector<Collector>::const_iterator;

  explicit CollectorsBase(size_t size, const Scorer* scorer)
    : _collectors(size), _scorer{scorer} {}

  CollectorsBase(CollectorsBase&&) = default;
  CollectorsBase& operator=(CollectorsBase&&) = default;

  iterator_type begin() const noexcept { return _collectors.begin(); }

  iterator_type end() const noexcept { return _collectors.end(); }

  bool empty() const noexcept { return _collectors.empty(); }

  void reset() {
    for (auto& collector : _collectors) {
      collector->reset();
    }
  }

  typename Collector::pointer front() const noexcept {
    SDB_ASSERT(!_collectors.empty());
    return _collectors.front().get();
  }

  typename Collector::pointer back() const noexcept {
    SDB_ASSERT(!_collectors.empty());
    return _collectors.back().get();
  }

  typename Collector::pointer operator[](size_t i) const noexcept {
    SDB_ASSERT(i < _collectors.size());
    return _collectors[i].get();
  }

 protected:
  std::vector<Collector> _collectors;
  const Scorer* _scorer{};
};

// Wrapper around FieldCollector which guarantees collector
// is not nullptr
class FieldCollectorWrapper
  : public CollectorWrapper<FieldCollectorWrapper, FieldCollector> {
 public:
  using collector_type = FieldCollector;
  using base_type = CollectorWrapper<FieldCollectorWrapper, collector_type>;

  static collector_type& noop() noexcept;

  FieldCollectorWrapper() = default;
  FieldCollectorWrapper(FieldCollectorWrapper&&) = default;
  FieldCollectorWrapper& operator=(FieldCollectorWrapper&&) = default;
  explicit FieldCollectorWrapper(collector_type::ptr&& collector) noexcept
    : base_type(collector.release()) {}
  FieldCollectorWrapper& operator=(collector_type::ptr&& collector) noexcept {
    base_type::operator=(collector.release());
    return *this;
  }
};

static_assert(std::is_nothrow_move_constructible_v<FieldCollectorWrapper>);
static_assert(std::is_nothrow_move_assignable_v<FieldCollectorWrapper>);

// Create an field level index statistics compound collector for
// all buckets
class FieldCollectors : public CollectorsBase<FieldCollectorWrapper> {
 public:
  explicit FieldCollectors(const Scorer* scorer);
  FieldCollectors(FieldCollectors&&) = default;
  FieldCollectors& operator=(FieldCollectors&&) = default;

  size_t size() const noexcept { return _collectors.size(); }

  // Collect field related statistics, i.e. field used in the filter
  // segment the segment being processed (e.g. for columnstore)
  // field the field matched by the filter in the 'segment'
  // Note called once for every field matched by a filter per each segment
  // Note always called on each matched 'field' irrespective of if it
  // contains a matching 'term'
  void collect(const SubReader& segment, const TermReader& field) const;

  // Store collected index statistics into 'stats' of the
  // current 'filter'
  // stats out-parameter to store statistics for later use in
  // calls to score(...)
  // Note called once on the 'index' for every term matched by a filter
  //       calling collect(...) on each of its segments
  // Note if not matched terms then called exactly once
  void finish(byte_type* stats_buf) const;
};

static_assert(std::is_nothrow_move_constructible_v<FieldCollectors>);
static_assert(std::is_nothrow_move_assignable_v<FieldCollectors>);

// Wrapper around TermCollector which guarantees collector
// is not nullptr
class TermCollectorWrapper
  : public CollectorWrapper<TermCollectorWrapper, TermCollector> {
 public:
  using collector_type = TermCollector;
  using base_type = CollectorWrapper<TermCollectorWrapper, collector_type>;

  static collector_type& noop() noexcept;

  TermCollectorWrapper() = default;
  TermCollectorWrapper(TermCollectorWrapper&&) = default;
  TermCollectorWrapper& operator=(TermCollectorWrapper&&) = default;
  explicit TermCollectorWrapper(collector_type::ptr&& collector) noexcept
    : base_type(collector.release()) {}
  TermCollectorWrapper& operator=(collector_type::ptr&& collector) noexcept {
    base_type::operator=(collector.release());
    return *this;
  }
};

static_assert(std::is_nothrow_move_constructible_v<TermCollectorWrapper>);
static_assert(std::is_nothrow_move_assignable_v<TermCollectorWrapper>);

// Create an term level index statistics compound collector for
// all buckets
class TermCollectors : public CollectorsBase<TermCollectorWrapper> {
 public:
  TermCollectors(const Scorer* scorer, size_t size);
  TermCollectors(TermCollectors&&) = default;
  TermCollectors& operator=(TermCollectors&&) = default;

  size_t size() const noexcept { return _scorer ? _collectors.size() : 0; }

  // Add collectors for another term and return term_offset
  size_t push_back();

  // Collect term related statistics, i.e. term used in the filter
  // segment the segment being processed (e.g. for columnstore)
  // field the field matched by the filter in the 'segment'
  // term_index index of term, value < constructor 'terms_count'
  // term_attributes the attributes of the matched term in the field
  // Note called once for every term matched by a filter in the 'field'
  //       per each segment
  // Note only called on a matched 'term' in the 'field' in the 'segment'
  void collect(const SubReader& segment, const TermReader& field,
               size_t term_idx, const AttributeProvider& attrs) const;

  // Store collected index statistics into 'stats' of the
  // current 'filter'
  // stats - out-parameter to store statistics for later use in
  // calls to score(...)
  // term_index - index of term, value < constructor 'terms_count'
  // index - the full index to collect statistics on
  // Note called once on the 'index' for every term matched by a filter
  //       calling collect(...) on each of its segments
  // Note if not matched terms then called exactly once
  void finish(byte_type* stats_buf, size_t term_idx,
              const FieldCollectors& field_collectors,
              const IndexReader& index) const;
};

static_assert(std::is_nothrow_move_constructible_v<TermCollectors>);
static_assert(std::is_nothrow_move_assignable_v<TermCollectors>);

}  // namespace irs
