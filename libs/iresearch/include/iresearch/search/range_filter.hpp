////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/search_range.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByRange;
struct FilterVisitor;

struct ByRangeFilterOptions {
  using range_type = SearchRange<bstring>;

  range_type range;

  bool operator==(const ByRangeFilterOptions& rhs) const noexcept {
    return range == rhs.range;
  }
};

struct ByRangeOptions : ByRangeFilterOptions {
  using FilterType = ByRange;
  using filter_options = ByRangeFilterOptions;

  size_t scored_terms_limit{1024};

  bool operator==(const ByRangeOptions& rhs) const noexcept {
    return filter_options::operator==(rhs) &&
           scored_terms_limit == rhs.scored_terms_limit;
  }
};

class ByRangeIterator {
 public:
  ByRangeIterator(const TermReader& reader, const ByRangeOptions& options);

  ByRangeIterator(SeekTermIterator::ptr&& impl,
                  const ByRangeFilterOptions::range_type& range)
    : _impl{std::move(impl)}, _range{&range} {
    bool res = false;
    if (_impl) {
      switch (_range->min_type) {
        case BoundType::Unbounded:
          res = _impl->next();
          break;
        case BoundType::Inclusive:
          res = seek_min<true>(*_impl, _range->min);
          break;
        case BoundType::Exclusive:
          res = seek_min<false>(*_impl, _range->min);
          break;
      }
    }
    if (!res || !InRange()) {
      _impl = SeekTermIterator::empty();
    }
  }

  SeekTermIterator& GetImpl() noexcept { return *_impl; }
  score_t Boost() const noexcept { return kNoBoost; }
  bytes_view value() const noexcept { return _impl->value(); }
  void read() { _impl->read(); }

  bool next() {
    if (_impl->next() && InRange()) {
      return true;
    }
    _impl = SeekTermIterator::empty();
    return false;
  }

 private:
  bool InRange() const {
    const bytes_view value = _impl->value();
    switch (_range->max_type) {
      case BoundType::Unbounded:
        return true;
      case BoundType::Inclusive:
        return value <= _range->max;
      case BoundType::Exclusive:
        return value < _range->max;
    }
    return false;
  }

  SeekTermIterator::ptr _impl;
  const ByRangeFilterOptions::range_type* _range;
};

class ByRange : public FilterWithField<ByRangeOptions> {
 public:
  static void visit(const SubReader& segment, const TermReader& reader,
                    const ByRangeOptions& options, FilterVisitor& visitor);

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;
  static QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          const irs::field_id field,
                                          const options_type::range_type& rng,
                                          score_t boost);

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;
};

}  // namespace irs
