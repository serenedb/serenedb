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
#include "iresearch/utils/string.hpp"

namespace irs {

class ByTerm;
struct FilterVisitor;

// Options for term filter
struct ByTermOptions {
  using FilterType = ByTerm;

  bstring term;

  bool operator==(const ByTermOptions& rhs) const noexcept = default;
};

class ByTermIterator {
 public:
  ByTermIterator(const TermReader& reader, const ByTermOptions& options);

  SeekTermIterator& GetImpl() noexcept { return *_impl; }
  score_t Boost() const noexcept { return kNoBoost; }
  bytes_view value() const noexcept { return _impl->value(); }
  bool Valid() const noexcept { return !IsNull(value()); }
  void read() { _impl->read(); }

  bool next() {
    _impl = SeekTermIterator::empty();
    return false;
  }

 private:
  SeekTermIterator::ptr _impl;
};

// User-side term filter
class ByTerm : public FilterWithField<ByTermOptions> {
 public:
  static void Visit(const SubReader& segment, const TermReader& field,
                    const ByTermOptions& options, FilterVisitor& visitor);

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final {
    auto sub_ctx = ctx;
    sub_ctx.boost *= Boost();
    return PrepareSegment(segment, sub_ctx, field_id(), options().term);
  }
  static QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          const irs::field_id field,
                                          const bytes_view term);

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;
};

}  // namespace irs
