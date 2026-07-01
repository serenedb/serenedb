////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct AutomatonOptions;

class AllTermIterator {
 public:
  AllTermIterator() : _impl{SeekTermIterator::empty()} {}
  explicit AllTermIterator(const TermReader& reader);
  AllTermIterator(const TermReader& reader, const AutomatonOptions& options);

  SeekTermIterator& GetImpl() noexcept { return *_impl; }
  score_t Boost() const noexcept { return kNoBoost; }
  bytes_view value() const noexcept { return _impl->value(); }
  bool Valid() const noexcept { return !IsNull(value()); }
  void read() { _impl->read(); }

  bool next() {
    if (_impl->next()) {
      return true;
    }
    _impl = SeekTermIterator::empty();
    return false;
  }

 private:
  SeekTermIterator::ptr _impl;
};

// Filter returning all documents
class All : public FilterWithBoost {
 public:
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;

  TypeInfo::type_id type() const noexcept final { return irs::Type<All>::id(); }
};

QueryBuilder::ptr MakeAllQuery(const SubReader& segment,
                               const PrepareContext& ctx, score_t boost);

}  // namespace irs
