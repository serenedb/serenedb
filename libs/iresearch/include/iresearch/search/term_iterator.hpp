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

#include <memory>
#include <utility>

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_predicate.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct TermBoost : Attribute {
  static constexpr std::string_view type_name() noexcept {
    return "term_boost";
  }

  score_t value{kNoBoost};
};

class WrappedTermIterator : public TermIterator {
 public:
  bytes_view value() const noexcept final { return _impl->value(); }
  void read() final { _impl->read(); }
  DocIterator::ptr postings(IndexFeatures features) const final {
    return _impl->postings(features);
  }
  Attribute* GetMutable(TypeInfo::type_id id) noexcept override {
    return _impl->GetMutable(id);
  }
  SeekTermIterator& GetImpl() noexcept { return *_impl; }

 protected:
  explicit WrappedTermIterator(SeekTermIterator::ptr&& impl) noexcept
    : _impl{std::move(impl)} {
    SDB_ASSERT(_impl);
  }

  SeekTermIterator::ptr _impl;
};

class FilteredTermIterator : public TermIterator {
 public:
  FilteredTermIterator(TermIterator::ptr&& inner,
                       TermPredicate::ptr&& predicate) noexcept
    : _inner{std::move(inner)}, _predicate{std::move(predicate)} {
    SDB_ASSERT(_inner);
    SDB_ASSERT(_predicate);
  }

  bool next() final {
    while (_inner->next()) {
      if (_predicate->Accepts(_inner->value())) {
        return true;
      }
    }
    return false;
  }
  bytes_view value() const noexcept final { return _inner->value(); }
  void read() final { _inner->read(); }
  DocIterator::ptr postings(IndexFeatures features) const final {
    return _inner->postings(features);
  }
  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return _inner->GetMutable(id);
  }

 private:
  TermIterator::ptr _inner;
  TermPredicate::ptr _predicate;
};

}  // namespace irs
