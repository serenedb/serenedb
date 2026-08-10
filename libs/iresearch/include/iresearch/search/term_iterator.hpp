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

inline bstring AfterKey(bytes_view key) {
  bstring after{key};
  after.push_back(0);
  return after;
}

inline bstring UpperBoundOf(bytes_view prefix) {
  bstring upper{prefix};
  while (!upper.empty()) {
    if (upper.back() != 0xFF) {
      ++upper.back();
      return upper;
    }
    upper.pop_back();
  }
  return upper;
}

class BoundedTermIterator : public SeekTermIterator {
 public:
  explicit BoundedTermIterator(SeekTermIterator::ptr&& impl,
                               bytes_view lower = {}, bytes_view upper = {},
                               const TermPredicate* predicate = nullptr)
    : _impl{std::move(impl)},
      _predicate{predicate},
      _lower{lower},
      _upper{upper} {
    SDB_ASSERT(_impl);
  }

  SeekTermIterator& GetImpl() noexcept { return *_impl; }

  bytes_view value() const noexcept final { return _value; }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return _impl->GetMutable(id);
  }

  const PostingMeta& cookie() const final { return _impl->cookie(); }

  DocIterator::ptr postings(IndexFeatures features) const final {
    return _impl->postings(features);
  }

  bool next() final {
    if (_done) {
      return false;
    }
    if (_started) {
      if (!_impl->next()) {
        return Stop();
      }
    } else {
      _started = true;
      if (_lower.empty() ? !_impl->next()
                         : SeekResult::End == _impl->seek_ge(_lower)) {
        return Stop();
      }
    }
    return Scan();
  }

  SeekResult seek_ge(bytes_view target) final {
    if (_done) {
      return SeekResult::End;
    }
    _started = true;
    const bytes_view from = std::max(target, bytes_view{_lower});
    if (SeekResult::End == _impl->seek_ge(from)) {
      Stop();
      return SeekResult::End;
    }
    if (!Scan()) {
      return SeekResult::End;
    }
    return _value == target ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view target) final {
    return SeekResult::Found == seek_ge(target);
  }

 private:
  bool Scan() {
    for (;;) {
      const auto key = _impl->value();
      if (!_upper.empty() && key >= _upper) {
        return Stop();
      }
      if (_predicate == nullptr || _predicate->Accepts(key)) {
        _value = key;
        return true;
      }
      if (!_impl->next()) {
        return Stop();
      }
    }
  }

  bool Stop() {
    _done = true;
    _value = {};
    return false;
  }

  SeekTermIterator::ptr _impl;
  const TermPredicate* _predicate;
  bstring _lower;
  bstring _upper;
  bytes_view _value;
  bool _started{false};
  bool _done{false};
};

class FilteredTermIterator : public TermIterator {
 public:
  FilteredTermIterator(TermIterator::ptr&& impl,
                       TermPredicate::ptr&& predicate) noexcept
    : _impl{std::move(impl)}, _predicate{std::move(predicate)} {
    SDB_ASSERT(_impl);
    SDB_ASSERT(_predicate);
  }

  bytes_view value() const noexcept final { return _impl->value(); }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return _impl->GetMutable(id);
  }

  const PostingMeta& cookie() const final { return _impl->cookie(); }

  DocIterator::ptr postings(IndexFeatures features) const final {
    return _impl->postings(features);
  }

  bool next() final {
    while (_impl->next()) {
      if (_predicate->Accepts(_impl->value())) {
        return true;
      }
    }
    return false;
  }

 private:
  TermIterator::ptr _impl;
  TermPredicate::ptr _predicate;
};

}  // namespace irs
