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

// Relays the whole term-iterator surface to the walk it wraps, so a wrapper
// states only what it changes and a method added to the contract is relayed
// here instead of in every wrapper. `Impl` is the walk's own interface, which
// is the seekable one even where the wrapper itself is not seekable.
template<typename Base, typename Impl = Base>
class ForwardingTermIterator : public Base {
 public:
  bytes_view value() const noexcept override { return _impl->value(); }
  bool next() override { return _impl->next(); }
  void read() final { _impl->read(); }
  DocIterator::ptr RowGroupPostings(IndexFeatures features,
                                    uint32_t rg) const final {
    return _impl->RowGroupPostings(features, rg);
  }
  std::span<const TermRowGroup> RowGroups() const final {
    return _impl->RowGroups();
  }
  Attribute* GetMutable(TypeInfo::type_id id) noexcept override {
    return _impl->GetMutable(id);
  }

 protected:
  explicit ForwardingTermIterator(typename Impl::ptr&& impl) noexcept
    : _impl{std::move(impl)} {
    SDB_ASSERT(_impl);
  }

  typename Impl::ptr _impl;
};

class ForwardingSeekTermIterator
  : public ForwardingTermIterator<SeekTermIterator> {
 public:
  SeekResult seek_ge(bytes_view target) override {
    return _impl->seek_ge(target);
  }
  bool seek(bytes_view target) override { return _impl->seek(target); }
  TermCookie cookie() const final { return _impl->cookie(); }

 protected:
  using ForwardingTermIterator<SeekTermIterator>::ForwardingTermIterator;
};

class WrappedTermIterator
  : public ForwardingTermIterator<TermIterator, SeekTermIterator> {
 public:
  SeekTermIterator& GetImpl() noexcept { return *_impl; }

 protected:
  using ForwardingTermIterator<TermIterator,
                               SeekTermIterator>::ForwardingTermIterator;
};

class FilteredTermIterator : public ForwardingTermIterator<TermIterator> {
 public:
  FilteredTermIterator(TermIterator::ptr&& inner,
                       TermPredicate::ptr&& predicate) noexcept
    : ForwardingTermIterator<TermIterator>{std::move(inner)},
      _predicate{std::move(predicate)} {
    SDB_ASSERT(_predicate);
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
  TermPredicate::ptr _predicate;
};

// A predicate over a key range, as a seekable term iterator: the range bounds
// the walk (`upper` is exclusive, an empty bound is unbounded) and the
// predicate decides what inside it is a match. This is what a filter whose
// language a dictionary cannot walk directly falls back to, and what composes a
// driver walk with the residual of a conjunction.
class FilteredSeekTermIterator : public ForwardingSeekTermIterator {
 public:
  FilteredSeekTermIterator(SeekTermIterator::ptr&& impl,
                           TermPredicate::ptr&& predicate, bstring lower = {},
                           bstring upper = {})
    : ForwardingSeekTermIterator{std::move(impl)},
      _predicate{std::move(predicate)},
      _lower{std::move(lower)},
      _upper{std::move(upper)} {
    SDB_ASSERT(_predicate);
  }

  bytes_view value() const noexcept final { return _value; }

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
      if (SeekResult::End == _impl->seek_ge(_lower)) {
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
    if (SeekResult::End == _impl->seek_ge(from) || !Scan()) {
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
      if (!_upper.empty() && key >= bytes_view{_upper}) {
        return Stop();
      }
      if (_predicate->Accepts(key)) {
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

  TermPredicate::ptr _predicate;
  bstring _lower;
  bstring _upper;
  bytes_view _value;
  bool _started{false};
  bool _done{false};
};

}  // namespace irs
