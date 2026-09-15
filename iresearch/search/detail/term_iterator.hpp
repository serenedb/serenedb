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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/term_predicate.hpp"
#include "iresearch/search/scorers/scorer.hpp"
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
  const PostingMeta& cookie() const final { return _impl->cookie(); }
  TermPostings::ptr postings(IndexFeatures features) const final {
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

class ByTermIterator : public TermIterator {
 public:
  ByTermIterator(const TermReader& reader, bytes_view term)
    : _reader{&reader}, _meta{reader.Lookup(term)} {
    _term.value = term;
  }

  bytes_view value() const noexcept final { return _term.value; }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return id == irs::Type<TermAttr>::id() ? &_term : nullptr;
  }

  const PostingMeta& cookie() const final { return _meta; }

  TermPostings::ptr postings(IndexFeatures features) const final {
    if (_meta.docs_count == 0) {
      return TermPostings::empty();
    }
    auto it = _reader->iterator();
    SDB_ASSERT(it);
    if (!it->seek(_term.value)) {
      return TermPostings::empty();
    }
    return it->postings(features);
  }

  bool next() final { return std::exchange(_found, false); }

 private:
  const TermReader* _reader;
  const PostingMeta _meta;
  TermAttr _term;
  bool _found{_meta.docs_count != 0};
};

struct PlainTerms {
  static bytes_view Term(const bstring& term) noexcept { return term; }
  static score_t Boost(const bstring&) noexcept { return kNoBoost; }
};

template<typename Cursor, typename Access = PlainTerms>
class SeekTermsIterator : public WrappedTermIterator {
 public:
  SeekTermsIterator(const TermReader& reader, Cursor begin, Cursor end)
    : WrappedTermIterator{reader.iterator()}, _cursor{begin}, _end{end} {}

  score_t Boost() const noexcept { return _boost; }

  bool next() final {
    for (; _cursor != _end; ++_cursor) {
      if (_impl->seek(Access::Term(*_cursor))) {
        _boost = Access::Boost(*_cursor);
        ++_cursor;
        return true;
      }
    }
    return false;
  }

 private:
  Cursor _cursor;
  Cursor _end;
  score_t _boost = kNoBoost;
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
  const PostingMeta& cookie() const final { return _inner->cookie(); }
  TermPostings::ptr postings(IndexFeatures features) const final {
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
