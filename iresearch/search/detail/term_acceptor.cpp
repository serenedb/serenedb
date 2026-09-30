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

#include "iresearch/search/detail/term_acceptor.hpp"

#include <algorithm>
#include <span>
#include <utility>

#include "iresearch/formats/term_reader.hpp"
#include "iresearch/search/detail/term_iterator.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/regexp_acceptor.hpp"

namespace irs {
namespace {

class LiteralSetIterator : public SeekTermIterator {
 public:
  LiteralSetIterator(SeekTermIterator::ptr&& impl,
                     std::shared_ptr<const RegexpAcceptor> acceptor) noexcept
    : _impl{std::move(impl)},
      _acceptor{std::move(acceptor)},
      _literals{_acceptor->Literals()} {
    SDB_ASSERT(_impl);
  }

  bytes_view value() const noexcept final { return _impl->value(); }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return _impl->GetMutable(id);
  }

  const PostingMeta& cookie() const final { return _impl->cookie(); }

  TermPostings::ptr postings(IndexFeatures features) const final {
    return _impl->postings(features);
  }

  bool next() final {
    while (_next != _literals.size()) {
      if (_impl->seek(_literals[_next++])) {
        return true;
      }
    }
    return false;
  }

  SeekResult seek_ge(bytes_view target) final {
    _next = static_cast<size_t>(
      std::lower_bound(_literals.begin(), _literals.end(), target,
                       [](const bstring& literal, bytes_view key) {
                         return bytes_view{literal} < key;
                       }) -
      _literals.begin());
    if (!next()) {
      return SeekResult::End;
    }
    return value() == target ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view target) final {
    return SeekResult::Found == seek_ge(target);
  }

 private:
  SeekTermIterator::ptr _impl;
  std::shared_ptr<const RegexpAcceptor> _acceptor;
  std::span<const bstring> _literals;
  size_t _next{0};
};

class LiteralSetSource final : public TermAcceptorSource {
 public:
  explicit LiteralSetSource(std::shared_ptr<const RegexpAcceptor> acceptor)
    : _acceptor{std::move(acceptor)} {}

  bool ok() const noexcept final { return true; }

  SeekTermIterator::ptr Iterator(const TermReader& reader) const final {
    if (_acceptor->Literals().empty()) {
      return SeekTermIterator::empty();
    }
    return memory::make_managed<LiteralSetIterator>(reader.iterator(),
                                                    _acceptor);
  }

  TermPredicate::ptr Predicate() const final {
    return MakeTermPredicate([acceptor = _acceptor](bytes_view term) {
      const auto literals = acceptor->Literals();
      return std::binary_search(literals.begin(), literals.end(), term,
                                [](const auto& lhs, const auto& rhs) {
                                  return bytes_view{lhs} < bytes_view{rhs};
                                });
    });
  }

 private:
  std::shared_ptr<const RegexpAcceptor> _acceptor;
};

class PatternSource final : public TermAcceptorSource {
 public:
  explicit PatternSource(std::shared_ptr<const RegexpAcceptor> acceptor)
    : _acceptor{std::move(acceptor)} {}

  bool ok() const noexcept final { return _acceptor->ok(); }

  SeekTermIterator::ptr Iterator(const TermReader& reader) const final {
    if (!_acceptor->ok()) {
      return SeekTermIterator::empty();
    }
    return reader.iterator(*_acceptor);
  }

  TermPredicate::ptr Predicate() const final {
    return MakeTermPredicate([acceptor = _acceptor](bytes_view term) {
      return acceptor->Matches(term);
    });
  }

  std::shared_ptr<const RegexpAcceptor> Automaton() const final {
    return _acceptor->ok() ? _acceptor : nullptr;
  }

 private:
  std::shared_ptr<const RegexpAcceptor> _acceptor;
};

template<typename A>
class WalkSource final : public TermAcceptorSource {
 public:
  explicit WalkSource(std::shared_ptr<const A> acceptor) noexcept
    : _acceptor{std::move(acceptor)} {}

  bool ok() const noexcept final { return true; }

  SeekTermIterator::ptr Iterator(const TermReader& reader) const final {
    return reader.iterator(*_acceptor);
  }

  TermPredicate::ptr Predicate() const final {
    return MakeTermPredicate([acceptor = _acceptor](bytes_view term) {
      return acceptor->Matches(term);
    });
  }

 private:
  std::shared_ptr<const A> _acceptor;
};

class BothPredicate final : public TermPredicate {
 public:
  BothPredicate(TermPredicate::ptr&& lhs, TermPredicate::ptr&& rhs) noexcept
    : _lhs{std::move(lhs)}, _rhs{std::move(rhs)} {}

  bool Accepts(bytes_view term) const final {
    return _lhs->Accepts(term) && _rhs->Accepts(term);
  }

 private:
  TermPredicate::ptr _lhs;
  TermPredicate::ptr _rhs;
};

class BorrowedPredicate final : public TermPredicate {
 public:
  explicit BorrowedPredicate(const TermPredicate& impl) noexcept
    : _impl{&impl} {}

  bool Accepts(bytes_view term) const final { return _impl->Accepts(term); }

 private:
  const TermPredicate* _impl;
};

class ConjunctionSource final : public TermAcceptorSource {
 public:
  ConjunctionSource(TermAcceptorSource::ptr&& driver, TermBounds&& bounds,
                    Filter::ptr&& residual)
    : _driver{std::move(driver)},
      _bounds{std::move(bounds)},
      _residual{std::move(residual)},
      _predicate{_residual ? _residual->CompileTermPredicate() : nullptr} {
    SDB_ASSERT(!_residual || _predicate);
  }

  bool ok() const noexcept final { return true; }

  SeekTermIterator::ptr Iterator(const TermReader& reader) const final {
    auto it = _driver ? _driver->Iterator(reader) : reader.iterator();
    if (!_predicate && _bounds.lower.empty() && _bounds.upper.empty()) {
      return it;
    }
    return memory::make_managed<BoundedTermIterator>(
      std::move(it), _bounds.lower, _bounds.upper, _predicate.get());
  }

  TermPredicate::ptr Predicate() const final {
    if (!_driver) {
      SDB_ASSERT(_predicate);
      return std::make_unique<BorrowedPredicate>(*_predicate);
    }
    auto exact = _driver->Predicate();
    if (!_predicate) {
      return exact;
    }
    return std::make_unique<BothPredicate>(
      std::move(exact), std::make_unique<BorrowedPredicate>(*_predicate));
  }

 private:
  TermAcceptorSource::ptr _driver;
  TermBounds _bounds;
  Filter::ptr _residual;
  TermPredicate::ptr _predicate;
};

}  // namespace

TermAcceptorSource::ptr MakePatternSource(bytes_view pattern,
                                          PatternKind kind) {
  SDB_ASSERT(kind != PatternKind::Fused);
  auto acceptor = PatternCache::Instance().Get(pattern, kind);
  if (acceptor->Finite()) {
    return std::make_shared<const LiteralSetSource>(std::move(acceptor));
  }
  return std::make_shared<const PatternSource>(std::move(acceptor));
}

TermAcceptorSource::ptr MakeJointSource(
  std::span<const std::shared_ptr<const RegexpAcceptor>> patterns,
  std::shared_ptr<const LevenshteinAcceptor> fuzzy) {
  if (!fuzzy) {
    SDB_ASSERT(patterns.size() > 1);
    return std::make_shared<const WalkSource<RegexpConjunction>>(
      std::make_shared<const RegexpConjunction>(patterns));
  }
  if (patterns.empty()) {
    return std::make_shared<const WalkSource<LevenshteinAcceptor>>(
      std::move(fuzzy));
  }
  return std::make_shared<const WalkSource<FuzzyConjunction>>(
    std::make_shared<const FuzzyConjunction>(patterns, std::move(fuzzy)));
}

TermAcceptorSource::ptr MakeConjunctionSource(TermAcceptorSource::ptr driver,
                                              TermBounds bounds,
                                              Filter::ptr residual) {
  SDB_ASSERT(driver || residual);
  return std::make_shared<const ConjunctionSource>(
    std::move(driver), std::move(bounds), std::move(residual));
}

}  // namespace irs
