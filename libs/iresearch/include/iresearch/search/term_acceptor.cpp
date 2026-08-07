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

#include "iresearch/search/term_acceptor.hpp"

#include <utility>

#include "basics/assert.h"
#include "iresearch/formats/formats.hpp"
#include "iresearch/search/term_iterator.hpp"
#include "iresearch/utils/regexp_acceptor.hpp"

namespace irs {
namespace {

std::shared_ptr<const RegexpAcceptor> MakeAcceptor(bytes_view pattern,
                                                   PatternKind kind) {
  if (kind == PatternKind::Wildcard) {
    return std::make_shared<const RegexpAcceptor>(RegexpAcceptor::WildcardTag{},
                                                  pattern);
  }
  SDB_ASSERT(kind == PatternKind::RegexpPerl ||
             kind == PatternKind::RegexpPosixEre);
  return std::make_shared<const RegexpAcceptor>(
    pattern, kind == PatternKind::RegexpPerl ? RegexpSyntax::Perl
                                             : RegexpSyntax::PosixEre);
}

class PatternSource final : public TermAcceptorSource {
 public:
  PatternSource(bytes_view pattern, PatternKind kind)
    : _acceptor{MakeAcceptor(pattern, kind)} {}

  bool ok() const noexcept final { return _acceptor->ok(); }

  SeekTermIterator::ptr Iterator(const TermReader& reader) const final {
    if (!_acceptor->ok()) {
      return SeekTermIterator::empty();
    }
    return reader.iterator(*_acceptor);
  }

  TermPredicate::ptr Predicate() const final {
    return MakeTermPredicate([acceptor = _acceptor](bytes_view term) noexcept {
      return acceptor->Matches(term);
    });
  }

 private:
  std::shared_ptr<const RegexpAcceptor> _acceptor;
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
    if (!_predicate) {
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
  return std::make_shared<const PatternSource>(pattern, kind);
}

TermAcceptorSource::ptr MakeConjunctionSource(TermAcceptorSource::ptr driver,
                                              TermBounds bounds,
                                              Filter::ptr residual) {
  SDB_ASSERT(driver || residual);
  return std::make_shared<const ConjunctionSource>(
    std::move(driver), std::move(bounds), std::move(residual));
}

}  // namespace irs
