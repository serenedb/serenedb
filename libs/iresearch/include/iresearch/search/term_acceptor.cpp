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

#include <optional>
#include <utility>

#include "basics/assert.h"
#include "iresearch/formats/formats.hpp"
#include "iresearch/search/term_iterator.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/regexp_acceptor.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace irs {
namespace {

// The `literal%` family, recognised from the pattern rather than from the shape
// of a compiled automaton. The dictionary range `[prefix,
// UpperBoundOf(prefix))` is the whole of the bound: it prunes at block
// granularity and settles each key against the bound once, so every key this
// walk sees already begins with the prefix and nothing here re-derives that.
// What the range cannot answer is the one thing left of the language: `%`
// matches runes, so a key whose tail is not valid UTF-8 is not a match.
//
// The bounds are the source's, which outlives every walk it hands out, so the
// range borrows them without a copy.
class PrefixTermIterator : public ForwardingSeekTermIterator {
 public:
  PrefixTermIterator(const TermReader& reader, bytes_view prefix,
                     bytes_view upper)
    : ForwardingSeekTermIterator{reader.RangeIterator({prefix, upper})},
      _prefix{prefix} {}

  bool next() final {
    while (_impl->next()) {
      if (Accepts(_impl->value())) {
        return true;
      }
    }
    return false;
  }

  SeekResult seek_ge(bytes_view target) final {
    if (SeekResult::End == _impl->seek_ge(target)) {
      return SeekResult::End;
    }
    if (!Accepts(_impl->value()) && !next()) {
      return SeekResult::End;
    }
    return _impl->value() == target ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view target) final {
    return SeekResult::Found == seek_ge(target);
  }

 private:
  bool Accepts(bytes_view term) const noexcept {
    SDB_ASSERT(term.size() >= _prefix.size());
    return AcceptsAnyUtf8(term.substr(_prefix.size()));
  }

  bytes_view _prefix;
};

// `literal%literal`: the second shape whose whole language a range decides,
// and the only other one the wildcard dialect can express without an
// automaton. The range settles the head, one byte comparison settles the tail,
// and what is left between them is the language of a bare `%` -- the same
// residual `PrefixTermIterator` answers, over a shorter run.
class PrefixSuffixTermIterator : public ForwardingSeekTermIterator {
 public:
  PrefixSuffixTermIterator(const TermReader& reader, bytes_view prefix,
                           bytes_view upper, bytes_view suffix)
    : ForwardingSeekTermIterator{reader.RangeIterator({prefix, upper})},
      _prefix{prefix},
      _suffix{suffix} {}

  bool next() final {
    while (_impl->next()) {
      if (Accepts(_impl->value())) {
        return true;
      }
    }
    return false;
  }

  SeekResult seek_ge(bytes_view target) final {
    if (SeekResult::End == _impl->seek_ge(target)) {
      return SeekResult::End;
    }
    if (!Accepts(_impl->value()) && !next()) {
      return SeekResult::End;
    }
    return _impl->value() == target ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view target) final {
    return SeekResult::Found == seek_ge(target);
  }

 private:
  bool Accepts(bytes_view term) const noexcept {
    SDB_ASSERT(term.size() >= _prefix.size());
    // The head and the tail may not overlap: `%` stands for a run of code
    // points, and the shortest such run is empty rather than negative.
    const auto rest = term.size() - _prefix.size();
    if (rest < _suffix.size()) {
      return false;
    }
    const auto middle = rest - _suffix.size();
    return term.substr(_prefix.size() + middle) == _suffix &&
           AcceptsAnyUtf8(term.substr(_prefix.size(), middle));
  }

  bytes_view _prefix;
  bytes_view _suffix;
};

std::optional<bstring> AsWildcardPrefix(bytes_view pattern) {
  bstring buf;
  return ExecuteWildcard(
    buf, pattern, [](bytes_view) { return std::optional<bstring>{}; },
    [](bytes_view prefix) { return std::optional<bstring>{prefix}; },
    [](bytes_view) { return std::optional<bstring>{}; });
}

struct PrefixSuffix {
  bstring prefix;
  bstring suffix;
};

// Recognised from the pattern, once per source: everything before the single
// `%` and everything after it, both taken literally. A `_` is a code point
// rather than a byte, so a pattern holding one is not this shape, and neither
// is one whose head is empty -- there the range bounds nothing and the walk it
// would replace is the one the automaton already drives.
std::optional<PrefixSuffix> AsPrefixSuffix(bytes_view pattern) {
  PrefixSuffix out;
  auto* part = &out.prefix;
  bool escaped = false;
  for (const auto c : pattern) {
    if (escaped) {
      part->push_back(c);
      escaped = false;
      continue;
    }
    switch (c) {
      case WildcardMatch::kEscape:
        escaped = true;
        break;
      case WildcardMatch::kAnyChr:
        return std::nullopt;
      case WildcardMatch::kAnyStr:
        if (part == &out.suffix) {
          return std::nullopt;
        }
        part = &out.suffix;
        break;
      default:
        part->push_back(c);
        break;
    }
  }
  if (part == &out.prefix || out.prefix.empty() || out.suffix.empty()) {
    return std::nullopt;
  }
  return out;
}

std::shared_ptr<const RegexpAcceptor> MakeAcceptor(bytes_view pattern,
                                                   PatternKind kind,
                                                   RegexpSyntax syntax) {
  if (kind == PatternKind::Wildcard) {
    return std::make_shared<const RegexpAcceptor>(RegexpAcceptor::WildcardTag{},
                                                  pattern);
  }
  SDB_ASSERT(kind == PatternKind::Regexp);
  return std::make_shared<const RegexpAcceptor>(pattern, syntax);
}

class PatternSource final : public TermAcceptorSource {
 public:
  PatternSource(bytes_view pattern, PatternKind kind, RegexpSyntax syntax)
    : _prefix{kind == PatternKind::Wildcard ? AsWildcardPrefix(pattern)
                                            : std::nullopt},
      _head_tail{kind == PatternKind::Wildcard && !_prefix
                   ? AsPrefixSuffix(pattern)
                   : std::nullopt},
      _upper{Upper()},
      _acceptor{MakeAcceptor(pattern, kind, syntax)} {}

  bool ok() const noexcept final { return _acceptor->ok(); }

  SeekTermIterator::ptr Iterator(const TermReader& reader) const final {
    if (_prefix) {
      return memory::make_managed<PrefixTermIterator>(reader, *_prefix, _upper);
    }
    if (_head_tail) {
      return memory::make_managed<PrefixSuffixTermIterator>(
        reader, _head_tail->prefix, _upper, _head_tail->suffix);
    }
    if (!_acceptor->ok()) {
      return SeekTermIterator::empty();
    }
    auto it = reader.iterator(*_acceptor);
    if (it) {
      return it;
    }
    // A dictionary with no direct-stepping backend is scanned instead.
    return memory::make_managed<FilteredSeekTermIterator>(
      reader.iterator(SeekMode::NORMAL), Predicate());
  }

  TermPredicate::ptr Predicate() const final {
    return MakeTermPredicate([acceptor = _acceptor](bytes_view term) noexcept {
      return acceptor->Matches(term);
    });
  }

 private:
  bstring Upper() const {
    if (_prefix) {
      return UpperBoundOf(*_prefix);
    }
    return _head_tail ? UpperBoundOf(_head_tail->prefix) : bstring{};
  }

  // The two literal-bounded families, recognised from the pattern once rather
  // than per walk; `_upper` is the exclusive end of the range either bounds.
  std::optional<bstring> _prefix;
  std::optional<PrefixSuffix> _head_tail;
  bstring _upper;
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

class ConjunctionSource final : public TermAcceptorSource {
 public:
  ConjunctionSource(TermAcceptorSource::ptr&& driver, TermBounds&& bounds,
                    Filter::ptr&& residual) noexcept
    : _driver{std::move(driver)},
      _bounds{std::move(bounds)},
      _residual{std::move(residual)} {}

  // Composed of operands the optimizer already accepted, so there is no
  // pattern here left to fail.
  bool ok() const noexcept final { return true; }

  SeekTermIterator::ptr Iterator(const TermReader& reader) const final {
    auto it =
      _driver ? _driver->Iterator(reader) : reader.iterator(SeekMode::NORMAL);
    auto predicate = ResidualPredicate();
    if (!predicate) {
      return it;
    }
    return memory::make_managed<FilteredSeekTermIterator>(
      std::move(it), std::move(predicate), _bounds.lower, _bounds.upper);
  }

  TermPredicate::ptr Predicate() const final {
    auto predicate = ResidualPredicate();
    if (!_driver) {
      SDB_ASSERT(predicate);
      return predicate;
    }
    auto exact = _driver->Predicate();
    if (!predicate) {
      return exact;
    }
    return std::make_unique<BothPredicate>(std::move(exact),
                                           std::move(predicate));
  }

 private:
  TermPredicate::ptr ResidualPredicate() const {
    if (!_residual) {
      return nullptr;
    }
    auto predicate = _residual->CompileTermPredicate();
    SDB_ASSERT(predicate);
    return predicate;
  }

  TermAcceptorSource::ptr _driver;
  TermBounds _bounds;
  Filter::ptr _residual;
};

}  // namespace

bstring UpperBoundOf(bytes_view prefix) {
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

TermAcceptorSource::ptr MakePatternSource(bstring pattern, PatternKind kind,
                                          RegexpSyntax syntax) {
  SDB_ASSERT(kind != PatternKind::Fused);
  return std::make_shared<const PatternSource>(pattern, kind, syntax);
}

TermAcceptorSource::ptr MakeConjunctionSource(TermAcceptorSource::ptr driver,
                                              TermBounds bounds,
                                              Filter::ptr residual) {
  SDB_ASSERT(driver || residual);
  return std::make_shared<const ConjunctionSource>(
    std::move(driver), std::move(bounds), std::move(residual));
}

}  // namespace irs
