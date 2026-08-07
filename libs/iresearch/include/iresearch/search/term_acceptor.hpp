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

#include "iresearch/formats/formats.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/term_iterator.hpp"
#include "iresearch/search/term_predicate.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

// The language a pattern is read in. A dialect is a property of the regexp
// languages alone, so it is spelled out here instead of riding along as a
// parameter the other kinds have to be handed and then ignore.
enum class PatternKind : uint8_t {
  // RE2, Perl dialect.
  RegexpPerl,
  // RE2, POSIX ERE dialect.
  RegexpPosixEre,
  // `%` / `_` wildcards over arbitrary bytes.
  Wildcard,
  // A conjunction of operands the optimizer fused; the pattern is a rendering
  // for display and equality only and parses as neither of the above.
  Fused,
};

constexpr PatternKind RegexpPattern(RegexpSyntax syntax) noexcept {
  return syntax == RegexpSyntax::Perl ? PatternKind::RegexpPerl
                                      : PatternKind::RegexpPosixEre;
}

// What a filter needs of the language it selects terms by: a walk over a term
// dictionary, and a whole-term test.
//
// Whatever a source compiles its language into is compiled once, when the
// source is made, and every walk and test it hands out drives that one thing --
// so a source must outlive them, which is what makes the query's own filter the
// right owner.
class TermAcceptorSource {
 public:
  using ptr = std::shared_ptr<const TermAcceptorSource>;

  virtual ~TermAcceptorSource() = default;

  // False when the language could not be compiled at all: nothing is selected,
  // so a caller that assembled the pattern itself may reject it instead.
  virtual bool ok() const noexcept = 0;

  // The terms of `reader` this source accepts, in dictionary order. Never null.
  virtual SeekTermIterator::ptr Iterator(const TermReader& reader) const = 0;

  virtual TermPredicate::ptr Predicate() const = 0;
};

// The bounds a driver walk is restricted to; `upper` is exclusive and an empty
// bound is unbounded on that side.
struct TermBounds {
  bstring lower;
  bstring upper;
};

TermAcceptorSource::ptr MakePatternSource(bytes_view pattern, PatternKind kind);

// A source whose language is `residual`'s, walked through `driver` -- a source
// whose language is a superset -- or, when there is none, through the key range
// `bounds`. `driver` is trusted to be exact, so its own test is not repeated.
TermAcceptorSource::ptr MakeConjunctionSource(TermAcceptorSource::ptr driver,
                                              TermBounds bounds,
                                              Filter::ptr residual);

}  // namespace irs
