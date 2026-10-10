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
#include <span>

#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/pattern_cache.hpp"
#include "iresearch/search/detail/term_iterator.hpp"
#include "iresearch/search/detail/term_predicate.hpp"
#include "iresearch/search/filters/filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class TermAcceptorSource {
 public:
  using ptr = std::shared_ptr<const TermAcceptorSource>;

  virtual ~TermAcceptorSource() = default;

  virtual bool ok() const noexcept = 0;

  virtual SeekTermIterator::ptr Iterator(const TermReader& reader) const = 0;

  virtual TermPredicate::ptr Predicate() const = 0;

  virtual std::shared_ptr<const RegexpAcceptor> Automaton() const {
    return nullptr;
  }
};

struct TermBounds {
  bstring lower;
  bstring upper;
};

TermAcceptorSource::ptr MakePatternSource(bytes_view pattern, PatternKind kind);

TermAcceptorSource::ptr MakeFuzzySource(
  std::shared_ptr<const LevenshteinAcceptor> fuzzy);

TermAcceptorSource::ptr MakeJointSource(
  std::span<const std::shared_ptr<const RegexpAcceptor>> patterns,
  std::shared_ptr<const LevenshteinAcceptor> fuzzy);

TermAcceptorSource::ptr MakeConjunctionSource(TermAcceptorSource::ptr driver,
                                              TermBounds bounds,
                                              Filter::ptr residual);

}  // namespace irs
