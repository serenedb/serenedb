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
#include "iresearch/search/filter.hpp"
#include "iresearch/search/term_acceptor.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class AutomatonFilter;
struct FilterVisitor;

struct AutomatonOptions {
  using FilterType = AutomatonFilter;

  bstring pattern;
  TermAcceptorSource::ptr source;
  PatternKind kind{PatternKind::Regexp};
  size_t scored_terms_limit{1024};

  AutomatonOptions() = default;
  AutomatonOptions(bytes_view pattern, PatternKind kind, RegexpSyntax syntax,
                   size_t scored_terms_limit);
  AutomatonOptions(bytes_view pattern, TermAcceptorSource::ptr source,
                   size_t scored_terms_limit);

  bool operator==(const AutomatonOptions& rhs) const noexcept {
    return pattern == rhs.pattern && kind == rhs.kind &&
           scored_terms_limit == rhs.scored_terms_limit;
  }
};

class AutomatonFilter final : public FilterWithField<AutomatonOptions> {
 public:
  static field_visitor visitor(TermAcceptorSource::ptr source);

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;

  TermPredicate::ptr CompileTermPredicate() const final;

  TermIterator::ptr CompileTermIterator(const TermReader& reader) const final;
};

// Instantiates a filter over the terms `source` accepts.
QueryBuilder::ptr PrepareAcceptorSegment(const SubReader& segment,
                                         const PrepareContext& ctx,
                                         irs::field_id field,
                                         const TermAcceptorSource& source,
                                         score_t boost);

}  // namespace irs
