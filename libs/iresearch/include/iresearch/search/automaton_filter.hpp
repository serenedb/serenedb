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
#include "iresearch/utils/automaton.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class AutomatonFilter;
struct FilterVisitor;
struct CompiledAcceptor;

struct AutomatonOptions {
  using FilterType = AutomatonFilter;

  bstring pattern;
  std::shared_ptr<const CompiledAcceptor> compiled;
  size_t scored_terms_limit{1024};

  AutomatonOptions() = default;
  AutomatonOptions(automaton acceptor, bytes_view pattern,
                   size_t scored_terms_limit);

  bool operator==(const AutomatonOptions& rhs) const noexcept {
    return pattern == rhs.pattern &&
           scored_terms_limit == rhs.scored_terms_limit;
  }
};

class AutomatonFilter final : public FilterWithField<AutomatonOptions> {
 public:
  static field_visitor visitor(const automaton& acceptor);

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;

  TermPredicate::ptr CompileTermPredicate() const final;

  TermIterator::ptr CompileTermIterator(const TermReader& reader) const final;
};

TermPredicate::ptr MakeAutomatonTermPredicate(
  std::shared_ptr<const CompiledAcceptor> compiled);

}  // namespace irs
