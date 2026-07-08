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

#include "automaton_filter.hpp"

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/limited_sample_selector.hpp"
#include "iresearch/utils/automaton_utils.hpp"

namespace irs {

AutomatonOptions::AutomatonOptions(automaton acceptor, bytes_view pattern,
                                   size_t scored_terms_limit)
  : pattern{pattern},
    compiled{std::make_shared<const CompiledAcceptor>(std::move(acceptor))},
    scored_terms_limit{scored_terms_limit} {}

field_visitor AutomatonFilter::visitor(const automaton& acceptor) {
  if (!Validate(acceptor)) {
    return [](const SubReader&, const TermReader&, FilterVisitor&) {};
  }

  struct AutomatonContext : util::Noncopyable {
    explicit AutomatonContext(const automaton& a)
      : matcher{MakeAutomatonMatcher(a)} {}

    automaton_table_matcher matcher;
  };

  auto ctx = AutomatonContext{acceptor};

  return [context = std::move(ctx)](const SubReader& segment,
                                    const TermReader& field,
                                    FilterVisitor& visitor) mutable {
    return irs::Visit(segment, field, context.matcher, visitor);
  };
}

QueryBuilder::ptr AutomatonFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  SDB_ASSERT(options().compiled);
  return PrepareAutomatonSegment(segment, ctx, field_id(),
                                 options().compiled->matcher, Boost());
}

PrepareCollector::ptr AutomatonFilter::MakeCollector(
  const Scorer* scorer) const {
  return std::make_unique<LimitedTermsCollector>(scorer,
                                                 options().scored_terms_limit);
}

namespace {

class AutomatonTermPredicate final : public TermPredicate {
 public:
  explicit AutomatonTermPredicate(
    std::shared_ptr<const CompiledAcceptor> compiled) noexcept
    : _compiled{std::move(compiled)} {}

  bool Accepts(bytes_view term) const final {
    return bool(Accept(_compiled->acceptor, term));
  }

 private:
  std::shared_ptr<const CompiledAcceptor> _compiled;
};

}  // namespace

TermPredicate::ptr MakeAutomatonTermPredicate(
  std::shared_ptr<const CompiledAcceptor> compiled) {
  if (!compiled) {
    return nullptr;
  }
  return std::make_unique<AutomatonTermPredicate>(std::move(compiled));
}

TermPredicate::ptr AutomatonFilter::CompileTermPredicate() const {
  return MakeAutomatonTermPredicate(options().compiled);
}

TermIterator::ptr AutomatonFilter::CompileTermIterator(
  const TermReader& reader) const {
  if (!options().compiled) {
    return nullptr;
  }
  auto it = reader.iterator(options().compiled->matcher);
  SDB_ASSERT(it);
  return it;
}

}  // namespace irs
