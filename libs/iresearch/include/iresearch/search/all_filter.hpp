////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <memory>

#include "iresearch/search/query_builder_impl.hpp"
#include "iresearch/search/term_iterator.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class AllQuery : public QueryBuilderImpl<AllQuery> {
 public:
  explicit AllQuery(const SubReader& segment, score_t boost)
    : QueryBuilderImpl{segment, static_cast<uint32_t>(segment.docs_count()),
                       QueryKind::All},
      _boost{boost} {}

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

  void SetBoost(score_t value) noexcept final { _boost = value; }

 private:
  score_t _boost;
};

class All : public Filter {
 public:
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollectorImpl(const Scorer* scorer,
                                          StatsArena& stats,
                                          uint32_t threads) const final;

  TypeInfo::type_id type() const noexcept final { return irs::Type<All>::id(); }

  TermPredicate::ptr CompileTermPredicate() const final {
    return MakeTermPredicate(AcceptAllTerms{});
  }

  TermIterator::ptr CompileTermIterator(const TermReader& reader) const final;
};

QueryBuilder::ptr MakeAllQuery(const SubReader& segment,
                               const PrepareContext& ctx, score_t boost);

}  // namespace irs
