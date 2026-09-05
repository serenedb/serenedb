////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "filter.hpp"

#include "basics/singleton.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/query_builder_impl.hpp"

namespace irs {

struct EmptyQueryBuilder : public QueryBuilderImpl<EmptyQueryBuilder> {
 public:
  EmptyQueryBuilder() noexcept
    : QueryBuilderImpl{SubReader::empty(), 0, QueryKind::Empty} {}

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return kNoBoost; }
};

namespace {

EmptyQueryBuilder gEmptyQuery;

}  // namespace

QueryBuilder::ptr QueryBuilder::Empty() {
  return memory::to_managed<QueryBuilder>(gEmptyQuery);
}

bool QueryBuilder::IsEmpty(const QueryBuilder& query) noexcept {
  SDB_ASSERT((&query == &gEmptyQuery) == (query.Kind() == QueryKind::Empty));
  return &query == &gEmptyQuery;
}

PrepareCollector::ptr Filter::MakeCollectorImpl(const Scorer*, StatsArena&,
                                                uint32_t) const {
  return nullptr;
}

Filter::ptr Filter::empty() { return std::make_unique<Empty>(); }

TermIterator::ptr Filter::CompileTermIterator(const TermReader& reader) const {
  auto predicate = CompileTermPredicate();
  if (!predicate) {
    return nullptr;
  }
  auto it = reader.iterator();
  SDB_ASSERT(it);
  return memory::make_managed<FilteredTermIterator>(std::move(it),
                                                    std::move(predicate));
}

QueryBuilder::ptr Empty::PrepareSegment(const SubReader&,
                                        const PrepareContext&) const {
  return QueryBuilder::Empty();
}

}  // namespace irs
