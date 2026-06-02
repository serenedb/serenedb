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

#pragma once

#include <vector>

#include "iresearch/search/exclusion.hpp"
#include "iresearch/search/filter.hpp"

namespace irs {

class BooleanFilter;
class Or;

// Base class for boolean queries
class BooleanQuery : public QueryBuilder {
 public:
  using queries_t = ManagedVector<QueryBuilder::ptr>;
  using iterator = queries_t::const_iterator;

  BooleanQuery(const SubReader& segment, queries_t&& queries, size_t excl,
               ScoreMergeType merge_type, score_t boost)
    : QueryBuilder{segment},
      _queries{std::move(queries)},
      _excl{excl},
      _merge_type{merge_type},
      _boost{boost} {}

  DocIterator::ptr Execute(const ExecutionContext& ctx) const final;

  void Visit(PreparedStateVisitor& visitor, score_t boost) const final;

  score_t Boost() const noexcept final { return _boost; }

  iterator begin() const { return _queries.begin(); }
  iterator excl_begin() const { return begin() + _excl; }
  iterator end() const { return _queries.end(); }

  bool empty() const { return _queries.empty(); }
  size_t size() const { return _queries.size(); }

 protected:
  virtual DocIterator::ptr Execute(const ExecutionContext& ctx, iterator begin,
                                   iterator end) const = 0;

  ScoreMergeType merge_type() const noexcept { return _merge_type; }

 private:
  // 0..excl_-1 - included queries
  // excl_..queries.end() - excluded queries
  queries_t _queries;
  // index of the first excluded query
  size_t _excl = 0;
  ScoreMergeType _merge_type = ScoreMergeType::Sum;
  score_t _boost = kNoBoost;
};

// Represent a set of queries joint by "And"
class AndQuery : public BooleanQuery {
 public:
  using BooleanQuery::BooleanQuery;

  DocIterator::ptr Execute(const ExecutionContext& ctx, iterator begin,
                           iterator end) const final;
};

// Represent a set of queries joint by "Or"
class OrQuery : public BooleanQuery {
 public:
  using BooleanQuery::BooleanQuery;

  DocIterator::ptr Execute(const ExecutionContext& ctx, iterator begin,
                           iterator end) const final;
};

// Represent a set of queries joint by "Or" with the specified
// minimum number of clauses that should satisfy criteria
class MinMatchQuery : public BooleanQuery {
 public:
  MinMatchQuery(const SubReader& segment, queries_t&& queries, size_t excl,
                ScoreMergeType merge_type, score_t boost,
                size_t min_match_count)
    : BooleanQuery{segment, std::move(queries), excl, merge_type, boost},
      _min_match_count{min_match_count} {
    SDB_ASSERT(_min_match_count > 1);
  }

  DocIterator::ptr Execute(const ExecutionContext& ctx, iterator begin,
                           iterator end) const final;

 private:
  size_t _min_match_count;
};

class BoostQuery : public QueryBuilder {
 public:
  BoostQuery(const SubReader& segment, QueryBuilder::ptr&& req,
             std::vector<QueryBuilder::ptr>&& opt)
    : QueryBuilder{segment}, _req{std::move(req)}, _opt{std::move(opt)} {}

  DocIterator::ptr Execute(const ExecutionContext& ctx) const final;

  void Visit(PreparedStateVisitor& visitor, score_t boost) const final;

  score_t Boost() const noexcept final {
    SDB_ASSERT(false);
    return {};
  }

 private:
  QueryBuilder::ptr _req;
  std::vector<QueryBuilder::ptr> _opt;
};

}  // namespace irs
