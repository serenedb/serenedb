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

// Base class for boolean queries
class BooleanQuery : public Filter::Query {
 public:
  using queries_t = ManagedVector<Filter::Query::ptr>;
  using iterator = queries_t::const_iterator;

  DocIterator::ptr execute(const ExecutionContext& ctx) const final;

  void visit(const irs::SubReader& segment, irs::PreparedStateVisitor& visitor,
             score_t boost) const final;

  score_t Boost() const noexcept final { return _boost; }

  void prepare(const PrepareContext& ctx, ScoreMergeType merge_type,
               queries_t queries, size_t exclude_start);

  void prepare(const PrepareContext& ctx, ScoreMergeType merge_type,
               std::span<const Filter* const> incl,
               std::span<const Filter* const> excl);

  iterator begin() const { return _queries.begin(); }
  iterator excl_begin() const { return begin() + _excl; }
  iterator end() const { return _queries.end(); }

  bool empty() const { return _queries.empty(); }
  size_t size() const { return _queries.size(); }

 protected:
  virtual DocIterator::ptr execute(const ExecutionContext& ctx, iterator begin,
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
  DocIterator::ptr execute(const ExecutionContext& ctx, iterator begin,
                           iterator end) const final;
};

// Represent a set of queries joint by "Or"
class OrQuery : public BooleanQuery {
 public:
  DocIterator::ptr execute(const ExecutionContext& ctx, iterator begin,
                           iterator end) const final;
};

// Represent a set of queries joint by "Or" with the specified
// minimum number of clauses that should satisfy criteria
class MinMatchQuery : public BooleanQuery {
 public:
  explicit MinMatchQuery(size_t min_match_count) noexcept
    : _min_match_count{min_match_count} {
    SDB_ASSERT(_min_match_count > 1);
  }

  DocIterator::ptr execute(const ExecutionContext& ctx, iterator begin,
                           iterator end) const final;

 private:
  size_t _min_match_count;
};

}  // namespace irs
