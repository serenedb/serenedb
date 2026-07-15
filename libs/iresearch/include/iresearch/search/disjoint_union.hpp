////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <tuple>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs {

// Union of DISJOINT scored sub-iterators (each document matches at most one
// child) that supports ONLY top-k Collect(): it streams every child in turn and
// lets the score-collector select by score. Because the children are not
// globally doc-id ordered, ordered traversal (advance/seek and the block/emit
// helpers built on them) is unsupported -- this mirrors MaxScoreIterator, which
// is likewise a collect-only driver. Use it only when the result is consumed
// purely by Collect: no deleted-docs mask wrap (mask() adds an ordered
// MaskDocIterator) and no positional/boolean parent that would seek it.
class DisjointUnion : public DocIterator {
 public:
  DisjointUnion(std::vector<DocIterator::ptr>&& itrs, doc_id_t docs_count)
    : _itrs{std::move(itrs)} {
    std::get<CostAttr>(_attrs).reset(docs_count);
    _doc = doc_limits::eof();
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    _scorers.clear();
    _scorers.reserve(_itrs.size());
    for (auto& it : _itrs) {
      _scorers.emplace_back(it->PrepareScore(ctx));
    }
    // Each child is collected with its own scorer (see Collect); the top-level
    // score function is unused.
    return ScoreFunction::Default();
  }

  void Collect(const ScoreFunction& /*scorer*/, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    SDB_ASSERT(_scorers.size() == _itrs.size());
    for (size_t i = 0, n = _itrs.size(); i < n; ++i) {
      _itrs[i]->Collect(_scorers[i], fetcher, collector);
    }
    _doc = doc_limits::eof();
  }

  // Collect-only: children are not globally sorted, so there is no ordered walk.
  doc_id_t advance() final {
    SDB_ASSERT(false);
    return _doc = doc_limits::eof();
  }

  doc_id_t seek(doc_id_t /*target*/) final {
    SDB_ASSERT(false);
    return _doc = doc_limits::eof();
  }

  uint32_t count() final {
    SDB_ASSERT(false);
    return 0;
  }

  uint32_t EmitDocs(doc_id_t* /*out*/, doc_id_t /*max*/) final {
    SDB_ASSERT(false);
    return 0;
  }

  uint32_t EmitScoredDocs(doc_id_t* /*out*/, score_t* /*scores*/,
                          doc_id_t /*max*/, const ScoreFunction& /*scorer*/,
                          ColumnArgsFetcher* /*fetcher*/,
                          doc_id_t /*min*/) final {
    SDB_ASSERT(false);
    return 0;
  }

  std::pair<doc_id_t, bool> FillBlock(doc_id_t /*min*/, doc_id_t /*max*/,
                                      uint64_t* /*mask*/,
                                      FillBlockScoreContext /*score*/,
                                      FillBlockMatchContext /*match*/) final {
    SDB_ASSERT(false);
    return {doc_limits::eof(), true};
  }

 private:
  std::vector<DocIterator::ptr> _itrs;
  std::vector<ScoreFunction> _scorers;
  std::tuple<CostAttr> _attrs;
};

}  // namespace irs
