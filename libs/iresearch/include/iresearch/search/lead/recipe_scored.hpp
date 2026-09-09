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

#include <utility>
#include <vector>

#include "basics/assert.h"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename Leaf>
class RecipeScored {
 public:
  template<typename... Args>
  RecipeScored(const SubReader& segment, const TermReader& field,
               const ScoreArgs& args, Args&&... leaf)
    : _leaf{std::forward<Args>(leaf)...},
      _segment{&segment},
      _field{&field},
      _args{args} {
    SDB_ASSERT(_args.scorer != nullptr);
  }

  template<typename... Args>
  RecipeScored(const SubReader& segment, const ScoreArgs& args, Args&&... leaf)
    : _leaf{std::forward<Args>(leaf)...}, _segment{&segment}, _args{args} {
    SDB_ASSERT(_args.scorer != nullptr);
  }

  RecipeScored(RecipeScored&&) = delete;
  RecipeScored& operator=(RecipeScored&&) = delete;

  doc_id_t Advance() { return _doc = _leaf.Advance(); }

  doc_id_t Seek(doc_id_t target) { return _doc = _leaf.Seek(target); }

  doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = _leaf.Seek(target);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t) noexcept {}

  ScoreFunction PrepareScore() {
    return _args.scorer->PrepareScorer({
      .segment = *_segment,
      .field = _field != nullptr ? _field->meta() : search::NoField(),
      .doc_attrs = search::NoAttributes(),
      .fetcher = _args.fetcher,
      .stats = _args.stats,
      .boost = _args.boost,
    });
  }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    search::AppendScorer(out, PrepareScore());
  }

 private:
  Leaf _leaf;
  const SubReader* _segment;
  const TermReader* _field = nullptr;
  ScoreArgs _args;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead
