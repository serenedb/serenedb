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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename Leaf>
class RecipeScored {
 public:
  template<typename... Args>
  RecipeScored(const SubReader& segment, const TermReader& field,
               const detail::ScoreArgs& args, Args&&... leaf)
    : _leaf{std::forward<Args>(leaf)...},
      _segment{&segment},
      _field{&field},
      _args{args} {
    SDB_ASSERT(_args.scorer != nullptr);
  }

  template<typename... Args>
  RecipeScored(const SubReader& segment, const detail::ScoreArgs& args,
               Args&&... leaf)
    : _leaf{std::forward<Args>(leaf)...}, _segment{&segment}, _args{args} {
    SDB_ASSERT(_args.scorer != nullptr);
  }

  RecipeScored(RecipeScored&&) = delete;
  RecipeScored& operator=(RecipeScored&&) = delete;

  doc_id_t Next() { return _doc = _leaf.Next(); }

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
      .field = _field != nullptr ? _field->meta() : detail::NoField(),
      .doc_attrs = detail::NoAttributes(),
      .fetcher = *_args.fetcher,
      .stats = _args.stats,
      .boost = _args.boost,
    });
  }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    detail::AppendScorer(out, PrepareScore());
  }

 private:
  Leaf _leaf;
  const SubReader* _segment;
  const TermReader* _field = nullptr;
  detail::ScoreArgs _args;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead
