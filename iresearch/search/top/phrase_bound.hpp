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

#include <algorithm>
#include <limits>
#include <utility>

#include "iresearch/formats/posting/block_index.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

class PhraseBound {
 public:
  PhraseBound(const SubReader& segment, const TermReader& field,
              const irs::detail::ScoreArgs& args) {
    SDB_ASSERT(args.scorer != nullptr);
    if (auto source = args.scorer->PrepareScoreBoundSource()) {
      _func = args.scorer->PrepareScorer({
        .segment = segment,
        .field = field.meta(),
        .doc_attrs = *source,
        .fetcher = *args.fetcher,
        .stats = args.stats,
        .boost = args.boost,
      });
      _source = std::move(source);
    }
  }

  bool Bounded() const noexcept { return _source != nullptr; }

  template<typename Slots>
  doc_id_t AdvanceBlock(Slots& slots, doc_id_t target) {
    const auto* const index = Index(slots);
    if (index == nullptr) {
      return doc_limits::eof();
    }
    _block = index->Find(_block, target);
    return _block < index->Size() ? index->Last(_block) : doc_limits::eof();
  }

  template<typename Slots>
  score_t MaxScore(Slots& slots, doc_id_t last) {
    const auto* const index = Index(slots);
    if (index == nullptr) {
      return std::numeric_limits<score_t>::max();
    }
    const auto n = index->Size();
    const auto end = index->Find(_block, last);
    score_t best = 0;
    for (auto k = _block; k < n && k <= end;) {
      const auto run = k / BlockIndex::kRun;
      const auto run_end = std::min(n, (run + 1) * BlockIndex::kRun);
      if (k == run * BlockIndex::kRun && run_end <= end + 1) {
        best = std::max(best, Score(slots, index->RunBound(run)));
        k = run_end;
        continue;
      }
      best = std::max(best, Score(slots, index->Bound(k)));
      ++k;
    }
    return best;
  }

 private:
  template<typename Slots>
  const BlockIndex* Index(Slots& slots) {
    auto* const leaf = _source ? slots.BoundLeaf() : nullptr;
    return leaf ? leaf->Blocks() : nullptr;
  }

  template<typename Slots>
  score_t Score(const Slots& slots, BoundPair bound) {
    _source->Set(slots.FreqBoundOf(bound.freq), bound.norm);
    return _func.Score();
  }

  ScoreFunction _func;
  ScoreBoundSource::ptr _source;
  uint32_t _block = 0;
};

}  // namespace irs::top
