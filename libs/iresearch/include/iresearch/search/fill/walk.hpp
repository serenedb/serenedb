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
#include <span>
#include <utility>

#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/lead/constant_scored.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<lead::Type Leaf>
class WalkDocs {
 public:
  template<typename... Args>
  explicit WalkDocs(Args&&... args) : _leaf{std::forward<Args>(args)...} {}

  doc_id_t FillOr(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    auto doc = From(min);
    while (doc < max) {
      const size_t offset = doc - min;
      SetBit(mask[offset / search::kWindowBits], offset % search::kWindowBits);
      doc = _leaf.Advance();
    }
    return _doc = doc;
  }

  doc_id_t FillAnd(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    search::AndCursor cursor{.words = mask};
    auto doc = From(min);
    while (doc < max) {
      cursor.Doc(doc - min);
      doc = _leaf.Advance();
    }
    cursor.Settle(max - min);
    return _doc = doc;
  }

  doc_id_t FillAndNot(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    auto doc = From(min);
    while (doc < max) {
      const size_t offset = doc - min;
      UnsetBit(mask[offset / search::kWindowBits],
               offset % search::kWindowBits);
      doc = _leaf.Advance();
    }
    return _doc = doc;
  }

 private:
  doc_id_t From(doc_id_t min) {
    auto doc = _doc;
    if (const auto start = std::max(min, doc_limits::min()); doc < start) {
      doc = _leaf.Seek(start);
    }
    return doc;
  }

  Leaf _leaf;
  doc_id_t _doc = doc_limits::invalid();
};

template<typename Leaf>
class WalkScored {
 public:
  template<typename... Args>
  explicit WalkScored(ScoreMergeType merge, ColumnArgsFetcher& fetcher,
                      Args&&... args)
    : _leaf{std::forward<Args>(args)...},
      _score{_leaf.PrepareScore()},
      _fetcher{fetcher},
      _merge{merge} {}

  WalkScored(WalkScored&&) = delete;
  WalkScored& operator=(WalkScored&&) = delete;

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                score_t* IRS_RESTRICT scores) {
    auto doc = From(min);
    scores_size_t n = 0;
    while (doc < max) {
      const uint32_t offset = doc - min;
      SetBit(mask[offset / search::kWindowBits], offset % search::kWindowBits);
      _docs[n] = doc;
      _leaf.FetchScoreArgs(n);
      if (++n == kScoreBlock) {
        Settle(min, n, scores);
        n = 0;
      }
      doc = _leaf.Advance();
    }
    Settle(min, n, scores);
    return _doc = doc;
  }

 private:
  doc_id_t From(doc_id_t min) {
    auto doc = _doc;
    if (const auto start = std::max(min, doc_limits::min()); doc < start) {
      doc = _leaf.Seek(start);
    }
    return doc;
  }

  void Settle(doc_id_t min, scores_size_t n,
              score_t* IRS_RESTRICT scores) noexcept {
    if (n == 0) {
      return;
    }
    if (n == kScoreBlock) [[likely]] {
      _fetcher.FetchScoreBlock(
        std::span<const doc_id_t, kScoreBlock>{_docs, kScoreBlock});
      _score.ScoreBlock(_scores);
    } else {
      _fetcher.Fetch(std::span<const doc_id_t>{_docs, n});
      _score.Score(_scores, n);
    }
    irs::ResolveMergeType(_merge, [&]<ScoreMergeType Merge> {
      for (scores_size_t i = 0; i != n; ++i) {
        irs::Merge<Merge>(scores[_docs[i] - min], _scores[i]);
      }
    });
  }

  ABSL_CACHELINE_ALIGNED doc_id_t _docs[kScoreBlock];
  ABSL_CACHELINE_ALIGNED score_t _scores[kScoreBlock];
  Leaf _leaf;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  ScoreMergeType _merge;
  doc_id_t _doc = doc_limits::invalid();
};

template<typename Node>
using ByWalkDocs = Impl<WalkDocs<Node>>;

template<typename Node>
using ByWalkScored = Impl<WalkScored<Node>>;

template<typename Node>
using WalkConstantScored = ByWalkScored<lead::ConstantScored<Node>>;

}  // namespace irs::fill
