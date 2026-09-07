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

#include <tuple>
#include <utility>
#include <vector>

#include "basics/bit_utils.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/common/score/make_probe.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename Head, typename Optional, size_t N = 0>
class SparseBoostScored {
 public:
  static constexpr uint32_t kBatch = kScoreBlock;
  static_assert(kBatch <= BitsRequired<uint32_t>(),
                "the slots a clause held are one word");

  template<typename Init>
  SparseBoostScored(ScoreMergeType inner, Head&& head, size_t size, Init&& init,
                    score_t absorbed = 0)
    : _head{std::move(head)},
      _optional{size, std::forward<Init>(init)},
      _held{size, [](uint32_t& word, size_t) noexcept { word = 0; }},
      _absorbed{absorbed},
      _inner{inner} {}

  template<typename HeadArgs, typename Init>
  SparseBoostScored(std::piecewise_construct_t, ScoreMergeType inner,
                    HeadArgs&& head, size_t size, Init&& init,
                    score_t absorbed = 0)
    : _head{std::make_from_tuple<Head>(std::forward<HeadArgs>(head))},
      _optional{size, std::forward<Init>(init)},
      _held{size, [](uint32_t& word, size_t) noexcept { word = 0; }},
      _absorbed{absorbed},
      _inner{inner} {}

  SparseBoostScored(SparseBoostScored&&) = delete;
  SparseBoostScored& operator=(SparseBoostScored&&) = delete;

  doc_id_t Value() const noexcept { return _doc; }

  doc_id_t Advance() { return _doc = _head.Advance(); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = _head.Seek(target);
  }

  doc_id_t Probe(doc_id_t target) { return Seek(target); }

  void FetchScoreArgs(uint32_t slot) {
    SDB_ASSERT(slot < kBatch);
    _head.FetchScoreArgs(slot);
    for (size_t i = 0; i != _optional.size(); ++i) {
      auto& opt = _optional[i];
      if (opt.Probe(_doc) != _doc) {
        continue;
      }
      opt.FetchScoreArgs(slot);
      SetBit(_held[i], slot);
    }
  }

  ScoreFunction PrepareScore() {
    std::vector<ScoreFunction> scorers;
    scorers.reserve(_optional.size());
    for (auto& opt : _optional) {
      scorers.emplace_back(opt.PrepareScore());
    }
    auto required = _head.PrepareScore();
    return search::MakeProbeScore(_inner, std::move(required),
                                  std::move(scorers), _held.data(), _absorbed);
  }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    search::AppendScorer(out, PrepareScore());
  }

 private:
  Head _head;
  search::RunOf<Optional, N> _optional;
  search::RunOf<uint32_t, N> _held;
  doc_id_t _doc = doc_limits::invalid();
  score_t _absorbed;
  ScoreMergeType _inner;
};

}  // namespace irs::lead
