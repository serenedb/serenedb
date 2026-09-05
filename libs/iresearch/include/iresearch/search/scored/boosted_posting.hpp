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

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/posting_batch.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scored/root.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::scored {

template<typename InputType, typename Table>
class BoostedPosting : public Root,
                       public search::PostingBatch<InputType, Table, true> {
  using Base = search::PostingBatch<InputType, Table, true>;

  using Base::_last;
  using Base::_left_in_list;
  using Base::kBlock;
  using Base::ReadDocs;
  using Base::ScoreBlock;
  using Base::ScoreTail;

 public:
  using Base::kTable;

  BoostedPosting(Table table, ColumnArgsFetcher& fetcher) noexcept
    : _fetcher{fetcher}, _table{table} {}

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               const SubReader& segment, const TermReader& field,
               const search::ScoreArgs& args, IndexFeatures layout, bool bounds,
               const PostingMeta& boost_meta, const TermReader& boost_field,
               const search::ScoreArgs& boost_args) {
    SDB_ASSERT(meta.docs_count > 1, "a single document has its own root");
    this->OpenInput(meta, doc_in, bounds);
    this->ArmWalk(meta, layout, bounds);
    this->MakeScore(segment, field, args);
    _boost.Prepare(boost_meta, doc_in, segment, boost_field, boost_args);
    _boost_score = _boost.PrepareScore();
  }

  uint32_t Run(doc_id_t* IRS_RESTRICT docs, score_t* IRS_RESTRICT scores,
               uint32_t capacity) final {
    uint32_t emitted = 0;

    while (_left_in_list != 0 && emitted + kBlock <= capacity) {
      if constexpr (kTable) {
        const auto from = _last + doc_limits::min();
        if (const auto live = _table.Live(from);
            live != from && !this->Step(live)) {
          break;
        }
      }
      const auto len = std::min(_left_in_list, kBlock);
      auto* const dest = docs + emitted;
      ReadDocs(dest, len);

      if (len == kBlock) {
        ScoreBlock(dest, scores + emitted);
      } else {
        ScoreTail(dest, scores + emitted, len);
      }
      Boost(dest, scores + emitted, len);
      emitted += len;
    }

    return emitted;
  }

 private:
  void Boost(const doc_id_t* IRS_RESTRICT docs, score_t* IRS_RESTRICT scores,
             uint32_t len) {
    const doc_id_t last = docs[len - 1];
    auto doc = _boost.Value();
    if (doc > last) {
      return;
    }
    uint32_t at = 0;
    uint32_t held = 0;
    for (;;) {
      if (doc < docs[at]) {
        doc = _boost.Seek(docs[at]);
        if (doc > last) {
          break;
        }
      }
      at = static_cast<uint32_t>(std::lower_bound(docs + at, docs + len, doc) -
                                 docs);
      if (docs[at] != doc) {
        continue;
      }
      _cand[held] = doc;
      _pos[held] = at;
      _boost.FetchScoreArgs(held);
      if (++held == kScoreBlock) {
        Flush(scores, held);
        held = 0;
      }
      doc = _boost.Advance();
      if (doc > last) {
        break;
      }
      ++at;
    }
    if (held != 0) {
      Flush(scores, held);
    }
  }

  void Flush(score_t* IRS_RESTRICT scores, uint32_t held) {
    std::fill(_cand + held, _cand + kScoreBlock, _cand[held - 1]);
    _fetcher.FetchScoreBlock(
      std::span<const doc_id_t, kScoreBlock>{_cand, kScoreBlock});
    _boost_score.ScoreBlock(_boosts);
    for (uint32_t i = 0; i != held; ++i) {
      scores[_pos[i]] += _boosts[i];
    }
  }

  ColumnArgsFetcher& _fetcher;
  search::PostingLeadScored<InputType> _boost;
  ScoreFunction _boost_score;
  ABSL_CACHELINE_ALIGNED doc_id_t _cand[kScoreBlock];
  ABSL_CACHELINE_ALIGNED score_t _boosts[kScoreBlock];
  uint32_t _pos[kScoreBlock];
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::scored
