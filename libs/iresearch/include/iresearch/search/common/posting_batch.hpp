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
#include <type_traits>

#include "basics/down_cast.h"
#include "basics/empty.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/enc_buf.hpp"
#include "iresearch/search/common/posting_leaf.hpp"
#include "iresearch/search/common/posting_skip.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/skip_walk.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename InputType, typename Table, bool Scored>
class PostingBatch {
 public:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;
  static constexpr uint32_t kBlock = doc_limits::kBlockSize;

  doc_id_t Last() const noexcept { return _last; }

  uint32_t Left() const noexcept { return _left_in_list; }

  bool Step(doc_id_t live) {
    static_assert(kTable);
    return StepToLive(_walk, In(), live, _left_in_list, _last);
  }

 protected:
  IRS_FORCE_INLINE InputType& In() const noexcept {
    return sdb::basics::downCast<InputType>(*_in);
  }

  IRS_FORCE_INLINE uint32_t* Enc() noexcept { return EncOf<InputType>(_enc); }

  void OpenInput(const PostingMeta& meta, const IndexInput& doc_in,
                 bool bounds) {
    _in = doc_in.Reopen();
    if (!_in) [[unlikely]] {
      throw IoError{"failed to reopen document input"};
    }
    auto& in = In();
    in.Seek(meta.doc_start);
    if (meta.docs_count < kBlock) {
      SkipScoreBounds(bounds, in);
    }
    _left_in_list = meta.docs_count;
  }

  void ArmWalk(const PostingMeta& meta, IndexFeatures layout, bool bounds) {
    if constexpr (kTable) {
      if (meta.docs_count > kBlock) {
        const auto skip = ToSkipLayout(layout);
        _walk.Arm(meta, {.bounds = bounds, .pos = skip.pos, .offs = skip.offs});
      }
    }
  }

  IRS_FORCE_INLINE void SetFreqLen(bool has_freq) noexcept {
    static_assert(!Scored);
    _freq_len.value = has_freq ? kBlock : 0;
  }

  void MakeScore(const SubReader& segment, const TermReader& field,
                 const ScoreArgs& args) {
    static_assert(Scored);
    _provider.freq.value = _freqs.data;
    _score.fetcher = args.fetcher;
    SDB_ASSERT(args.scorer != nullptr);
    _score.score = args.scorer->PrepareScorer({
      .segment = segment,
      .field = field.meta(),
      .doc_attrs = _provider,
      .fetcher = args.fetcher,
      .stats = args.stats,
      .boost = args.boost,
    });
  }

  IRS_FORCE_INLINE void SkipFreqs(uint32_t len) {
    static_assert(!Scored);
    SDB_ASSERT(len != 0);
    if (len == _freq_len.value) {
      FormatTraits128::SkipBlock(In());
    }
  }

  IRS_FORCE_INLINE void ReadDocs(doc_id_t* IRS_RESTRICT dest, uint32_t len) {
    FormatTraits128::ReadTailDeltaAt(len, In(), Enc(), dest, _last);
    _last = dest[len - 1];
    _left_in_list -= len;
  }

  void ScoreBlock(const doc_id_t* docs, score_t* scores) {
    static_assert(Scored);
    FormatTraits128::ReadBlock(In(), Enc(), _freqs.data);
    if (_score.fetcher != nullptr) {
      _score.fetcher->FetchPostingBlock(
        std::span<const doc_id_t, kBlock>{docs, kBlock});
    }
    _score.score.ScorePostingBlock(scores);
  }

  void ScoreTail(const doc_id_t* docs, score_t* scores, uint32_t len) {
    static_assert(Scored);
    FormatTraits128::ReadTail(len, In(), Enc(), _freqs.data);
    _provider.freq.value = _freqs.data + (kBlock - len);
    if (_score.fetcher != nullptr) {
      _score.fetcher->Fetch(std::span<const doc_id_t>{docs, len});
    }
    _score.score.Score(scores, static_cast<scores_size_t>(len));
    _provider.freq.value = _freqs.data;
  }

  [[no_unique_address]] NeedEnc<InputType> _enc;
  [[no_unique_address]] utils::Need<Scored, FreqBuf> _freqs;
  IndexInput::ptr _in;
  doc_id_t _last = 0;
  uint32_t _left_in_list = 0;
  [[no_unique_address]] utils::Need<!Scored, FreqLen> _freq_len;
  [[no_unique_address]] utils::Need<Scored, LeafScore> _score;
  [[no_unique_address]] utils::Need<Scored, LeafProvider> _provider;
  [[no_unique_address]] utils::Need<kTable, SkipWalk<InputType>> _walk;
};

}  // namespace irs::search
