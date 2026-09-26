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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/doc_input.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/enc_buf.hpp"
#include "iresearch/search/detail/posting_leaf.hpp"
#include "iresearch/search/detail/posting_skip.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

inline IRS_FORCE_INLINE uint32_t
CopyBelow16(const doc_id_t* IRS_RESTRICT first, doc_id_t max,
            doc_id_t* IRS_RESTRICT out) noexcept {
#ifdef __AVX2__
  const auto lo = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(first));
  const auto hi =
    _mm256_loadu_si256(reinterpret_cast<const __m256i*>(first + 8));
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(out), lo);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + 8), hi);
  const auto edge = _mm256_set1_epi32(static_cast<int32_t>(max));
  const auto mask =
    static_cast<uint32_t>(
      _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(edge, lo)))) |
    (static_cast<uint32_t>(
       _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(edge, hi))))
     << 8);
  return static_cast<uint32_t>(std::countr_zero(~mask));
#else
  uint32_t n = 0;
  while (n != 16 && first[n] < max) {
    out[n] = first[n];
    ++n;
  }
  return n;
#endif
}

inline IRS_FORCE_INLINE uint32_t
CopyBelow(const doc_id_t* IRS_RESTRICT first, const doc_id_t* IRS_RESTRICT last,
          doc_id_t max, doc_id_t* IRS_RESTRICT out) noexcept {
  uint32_t n = 0;
#ifdef __AVX2__
  const auto edge = _mm256_set1_epi32(static_cast<int32_t>(max));
  for (; first + 8 <= last; first += 8) {
    const auto ids =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(first));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + n), ids);
    const auto keep = _mm256_cmpgt_epi32(edge, ids);
    const auto mask =
      static_cast<uint32_t>(_mm256_movemask_ps(_mm256_castsi256_ps(keep)));
    n += static_cast<uint32_t>(std::popcount(mask));
    if (mask != 0xFF) {
      return n;
    }
  }
#endif
  while (first != last && *first < max) {
    out[n++] = *first++;
  }
  return n;
}

inline IRS_FORCE_INLINE uint32_t CopyBelow(const doc_id_t* IRS_RESTRICT first,
                                           const doc_id_t* IRS_RESTRICT last,
                                           doc_id_t max,
                                           doc_id_t* IRS_RESTRICT out,
                                           const score_t* IRS_RESTRICT src,
                                           score_t* IRS_RESTRICT dst) noexcept {
  uint32_t n = 0;
#ifdef __AVX2__
  const auto edge = _mm256_set1_epi32(static_cast<int32_t>(max));
  for (; first + 8 <= last; first += 8, src += 8) {
    const auto ids =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(first));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + n), ids);
    _mm256_storeu_ps(dst + n, _mm256_loadu_ps(src));
    const auto keep = _mm256_cmpgt_epi32(edge, ids);
    const auto mask =
      static_cast<uint32_t>(_mm256_movemask_ps(_mm256_castsi256_ps(keep)));
    n += static_cast<uint32_t>(std::popcount(mask));
    if (mask != 0xFF) {
      return n;
    }
  }
#endif
  while (first != last && *first < max) {
    out[n] = *first++;
    dst[n] = *src++;
    ++n;
  }
  return n;
}

template<typename InputType, bool Scored>
class PostingBatch {
 public:
  static constexpr uint32_t kBlock = doc_limits::kBlockSize;
  static constexpr bool kDefaultInit = true;

  doc_id_t Last() const noexcept { return _last; }

  uint32_t Left() const noexcept { return _left_in_list; }

  bool Step(doc_id_t live) {
    return StepToLive(_walk, In(), live, _left_in_list, _last);
  }

  bool Start(doc_id_t min) {
    if (min <= _last + 1) {
      return true;
    }
    if (_left_in_list == 0) {
      return false;
    }
    return Step(min);
  }

 protected:
  IRS_FORCE_INLINE InputType& In() const noexcept {
    return irs::utils::downCast<InputType>(*_in);
  }

  IRS_FORCE_INLINE uint32_t* Enc() noexcept { return EncOf<InputType>(_enc); }

  void OpenInput(const PostingMeta& meta, const IndexInput& doc_in,
                 bool bounds) {
    _in = OpenDocInput(meta, doc_in);
    auto& in = In();
    LimitDocReadahead(in, meta);
    if (meta.docs_count < kBlock) {
      SkipScoreBounds(bounds, in);
    }
    _left_in_list = meta.docs_count;
  }

  void ArmWalk(const PostingMeta& meta, IndexFeatures layout, bool bounds) {
    if (meta.docs_count > kBlock) {
      _walk.Arm(meta, BlockIndexShapeOf(layout, bounds));
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
      .fetcher = *args.fetcher,
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
    _score.fetcher->FetchPostingBlock(
      std::span<const doc_id_t, kBlock>{docs, kBlock});
    _score.score.ScorePostingBlock(scores);
  }

  void ScoreTail(const doc_id_t* docs, score_t* scores, uint32_t len) {
    static_assert(Scored);
    FormatTraits128::ReadTail(len, In(), Enc(), _freqs.data);
    _provider.freq.value = _freqs.data + (kBlock - len);
    _score.fetcher->Fetch(std::span<const doc_id_t>{docs, len});
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
  BlockCursor _walk;
};

}  // namespace irs::detail
