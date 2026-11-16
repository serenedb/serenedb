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
#include "iresearch/formats/posting/block_index.hpp"
#include "iresearch/formats/posting/block_io.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/doc_input.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/enc_buf.hpp"
#include "iresearch/search/detail/posting_leaf.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

inline IRS_FORCE_INLINE uint32_t
CopyBelow8(const doc_id_t* IRS_RESTRICT first, doc_id_t max,
           doc_id_t* IRS_RESTRICT out) noexcept {
#ifdef __AVX2__
  const auto ids = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(first));
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(out), ids);
  const auto below =
    static_cast<uint32_t>(_mm256_movemask_ps(_mm256_castsi256_ps(
      _mm256_cmpgt_epi32(_mm256_set1_epi32(static_cast<int32_t>(max)), ids))));
  return static_cast<uint32_t>(std::countr_one(below));
#else
  uint32_t n = 0;
  while (n != 8 && first[n] < max) {
    out[n] = first[n];
    ++n;
  }
  return n;
#endif
}

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
class PostingBatch
  : public PostingLeaf<InputType, Scored ? kScoredBatchShape : kBatchShape> {
  using Base = PostingLeaf<InputType, Scored ? kScoredBatchShape : kBatchShape>;

 public:
  using Base::kBlock;

  doc_id_t Last() const noexcept { return _last; }

  bool Step(doc_id_t live) {
    if (live - _last <= kBlock || !_walk.Armed()) {
      return true;
    }
    _left_in_list = _walk.Seek(live, In());
    if (_left_in_list == 0) {
      return false;
    }
    const auto landing = _walk.Landing();
    In().Seek(landing.doc_ptr);
    _last = landing.doc;
    return true;
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
  using Base::_freqs;
  using Base::_hint;
  using Base::_in;
  using Base::_last;
  using Base::_left_in_list;
  using Base::_provider;
  using Base::_score;
  using Base::_walk;
  using Base::ArmWalk;
  using Base::Enc;
  using Base::In;
  using Base::MakeScore;
  using Base::OpenInput;
  using Base::OpenList;
  using Base::SetFreqLen;
  using Base::SkipFreqs;

  IRS_FORCE_INLINE void ReadDocs(doc_id_t* IRS_RESTRICT dest, uint32_t len) {
    _hint.Advance(In(), In().Position());
    block_io::ReadTailDeltaAt(len, In(), Enc(), dest, _last);
    _last = dest[len - 1];
    _left_in_list -= len;
  }

  IRS_FORCE_INLINE void ScoreBlock(const doc_id_t* docs, score_t* scores) {
    static_assert(Scored);
    block_io::ReadBlock<block_io::kFreqBias>(In(), Enc(), _freqs.data);
    FetchAndScorePostingBlock(*_score.fetcher, _score.score, docs, scores,
                              this->NextLast());
  }

  void ScoreTail(const doc_id_t* docs, score_t* scores, uint32_t len) {
    static_assert(Scored);
    block_io::ReadTail<block_io::kFreqBias>(len, In(), Enc(), _freqs.data);
    _provider.freq.value = _freqs.data + (kBlock - len);
    FetchAndScore(*_score.fetcher, _score.score, docs, scores, len);
    _provider.freq.value = _freqs.data;
  }
};

}  // namespace irs::detail
