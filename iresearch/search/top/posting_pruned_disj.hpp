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

#include "iresearch/search/top/pruned_leaf.hpp"

namespace irs::top {

template<typename InputType>
class PostingPrunedDisj : public PrunedProbeBase<InputType> {
  using Base = PrunedProbeBase<InputType>;

  using Base::_base;
  using Base::_cursor;
  using Base::_doc;
  using Base::_docs;
  using Base::_fetcher;
  using Base::_freqs;
  using Base::_gather;
  using Base::_lazy;
  using Base::_leaf;
  using Base::_left_in_leaf;
  using Base::_left_in_list;
  using Base::_len;
  using Base::_max_in_leaf;
  using Base::_needs_reposition;
  using Base::_provider;
  using Base::_score;
  using Base::_scored;
  using Base::DecodeFreqs;
  using Base::FirstNotBelow;
  using Base::Members;
  using Base::ReadLeaf;

 public:
  using Base::AdvanceBlock;
  using Base::MaxScore;
  using Base::Value;

  PostingPrunedDisj() = default;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, const SubReader& segment,
               const TermReader& field, const detail::ScoreArgs& args) {
    _masked = false;
    if (Base::PrepareCommon(meta, doc_in, layout, segment, field, args)) {
      _left_in_leaf = 0;
      _doc = doc_limits::min() + meta.doc_delta;
      _max_in_leaf = _doc;
    }
  }

  PostingPrunedDisj(const PostingMeta& meta, const IndexInput& doc_in,
                    IndexFeatures layout, const SubReader& segment,
                    const TermReader& field, const detail::ScoreArgs& args) {
    Prepare(meta, doc_in, layout, segment, field, args);
  }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    Unmask(target);
    AdvanceBlock(target);
    if (_left_in_leaf == 0) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      Base::Reposition();
      ReadLeaf(_doc);
    }
    for (;;) {
      if (_left_in_leaf != 0 && target <= _max_in_leaf) {
        return _doc = TakeFrom(target);
      }
      _left_in_leaf = 0;
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      ReadLeaf(*(std::end(_docs) - 1));
    }
  }

  template<typename Visitor>
  void ForEachScoredBlock(doc_id_t max, Visitor&& visit) {
    if (_doc >= max) [[unlikely]] {
      return;
    }
    Unmask(_doc);
    Base::ForEachScoredBlock(max, std::forward<Visitor>(visit));
  }

  void Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
            score_t* IRS_RESTRICT window) {
    ForEachScoredBlock(
      max, [&](const doc_id_t* IRS_RESTRICT docs, uint32_t len,
               const score_t* IRS_RESTRICT scores) IRS_FORCE_INLINE {
        static constexpr auto kBits = BitsRequired<uint64_t>();
        const auto add = [&](uint32_t i) IRS_FORCE_INLINE {
          const size_t offset = docs[i] - min;
          SetBit(mask[offset / kBits], offset % kBits);
          window[offset] += scores[i];
        };
        if (len == doc_limits::kBlockSize) [[likely]] {
          VisitDocs<doc_limits::kBlockSize>(doc_limits::kBlockSize, add);
        } else {
          for (uint32_t i = 0; i != len; ++i) {
            add(i);
          }
        }
      });
  }

  void FillCounted(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                   score_t* IRS_RESTRICT window,
                   uint32_t* IRS_RESTRICT counts) {
    ForEachScoredBlock(
      max, [&](const doc_id_t* IRS_RESTRICT docs, uint32_t len,
               const score_t* IRS_RESTRICT scores) IRS_FORCE_INLINE {
        static constexpr auto kBits = BitsRequired<uint64_t>();
        const auto add = [&](uint32_t i) IRS_FORCE_INLINE {
          const size_t offset = docs[i] - min;
          SetBit(mask[offset / kBits], offset % kBits);
          window[offset] += scores[i];
          ++counts[offset];
        };
        if (len == doc_limits::kBlockSize) [[likely]] {
          VisitDocs<doc_limits::kBlockSize>(doc_limits::kBlockSize, add);
        } else {
          for (uint32_t i = 0; i != len; ++i) {
            add(i);
          }
        }
      });
  }

  template<typename DocsBuffer, typename ScoresBuffer>
  void ScoreCandidates(DocsBuffer& cand_docs, ScoresBuffer& cand_scores,
                       bool required) {
    const auto count = static_cast<uint32_t>(cand_docs.size());
    SDB_ASSERT(count != 0);
    Mask();
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kScoreBlock];
    ABSL_CACHELINE_ALIGNED uint32_t slots[kScoreBlock];
    ABSL_CACHELINE_ALIGNED score_t scores[kScoreBlock];
    _provider.freq.value = _gather;
    uint32_t out = 0;
    for (uint32_t begin = 0; begin < count && !doc_limits::eof(_doc);
         begin += kScoreBlock) {
      const auto n = std::min<uint32_t>(count - begin, kScoreBlock);
      const auto* const chunk = &cand_docs[begin];
      uint32_t hits = 0;
      Members(chunk, n,
              [&](uint32_t i, bool hit, uint32_t freq) IRS_FORCE_INLINE {
                docs[hits] = chunk[i];
                slots[hits] = begin + i;
                _gather[hits] = freq;
                hits += static_cast<uint32_t>(hit);
              });
      if (hits == 0) {
        continue;
      }
      _fetcher->Fetch(std::span<const doc_id_t>{docs, hits});
      if (hits == kScoreBlock) {
        _score.ScoreBlock(scores);
      } else {
        _score.Score(scores, static_cast<scores_size_t>(hits));
      }
      if (required) {
        for (uint32_t j = 0; j != hits; ++j) {
          cand_docs[out] = docs[j];
          cand_scores[out] = cand_scores[slots[j]] + scores[j];
          ++out;
        }
      } else {
        for (uint32_t j = 0; j != hits; ++j) {
          cand_scores[slots[j]] += scores[j];
        }
      }
    }
    _provider.freq.value = _freqs.data;
    if (required) {
      cand_docs.resize(out);
      cand_scores.resize(out);
    }
  }

 private:
  void Mask() noexcept {
    if (_masked) {
      return;
    }
    _masked = true;
    _lazy = nullptr;
    _leaf.Reset({.bitset = nullptr,
                 .words = 0,
                 .max = _max_in_leaf,
                 .kind = block_io::FillLeaf::Kind::Docs},
                nullptr, _len);
  }

  void Unmask(doc_id_t target) {
    if (!_masked) [[likely]] {
      return;
    }
    _masked = false;
    if (_needs_reposition) {
      return;
    }
    if (target > _max_in_leaf) {
      _lazy = nullptr;
      _left_in_leaf = 0;
      _doc = _max_in_leaf;
      return;
    }
    const auto len = _leaf.Len();
    _leaf.Materialize(_base, std::end(_docs) - len);
    if (_lazy != nullptr) {
      DecodeFreqs();
    }
    _len = len;
    _scored = false;
    const auto* const it = FirstNotBelow(std::end(_docs) - len, _doc);
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - it) - 1;
  }

  IRS_FORCE_INLINE doc_id_t TakeFrom(doc_id_t target) noexcept {
    if (_len == doc_limits::kBlockSize) [[likely]] {
      const auto* const it =
        BranchlessLowerBound<doc_limits::kBlockSize>(std::begin(_docs), target);
      _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - it) - 1;
      return *it;
    }
    for (;;) {
      const auto doc = *(std::end(_docs) - _left_in_leaf);
      --_left_in_leaf;
      if (target <= doc) {
        return doc;
      }
    }
  }

  bool _masked = false;
};

}  // namespace irs::top
