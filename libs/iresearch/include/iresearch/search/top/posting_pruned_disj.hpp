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

#include "iresearch/search/top/detail/prune_leaf.hpp"

namespace irs::search {

template<typename InputType>
class PostingPrunedDisj : public PruneLeafBase<InputType, false> {
  using Base = PruneLeafBase<InputType, false>;

  using Base::_base;
  using Base::_doc;
  using Base::_docs;
  using Base::_enc;
  using Base::_fetcher;
  using Base::_freqs;
  using Base::_left_in_leaf;
  using Base::_left_in_list;
  using Base::_len;
  using Base::_max_in_leaf;
  using Base::_needs_reposition;
  using Base::_provider;
  using Base::_score;
  using Base::_skip;
  using Base::_upper_bound;
  using Base::Emit;
  using Base::In;
  using Base::ReadLeaf;
  using Base::RepositionForWindow;

 public:
  using Base::MaxScore;
  using Base::SetSkipBoundsBelow;
  using Base::Value;

  PostingPrunedDisj() = default;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, const SubReader& segment,
               const TermReader& field, const ScoreArgs& args) {
    if (Base::PrepareCommon(meta, doc_in, layout, segment, field, args)) {
      _left_in_leaf = 0;
      _doc = doc_limits::min() + meta.doc_delta;
    }
  }

  PostingPrunedDisj(const PostingMeta& meta, const IndexInput& doc_in,
                    IndexFeatures layout, const SubReader& segment,
                    const TermReader& field, const ScoreArgs& args) {
    Prepare(meta, doc_in, layout, segment, field, args);
  }

  doc_id_t SeekToBlock(doc_id_t target) {
    if (_skip.NumLevels() == 0) [[unlikely]] {
      return doc_limits::eof();
    }
    auto& reader = _skip.Reader();
    const auto upper_bound = reader.UpperBound();
    if (upper_bound >= target) {
      return upper_bound;
    }
    const auto below = reader.SkipBoundsBelow();
    reader.SetSkipBoundsBelow(std::max(below, target));
    _left_in_list = _skip.Seek(target);
    reader.SetSkipBoundsBelow(below);
    _left_in_leaf = 0;
    _needs_reposition = true;
    _upper_bound = reader.UpperBound();
    return _upper_bound;
  }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    if (_skip.Reader().IsLessThanUpperBound(target)) [[unlikely]] {
      if (!doc_limits::eof(SeekToBlock(target))) {
        _doc = _skip.Reader().State().doc;
      }
    }
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
    RepositionForWindow(_doc);

    SDB_ASSERT(_left_in_leaf < doc_limits::kBlockSize);
    doc_id_t last = *(std::end(_docs) - 1);
    {
      const auto count = _left_in_leaf + 1;
      if (last >= max) {
        _left_in_leaf = count;
        goto tail;
      }
      if (count == doc_limits::kBlockSize) {
        goto full;
      }
      Emit(std::end(_docs) - count, count, visit);
    }

    for (;;) {
      if (_left_in_list == 0) [[unlikely]] {
        _left_in_leaf = 0;
        goto done;
      }
      ReadLeaf(last);
      last = *(std::end(_docs) - 1);
      if (last >= max || _left_in_leaf != doc_limits::kBlockSize) {
        goto tail;
      }
    full:
      Emit(std::begin(_docs), doc_limits::kBlockSize, visit);
    }

  tail: {
    auto* const begin = std::end(_docs) - _left_in_leaf;
    auto* const end = std::find_if(begin, std::end(_docs),
                                   [max](doc_id_t doc) { return doc >= max; });
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - end);
    if (end != begin) {
      Emit(begin, static_cast<uint32_t>(end - begin), visit);
    }
  }

  done:
    if (_left_in_leaf != 0) {
      _doc = *(std::end(_docs) - _left_in_leaf);
      --_left_in_leaf;
    } else {
      _doc = doc_limits::eof();
    }
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
          VisitDocs<std::dynamic_extent>(len, add);
        }
      });
  }

  template<typename DocsBuffer, typename ScoresBuffer>
  void ScoreCandidates(DocsBuffer& cand_docs, ScoresBuffer& cand_scores,
                       bool required, doc_id_t window_max) {
    SDB_ASSERT(!cand_docs.empty());
    size_t out = 0;
    SetSkipBoundsBelow(window_max);

    const size_t cand_count = cand_docs.size();
    const doc_id_t max = cand_docs[cand_count - 1] + 1;

    if (_doc >= max) [[unlikely]] {
      SetSkipBoundsBelow(0);
      if (required) {
        cand_docs.resize(0);
        cand_scores.resize(0);
      }
      return;
    }

    ABSL_CACHELINE_ALIGNED doc_id_t docs[kScoreBlock];
    ABSL_CACHELINE_ALIGNED uint32_t freqs[kScoreBlock];
    ABSL_CACHELINE_ALIGNED uint32_t indices[kScoreBlock];
    size_t count = 0;
    _provider.freq.value = freqs;

    auto score_block = [&](uint32_t len) {
      SDB_ASSERT(len != 0);
      _fetcher->Fetch(std::span<const doc_id_t>{docs, len});
      auto* const p = reinterpret_cast<score_t*>(std::end(_enc.data) - len);
      if (len == kScoreBlock) {
        _score.ScoreBlock(p);
      } else {
        _score.Score(p, static_cast<scores_size_t>(len));
      }
      for (uint32_t j = 0; j != len; ++j) {
        cand_scores[indices[j]] += p[j];
      }
      count = 0;
    };

    size_t cand_idx = 0;

    auto find_in_block = [&](const doc_id_t* begin, const doc_id_t* end) {
      while (cand_idx < cand_count && begin < end) {
        const doc_id_t cand = cand_docs[cand_idx];
        if (cand > *(end - 1)) {
          break;
        }
        size_t step = 1;
        while (begin + step < end && begin[step] < cand) {
          begin += step;
          step <<= 1;
        }
        begin = std::lower_bound(begin, std::min(begin + step, end), cand);
        const auto* const it = begin;
        if (*it == cand) {
          if (required) {
            cand_docs[out] = cand_docs[cand_idx];
            cand_scores[out] = cand_scores[cand_idx];
            indices[count] = out;
            ++out;
          } else {
            indices[count] = cand_idx;
          }
          docs[count] = cand;
          freqs[count] =
            _freqs.data[static_cast<size_t>(it - std::begin(_docs))];
          ++count;
          if (count == kScoreBlock) {
            score_block(kScoreBlock);
          }
          begin = it + 1;
        }
        ++cand_idx;
      }
    };

    RepositionForWindow(_doc);
    SDB_ASSERT(_left_in_leaf < doc_limits::kBlockSize);

    {
      const auto n = _left_in_leaf + 1;
      if (*(std::end(_docs) - 1) >= max) {
        _left_in_leaf = n;
        goto cand_tail;
      }
      find_in_block(std::end(_docs) - n, std::end(_docs));
      if (cand_idx >= cand_count) {
        goto cand_done;
      }
    }

    for (;;) {
      if (_left_in_list == 0) [[unlikely]] {
        _left_in_leaf = 0;
        goto cand_done;
      }
      {
        const doc_id_t next_cand = cand_docs[cand_idx];
        if (next_cand > _skip.Reader().UpperBound()) {
          _left_in_list = _skip.Seek(next_cand);
          _needs_reposition = false;
          auto& state = _skip.Reader().State();
          if (state.doc_ptr != 0) [[likely]] {
            In().Seek(state.doc_ptr);
          }
          if (_left_in_list == 0) {
            _left_in_leaf = 0;
            goto cand_done;
          }
          ReadLeaf(state.doc);
        } else {
          ReadLeaf(*(std::end(_docs) - 1));
        }
      }
      if (*(std::end(_docs) - 1) >= max ||
          _left_in_leaf != doc_limits::kBlockSize) {
        goto cand_tail;
      }
      find_in_block(std::begin(_docs), std::end(_docs));
      if (cand_idx >= cand_count) {
        goto cand_done;
      }
    }

  cand_tail: {
    const auto* const begin = std::end(_docs) - _left_in_leaf;
    const auto* const end = std::find_if(
      begin, std::cend(_docs), [max](doc_id_t doc) { return doc >= max; });
    if (end != begin) {
      find_in_block(begin, end);
    }
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - end);
  }

  cand_done:
    if (count != 0) {
      score_block(static_cast<uint32_t>(count));
    }
    if (_left_in_leaf != 0) {
      _doc = *(std::end(_docs) - _left_in_leaf);
      --_left_in_leaf;
    } else if (_left_in_list != 0) {
      _doc = *(std::end(_docs) - 1);
    } else {
      _doc = doc_limits::eof();
    }
    _provider.freq.value = _freqs.data;
    SetSkipBoundsBelow(0);
    if (required) {
      cand_docs.resize(out);
      cand_scores.resize(out);
    }
  }

 private:
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
};

}  // namespace irs::search
