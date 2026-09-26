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

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting/block_index.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/enc_buf.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename InputType, bool Standalone>
class PruneLeafBase {
 protected:
  class NoBoundSource final : public ScoreBoundSource {
   public:
    Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }
    void Read(DataInput& in, size_t size) final { in.Skip(size); }
    void Set(uint32_t, uint32_t) noexcept final {}
  };

 public:
  static constexpr bool kDefaultInit = true;

  PruneLeafBase() = default;

  PruneLeafBase(const PruneLeafBase&) = delete;
  PruneLeafBase& operator=(const PruneLeafBase&) = delete;
  PruneLeafBase(PruneLeafBase&&) = delete;
  PruneLeafBase& operator=(PruneLeafBase&&) = delete;

  bool PrepareCommon(const PostingMeta& meta, const IndexInput& doc_in,
                     IndexFeatures layout, const SubReader& segment,
                     const TermReader& field, const detail::ScoreArgs& args) {
    SDB_ASSERT(meta.docs_count != 0);
    SDB_ASSERT(args.scorer != nullptr);
    SDB_ASSERT(args.fetcher != nullptr);
    SDB_ASSERT(FeaturesHaveFreq(layout));
    _fetcher = args.fetcher;
    _recipe = {&segment, &field, args};
    _provider.freq.value = _freqs.data;

    auto source = args.scorer->PrepareScoreBoundSource();
    if (source) {
      _bound_func = args.scorer->PrepareScorer({
        .segment = segment,
        .field = field.meta(),
        .doc_attrs = *source,
        .fetcher = *args.fetcher,
        .stats = args.stats,
        .boost = args.boost,
      });
      _bound_source = std::move(source);
    } else {
      _bound_func =
        ScoreFunction::Constant(std::numeric_limits<score_t>::max());
      _bound_source = std::make_unique<NoBoundSource>();
    }

    _score = args.scorer->PrepareScorer({
      .segment = segment,
      .field = field.meta(),
      .doc_attrs = _provider,
      .fetcher = *args.fetcher,
      .stats = args.stats,
      .boost = args.boost,
    });

    _cursor.Disarm();
    _cached.fill(kNoBlock);
    _cached_run = kNoBlock;
    _root_score = std::numeric_limits<score_t>::max();
    _threshold = std::numeric_limits<score_t>::lowest();
    _upper_bound = doc_limits::eof();

    if (meta.docs_count == 1) {
      *(std::end(_docs) - 1) = doc_limits::min() + meta.doc_delta;
      *(std::end(_freqs.data) - 1) = meta.freq;
      _left_in_list = 0;
      _len = 1;
      _scored = false;
      return true;
    }

    _in = doc_in.Reopen();
    if (!_in) [[unlikely]] {
      throw IoError{"failed to reopen document input"};
    }
    auto& in = In();
    in.Seek(meta.doc_start);
    LimitDocReadahead(in, meta);
    _left_in_list = meta.docs_count;

    if (meta.docs_count > doc_limits::kBlockSize) {
      _cursor.Arm(meta, BlockIndexShapeOf(layout, true));
      _cursor.Load(in);
      _root_score = BoundScore(_cursor.Index().Root());
      _upper_bound = doc_limits::invalid();
    } else if (meta.docs_count < doc_limits::kBlockSize) {
      const auto size = in.ReadByte();
      _bound_source->Read(in, size);
      _root_score = _bound_func.Score();
    }
    return false;
  }

  doc_id_t Value() const noexcept { return _doc; }

  score_t MaxScore(doc_id_t doc) {
    if (!_cursor.Armed()) {
      return _root_score;
    }
    const auto& index = _cursor.Index();
    const auto n = index.Size();
    const auto b = _cursor.Block();
    if (b == n) {
      return _root_score;
    }
    if (doc <= index.Last(b)) [[likely]] {
      return BlockScore(b);
    }
    const auto e = index.Find(b, doc);
    if (e == n) {
      return _root_score;
    }
    if (e - b < kMaxScoreBlocks) {
      auto score = BlockScore(b);
      for (auto k = b + 1; k <= e; ++k) {
        score = std::max(score, BlockScore(k));
      }
      return score;
    }
    const auto r = b / BlockIndex::kRun;
    if (e / BlockIndex::kRun == r) {
      return RunScore(r);
    }
    return _root_score;
  }

 protected:
  static constexpr uint32_t kNoBlock = std::numeric_limits<uint32_t>::max();
  static constexpr uint32_t kMaxScoreBlocks = 8;
  static constexpr uint32_t kCachedBlocks = 8;

  IRS_FORCE_INLINE InputType& In() const noexcept {
    return irs::utils::downCast<InputType>(*_in);
  }

  score_t BoundScore(const byte_type* bound) {
    _bound_source->Set(absl::little_endian::Load32(bound),
                       absl::little_endian::Load32(bound + sizeof(uint32_t)));
    return _bound_func.Score();
  }

  score_t BlockScore(uint32_t k) {
    const auto slot = k % kCachedBlocks;
    if (_cached[slot] != k) {
      _cached[slot] = k;
      _cached_scores[slot] = BoundScore(_cursor.Index().Bound(k));
    }
    return _cached_scores[slot];
  }

  score_t RunScore(uint32_t r) {
    if (_cached_run != r) {
      _cached_run = r;
      _run_score = BoundScore(_cursor.Index().RunBound(r));
    }
    return _run_score;
  }

  uint32_t SeekCursor(doc_id_t target) {
    if constexpr (Standalone) {
      const auto& index = _cursor.Index();
      const auto n = index.Size();
      auto b = index.Find(_cursor.Block(), target);
      while (b != n) {
        const auto r = b / BlockIndex::kRun;
        if (RunScore(r) <= _threshold) {
          b = std::min(n, (r + 1) * BlockIndex::kRun);
          continue;
        }
        if (BlockScore(b) > _threshold) {
          break;
        }
        ++b;
      }
      return _cursor.MoveTo(b);
    } else {
      return _cursor.Seek(target, In());
    }
  }

  IRS_FORCE_INLINE void Reposition() {
    if (!_needs_reposition) {
      return;
    }
    _needs_reposition = false;
    const auto& state = _cursor.Landing();
    In().Seek(state.doc_ptr);
    _doc = state.doc;
  }

  doc_id_t SeekToBlock(doc_id_t target) {
    if (!_cursor.Armed()) [[unlikely]] {
      return doc_limits::eof();
    }
    const auto upper_bound = _cursor.UpperBound();
    if (upper_bound >= target) {
      return upper_bound;
    }
    const auto left = SeekCursor(target);
    _upper_bound = _cursor.UpperBound();
    if (_needs_reposition || target > _max_in_leaf) {
      _left_in_list = left;
      _left_in_leaf = 0;
      _needs_reposition = true;
    }
    return _upper_bound;
  }

  void RepositionForWindow(doc_id_t min) {
    if (!_needs_reposition || _left_in_list == 0) [[likely]] {
      return;
    }
    _needs_reposition = false;
    const auto& state = _cursor.Landing();
    In().Seek(state.doc_ptr);
    ReadLeaf(state.doc);
    const auto* const first =
      FirstNotBelow(std::end(_docs) - _left_in_leaf, min);
    SDB_ASSERT(first != std::end(_docs));
    _doc = *first;
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - first) - 1;
  }

  IRS_FORCE_INLINE const doc_id_t* FirstNotBelow(const doc_id_t* begin,
                                                 doc_id_t max) const noexcept {
    if (_len == doc_limits::kBlockSize) [[likely]] {
      return BranchlessLowerBound<doc_limits::kBlockSize>(std::cbegin(_docs),
                                                          max);
    }
    return std::find_if(begin, std::cend(_docs),
                        [max](doc_id_t doc) { return doc >= max; });
  }

  void ReadLeaf(doc_id_t prev) {
    auto& in = In();
    const auto len = std::min(_left_in_list, doc_limits::kBlockSize);
    FormatTraits128::ReadTailDelta(len, in, _enc.data, _docs, prev);
    FormatTraits128::ReadTail(len, in, _enc.data, _freqs.data);
    _left_in_leaf = len;
    _len = len;
    _left_in_list -= len;
    _base = prev;
    _max_in_leaf = *(std::end(_docs) - 1);
    _scored = false;
  }

  template<typename Visitor>
  IRS_FORCE_INLINE void Emit(doc_id_t* docs, uint32_t len, Visitor&& visit) {
    if (!_scored) {
      ScoreLeaf();
    }
    visit(docs, len, Scores() + (docs - std::begin(_docs)));
  }

  IRS_FORCE_INLINE score_t* Scores() noexcept {
    static_assert(sizeof(score_t) == sizeof(_enc.data[0]));
    return reinterpret_cast<score_t*>(std::begin(_enc.data));
  }

  void ScoreLeaf() {
    _scored = true;
    if (_len == doc_limits::kBlockSize) {
      _fetcher->FetchPostingBlock(
        std::span<const doc_id_t, doc_limits::kBlockSize>{
          std::begin(_docs), doc_limits::kBlockSize});
      _score.ScorePostingBlock(Scores());
      return;
    }
    const auto offset = doc_limits::kBlockSize - _len;
    _fetcher->Fetch(
      std::span<const doc_id_t>{std::begin(_docs) + offset, _len});
    _provider.freq.value = _freqs.data + offset;
    _score.Score(Scores() + offset, static_cast<scores_size_t>(_len));
    _provider.freq.value = _freqs.data;
  }

  detail::EncBuf _enc;
  detail::FreqBuf _freqs;
  DocsBuf _docs;
  IndexInput::ptr _in;
  ColumnArgsFetcher* _fetcher = nullptr;
  ScoreFunction _score;
  detail::LeafProvider _provider;
  detail::LeafRecipe _recipe;
  BlockCursor _cursor;
  ScoreFunction _bound_func;
  ScoreBoundSource::ptr _bound_source;
  std::array<uint32_t, kCachedBlocks> _cached;
  std::array<score_t, kCachedBlocks> _cached_scores;
  uint32_t _cached_run = kNoBlock;
  score_t _run_score = 0;
  score_t _root_score = std::numeric_limits<score_t>::max();
  score_t _threshold = std::numeric_limits<score_t>::lowest();
  doc_id_t _doc = 0;
  uint32_t _left_in_leaf = 0;
  uint32_t _len = 0;
  doc_id_t _base = 0;
  doc_id_t _max_in_leaf = doc_limits::invalid();
  doc_id_t _upper_bound = doc_limits::eof();
  uint32_t _left_in_list = 0;
  bool _needs_reposition = false;
  bool _scored = false;
};

}  // namespace irs::top
