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

#include "iresearch/search/detail/masked_leaf.hpp"
#include "iresearch/search/top/prune_leaf.hpp"

namespace irs::top {

template<typename InputType>
class PostingPrunedClause : public PruneLeafBase<InputType, false> {
  using Base = PruneLeafBase<InputType, false>;

  using Base::_base;
  using Base::_cursor;
  using Base::_doc;
  using Base::_docs;
  using Base::_enc;
  using Base::_freqs;
  using Base::_hint;
  using Base::_left_in_list;
  using Base::_max_in_leaf;
  using Base::_needs_reposition;
  using Base::_provider;
  using Base::_recipe;
  using Base::In;

 public:
  using Base::AdvanceBlock;
  using Base::MaxScore;
  using Base::Value;

  PostingPrunedClause() = default;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, const SubReader& segment,
               const TermReader& field, const detail::ScoreArgs& args) {
    _leaf.Single();
    _lazy = nullptr;
    if (Base::PrepareCommon(meta, doc_in, layout, segment, field, args)) {
      _doc = doc_limits::min() + meta.doc_delta;
    }
  }

  PostingPrunedClause(const PostingMeta& meta, const IndexInput& doc_in,
                      IndexFeatures layout, const SubReader& segment,
                      const TermReader& field, const detail::ScoreArgs& args) {
    Prepare(meta, doc_in, layout, segment, field, args);
  }

  ScoreFunction PrepareScore() {
    SDB_ASSERT(_recipe.segment != nullptr && _recipe.field != nullptr);
    SDB_ASSERT(_recipe.args.scorer != nullptr);
    _provider.freq.value = _gather;
    return _recipe.args.scorer->PrepareScorer({
      .segment = *_recipe.segment,
      .field = _recipe.field->meta(),
      .doc_attrs = _provider,
      .fetcher = *_recipe.args.fetcher,
      .stats = _recipe.args.stats,
      .boost = _recipe.args.boost,
    });
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) noexcept {
    SDB_ASSERT(slot < kScoreBlock);
    if constexpr (InputType::kVolatileAlways) {
      if (_lazy != nullptr) {
        DecodeFreqs();
      }
    }
    _gather[slot] = _freqs.data[_leaf.Index()];
  }

  uint32_t TakeReads() noexcept { return std::exchange(_reads, 0); }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    if (target <= _max_in_leaf) [[likely]] {
      return _doc = _leaf.Find(std::begin(_docs), _base, target);
    }
    return ProbeSlow(target);
  }

 private:
  static constexpr uint32_t kBlock = doc_limits::kBlockSize;

  void ReadFill(doc_id_t prev) {
    ++_reads;
    auto& in = In();
    _hint.Advance(in, in.Position());
    const auto len = std::min(_left_in_list, kBlock);
    const auto leaf =
      block_io::ReadTailForFill(len, in, _enc.data, nullptr, _docs, prev);
    _left_in_list -= len;
    const auto* bitset = leaf.bitset;
    if constexpr (!InputType::kVolatileAlways) {
      if (leaf.IsBitset()) {
        std::memcpy(std::begin(_docs), leaf.bitset,
                    size_t{leaf.words} * sizeof(uint64_t));
        bitset = reinterpret_cast<const uint64_t*>(std::begin(_docs));
      }
    }
    if constexpr (InputType::kVolatileAlways) {
      _lazy = in.Current();
      block_io::SkipTail(len, in);
    } else {
      block_io::ReadTail<block_io::kFreqBias>(len, in, _enc.data, _freqs.data);
    }
    _base = prev;
    _max_in_leaf = leaf.max;
    _leaf.Reset(leaf, bitset, len);
  }

  IRS_NO_INLINE void DecodeFreqs() noexcept {
    using Codec = block_io::Codec;
    const auto len = _leaf.Len();
    if (len == kBlock) {
      Codec::DecodeValuesBlock<block_io::kFreqBias>(_lazy, _freqs.data);
    } else {
      Codec::DecodeValuesTail<block_io::kFreqBias>(
        _lazy, len, _freqs.data + (kBlock - len));
    }
    _lazy = nullptr;
  }

  IRS_NO_INLINE doc_id_t ProbeSlow(doc_id_t target) {
    if (!_needs_reposition) [[likely]] {
      if (const auto span = _max_in_leaf - _base;
          target - _max_in_leaf <= span || target <= _cursor.UpperBound())
        [[likely]] {
        if (_left_in_list == 0) [[unlikely]] {
          return _doc = doc_limits::eof();
        }
        ReadFill(_max_in_leaf);
        if (target <= _max_in_leaf) [[likely]] {
          return _doc = _leaf.Find(std::begin(_docs), _base, target);
        }
      }
    }
    AdvanceBlock(target);
    if (_needs_reposition) {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      Base::Reposition();
      ReadFill(_doc);
    }
    while (_max_in_leaf < target) {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      ReadFill(_max_in_leaf);
    }
    return _doc = _leaf.Find(std::begin(_docs), _base, target);
  }

  ABSL_CACHELINE_ALIGNED uint32_t _gather[kScoreBlock]{};
  const byte_type* _lazy = nullptr;
  detail::MaskedLeaf _leaf;
  uint32_t _reads = 0;
};

}  // namespace irs::top
