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
  using Base::_left_in_list;
  using Base::_len;
  using Base::_max_in_leaf;
  using Base::_needs_reposition;
  using Base::_provider;
  using Base::_recipe;
  using Base::_upper_bound;
  using Base::In;

 public:
  using Base::MaxScore;
  using Base::Value;

  PostingPrunedClause() = default;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, const SubReader& segment,
               const TermReader& field, const detail::ScoreArgs& args) {
    _index = kBlock - 1;
    _packed = true;
    if (Base::PrepareCommon(meta, doc_in, layout, segment, field, args)) {
      _doc = doc_limits::min() + meta.doc_delta;
    }
  }

  PostingPrunedClause(const PostingMeta& meta, const IndexInput& doc_in,
                      IndexFeatures layout, const SubReader& segment,
                      const TermReader& field, const detail::ScoreArgs& args) {
    Prepare(meta, doc_in, layout, segment, field, args);
  }

  doc_id_t AdvanceBlock(doc_id_t target) {
    if (!_cursor.Armed()) [[unlikely]] {
      return doc_limits::eof();
    }
    const auto upper_bound = _cursor.UpperBound();
    if (upper_bound >= target) {
      return upper_bound;
    }
    const auto left = _cursor.Seek(target, In());
    _upper_bound = _cursor.UpperBound();
    if (_needs_reposition || target > _max_in_leaf) {
      _left_in_list = left;
      _needs_reposition = true;
      _max_in_leaf = doc_limits::invalid();
      _doc = _cursor.Landing().doc;
    }
    return _upper_bound;
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
    _gather[slot] = _freqs.data[_index];
  }

  uint32_t TakeReads() noexcept { return std::exchange(_reads, 0); }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    if (target <= _max_in_leaf) [[likely]] {
      return _doc = Find(target);
    }
    return ProbeSlow(target);
  }

 private:
  static constexpr uint32_t kBlock = doc_limits::kBlockSize;
  static constexpr auto kBits = BitsRequired<uint64_t>();

  IRS_FORCE_INLINE doc_id_t Find(doc_id_t target) noexcept {
    if (_packed) [[likely]] {
      if (_len == kBlock) [[likely]] {
        const auto* const it =
          BranchlessLowerBound<kBlock>(std::begin(_docs), target);
        _index = static_cast<uint32_t>(it - std::begin(_docs));
        return *it;
      }
      for (auto i = _at;; ++i) {
        if (target <= _docs[i]) {
          _at = i;
          _index = i;
          return _docs[i];
        }
      }
    }
    return FindMasked(target);
  }

  IRS_NO_INLINE doc_id_t FindMasked(doc_id_t target) noexcept {
    const auto first = _base + 1;
    if (target < first) {
      target = first;
    }
    if (_run) {
      _index = kBlock - _len + (target - first);
      return target;
    }
    auto bit = static_cast<uint64_t>(target) - first;
    for (auto w = bit / kBits; w != _words; ++w) {
      const auto word = _bitset[w] & (~uint64_t{0} << (bit % kBits));
      if (word != 0) {
        const auto tz = static_cast<uint32_t>(std::countr_zero(word));
        for (; _prefix_word != w; ++_prefix_word) {
          _prefix_bits +=
            static_cast<uint32_t>(std::popcount(_bitset[_prefix_word]));
        }
        _index = kBlock - _len + _prefix_bits +
                 static_cast<uint32_t>(
                   std::popcount(_bitset[w] & ((uint64_t{1} << tz) - 1)));
        return static_cast<doc_id_t>(first + w * kBits + tz);
      }
      bit = (w + 1) * kBits;
    }
    return doc_limits::eof();
  }

  void ReadFill(doc_id_t prev) {
    ++_reads;
    auto& in = In();
    const auto len = std::min(_left_in_list, kBlock);
    const auto leaf = FormatTraits128::ReadTailForFill(len, in, _enc.data,
                                                       nullptr, _docs, prev);
    _left_in_list -= len;
    _bitset = leaf.bitset;
    if constexpr (!InputType::kVolatileAlways) {
      if (leaf.IsBitset()) {
        std::memcpy(std::begin(_docs), leaf.bitset,
                    size_t{leaf.words} * sizeof(uint64_t));
        _bitset = reinterpret_cast<const uint64_t*>(std::begin(_docs));
      }
    }
    FormatTraits128::ReadTail(len, in, _enc.data, _freqs.data);
    _base = prev;
    _max_in_leaf = leaf.max;
    _len = len;
    _at = kBlock - len;
    _words = leaf.words;
    _prefix_word = 0;
    _prefix_bits = 0;
    _run = leaf.IsRun();
    _packed = !leaf.Maskable();
  }

  doc_id_t SeekToBlock(doc_id_t target) {
    if (!_cursor.Armed()) [[unlikely]] {
      return doc_limits::eof();
    }
    const auto upper_bound = _cursor.UpperBound();
    if (upper_bound >= target) {
      return upper_bound;
    }
    _left_in_list = _cursor.Seek(target, In());
    _needs_reposition = true;
    _max_in_leaf = doc_limits::invalid();
    _upper_bound = _cursor.UpperBound();
    return _upper_bound;
  }

  IRS_NO_INLINE doc_id_t ProbeSlow(doc_id_t target) {
    if (!_needs_reposition) [[likely]] {
      if (const auto span = _max_in_leaf - _base;
          target - _max_in_leaf <= span || target <= _upper_bound) [[likely]] {
        if (_left_in_list == 0) [[unlikely]] {
          return _doc = doc_limits::eof();
        }
        ReadFill(_max_in_leaf);
        if (target <= _max_in_leaf) [[likely]] {
          return _doc = Find(target);
        }
      }
    }
    if (_cursor.UpperBound() < target) [[unlikely]] {
      SeekToBlock(target);
    }
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
    return _doc = Find(target);
  }

  ABSL_CACHELINE_ALIGNED uint32_t _gather[kScoreBlock]{};
  const uint64_t* _bitset = nullptr;
  uint64_t _prefix_word = 0;
  uint32_t _words = 0;
  uint32_t _at = kBlock;
  uint32_t _index = kBlock - 1;
  uint32_t _prefix_bits = 0;
  uint32_t _reads = 0;
  bool _run = false;
  bool _packed = true;
};

}  // namespace irs::top
