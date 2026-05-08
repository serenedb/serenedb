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

#include <iresearch/search/scorer.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/type_limits.hpp>

#include "basics/assert.h"
#include "basics/empty.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/posting/iterator_pos.hpp"
#include "iresearch/formats/posting/skip_list.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs {

template<typename IteratorTraits>
class PostingIteratorBase : public DocIterator {
 public:
  static_assert(doc_limits::kBlockSize % kScoreBlock == 0,
                "kBlockSize must be a multiple of kScoreBlock");

  ~PostingIteratorBase() {
    if constexpr (IteratorTraits::Frequency()) {
      if (_doc_in) {
        std::allocator<uint32_t>{}.deallocate(_collected_freqs, kScoreBlock);
      }
    }
  }

  IRS_NO_INLINE Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  IRS_FORCE_INLINE doc_id_t advance() final;

  IRS_FORCE_INLINE doc_id_t seek(doc_id_t target) final;

  IRS_FORCE_INLINE doc_id_t LazySeek(doc_id_t target) final;

  uint32_t count() final {
    _doc = doc_limits::eof();
    const auto left_in_leaf = std::exchange(_left_in_leaf, 0);
    const auto left_in_list = std::exchange(_left_in_list, 0);
    return left_in_leaf + left_in_list;
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    SDB_ASSERT(ctx.scorer);
    return ctx.scorer->PrepareScorer({
      .segment = *ctx.segment,
      .field = _field,
      .doc_attrs = *this,
      .fetcher = ctx.fetcher,
      .stats = _stats,
      .boost = _boost,
    });
  }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final;

  IRS_FORCE_INLINE uint32_t GetFreq() const final {
    if constexpr (IteratorTraits::Frequency()) {
      SDB_ASSERT(_left_in_leaf < doc_limits::kBlockSize);
      return *(std::end(_freqs) - _left_in_leaf - 1);
    } else {
      return 0;
    }
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint16_t index) final {
    if constexpr (IteratorTraits::Frequency()) {
      SDB_ASSERT(_collected_freqs);
      _collected_freqs[index] = GetFreq();
    }
  }

  IRS_FORCE_INLINE void Init(const PostingCookie& cookie) noexcept {
    _field = cookie.field;
    _stats = cookie.stats;
    _boost = cookie.boost;
  }

 protected:
  using Position = PositionImpl<IteratorTraits>;
  using Attributes =
    std::conditional_t<IteratorTraits::Position(),
                       std::tuple<FreqBlockAttr, CostAttr, Position>,
                       AttributesImpl<IteratorTraits>>;

  virtual void ReadLeaf(doc_id_t prev_doc) = 0;
  virtual bool SeekToLeaf(doc_id_t target) = 0;

  template<size_t N>
  IRS_FORCE_INLINE const score_t* ScoreBlock(std::span<const doc_id_t, N> docs,
                                             const ScoreFunction& score,
                                             ColumnArgsFetcher* fetcher);

  template<ScoreMergeType MergeType, bool TrackMatch, size_t N>
  bool ProcessBatch(std::span<const doc_id_t, N> docs, const doc_id_t min,
                    uint64_t* IRS_RESTRICT doc_mask,
                    [[maybe_unused]] FillBlockScoreContext score,
                    [[maybe_unused]] FillBlockMatchContext match);

  FieldProperties _field;
  const byte_type* _stats = nullptr;
  score_t _boost = kNoBoost;

  uint32_t _enc_buf[doc_limits::kBlockSize];
  // TODO(gnusi) we don't need collected freqs if we don't compute score
  // But for positions we need freqs, even without score
  [[no_unique_address]] utils::Need<IteratorTraits::Frequency(), uint32_t*>
    _collected_freqs = nullptr;

  std::conditional_t<IteratorTraits::Frequency(),
                     uint32_t[doc_limits::kBlockSize], uint32_t*>
    _freqs;
  // [[no_unique_address]] utils::Need<IteratorTraits::Frequency(),
  //                                   uint32_t[doc_limits::kBlockSize]> _freqs;
  doc_id_t _docs[doc_limits::kBlockSize];
#ifdef __AVX2__
  [[maybe_unused]] doc_id_t _placeholder_for_bitset_materialize[8];
#endif
  doc_id_t _max_in_leaf = doc_limits::invalid();
  uint32_t _left_in_leaf = 0;
  uint32_t _left_in_list = 0;
  IndexInput::ptr _doc_in;
  Attributes _attrs;
  uint32_t _docs_count{};
};

template<typename IteratorTraits>
doc_id_t PostingIteratorBase<IteratorTraits>::advance() {
  if (_left_in_leaf == 0) [[unlikely]] {
    if (_left_in_list == 0) [[unlikely]] {
      return _doc = doc_limits::eof();
    }

    ReadLeaf(_doc);
  }

  _doc = *(std::end(_docs) - _left_in_leaf);

  if constexpr (IteratorTraits::Position()) {
    auto& pos = std::get<Position>(_attrs);
    const auto freq = *(std::end(_freqs) - _left_in_leaf);
    pos.Notify(freq, freq);
    pos.Clear();
  }

  --_left_in_leaf;
  return _doc;
}

template<typename IteratorTraits>
doc_id_t PostingIteratorBase<IteratorTraits>::seek(doc_id_t target) {
  if (target <= _doc) [[unlikely]] {
    return _doc;
  }

  if (_docs_count == 1) [[unlikely]] {
    if (_max_in_leaf < target) {
      _left_in_leaf = 0;
      return _doc = doc_limits::eof();
    }
  } else if (_max_in_leaf < target && !SeekToLeaf(target)) [[unlikely]] {
    _left_in_leaf = 0;
    return _doc = doc_limits::eof();
  }

  [[maybe_unused]] uint32_t notify = 0;
  for (auto left_in_leaf = _left_in_leaf; left_in_leaf != 0; --left_in_leaf) {
    const auto doc = *(std::end(_docs) - left_in_leaf);

    if constexpr (IteratorTraits::Position()) {
      notify += *(std::end(_freqs) - left_in_leaf);
    }

    if (target <= doc) {
      if constexpr (IteratorTraits::Position()) {
        auto& pos = std::get<Position>(_attrs);
        pos.Notify(*(std::end(_freqs) - left_in_leaf), notify);
        pos.Clear();
      }

      _left_in_leaf = left_in_leaf - 1;
      return _doc = doc;
    }
  }
  SDB_ASSERT(_docs_count <
             doc_limits::kBlockSize);  // It seems that we should find

  SDB_ASSERT(_left_in_list == 0);  // It seems that it should be tail

  _left_in_leaf = 0;
  return _doc = doc_limits::eof();
}

template<typename IteratorTraits>
doc_id_t PostingIteratorBase<IteratorTraits>::LazySeek(doc_id_t target) {
  if constexpr (IteratorTraits::Position()) {
    SDB_ASSERT(target >= value());
    return seek(target);
  } else {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }

    auto seal = [&] IRS_FORCE_INLINE {
      _left_in_leaf = 0;
      return _doc = doc_limits::eof();
    };

    if (_docs_count == 1) [[unlikely]] {
      if (_max_in_leaf < target) {
        seal();
      }
    } else if (_max_in_leaf < target && !SeekToLeaf(target)) [[unlikely]] {
      seal();
    }

    auto next = [&](uint32_t left_in_leaf, doc_id_t doc) IRS_FORCE_INLINE {
      _left_in_leaf = left_in_leaf - 1;
      return _doc = doc;
    };

    // If this posting have only tail, this tail will be filled with garbage
    // values, so we cannot use it.
    if (_left_in_list != 0) [[likely]] {
      auto it =
        branchless_lower_bound(std::begin(_docs), std::end(_docs), target);
      return next(std::end(_docs) - it, *it);
    }

    for (auto left_in_leaf = _left_in_leaf; left_in_leaf != 0; --left_in_leaf) {
      const auto doc = *(std::end(_docs) - left_in_leaf);
      if (target <= doc) {
        return next(left_in_leaf, doc);
      }
    }
    return seal();
  }
}

template<typename IteratorTraits>
void PostingIteratorBase<IteratorTraits>::Collect(const ScoreFunction& scorer,
                                                  ColumnArgsFetcher& fetcher,
                                                  ScoreCollector& collector) {
  ResolveScoreCollector(collector, [&](auto& collector) IRS_FORCE_INLINE {
    auto process_block = [&]<size_t N>(size_t left_in_leaf) IRS_FORCE_INLINE {
      std::span<const doc_id_t, N> docs{std::end(_docs) - left_in_leaf,
                                        left_in_leaf};
      const auto* scores = ScoreBlock(docs, scorer, &fetcher);
      // TODO(mbkkt): bulk threshold check will make it faster
      for (size_t i = 0; i < docs.size(); ++i) {
        collector.Add(scores[i], docs[i]);
      }
    };

    if (const auto left_in_leaf = std::exchange(_left_in_leaf, 0))
      [[unlikely]] {
      process_block.template operator()<std::dynamic_extent>(left_in_leaf);
    } else {
      *(std::end(_docs) - 1) = _doc;
    }

    while (_left_in_list >= kPostingBlock) {
      ReadLeaf(*(std::end(_docs) - 1));
      process_block.template operator()<kPostingBlock>(kPostingBlock);
    }

    if (_left_in_list) {
      ReadLeaf(*(std::end(_docs) - 1));
      process_block.template operator()<std::dynamic_extent>(
        std::exchange(_left_in_leaf, 0));
    }
  });

  _doc = doc_limits::eof();
}

template<typename IteratorTraits>
template<size_t N>
const score_t* PostingIteratorBase<IteratorTraits>::ScoreBlock(
  std::span<const doc_id_t, N> docs, const ScoreFunction& score,
  ColumnArgsFetcher* fetcher) {
  if constexpr (N == kPostingBlock) {
    SDB_ASSERT(std::data(_docs) == docs.data());
    if (fetcher) {
      fetcher->FetchPostingBlock(docs);
    }
    if constexpr (IteratorTraits::Frequency()) {
      std::get<FreqBlockAttr>(_attrs).value = std::begin(_freqs);
    }
    auto* p = reinterpret_cast<score_t*>(std::end(_enc_buf) - N);
    score.ScorePostingBlock(p);
    return p;
  } else {
    SDB_ASSERT(std::data(_docs) <= docs.data());
    SDB_ASSERT(docs.data() <= std::data(_docs) + std::size(_docs));
    if (fetcher) {
      fetcher->Fetch(docs);
    }
    if constexpr (IteratorTraits::Frequency()) {
      const auto offset = docs.data() - std::data(_docs);
      std::get<FreqBlockAttr>(_attrs).value = std::begin(_freqs) + offset;
    }
    auto* p = reinterpret_cast<score_t*>(std::end(_enc_buf) - docs.size());
    score.Score(p, docs.size());
    return p;
  }
}

template<typename IteratorTraits>
template<ScoreMergeType MergeType, bool TrackMatch, size_t N>
bool PostingIteratorBase<IteratorTraits>::ProcessBatch(
  std::span<const doc_id_t, N> docs, const doc_id_t min,
  uint64_t* IRS_RESTRICT doc_mask, [[maybe_unused]] FillBlockScoreContext score,
  [[maybe_unused]] FillBlockMatchContext match) {
  [[maybe_unused]] auto* IRS_RESTRICT const score_window = score.score_window;
  [[maybe_unused]] const score_t* IRS_RESTRICT score_ptr;
  if constexpr (MergeType != ScoreMergeType::Noop) {
    score_ptr = ScoreBlock(docs, *score.score, score.fetcher);
  }

  if constexpr (!TrackMatch && MergeType == ScoreMergeType::Noop) {
    const size_t first = (docs.front() - min) / BitsRequired<uint64_t>();
    const size_t last = (docs.back() - min) / BitsRequired<uint64_t>();
    if (last - first <= 1) [[likely]] {
      uint64_t words[2] = {};
      for (size_t i = 0; i < docs.size(); ++i) {
        const size_t offset = docs[i] - min;
        SetBit(words[(offset / BitsRequired<uint64_t>()) - first],
               offset % BitsRequired<uint64_t>());
      }
      doc_mask[first] |= words[0];
      doc_mask[last] |= words[1];
      return false;
    }
  }

  [[maybe_unused]] bool empty = true;
  for (size_t i = 0; i < docs.size(); ++i) {
    const size_t offset = docs[i] - min;
    if constexpr (TrackMatch) {
      const bool has_match = ++match.matches[offset] >= match.min_match_count;
      SetBit(doc_mask[offset / BitsRequired<uint64_t>()],
             offset % BitsRequired<uint64_t>(), has_match);
      empty &= !has_match;
    } else {
      SetBit(doc_mask[offset / BitsRequired<uint64_t>()],
             offset % BitsRequired<uint64_t>());
    }
    if constexpr (MergeType != ScoreMergeType::Noop) {
      Merge<MergeType>(score_window[offset], score_ptr[i]);
    }
  }
  if constexpr (TrackMatch) {
    return empty;
  } else {
    return false;
  }
}

// Iterator over posting list.
// IteratorTraits defines requested features.
// FieldTraits defines requested features.
template<typename IteratorTraits, typename FieldTraits, bool HasWand,
         typename InputType>
class PostingIteratorImpl : public PostingIteratorBase<IteratorTraits> {
  static_assert((IteratorTraits::Features() & FieldTraits::Features()) ==
                IteratorTraits::Features());

  using Base = PostingIteratorBase<IteratorTraits>;
  using typename Base::Position;

  static_assert(doc_limits::kBlockSize % kScoreBlock == 0,
                "kBlockSize must be a multiple of kScoreBlock");

 public:
  PostingIteratorImpl() = default;

  void Prepare(const PostingCookie& meta, const IndexInput* doc_in,
               const IndexInput* pos_in, const IndexInput* pay_in,
               bool wand_enabled = false);

  std::pair<doc_id_t, bool> FillBlock(const doc_id_t min, const doc_id_t max,
                                      uint64_t* IRS_RESTRICT const doc_mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final;

 private:
  IRS_FORCE_INLINE InputType& GetDocIn() const noexcept {
    return sdb::basics::downCast<InputType>(*this->_doc_in);
  }

  IRS_FORCE_INLINE void ReadTail(doc_id_t prev_doc);
  IRS_FORCE_INLINE void ReadBlock(doc_id_t prev_doc);
  IRS_FORCE_INLINE void ReadLeaf(doc_id_t prev_doc) final;
  bool SeekToLeaf(doc_id_t target) final;

  uint64_t _skip_offs{};
  NewSkipReader<FieldTraits, IteratorTraits, HasWand, InputType>
    _new_skip_reader;
};

template<typename IteratorTraits, typename FieldTraits, bool HasWand,
         typename InputType>
void PostingIteratorImpl<IteratorTraits, FieldTraits, HasWand,
                         InputType>::Prepare(const PostingCookie& meta,
                                             const IndexInput* doc_in,
                                             const IndexInput* pos_in,
                                             const IndexInput* pay_in,
                                             bool wand_enabled) {
  this->Init(meta);

  auto& term_state = sdb::basics::downCast<CookieImpl>(meta.cookie)->meta;
  std::get<CostAttr>(this->_attrs).reset(term_state.docs_count);

  if constexpr (!IteratorTraits::Frequency()) {
    this->_freqs = nullptr;
  }

  if (term_state.docs_count > 1) {
    this->_left_in_list = term_state.docs_count;
    SDB_ASSERT(this->_left_in_leaf == 0);
    SDB_ASSERT(this->_max_in_leaf == doc_limits::invalid());

    if (!this->_doc_in) {
      this->_doc_in = doc_in->Reopen();  // Reopen thread-safe stream

      if (!this->_doc_in) {
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  "Failed to reopen document input");
        throw IoError("failed to reopen document input");
      }
    }

    if constexpr (IteratorTraits::Frequency()) {
      auto& freq_block = std::get<FreqBlockAttr>(this->_attrs);
      this->_collected_freqs = std::allocator<uint32_t>{}.allocate(kScoreBlock);
      freq_block.value = this->_collected_freqs;
    }

    GetDocIn().Seek(term_state.doc_start);
    SDB_ASSERT(!GetDocIn().IsEOF());
  } else {
    SDB_ASSERT(term_state.docs_count == 1);
    auto* doc = std::end(this->_docs) - 1;
    *doc = doc_limits::min() + term_state.e_single_doc;
    if constexpr (IteratorTraits::Frequency()) {
      auto* freq = std::end(this->_freqs) - 1;
      *freq = term_state.freq;

      this->_collected_freqs = freq;

      auto& freq_block = std::get<FreqBlockAttr>(this->_attrs);
      freq_block.value = freq;
    }
    this->_left_in_list = 0;
    this->_left_in_leaf = 1;
    this->_max_in_leaf = *doc;
  }

  SDB_ASSERT(!IteratorTraits::Frequency() || term_state.freq);
  if constexpr (IteratorTraits::Position()) {
    static_assert(IteratorTraits::Frequency());

    const DocState state{
      .pos_in = pos_in,
      .pay_in = pay_in,
      .term_state = &term_state,
      .enc_buf = this->_enc_buf,
    };

    std::get<Position>(this->_attrs).template Prepare<InputType>(state);
  }

  if (term_state.docs_count >= doc_limits::kBlockSize) {
    // Allow using skip-list for long enough postings
    _new_skip_reader.Prepare(term_state, GetDocIn());
    _skip_offs = term_state.doc_start + term_state.e_skip_start;
  } else if (1 < term_state.docs_count &&
             term_state.docs_count < doc_limits::kBlockSize && !wand_enabled) {
    _new_skip_reader.Reader().SkipWandRoot(GetDocIn());
  }
  this->_docs_count = term_state.docs_count;
}

template<typename IteratorTraits, typename FieldTraits, bool HasWand,
         typename InputType>
std::pair<doc_id_t, bool>
PostingIteratorImpl<IteratorTraits, FieldTraits, HasWand, InputType>::FillBlock(
  const doc_id_t min, const doc_id_t max, uint64_t* IRS_RESTRICT const doc_mask,
  FillBlockScoreContext score, FillBlockMatchContext match) {
  SDB_ASSERT(min < max);
  SDB_ASSERT(this->value() >= min);
  // value() was consumed by advance/seek/previous FillBlock
  // but still sits in _docs just before the leftover range
  SDB_ASSERT(this->_left_in_leaf < kPostingBlock);
  if constexpr (!IteratorTraits::Position()) {
    if (!score.score || score.score->IsDefault()) {
      score.merge_type = ScoreMergeType::Noop;
    }

    return ResolveBool(match.matches, [&]<bool TrackMatch> {
      return ResolveMergeType(score.merge_type, [&]<ScoreMergeType MergeType> {
        bool empty = true;

        // leftover from previous call
        {
          auto count = this->_left_in_leaf;

          if (*(std::end(this->_docs) - count - 1) == this->value()) {
            ++count;
          }

          if (count > 0) {
            if (*(std::end(this->_docs) - 1) >= max) {
              this->_left_in_leaf = count;
              goto fill_block_tail;
            }
            empty &= this->template ProcessBatch<MergeType, TrackMatch>(
              std::span<const doc_id_t>{std::end(this->_docs) - count, count},
              min, doc_mask, score, match);
          }
        }

        // full blocks only
        for (;;) {
          if (this->_left_in_list == 0) [[unlikely]] {
            this->_left_in_leaf = 0;
            goto fill_block_done;
          }
          if constexpr (!TrackMatch && MergeType == ScoreMergeType::Noop) {
            SDB_ASSERT(!IteratorTraits::Frequency());

            size_t tail = 0;
            const auto base = *(std::end(this->_docs) - 1);

            const uint64_t* IRS_RESTRICT bitset;
            const byte_type* data;
            uint32_t words;
            if (this->_left_in_list >= doc_limits::kBlockSize) {
              // Full Block
              const auto [d, w] = _new_skip_reader.ReadInlineBlockForFillBlock(
                GetDocIn(), this->_enc_buf, this->_docs, this->_freqs, base);
              bitset = reinterpret_cast<const uint64_t*>(d);
              data = d;
              words = w;
              tail = doc_limits::kBlockSize;
            } else {
              const auto [d, w] = IteratorTraits::ReadTailForFill(
                this->_left_in_list, GetDocIn(), this->_enc_buf, this->_docs,
                base);
              bitset = reinterpret_cast<const uint64_t*>(d);
              data = d;
              words = w;
              tail = this->_left_in_list;
              if constexpr (FieldTraits::Frequency()) {
                IteratorTraits::SkipTail(tail, GetDocIn());
              }
            }
            SDB_ASSERT(tail > 0);

            if (bitset) {
              const doc_id_t max_offset =
                (words - 1) * BitsRequired<uint64_t>() +
                (BitsRequired<uint64_t>() - 1 -
                 std::countl_zero(bitset[words - 1]));
              this->_max_in_leaf = base + max_offset;
            } else {
              this->_max_in_leaf = *(std::end(this->_docs) - 1);
            }
            this->_left_in_leaf = tail;
            this->_left_in_list -= tail;
            if (this->_max_in_leaf >= max) {
              if (bitset) {
                IteratorTraits::MaterializeBitset(
                  base, data, words, std::end(this->_docs) - tail, tail);
              }
            } else if (bitset) {
              const uint32_t mask_words = (max - min) >> 6;
              const auto offset = base >= min ? base - min : min - base;
              const uint32_t word_offset = offset >> 6;
              const uint32_t bit_offset = offset % BitsRequired<uint64_t>();
              if (base >= min) {
                if (bit_offset == 0) {
                  for (uint8_t j = 0; j < words; ++j) {
                    if (word_offset + j < mask_words) {
                      doc_mask[word_offset + j] |= bitset[j];
                    }
                  }
                } else {
                  for (uint8_t j = 0; j < words; ++j) {
                    if (word_offset + j < mask_words) {
                      doc_mask[word_offset + j] |= bitset[j] << bit_offset;
                    }
                    if (word_offset + j + 1 < mask_words) {
                      doc_mask[word_offset + j + 1] |=
                        bitset[j] >> (BitsRequired<uint64_t>() - bit_offset);
                    }
                  }
                }
              } else {
                for (uint32_t i = 0; i < mask_words; ++i) {
                  const uint32_t j = word_offset + i;
                  if (j < words) {
                    doc_mask[i] |= bitset[j] >> bit_offset;
                  }
                  if (bit_offset != 0 && j + 1 < words) {
                    doc_mask[i] |= bitset[j + 1]
                                   << (BitsRequired<uint64_t>() - bit_offset);
                  }
                }
              }
              *(std::end(this->_docs) - 1) = this->_max_in_leaf;
              empty = false;
              continue;
            }
          } else {
            ReadLeaf(*(std::end(this->_docs) - 1));
          }
          if (*(std::end(this->_docs) - 1) >= max ||
              this->_left_in_leaf != kPostingBlock) {
            goto fill_block_tail;
          }
          empty &= this->template ProcessBatch<MergeType, TrackMatch>(
            std::span<const doc_id_t, kPostingBlock>{std::begin(this->_docs),
                                                     kPostingBlock},
            min, doc_mask, score, match);
        }

      fill_block_tail: {
        const auto* begin = std::end(this->_docs) - this->_left_in_leaf;
        const auto* tail_end =
          std::find_if(begin, std::cend(this->_docs),
                       [&](doc_id_t doc) { return doc >= max; });
        if (tail_end != begin) {
          empty &= this->template ProcessBatch<MergeType, TrackMatch>(
            std::span{begin, tail_end}, min, doc_mask, score, match);
        }
        this->_left_in_leaf =
          static_cast<uint32_t>(std::end(this->_docs) - tail_end);
      }

      fill_block_done:
        if (this->_left_in_leaf > 0) {
          this->_doc = *(std::end(this->_docs) - this->_left_in_leaf);
          --this->_left_in_leaf;
        } else {
          this->_doc = doc_limits::eof();
        }

        if constexpr (IteratorTraits::Frequency()) {
          std::get<FreqBlockAttr>(this->_attrs).value = this->_collected_freqs;
        }
        return std::pair{this->_doc, empty};
      });
    });
  } else {
    SDB_ASSERT(false);
    return std::pair{this->_doc, true};
  }
}

template<typename IteratorTraits, typename FieldTraits, bool HasWand,
         typename InputType>
void PostingIteratorImpl<IteratorTraits, FieldTraits, HasWand,
                         InputType>::ReadTail(doc_id_t prev_doc) {
  const auto tail = this->_left_in_list;
  SDB_ASSERT(tail < doc_limits::kBlockSize);
  SDB_ASSERT(tail > 0);
  IteratorTraits::ReadTailDelta(tail, GetDocIn(), this->_enc_buf, this->_docs,
                                prev_doc);
  this->_max_in_leaf = *(std::end(this->_docs) - 1);
  this->_left_in_leaf = tail;
  this->_left_in_list = 0;
  if constexpr (IteratorTraits::Frequency()) {
    IteratorTraits::ReadTail(tail, GetDocIn(), this->_enc_buf, this->_freqs);
  }
}

template<typename IteratorTraits, typename FieldTraits, bool HasWand,
         typename InputType>
void PostingIteratorImpl<IteratorTraits, FieldTraits, HasWand,
                         InputType>::ReadBlock(doc_id_t prev_doc) {
  _new_skip_reader.ReadInlineBlock(GetDocIn(), this->_enc_buf, this->_docs,
                                   this->_freqs, prev_doc);
  this->_max_in_leaf = *(std::end(this->_docs) - 1);
  this->_left_in_leaf = doc_limits::kBlockSize;
  this->_left_in_list -= doc_limits::kBlockSize;
  SDB_ASSERT(this->_left_in_list == _new_skip_reader.LeftDocsCount());
}

template<typename IteratorTraits, typename FieldTraits, bool HasWand,
         typename InputType>
void PostingIteratorImpl<IteratorTraits, FieldTraits, HasWand,
                         InputType>::ReadLeaf(doc_id_t prev_doc) {
  if (this->_left_in_list >= doc_limits::kBlockSize) [[likely]] {
    ReadBlock(prev_doc);
  } else {
    ReadTail(prev_doc);
  }
}

template<typename IteratorTraits, typename FieldTraits, bool HasWand,
         typename InputType>
bool PostingIteratorImpl<IteratorTraits, FieldTraits, HasWand,
                         InputType>::SeekToLeaf(doc_id_t target) {
  if (target <= this->_max_in_leaf) {
    // don't need seek
    return true;
  } else if (this->_left_in_list == 0) {
    return false;
  }

  bool found = false;
  auto prev_doc = this->_doc;
  if (this->_docs_count >= doc_limits::kBlockSize) {
    std::pair<doc_id_t, bool> tmp;
    if constexpr (IteratorTraits::Position()) {
      tmp = _new_skip_reader.SeekAndReadNewBlock(
        target, GetDocIn(), this->_enc_buf, this->_docs, this->_freqs,
        &std::get<Position>(this->_attrs));
    } else {
      tmp = _new_skip_reader.SeekAndReadNewBlock(
        target, GetDocIn(), this->_enc_buf, this->_docs, this->_freqs, nullptr);
    }

    prev_doc = tmp.first;
    found = tmp.second;

    this->_left_in_list = _new_skip_reader.LeftDocsCount();
    // found = prev_doc >= target;
  }

  if (!found) {
    if (this->_left_in_list > 0) {
      ReadTail(prev_doc);
      return this->_max_in_leaf >= target;
    }
    return false;
  }

  this->_max_in_leaf = *(std::end(this->_docs) - 1);
  this->_left_in_leaf = doc_limits::kBlockSize;
  return true;
}

}  // namespace irs
