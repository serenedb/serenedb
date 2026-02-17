////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/algorithm/container.h>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/disjunction.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

// Disjunction is template for Adapter instead of direct use of ScoreAdapter
// only because of variadic phrase
namespace irs {
namespace detail {

template<size_t Size>
struct MinMatchBuffer {
  explicit MinMatchBuffer(size_t min_match_count) noexcept
    : _min_match_count(std::max(size_t(1), min_match_count)) {}

  uint32_t match_count(size_t i) const noexcept {
    SDB_ASSERT(i < Size);
    return _match_count[i];
  }

  uint32_t count() const noexcept {
    uint32_t count = 0;
    for (const auto match_count : _match_count) {
      count += match_count >= _min_match_count;
    }
    return count;
  }

  bool inc(size_t i) noexcept { return ++_match_count[i] < _min_match_count; }

  void clear() noexcept {
    std::memset(_match_count.data(), 0, sizeof _match_count);
  }

  void min_match_count(size_t min_match_count) noexcept {
    _min_match_count = std::max(min_match_count, _min_match_count);
  }
  size_t min_match_count() const noexcept { return _min_match_count; }

  auto* data() noexcept { return _match_count.data(); }

 private:
  size_t _min_match_count;
  std::array<uint32_t, Size> _match_count;
};

}  // namespace detail

template<typename Adapter>
using IteratorVisitor = bool (*)(void*, Adapter&);

enum class MatchType {
  Match,
  MinMatchFast,
  MinMatch,
};

template<MatchType MinMatch, bool SeekReadahead, size_t NumBlocks = 64>
struct BlockDisjunctionTraits {
  static_assert(NumBlocks >= 1);

  // "false" - iterator is used for min match filtering,
  // "true" - otherwise
  static constexpr bool kMinMatchEarlyPruning =
    MatchType::MinMatchFast == MinMatch;

  // "false" - iterator is used for min match filtering,
  // "true" - otherwise
  static constexpr bool kMinMatch =
    kMinMatchEarlyPruning || MatchType::Match != MinMatch;

  // Use readahead buffer for random access
  static constexpr bool kSeekReadahead = SeekReadahead;

  // Size of the readhead buffer in blocks
  static constexpr auto kNumBlocks = NumBlocks;
};

// The implementation reads ahead 64*NumBlocks documents.
// It isn't optimized for conjunction case when the requested min match
// count equals to a number of input iterators.
// It's better to to use a dedicated "conjunction" iterator.
template<typename Adapter, ScoreMergeType MergeType, typename Traits>
class BlockDisjunction : public DocIterator {
 public:
  using Adapters = std::vector<Adapter>;

  static constexpr auto kMergeType = MergeType;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;

  explicit BlockDisjunction(Adapters&& itrs)
    : BlockDisjunction{std::move(itrs), 1} {}
  BlockDisjunction(Adapters&& itrs, size_t min_match_count, CostAttr::Type est)
    : BlockDisjunction{std::move(itrs), min_match_count, est,
                       ResolveOverloadTag{}} {}
  BlockDisjunction(Adapters&& itrs, size_t min_match_count)
    : BlockDisjunction{std::move(itrs), min_match_count,
                       [&] noexcept {
                         return absl::c_accumulate(
                           _itrs, CostAttr::Type{0},
                           [](CostAttr::Type lhs, const Adapter& rhs) noexcept {
                             return lhs + CostAttr::extract(rhs, 0);
                           });
                       },
                       ResolveOverloadTag{}} {}

  IRS_FORCE_INLINE size_t MatchCount() const noexcept { return _match_count; }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  IRS_FORCE_INLINE doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  doc_id_t advance() final { return AdvanceImpl().doc; }

  doc_id_t seek(doc_id_t target) final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (target <= doc_value) [[unlikely]] {
      return doc_value;
    }

    if (target < _max) {
      const doc_id_t block_base = (_max - kWindow);

      target -= block_base;
      const doc_id_t block_offset = target / kBlockSize;

      _doc_base = block_base + block_offset * kBlockSize;
      _begin = _mask + block_offset + 1;

      SDB_ASSERT(_begin > std::begin(_mask) && _begin <= std::end(_mask));
      _cur = _begin[-1] & ((~UINT64_C(0)) << target % kBlockSize);

      return advance();
    }

    doc_value = doc_limits::eof();

    if constexpr (Traits::kMinMatch) {
      _match_count = 0;
    }

    VisitAndPurge([&](auto v) {
      auto& [it, _] = v;

      const auto value = it.seek(target);

      if (doc_limits::eof(value)) {
        // exhausted
        return false;
      }

      if (value < doc_value) {
        doc_value = value;
        if constexpr (Traits::kMinMatch) {
          _match_count = 1;
        }
      } else if constexpr (Traits::kMinMatch) {
        if (target == value) {
          ++_match_count;
        }
      }

      return true;
    });

    if (_itrs.empty()) {
      _match_count = 0;
      return doc_value = doc_limits::eof();
    }

    SDB_ASSERT(!doc_limits::eof(doc_value));
    _cur = 0;
    _begin = std::end(_mask);  // enforce "refill()" for upcoming "next()"
    _max = doc_value;

    if constexpr (Traits::kSeekReadahead) {
      _min = doc_value;
      return advance();
    } else {
      _min = doc_value + 1;
      _buf_offset = 0;

      if constexpr (Traits::kMinMatch) {
        if (_match_count < _match_buf.min_match_count()) {
          return advance();
        }
      }

      if constexpr (kHasScore) {
        _score_buf.score_window[0] = 0;
        _doc_base = doc_value;

        for (auto [it, scorer] : std::views::zip(_itrs, _scorers)) {
          if (scorer.IsDefault() || it.value() != doc_value) {
            continue;
          }

          it.FetchScoreArgs(0);
          const auto sub_score = scorer.Score();
          Merge<MergeType>(_score_buf.score_window[0], sub_score);
        }
      }

      return doc_value;
    }
  }

  uint32_t count() final {
    uint32_t count = 0;

    while (RefillImpl()) {
      for (const auto word : _mask) {
        count += std::popcount(word);
      }
    }

    _match_count = 0;
    std::get<DocAttr>(_attrs).value = doc_limits::eof();
    return count;
  }

  void FetchScoreArgs(uint16_t index) final {
    if constexpr (kHasScore) {
      SDB_ASSERT(doc_limits::valid(value()));
      SDB_ASSERT(!doc_limits::eof(value()));
      _score_buf.FetchScoreArgs(
        static_cast<uint16_t>(_buf_offset + value() -
                              _doc_base),  // TODO(gnusi): make better
        index);
    }
  }

  std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                      uint64_t* mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final {
    // TODO(gnusi): optimize
    return FillBlockImpl(*this, min, max, mask, score, match);
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    if constexpr (kHasScore) {
      const PrepareScoreContext sub{
        .scorer = ctx.scorer,
        .segment = ctx.segment,
        .fetcher = &_fetcher,
      };

      bool no_score = true;
      _scorers.assign_range(_itrs | std::views::transform([&](auto& it) {
                              auto score = it.PrepareScore(sub);
                              no_score &= score.IsDefault();
                              return score;
                            }));
      if (no_score) {
        return ScoreFunction::Default();
      }

      return ScoreFunction::Wrap(_score_buf);
    } else {
      return ScoreFunction::Default();
    }
  }

 private:
  static constexpr doc_id_t kBlockSize = BitsRequired<uint64_t>();

  static constexpr auto kNumBlocks = Traits::kNumBlocks;

  static constexpr doc_id_t kWindow = kBlockSize * kNumBlocks;

  static_assert(kBlockSize * kNumBlocks < std::numeric_limits<doc_id_t>::max());

  using Attributes = std::tuple<DocAttr, CostAttr>;

  struct ResolveOverloadTag {};

  struct AdvanceResult {
    doc_id_t doc;
    uint16_t score_idx;
  };

  IRS_FORCE_INLINE AdvanceResult AdvanceImpl() {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    while (_cur == 0) {
      if (_begin >= std::end(_mask)) {
        if (Refill()) {
          SDB_ASSERT(_cur);
          break;
        }

        _match_count = 0;
        doc_value = doc_limits::eof();
        return {doc_value, 0};
      }

      _cur = *_begin++;
      _doc_base += BitsRequired<uint64_t>();
      if constexpr (Traits::kMinMatch || kHasScore) {
        _buf_offset += BitsRequired<uint64_t>();
      }
    }

    const size_t offset = std::countr_zero(_cur);
    _cur = PopBit(_cur);

    if constexpr (Traits::kMinMatch) {
      const size_t buf_offset = _buf_offset + offset;
      _match_count = _match_buf.match_count(buf_offset);
      SDB_ASSERT(_match_count >= _match_buf.min_match_count());
    }

    doc_value = _doc_base + static_cast<doc_id_t>(offset);

    return {doc_value, static_cast<uint16_t>(_buf_offset + offset)};
  }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    ResolveScoreCollector(collector, [&](auto& collector) IRS_FORCE_INLINE {
      const score_t* IRS_RESTRICT score_window = nullptr;
      if constexpr (kHasScore) {
        score_window = _score_buf.score_window.data();
      }

      auto& doc_value = std::get<DocAttr>(_attrs).value;
      auto cur = _cur;
      auto doc_base = _doc_base;
      [[maybe_unused]] auto buf_offset = _buf_offset;
      auto* IRS_RESTRICT begin = _begin;
      const auto* IRS_RESTRICT const end = std::end(_mask);

      while (true) {
        while (cur == 0) {
          if (begin >= end) {
            if (Refill()) {
              SDB_ASSERT(_cur);
              cur = _cur;
              doc_base = _doc_base;
              begin = _begin;
              if constexpr (Traits::kMinMatch || kHasScore) {
                buf_offset = _buf_offset;
              }
              break;
            }

            _match_count = 0;
            _cur = 0;
            doc_value = doc_limits::eof();
            _begin = begin;
            return;
          }

          cur = *begin++;
          doc_base += BitsRequired<uint64_t>();
          if constexpr (Traits::kMinMatch || kHasScore) {
            buf_offset += BitsRequired<uint64_t>();
          }
        }
        const auto bit_offset = std::countr_zero(cur);
        cur = PopBit(cur);

        if constexpr (Traits::kMinMatch) {
          _match_count = _match_buf.match_count(buf_offset + bit_offset);
          SDB_ASSERT(_match_count >= _match_buf.min_match_count());
        }

        const auto doc = doc_base + static_cast<doc_id_t>(bit_offset);
        collector.Add(score_window[buf_offset + bit_offset], doc);
      }
    });
  }

  template<typename Estimation>
  BlockDisjunction(Adapters&& itrs, size_t min_match_count,
                   Estimation&& estimation, ResolveOverloadTag)
    : _itrs(std::move(itrs)),
      _scorers(_itrs.size()),
      _match_count(_itrs.empty() ? size_t(0)
                                 : static_cast<size_t>(!Traits::kMinMatch)),
      _match_buf(min_match_count) {
    std::get<CostAttr>(_attrs).reset(std::forward<Estimation>(estimation));

    if (_itrs.empty()) {
      std::get<DocAttr>(_attrs).value = doc_limits::eof();
    }

    if (Traits::kMinMatch && min_match_count > 1) {
      // sort subnodes in ascending order by their cost
      // FIXME(gnusi) don't use extract
      absl::c_sort(_itrs, [](const auto& lhs, const auto& rhs) noexcept {
        return CostAttr::extract(lhs, 0) < CostAttr::extract(rhs, 0);
      });

      // FIXME(gnusi): fix estimation, we have to estimate only min_match
      // iterators
    }
  }

  template<typename Visitor>
  void VisitAndPurge(Visitor visitor) {
    auto itrs = std::views::zip(_itrs, _scorers);
    auto begin = itrs.begin();
    auto end = itrs.end();

    while (begin != end) {
      if (!visitor(*begin)) {
        // TODO(mbkkt) It looks good, but only for wand case
        // scores_.unscored -= begin->score().IsDefault();
        std::iter_swap(begin, --end);
        _scorers.pop_back();
        _itrs.pop_back();

        if constexpr (Traits::kMinMatchEarlyPruning) {
          // we don't need precise match count
          if (_itrs.size() < _match_buf.min_match_count()) {
            // can't fulfill min match requirement anymore
            _scorers.clear();
            _itrs.clear();
            return;
          }
        }
      } else {
        ++begin;
      }
    }

    if constexpr (Traits::kMinMatch && !Traits::kMinMatchEarlyPruning) {
      // we need precise match count, so can't break earlier
      if (_itrs.size() < _match_buf.min_match_count()) {
        // can't fulfill min match requirement anymore
        _scorers.clear();
        _itrs.clear();
        return;
      }
    }
  }

  void Reset() noexcept {
    std::memset(_mask, 0, sizeof _mask);
    if constexpr (kHasScore) {
      std::memset(_score_buf.score_window.data(), 0,
                  sizeof(score_t) * _score_buf.score_window.size());
    }
    if constexpr (Traits::kMinMatch) {
      _match_buf.clear();
    }
  }

  bool RefillImpl() {
    if (_itrs.empty()) {
      return false;
    }

    if constexpr (!Traits::kMinMatch) {
      Reset();
    }

    [[maybe_unused]] FillBlockScoreContext score_ctx;
    if constexpr (kHasScore) {
      score_ctx.fetcher = &_fetcher;
      score_ctx.score_window = _score_buf.score_window.data();
      score_ctx.merge_type = MergeType;
    }
    [[maybe_unused]] FillBlockMatchContext match_ctx;
    if constexpr (Traits::kMinMatch) {
      match_ctx.matches = _match_buf.data();
      match_ctx.min_match_count = _match_buf.min_match_count();
    }

    bool empty = true;
    do {
      if constexpr (Traits::kMinMatch) {
        // in min match case we need to clear
        // internal buffers on every iteration
        Reset();
      }

      _doc_base = _min;
      SDB_ASSERT(_min <= doc_limits::eof() - kWindow);  // TODO(gnusi): ensure
      _max = _min + kWindow;
      _min = doc_limits::eof();

      VisitAndPurge([&](auto v) mutable {
        // FIXME
        // for min match case we can skip the whole block if
        // we can't satisfy match_buf_.min_match_count() conditions, namely
        // if constexpr (Traits::kMinMatch) {
        //  if (empty && (&it + (match_buf_.min_match_count() -
        //  match_buf_.max_match_count()) < (itrs_.data() + itrs_.size()))) {
        //    // skip current block
        //    return true;
        //  }
        //}

        auto& [it, score] = v;

        if constexpr (kHasScore) {
          score_ctx.score = &score;
        }

        const auto [doc, has_hits] =
          it.FillBlock(_doc_base, _max, _mask, score_ctx, match_ctx);

        empty &= has_hits;
        _min = std::min(doc, _min);
        return !doc_limits::eof(doc);
      });
    } while (empty && !_itrs.empty());

    return !empty;
  }

  bool Refill() {
    const bool empty = !RefillImpl();
    if (empty) {
      // exhausted
      SDB_ASSERT(_itrs.empty());
      return false;
    }

    _cur = *_mask;
    _begin = _mask + 1;
    if constexpr (Traits::kMinMatch || kHasScore) {
      _buf_offset = 0;
    }
    while (!_cur) {
      _cur = *_begin++;
      _doc_base += BitsRequired<uint64_t>();
      if constexpr (Traits::kMinMatch || kHasScore) {
        _buf_offset += BitsRequired<uint64_t>();
      }
    }
    SDB_ASSERT(_cur);

    return true;
  }

  static_assert(kWindow <= std::numeric_limits<uint16_t>::max());

  struct BlockScore final : ScoreOperator {
    alignas(64) std::array<score_t, kScoreBlock> result;
    alignas(4096) std::array<score_t, kWindow> score_window;

    void FetchScoreArgs(uint16_t offset, uint16_t index) noexcept {
      result[index] = score_window[offset];
    }

    score_t Score() noexcept final { return result.front(); }

    void ScoreBlock(score_t* res) noexcept final {
      std::memcpy(res, result.data(), kScoreBlock * sizeof(score_t));
    }

    void Score(score_t* res, size_t n) noexcept final {
      std::memcpy(res, result.data(), n * sizeof(score_t));
    }
  };

  static_assert(kWindow % kScoreBlock == 0,
                "kWindow must be a multiple of kScoreBlock");

  ColumnArgsFetcher _fetcher;
  doc_id_t _min{doc_limits::min()};      // base doc id for the next mask
  doc_id_t _max{doc_limits::invalid()};  // max doc id in the current mask
  doc_id_t _doc_base{doc_limits::invalid()};
  size_t _buf_offset{};  // offset within a buffer
  uint64_t _mask[kNumBlocks]{};
  [[no_unique_address]] utils::Need<kHasScore, BlockScore> _score_buf;
  [[no_unique_address]] utils::Need<Traits::kMinMatch,
                                    detail::MinMatchBuffer<kWindow>> _match_buf;
  Adapters _itrs;
  std::vector<ScoreFunction> _scorers;
  uint64_t* IRS_RESTRICT _begin{std::end(_mask)};
  uint64_t _cur{};
  Attributes _attrs;
  size_t _match_count;
};

template<typename Adapter, ScoreMergeType MergeType>
using DisjunctionIterator =
  BlockDisjunction<Adapter, MergeType,
                   BlockDisjunctionTraits<MatchType::Match, false>>;

template<typename Adapter, ScoreMergeType MergeType>
using MinMatchIterator =
  BlockDisjunction<Adapter, MergeType,
                   BlockDisjunctionTraits<MatchType::MinMatch, false>>;

template<typename T>
struct RebindIterator;

template<typename Adapter, ScoreMergeType MergeType>
struct RebindIterator<DisjunctionIterator<Adapter, MergeType>> {
  using Unary = void;  // block disjunction doesn't support visitor
  using Basic = void;  // basic disjunction always faster than small
  using Small = void;  // block disjunction always faster than small
  using Wand = DisjunctionIterator<Adapter, MergeType>;
};

template<typename Adapter, ScoreMergeType MergeType>
struct RebindIterator<MinMatchIterator<Adapter, MergeType>> {
  using Disjunction = DisjunctionIterator<Adapter, MergeType>;
  using Wand = MinMatchIterator<Adapter, MergeType>;
};

}  // namespace irs
