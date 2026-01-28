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

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iresearch/search/score_function.hpp>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "basics/std.hpp"
#include "basics/system-compiler.h"
#include "conjunction.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/disjunction.hpp"
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

 private:
  size_t _min_match_count;
  std::array<uint32_t, Size> _match_count;
};

struct SubScoresCtx : SubScores {
  size_t unscored = 0;

  size_t Size() const noexcept { return unscored + scores.size(); }

  void Clear() noexcept {
    scores.clear();
    sum_score = 0.f;
    unscored = 0;
  }
};

inline bool MakeSubScores(const auto& itrs, SubScoresCtx& scores) {
  SDB_ASSERT(scores.Size() == 0);
  scores.scores.reserve(itrs.size());
  for (auto& it : itrs) {
    const auto& score = it.score();
    if (score.IsDefault()) {
      ++scores.unscored;
      continue;
    }
    scores.scores.emplace_back(&score);
    const auto tail = score.max.tail;
    if (tail == std::numeric_limits<score_t>::max()) {
      return false;
    }
    scores.sum_score += tail;
  }
  absl::c_sort(scores.scores, [](const auto* lhs, const auto* rhs) noexcept {
    return lhs->max.tail > rhs->max.tail;
  });
  SDB_ASSERT(scores.Size() == itrs.size());
  return !scores.scores.empty();
}

}  // namespace detail

template<typename Adapter>
using IteratorVisitor = bool (*)(void*, Adapter&);

enum class MatchType {
  Match,
  MinMatchFast,
  MinMatch,
};

template<MatchType MinMatch, bool SeekReadahead, size_t NumBlocks = 8>
struct BlockDisjunctionTraits {
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
  static constexpr size_t kNumBlocks = NumBlocks;
};

// The implementation reads ahead 64*NumBlocks documents.
// It isn't optimized for conjunction case when the requested min match
// count equals to a number of input iterators.
// It's better to to use a dedicated "conjunction" iterator.
template<typename Adapter, ScoreMergeType MergeType, typename Traits>
class BlockDisjunction : public DocIterator, private ScoreCtx {
 public:
  using Adapters = std::vector<Adapter>;

  static constexpr auto kMergeType = MergeType;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;

  explicit BlockDisjunction(Adapters&& itrs)
    : BlockDisjunction{std::move(itrs), 1} {}
  BlockDisjunction(Adapters&& itrs, size_t min_match_count)
    : BlockDisjunction{std::move(itrs), min_match_count,
                       detail::SubScoresCtx{}} {}
  BlockDisjunction(Adapters&& itrs, size_t min_match_count, CostAttr::Type est)
    : BlockDisjunction{std::move(itrs), min_match_count, detail::SubScoresCtx{},
                       est} {}
  BlockDisjunction(Adapters&& itrs, size_t min_match_count,
                   detail::SubScoresCtx&& scores, CostAttr::Type est)
    : BlockDisjunction{std::move(itrs), min_match_count, est, std::move(scores),
                       ResolveOverloadTag{}} {}
  BlockDisjunction(Adapters&& itrs, size_t min_match_count,
                   detail::SubScoresCtx&& scores)
    : BlockDisjunction{std::move(itrs), min_match_count,
                       [&] noexcept {
                         return absl::c_accumulate(
                           _itrs, CostAttr::Type{0},
                           [](CostAttr::Type lhs, const Adapter& rhs) noexcept {
                             return lhs + CostAttr::extract(rhs, 0);
                           });
                       },
                       std::move(scores), ResolveOverloadTag{}} {}

  IRS_FORCE_INLINE size_t MatchCount() const noexcept { return _match_count; }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  IRS_FORCE_INLINE doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  doc_id_t advance() final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    do {
      while (_cur == 0) {
        if (_begin >= std::end(_mask)) {
          if (Refill()) {
            SDB_ASSERT(_cur);
            break;
          }

          _match_count = 0;
          return doc_value = doc_limits::eof();
        }

        _cur = *_begin++;
        _doc_base += BitsRequired<uint64_t>();
        if constexpr (Traits::kMinMatch || kHasScore) {
          _buf_offset += BitsRequired<uint64_t>();
        }
      }

      const size_t offset = std::countr_zero(_cur);
      irs::UnsetBit(_cur, offset);

      [[maybe_unused]] const size_t buf_offset = _buf_offset + offset;

      if constexpr (Traits::kMinMatch) {
        _match_count = _match_buf.match_count(buf_offset);

        if (_match_count < _match_buf.min_match_count()) {
          continue;
        }
      }

      doc_value = _doc_base + doc_id_t(offset);

      return doc_value;
    } while (Traits::kMinMatch);
    SDB_UNREACHABLE();
  }

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

    VisitAndPurge([&](auto& it) {
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
        for (auto& it : _itrs) {
          // TODO(gnusi): we have to set a new score/collect function to support
          // seek
          if (!it.score().IsDefault() && doc_value == it.value()) {
            it.CollectData(_score_buf.stream.Index());
            _score_buf.stream.CollectData(0);  // TODO(gnusi): fix index
          }
        }
      }
      return doc_value;
    }
  }

  uint32_t count() final {
    uint32_t count = 0;

    while (_cur != 0 && next()) [[unlikely]] {
      ++count;
    }

    while (Refill()) {
      if constexpr (Traits::kMinMatch) {
        count += _match_buf.count();
      } else {
        for (const auto word : _mask) {
          count += std::popcount(word);
        }
      }
    }

    _match_count = 0;
    std::get<DocAttr>(_attrs).value = doc_limits::eof();
    return count;
  }

  void CollectData(uint16_t index) final {
    if constexpr (kHasScore) {
      _score_buf.CollectData(
        static_cast<uint16_t>(_buf_offset + value() -
                              _doc_base),  // TODO(gnusi): make better
        index);
    }
  }

 private:
  static constexpr doc_id_t kBlockSize = BitsRequired<uint64_t>();

  static constexpr doc_id_t kNumBlocks =
    static_cast<doc_id_t>(std::max(size_t(1), Traits::kNumBlocks));

  static constexpr doc_id_t kWindow = kBlockSize * kNumBlocks;

  static_assert(kBlockSize * size_t(kNumBlocks) <
                std::numeric_limits<doc_id_t>::max());

  using Attributes = std::tuple<DocAttr, ScoreAttr, CostAttr>;

  struct ResolveOverloadTag {};

  uint32_t collect(std::span<doc_id_t> docs) final {
    return DocIterator::Collect(*this, docs);
  }

  template<typename Estimation>
  BlockDisjunction(Adapters&& itrs, size_t min_match_count,
                   Estimation&& estimation, detail::SubScoresCtx&& scores,
                   ResolveOverloadTag)
    : _itrs(std::move(itrs)),
      _match_count(_itrs.empty() ? size_t(0)
                                 : static_cast<size_t>(!Traits::kMinMatch)),
      _match_buf(min_match_count),
      _scores(std::move(scores)) {
    std::get<CostAttr>(_attrs).reset(std::forward<Estimation>(estimation));

    if (_itrs.empty()) {
      std::get<DocAttr>(_attrs).value = doc_limits::eof();
    }

    if constexpr (kHasScore) {
      this->PrepareScore();
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
    auto* begin = _itrs.data();
    auto* end = _itrs.data() + _itrs.size();

    while (begin != end) {
      if (!visitor(*begin)) {
        // TODO(mbkkt) It looks good, but only for wand case
        // scores_.unscored -= begin->score().IsDefault();
        irstd::SwapRemove(_itrs, begin);
        --end;

        if constexpr (Traits::kMinMatchEarlyPruning) {
          // we don't need precise match count
          if (_itrs.size() < _match_buf.min_match_count()) {
            // can't fulfill min match requirement anymore
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
        _itrs.clear();
        return;
      }
    }
  }

  void Reset() noexcept {
    std::memset(_mask, 0, sizeof _mask);
    if constexpr (Traits::kMinMatch) {
      _match_buf.clear();
    }
  }

  bool Refill() {
    if (_itrs.empty()) {
      return false;
    }

    if constexpr (!Traits::kMinMatch) {
      Reset();
    }

    bool empty = true;

    if constexpr (kHasScore) {
      // TODO(gnusi): can we avoid that?
      _score_buf.stream.ClearWindow();
    }

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

      VisitAndPurge([&](auto& it) mutable {
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

        auto update_min = [&](doc_id_t doc) {
          _min = std::min(doc, _min);
          return !doc_limits::eof(doc);
        };

        auto track_match = [&](size_t offset) {
          if constexpr (Traits::kMinMatch) {
            empty &= _match_buf.inc(offset);
          } else {
            empty = false;
          }
        };

        // disjunction is 1 step next behind, that may happen:
        // - before the very first next()
        // - after seek() in case of 'kSeekReadahead == false'
        if (it.value() < _doc_base) {
          it.advance();
        }

        if constexpr (kHasScore) {
          if (!it.score().IsDefault()) {
            _score_buf.stream.SetScore(it.score());
            const auto doc =
              Refill(it, _mask, _doc_base, _max, [&](size_t offset) {
                it.CollectData(_score_buf.stream.Index());
                _score_buf.stream.CollectData(offset);
                track_match(offset);
              });
            _score_buf.stream.Finish();
            return update_min(doc);
          }
        }

        return update_min(
          Refill(it, _mask, _doc_base, _max, std::move(track_match)));
      });
    } while (empty && !_itrs.empty());

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

  template<typename Func>
  static doc_id_t Refill(Adapter& it, uint64_t* mask, doc_id_t min,
                         doc_id_t max, Func&& callback) {
    auto value = it.value();
    for (; value < max; value = it.advance()) {
      const auto offset = value - min;

      SetBit(mask[offset / kBlockSize], offset % kBlockSize);

      callback(offset);
    }
    return value;
  }

  static_assert(kWindow <= std::numeric_limits<uint16_t>::max());

  class ScoreStream {
   public:
    void Init() { _hits.Realloc(kScoreBlock); }

    uint16_t Index() const noexcept {
      return static_cast<uint16_t>(_hits.Size());
    }

    void CollectData(uint16_t hit) {
      _hits.PushBack(hit);

      if (_hits.Size() == _hits.Capacity()) {
        Flush();
      }
    }

    void ClearWindow() noexcept {
      std::memset(_score_window.data(), 0, sizeof(score_t) * kWindow);
    }

    void Finish() {
      if (_hits.Size()) {
        Flush();
      }
    }

    auto GetScore(uint16_t index) noexcept { return _score_window[index]; }

    auto GetHits() noexcept { return std::span{_hits.Data(), _hits.Size()}; }

    void SetScore(const ScoreFunction& score) noexcept {
      _score = &score;
      SDB_ASSERT(_hits.Size() == 0);
    }

    void Flush() {
      SDB_ASSERT(_hits.Size());

      _score->Score(_scores.data(), _hits.Size());
      // TODO(gnusi): can optimize if dense
      Merge<MergeType>(_score_window.data(), _hits.Data(), _scores.data(),
                       _hits.Size());
      _hits.Clear();
    }

   private:
    std::array<score_t, kWindow> _score_window;
    std::array<score_t, kScoreBlock> _scores;
    FixedBuffer<uint16_t> _hits;
    const ScoreFunction* _score = nullptr;
  };

  struct ScoreState {
    // TODO(gnuis): it's big allocate on heap?
    ScoreStream stream;
    std::array<score_t, kScoreBlock> result;

    void CollectData(uint16_t offset, uint16_t index) noexcept {
      result[index] = stream.GetScore(offset);
    }

    ScoreFunction PrepareScore(ScoreCtx* ctx, auto min) {
      stream.Init();

      return {ctx,
              [](ScoreCtx* ctx, score_t* res, size_t n) noexcept {
                auto& self = static_cast<BlockDisjunction&>(*ctx);
                std::memcpy(res, self._score_buf.result.data(),
                            n * sizeof(score_t));
              },
              min};
    }
  };

  void PrepareScore() {
    static_assert(kHasScore);
    auto& score = std::get<ScoreAttr>(_attrs);
    auto min = ScoreFunction::NoopMin;
    if (!_scores.scores.empty()) {
      score.max.leaf = score.max.tail = _scores.sum_score;
      min = [](ScoreCtx* ctx, score_t arg) noexcept {
        auto& self = static_cast<BlockDisjunction&>(*ctx);
        if (self._scores.Size() != self._itrs.size()) [[unlikely]] {
          self._scores.Clear();
          detail::MakeSubScores(self._itrs, self._scores);
          auto& score = std::get<ScoreAttr>(self._attrs);
          // TODO(mbkkt) We cannot change tail now
          // Because it needs to recompute sum_score for our parent iterator
          score.max.leaf /* = score.max.tail */ = self._scores.sum_score;
        }
        auto it = self._scores.scores.begin();
        auto end = self._scores.scores.end();
        [[maybe_unused]] size_t min_match = 0;
        [[maybe_unused]] score_t sum = 0.f;
        while (it != end) {
          auto next = end;
          if constexpr (Traits::kMinMatch) {
            if (arg > sum) {  // TODO(mbkkt) strict wand: >=
              ++min_match;
              next = it + 1;
            }
            sum += (*it)->max.tail;
          }
          const auto others = self._scores.sum_score - (*it)->max.tail;
          if (arg > others) {
            // For common usage `arg - others <= (*it)->max.tail` -- is true
            (*it)->Min(arg - others);
            next = it + 1;
          }
          it = next;
        }
        if constexpr (Traits::kMinMatch) {
          self._match_buf.min_match_count(min_match);
        }
      };
    }
    score = _score_buf.PrepareScore(this, min);
  }

  static_assert(kWindow % kScoreBlock == 0,
                "kWindow must be a multiple of kScoreBlock");

  uint64_t _mask[kNumBlocks]{};
  Adapters _itrs;
  uint64_t* _begin{std::end(_mask)};
  uint64_t _cur{};
  doc_id_t _doc_base{doc_limits::invalid()};
  doc_id_t _min{doc_limits::min()};      // base doc id for the next mask
  doc_id_t _max{doc_limits::invalid()};  // max doc id in the current mask
  Attributes _attrs;
  size_t _match_count;
  size_t _buf_offset{};  // offset within a buffer
  [[no_unique_address]] utils::Need<kHasScore, ScoreState> _score_buf;
  [[no_unique_address]] utils::Need<Traits::kMinMatch,
                                    detail::MinMatchBuffer<kWindow>> _match_buf;
  // TODO(mbkkt) We don't need scores_ for not wand,
  // but we don't want to generate more functions, than necessary
  [[no_unique_address]] utils::Need<kHasScore, detail::SubScoresCtx> _scores;
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
  using Basic = void;
  using Small = void;  // block disjunction always faster than small
  using Wand = DisjunctionIterator<Adapter, MergeType>;
};

template<typename Adapter, ScoreMergeType MergeType>
struct RebindIterator<MinMatchIterator<Adapter, MergeType>> {
  using Disjunction = DisjunctionIterator<Adapter, MergeType>;
  using Wand = MinMatchIterator<Adapter, MergeType>;
};

}  // namespace irs
