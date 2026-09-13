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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <tuple>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/iterator_pos.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/phrase_matcher.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs {
namespace detail::slop {

struct MatchResult {
  uint64_t freq = 0;
  double weight = 0.0;
  score_t boost = 0.f;
  PosAttr::value_t best_distance = 0;
  bool any = false;
};

struct MatchScratch {
  std::vector<int64_t> offsets;
  std::vector<int64_t> shift;
  std::vector<size_t> cursor;
  std::vector<PosAttr::value_t> snap;
  std::vector<uint32_t> snap_start;
  std::vector<uint32_t> snap_end;
  std::vector<score_t> snap_boost;
};

struct GroupPair {
  uint32_t a;
  uint32_t b;
};

inline void BuildGroupPairs(std::span<const uint32_t> groups, size_t n,
                            std::vector<GroupPair>& out) {
  out.clear();
  for (size_t a = 0; a != n; ++a) {
    for (size_t b = a + 1; b != n; ++b) {
      if (groups.empty() || groups[a] == groups[b]) {
        out.push_back({static_cast<uint32_t>(a), static_cast<uint32_t>(b)});
      }
    }
  }
}

template<size_t N, bool Enforce, typename Cursors, typename Collect>
MatchResult SweepOf(Cursors& cur, std::span<const int64_t> offsets,
                    int64_t slop, std::span<const GroupPair> pairs,
                    MatchScratch& scratch, bool early_exit, Collect&& collect) {
  MatchResult res{};
  constexpr bool kStatic = N != 0;
  const size_t n = kStatic ? N : offsets.size();
  SDB_ASSERT(n == offsets.size());

  [[maybe_unused]] std::array<int64_t, kStatic ? N : 1> inplace;
  auto& shift = [&]() -> auto& {
    if constexpr (kStatic) {
      return inplace;
    } else {
      scratch.shift.resize(n);
      return scratch.shift;
    }
  }();

  const auto emit = [&](int64_t width) {
    ++res.freq;
    res.weight += 1.0 / (1.0 + static_cast<double>(width));
    res.boost = std::max(res.boost, cur.Boost());
    const auto distance = static_cast<PosAttr::value_t>(width);
    if (!res.any || distance < res.best_distance) {
      res.best_distance = distance;
    }
    res.any = true;
    collect();
    return !early_exit;
  };

  for (size_t i = 0; i != n; ++i) {
    const auto pos = cur.Start(i);
    if (pos_limits::eof(pos)) {
      return res;
    }
    shift[i] = static_cast<int64_t>(pos) - offsets[i];
  }
  int64_t end = *std::max_element(shift.data(), shift.data() + n);

  const auto take = [&](size_t i, PosAttr::value_t pos) {
    if (pos_limits::eof(pos)) {
      return false;
    }
    shift[i] = static_cast<int64_t>(pos) - offsets[i];
    end = std::max(end, shift[i]);
    return true;
  };
  const auto advance = [&](size_t i) { return take(i, cur.Advance(i)); };

  const auto resolve = [&] {
    if constexpr (!Enforce) {
      return true;
    } else {
      for (;;) {
        size_t a = n;
        size_t b = n;
        for (const auto& pair : pairs) {
          if (shift[pair.a] + offsets[pair.a] ==
              shift[pair.b] + offsets[pair.b]) {
            a = pair.a;
            b = pair.b;
            break;
          }
        }
        if (a == n) {
          return true;
        }
        const size_t lesser = (shift[a] < shift[b] || (shift[a] == shift[b] &&
                                                       offsets[a] < offsets[b]))
                                ? a
                                : b;
        if (!advance(lesser)) {
          return false;
        }
      }
    }
  };

  size_t least = 0;
  int64_t runner = 0;
  int64_t window = 0;
  const auto refresh = [&] {
    runner = std::numeric_limits<int64_t>::max();
    for (size_t i = 0; i != n; ++i) {
      if (i != least) {
        runner = std::min(runner, shift[i]);
      }
    }
  };
  const auto pick = [&] {
    least = 0;
    for (size_t i = 1; i != n; ++i) {
      if (shift[i] < shift[least]) {
        least = i;
      }
    }
    refresh();
  };

  const auto step = [&](size_t i) {
    if (window > slop) {
      int64_t target = std::min(end - slop, runner + 1);
      if constexpr (Enforce) {
        const int64_t here = shift[i] + offsets[i];
        for (const auto& pair : pairs) {
          const size_t peer = pair.a == i ? pair.b : (pair.b == i ? pair.a : i);
          if (peer == i) {
            continue;
          }
          const int64_t at = shift[peer] + offsets[peer];
          if (at > here) {
            target = std::min(target, at - offsets[i]);
          }
        }
      }
      const int64_t pos = target + offsets[i];
      if (pos > shift[i] + offsets[i]) {
        return take(i, cur.Seek(i, static_cast<PosAttr::value_t>(pos)));
      }
    }
    return advance(i);
  };

  if (!resolve()) {
    return res;
  }
  pick();
  window = end - shift[least];
  cur.Capture();

  for (;;) {
    if (!step(least)) {
      break;
    }
    if (!resolve()) {
      break;
    }
    refresh();
    if (shift[least] > runner) {
      if (window <= slop && !emit(window)) {
        return res;
      }
      pick();
      window = end - shift[least];
      cur.Capture();
    } else {
      const auto tightened = end - shift[least];
      if (tightened < window) {
        window = tightened;
        cur.Capture();
      }
    }
  }
  if (window <= slop) {
    emit(window);
  }
  return res;
}

template<size_t N, typename Cursors, typename Collect>
MatchResult Sweep(Cursors& cur, std::span<const int64_t> offsets, int64_t slop,
                  std::span<const GroupPair> pairs, MatchScratch& scratch,
                  bool early_exit, Collect&& collect) {
  if (!pairs.empty()) {
    return SweepOf<N, true>(cur, offsets, slop, pairs, scratch, early_exit,
                            std::forward<Collect>(collect));
  }
  return SweepOf<N, false>(cur, offsets, slop, pairs, scratch, early_exit,
                           std::forward<Collect>(collect));
}

class SpanCursors {
 public:
  SpanCursors(const std::vector<std::vector<PosAttr::value_t>>& slots,
              MatchScratch& scratch)
    : _slots{&slots}, _idx{&scratch.cursor}, _snap{&scratch.snap} {
    _idx->assign(slots.size(), 0);
    _snap->resize(slots.size());
  }

  PosAttr::value_t Start(size_t i) {
    (*_idx)[i] = 0;
    const auto& slot = (*_slots)[i];
    return slot.empty() ? pos_limits::eof() : slot.front();
  }

  PosAttr::value_t Advance(size_t i) {
    const auto& slot = (*_slots)[i];
    const auto idx = ++(*_idx)[i];
    return idx < slot.size() ? slot[idx] : pos_limits::eof();
  }

  PosAttr::value_t Seek(size_t i, PosAttr::value_t target) {
    const auto& slot = (*_slots)[i];
    auto& idx = (*_idx)[i];
    while (idx != slot.size() && slot[idx] < target) {
      ++idx;
    }
    return idx != slot.size() ? slot[idx] : pos_limits::eof();
  }

  PosAttr::value_t Value(size_t i) const { return (*_slots)[i][(*_idx)[i]]; }

  void Capture() {
    for (size_t i = 0; i != _slots->size(); ++i) {
      (*_snap)[i] = Value(i);
    }
  }

  score_t Boost() const noexcept { return kNoBoost; }

  const std::vector<PosAttr::value_t>& Snapshot() const noexcept {
    return *_snap;
  }

 private:
  const std::vector<std::vector<PosAttr::value_t>>* _slots;
  std::vector<size_t>* _idx;
  std::vector<PosAttr::value_t>* _snap;
};

struct MatchSpan {
  PosAttr::value_t leftmost;
  PosAttr::value_t rightmost;
  uint32_t leftmost_slot;
  uint32_t rightmost_slot;
};

inline MatchSpan SpanOf(const std::vector<PosAttr::value_t>& snap) noexcept {
  SDB_ASSERT(!snap.empty());
  MatchSpan span{snap[0], snap[0], 0, 0};
  for (size_t i = 1; i != snap.size(); ++i) {
    if (snap[i] < span.leftmost) {
      span.leftmost = snap[i];
      span.leftmost_slot = static_cast<uint32_t>(i);
    } else if (snap[i] > span.rightmost) {
      span.rightmost = snap[i];
      span.rightmost_slot = static_cast<uint32_t>(i);
    }
  }
  return span;
}

template<typename Positions, bool Offs, bool HasBoost>
class IterCursors {
 public:
  using Traits = TermPositionTraits<typename Positions::value_type>;

  static constexpr bool kSnapshots = Offs || HasBoost;

  IterCursors(Positions& pos, MatchScratch& scratch)
    : _pos{&pos}, _scratch{&scratch} {
    if constexpr (kSnapshots) {
      scratch.snap.resize(pos.size());
    }
    if constexpr (Offs) {
      scratch.snap_start.resize(pos.size());
      scratch.snap_end.resize(pos.size());
    }
    if constexpr (HasBoost) {
      scratch.snap_boost.resize(pos.size());
    }
  }

  PosAttr::value_t Start(size_t i) { return Advance(i); }

  PosAttr::value_t Advance(size_t i) {
    auto& it = *(*_pos)[i].first;
    return it.next() ? it.value() : pos_limits::eof();
  }

  PosAttr::value_t Seek(size_t i, PosAttr::value_t target) {
    return (*_pos)[i].first->seek(target);
  }

  PosAttr::value_t Value(size_t i) const { return (*_pos)[i].first->value(); }

  void Capture() {
    if constexpr (kSnapshots) {
      for (size_t i = 0; i != _pos->size(); ++i) {
        _scratch->snap[i] = Value(i);
        if constexpr (Offs) {
          const auto& offs = Traits::Offsets((*_pos)[i]);
          _scratch->snap_start[i] = offs.start;
          _scratch->snap_end[i] = offs.end;
        }
        if constexpr (HasBoost) {
          _scratch->snap_boost[i] = Traits::Boost((*_pos)[i]);
        }
      }
    }
  }

  score_t Boost() const noexcept {
    if constexpr (HasBoost) {
      return *absl::c_min_element(_scratch->snap_boost);
    } else {
      return kNoBoost;
    }
  }

  const std::vector<PosAttr::value_t>& Snapshot() const noexcept {
    return _scratch->snap;
  }

  uint32_t StartOffset(uint32_t slot) const noexcept
    requires(Offs)
  {
    return _scratch->snap_start[slot];
  }

  uint32_t EndOffset(uint32_t slot) const noexcept
    requires(Offs)
  {
    return _scratch->snap_end[slot];
  }

 private:
  Positions* _pos;
  MatchScratch* _scratch;
};

#ifdef SDB_DEV
inline std::atomic<bool> gStaticArityDisabled{false};

inline bool StaticArityDisabled() noexcept {
  return gStaticArityDisabled.load(std::memory_order_relaxed);
}
#endif

struct MatchRecord {
  PosAttr::value_t leftmost;
  PosAttr::value_t rightmost;
  uint32_t leftmost_slot;
  uint32_t rightmost_slot;
  uint32_t start_offset;
  uint32_t end_offset;
};

inline void SortMatches(std::vector<MatchRecord>& matches) {
  absl::c_sort(matches, [](const MatchRecord& a,
                           const MatchRecord& b) noexcept {
    return std::tie(a.leftmost, a.rightmost, a.leftmost_slot,
                    a.rightmost_slot) <
           std::tie(b.leftmost, b.rightmost, b.leftmost_slot, b.rightmost_slot);
  });
}

inline std::span<const int64_t> BuildOffsets(
  const std::vector<PosAttr::value_t>& expected_steps, MatchScratch& scratch) {
  auto& offsets = scratch.offsets;
  offsets.resize(expected_steps.size() + 1);
  offsets[0] = 0;
  for (size_t i = 0; i != expected_steps.size(); ++i) {
    offsets[i + 1] = offsets[i] + expected_steps[i];
  }
  return offsets;
}

template<size_t N = 0>
MatchResult Run(const std::vector<std::vector<PosAttr::value_t>>& slot_pos,
                PosAttr::value_t slop,
                const std::vector<PosAttr::value_t>& expected_steps,
                MatchScratch& scratch, bool early_exit,
                const std::vector<uint32_t>& groups = {},
                std::vector<MatchSpan>* out = nullptr) {
  if (out) {
    out->clear();
  }
  const size_t n = slot_pos.size();
  if (n < 2) {
    return {};
  }
  SDB_ASSERT(expected_steps.size() == n - 1);
  SDB_ASSERT(groups.empty() || groups.size() == n);

  const auto offsets = BuildOffsets(expected_steps, scratch);
  SpanCursors cur{slot_pos, scratch};
  std::vector<GroupPair> pairs;
  BuildGroupPairs(groups, n, pairs);
  auto res = Sweep<N>(cur, offsets, slop, pairs, scratch, early_exit, [&] {
    if (out) {
      out->push_back(SpanOf(cur.Snapshot()));
    }
  });

  if (early_exit) {
    res.freq = res.any ? 1 : 0;
  }
  if (out) {
    absl::c_sort(*out, [](const MatchSpan& a, const MatchSpan& b) noexcept {
      return std::tie(a.leftmost, a.rightmost, a.leftmost_slot,
                      a.rightmost_slot) < std::tie(b.leftmost, b.rightmost,
                                                   b.leftmost_slot,
                                                   b.rightmost_slot);
    });
  }
  return res;
}

}  // namespace detail::slop

template<typename PositionsT>
inline std::vector<PosAttr::value_t> BuildExpectedSteps(
  const PositionsT& positions) {
  std::vector<PosAttr::value_t> steps;
  if (positions.size() < 2) {
    return steps;
  }
  steps.reserve(positions.size() - 1);
  for (size_t i = 1; i < positions.size(); ++i) {
    SDB_ASSERT(positions[i].lead_offset >= positions[i - 1].lead_offset);
    steps.push_back(positions[i].lead_offset - positions[i - 1].lead_offset);
  }
  return steps;
}

template<typename TermPositionT, bool Offs, bool HasFreq, size_t N = 0,
         bool HasBoost = false>
class PhraseSlopMatcher {
 public:
  using TermPosition = TermPositionT;
  using Traits = TermPositionTraits<TermPosition>;
  using Positions = containers::Fixed<TermPosition, N>;

  static constexpr bool kHasScale = HasFreq;
  static constexpr bool kHasFreq = HasFreq;
  static constexpr bool kOffsets = Offs;

  static_assert(HasFreq || !HasBoost);

  PhraseSlopMatcher(size_t size, PosAttr::value_t max_slop,
                    std::vector<PosAttr::value_t>&& expected_steps)
    : _pos{size},
      _max_slop{max_slop},
      _expected_steps{expected_steps.size(),
                      [&](PosAttr::value_t& slot, size_t i) noexcept {
                        slot = expected_steps[i];
                      }},
      _offsets{size},
      _term_groups{size} {}

  TermPosition& Position(size_t i) noexcept {
    SDB_ASSERT(i < _pos.size());
    return _pos[i];
  }

  void Finish() {
    SDB_ASSERT(_pos.size() >= 2);
    SDB_ASSERT(_max_slop > 0);
    SDB_ASSERT(_expected_steps.size() == _pos.size() - 1);

    const size_t n = _pos.size();
    _offsets[0] = 0;
    for (size_t i = 1; i != n; ++i) {
      _offsets[i] = _offsets[i - 1] + _expected_steps[i - 1];
    }
    for (size_t i = 0; i != n; ++i) {
      _term_groups[i] = _pos[i].second.term_group;
    }
    detail::slop::BuildGroupPairs({_term_groups.data(), _term_groups.size()}, n,
                                  _pairs);
  }

  IRS_FORCE_INLINE bool Match() {
    _phrase_freq = 0;
    if constexpr (Offs) {
      _start_offset = 0;
      _end_offset = 0;
      _matches.clear();
      _match_idx = 0;
    }

#ifdef SDB_DEV
    if constexpr (N != 0) {
      if (detail::slop::StaticArityDisabled()) [[unlikely]] {
        return Run<0>();
      }
    }
#endif
    return Run<N>();
  }

  uint32_t GetFreq() const noexcept { return _phrase_freq; }

  score_t GetScale() const noexcept
    requires(kHasScale)
  {
    if constexpr (HasBoost) {
      return _phrase_scale * _phrase_boost;
    } else {
      return _phrase_scale;
    }
  }

  std::pair<uint32_t, uint32_t> Offsets() const noexcept
    requires(Offs)
  {
    return {_start_offset, _end_offset};
  }

  bool NextAlignment()
    requires(Offs)
  {
    return NextPosition() != 0;
  }

 private:
  using Cursors = detail::slop::IterCursors<Positions, Offs, HasBoost>;

  uint32_t NextPosition() {
    if constexpr (!Offs || !HasFreq) {
      return 0;
    } else {
      if (_match_idx >= _matches.size()) {
        return 0;
      }
      _start_offset = _matches[_match_idx].start_offset;
      _end_offset = _matches[_match_idx].end_offset;
      ++_match_idx;
      return 1;
    }
  }

  template<size_t Arity>
  bool Run() {
    Cursors cur{_pos, _match_scratch};
    const auto res = detail::slop::Sweep<Arity>(
      cur, {_offsets.data(), _offsets.size()}, _max_slop, _pairs,
      _match_scratch, !HasFreq, [&] {
        if constexpr (Offs && HasFreq) {
          const auto span = detail::slop::SpanOf(cur.Snapshot());
          _matches.push_back({span.leftmost, span.rightmost, span.leftmost_slot,
                              span.rightmost_slot,
                              cur.StartOffset(span.leftmost_slot),
                              cur.EndOffset(span.rightmost_slot)});
        }
      });
    if (!res.any) {
      return false;
    }
    if constexpr (Offs && HasFreq) {
      detail::slop::SortMatches(_matches);
    }
    Take(res);
    return true;
  }

  void Take(const detail::slop::MatchResult& res) {
    if constexpr (HasFreq) {
      _phrase_freq = static_cast<uint32_t>(res.freq);
      _phrase_scale =
        static_cast<score_t>(res.weight / static_cast<double>(res.freq));
      if constexpr (HasBoost) {
        _phrase_boost = res.boost;
      }
      if constexpr (Offs) {
        SDB_ASSERT(static_cast<uint32_t>(_matches.size()) == _phrase_freq);
        _start_offset = _matches[0].start_offset;
        _end_offset = _matches[0].end_offset;
        _match_idx = 1;
      }
    } else {
      _phrase_freq = 1;
      _phrase_scale = kNoBoost;
    }
  }

  Positions _pos;
  PosAttr::value_t _max_slop;
  containers::Fixed<PosAttr::value_t> _expected_steps;
  containers::Fixed<int64_t, N> _offsets;
  containers::Fixed<uint32_t, N> _term_groups;
  std::vector<detail::slop::GroupPair> _pairs;
  detail::slop::MatchScratch _match_scratch;
  std::vector<detail::slop::MatchRecord> _matches;
  size_t _match_idx = 0;

  uint32_t _phrase_freq = 0;
  score_t _phrase_scale = kNoBoost;
  [[no_unique_address]] utils::Need<HasBoost, score_t> _phrase_boost{};
  uint32_t _start_offset{0};
  uint32_t _end_offset{0};
};

template<bool Offs, bool HasFreq, size_t N = 0>
using FixedPhraseSlopMatcher =
  PhraseSlopMatcher<FixedTermPosition<Offs>, Offs, HasFreq, N>;

}  // namespace irs
