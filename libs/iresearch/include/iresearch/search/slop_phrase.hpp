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
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <tuple>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/iterator_pos.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/phrase_iterator.hpp"
#include "iresearch/search/score_function.hpp"

namespace irs {
namespace detail::slop {

constexpr PosAttr::value_t StepCost(int64_t delta,
                                    PosAttr::value_t expected) noexcept {
  if (expected == 1) {
    if (delta >= 2) {
      return static_cast<PosAttr::value_t>(delta - 1);
    }
    if (delta <= -1) {
      return static_cast<PosAttr::value_t>(-delta + 1);
    }
    if (delta == 0) {
      return 1;
    }
    return 0;
  }
  const int64_t exp = static_cast<int64_t>(expected);
  if (delta == exp) {
    return 0;
  }
  if (delta > exp) {
    return static_cast<PosAttr::value_t>(delta - exp);
  }
  if (delta >= 0) {
    return static_cast<PosAttr::value_t>(exp - delta);
  }
  return static_cast<PosAttr::value_t>(exp - delta + 1);
}

struct MatchResult {
  uint64_t freq = 0;
  PosAttr::value_t best_distance = 0;
  bool any = false;
};

struct MatchScratch {
  std::vector<PosAttr::value_t> chain;
  std::vector<uint32_t> order;
};

#ifdef SDB_DEV
inline std::atomic<bool> gPairJoinDisabled{false};
inline std::atomic<bool> gOffsBulkGatherDisabled{false};

inline bool PairJoinDisabled() noexcept {
  return gPairJoinDisabled.load(std::memory_order_relaxed);
}

inline bool OffsBulkGatherDisabled() noexcept {
  return gOffsBulkGatherDisabled.load(std::memory_order_relaxed);
}
#else
constexpr bool PairJoinDisabled() noexcept { return false; }

constexpr bool OffsBulkGatherDisabled() noexcept { return false; }
#endif

struct PosOffset {
  uint32_t start;
  uint32_t end;
};

struct PairMatch {
  PosAttr::value_t leftmost;
  PosAttr::value_t rightmost;
  uint32_t leftmost_slot;
  uint32_t rightmost_slot;
  uint32_t start_offset;
  uint32_t end_offset;
};

struct PairScratch {
  std::vector<PosAttr::value_t> buf_pos;
  std::vector<PosOffset> buf_offs;
};

class UninitU32Buf {
 public:
  size_t Size() const noexcept { return _size; }

  void Clear() noexcept { _size = 0; }

  void ResizeUninit(size_t n) {
    if (n > _cap) {
      _data = std::make_unique_for_overwrite<uint32_t[]>(n);
      _cap = n;
    }
    _size = n;
  }

  void PushBack(uint32_t v) {
    if (_size == _cap) [[unlikely]] {
      Grow();
    }
    _data[_size++] = v;
  }

  uint32_t* Data() noexcept { return _data.get(); }
  const uint32_t* Data() const noexcept { return _data.get(); }

  uint32_t operator[](size_t i) const noexcept {
    SDB_ASSERT(i < _size);
    return _data[i];
  }

 private:
  void Grow() {
    const size_t cap = _cap == 0 ? 16 : _cap * 2;
    auto data = std::make_unique_for_overwrite<uint32_t[]>(cap);
    if (_size != 0) {
      std::memcpy(data.get(), _data.get(), _size * sizeof(uint32_t));
    }
    _data = std::move(data);
    _cap = cap;
  }

  std::unique_ptr<uint32_t[]> _data;
  size_t _size = 0;
  size_t _cap = 0;
};

template<bool Offs, bool HasFreq, typename AnchorIt, typename PartnerIt>
MatchResult JoinPair(AnchorIt& anchor, PartnerIt& partner,
                     const OffsAttr* anchor_offs, const OffsAttr* partner_offs,
                     bool anchor_is_slot0, PosAttr::value_t slop,
                     PosAttr::value_t expected, bool enforce_uniqueness,
                     PairScratch& scratch, std::vector<PairMatch>* out) {
  if constexpr (!HasFreq) {
    SDB_ASSERT(out == nullptr);
  }
  MatchResult res{};
  if (out) {
    out->clear();
  }

  constexpr PosAttr::value_t kMax =
    std::numeric_limits<PosAttr::value_t>::max();
  const PosAttr::value_t w = (expected > kMax - slop)
                               ? kMax
                               : static_cast<PosAttr::value_t>(slop + expected);

  auto& buf = scratch.buf_pos;
  buf.clear();
  if constexpr (Offs) {
    scratch.buf_offs.clear();
  }
  size_t head = 0;

  bool partner_eof = false;
  bool partner_primed = false;
  PosAttr::value_t pv = 0;

  bool anchor_live = anchor.next();
  while (anchor_live) {
    const PosAttr::value_t pa = anchor.value();
    if (pos_limits::eof(pa)) {
      break;
    }
    const PosAttr::value_t lo = (pa > w) ? (pa - w) : pos_limits::min();
    const PosAttr::value_t hi = (pa > kMax - w) ? kMax : (pa + w);

    while (head < buf.size() && buf[head] < lo) {
      ++head;
    }
    if (head == buf.size()) {
      if (head != 0) {
        buf.clear();
        if constexpr (Offs) {
          scratch.buf_offs.clear();
        }
        head = 0;
      }
    } else if (head > 32 && head * 2 >= buf.size()) {
      buf.erase(buf.begin(), buf.begin() + static_cast<ptrdiff_t>(head));
      if constexpr (Offs) {
        scratch.buf_offs.erase(
          scratch.buf_offs.begin(),
          scratch.buf_offs.begin() + static_cast<ptrdiff_t>(head));
      }
      head = 0;
    }

    if (!partner_eof) {
      if (!partner_primed || pv < lo) {
        pv = partner.seek(lo);
        partner_primed = true;
        if (!pos_limits::valid(pv) || pos_limits::eof(pv)) {
          partner_eof = true;
        }
      }
      while (!partner_eof && pv <= hi) {
        buf.push_back(pv);
        if constexpr (Offs) {
          if (partner_offs) {
            scratch.buf_offs.push_back(
              {partner_offs->start, partner_offs->end});
          } else {
            scratch.buf_offs.push_back({0, 0});
          }
        }
        if (!partner.next()) {
          partner_eof = true;
          break;
        }
        pv = partner.value();
        if (pos_limits::eof(pv)) {
          partner_eof = true;
        }
      }
    }

    if (head == buf.size()) {
      if (partner_eof) {
        break;
      }
      const PosAttr::value_t target = (pv > w) ? (pv - w) : pos_limits::min();
      if (target > pa) {
        const PosAttr::value_t av = anchor.seek(target);
        anchor_live = pos_limits::valid(av) && !pos_limits::eof(av);
      } else {
        anchor_live = anchor.next();
      }
      continue;
    }

    PosOffset aoffs{0, 0};
    if constexpr (Offs) {
      if (anchor_offs) {
        aoffs = {anchor_offs->start, anchor_offs->end};
      }
    }

    for (size_t i = head; i < buf.size(); ++i) {
      const PosAttr::value_t v = buf[i];
      if (enforce_uniqueness && v == pa) {
        continue;
      }
      const int64_t delta =
        anchor_is_slot0 ? static_cast<int64_t>(v) - static_cast<int64_t>(pa)
                        : static_cast<int64_t>(pa) - static_cast<int64_t>(v);
      const PosAttr::value_t step = StepCost(delta, expected);
      if (step > slop) {
        continue;
      }
      ++res.freq;
      if (!res.any || step < res.best_distance) {
        res.best_distance = step;
      }
      res.any = true;
      if constexpr (!HasFreq) {
        return res;
      }
      if (out) {
        const PosAttr::value_t p0 = anchor_is_slot0 ? pa : v;
        const PosAttr::value_t p1 = anchor_is_slot0 ? v : pa;
        const uint32_t ls = (p1 < p0) ? 1u : 0u;
        const uint32_t rs = (p1 > p0) ? 1u : 0u;
        PosOffset o0;
        PosOffset o1;
        if constexpr (Offs) {
          o0 = anchor_is_slot0 ? aoffs : scratch.buf_offs[i];
          o1 = anchor_is_slot0 ? scratch.buf_offs[i] : aoffs;
        } else {
          o0 = {0, 0};
          o1 = {0, 0};
        }
        out->push_back({p0 < p1 ? p0 : p1, p0 < p1 ? p1 : p0, ls, rs,
                        (ls == 0 ? o0 : o1).start, (rs == 0 ? o0 : o1).end});
      }
    }
    anchor_live = anchor.next();
  }

  if (out) {
    absl::c_sort(*out, [](const PairMatch& a, const PairMatch& b) noexcept {
      return std::tie(a.leftmost, a.rightmost, a.leftmost_slot,
                      a.rightmost_slot) < std::tie(b.leftmost, b.rightmost,
                                                   b.leftmost_slot,
                                                   b.rightmost_slot);
    });
  }
  return res;
}

struct EnumeratedMatch {
  PosAttr::value_t leftmost;
  PosAttr::value_t rightmost;
  uint32_t leftmost_slot;
  uint32_t rightmost_slot;
};

inline bool EnforceUniqueness(const std::vector<uint32_t>& groups) noexcept {
  if (groups.empty()) {
    return true;
  }
  for (size_t a = 0; a < groups.size(); ++a) {
    for (size_t b = a + 1; b < groups.size(); ++b) {
      if (groups[a] == groups[b]) {
        return true;
      }
    }
  }
  return false;
}

inline void CountFromAnchor(
  const std::vector<std::vector<PosAttr::value_t>>& slots,
  PosAttr::value_t slop, const std::vector<PosAttr::value_t>& expected_steps,
  std::vector<PosAttr::value_t>& chain, const std::vector<uint32_t>& order,
  uint32_t anchor, size_t d, PosAttr::value_t cost_so_far, MatchResult& res,
  bool early_exit, const std::vector<uint32_t>& groups, bool enforce_uniqueness,
  std::vector<EnumeratedMatch>* out) {
  const size_t n = slots.size();
  if (d == n) {
    ++res.freq;
    if (!res.any || cost_so_far < res.best_distance) {
      res.best_distance = cost_so_far;
    }
    res.any = true;
    if (out) {
      PosAttr::value_t lp = chain[0];
      PosAttr::value_t rp = chain[0];
      uint32_t ls = 0;
      uint32_t rs = 0;
      for (size_t k = 1; k < n; ++k) {
        if (chain[k] < lp) {
          lp = chain[k];
          ls = static_cast<uint32_t>(k);
        }
        if (chain[k] > rp) {
          rp = chain[k];
          rs = static_cast<uint32_t>(k);
        }
      }
      out->push_back({lp, rp, ls, rs});
    }
    return;
  }

  constexpr PosAttr::value_t kMax =
    std::numeric_limits<PosAttr::value_t>::max();

  const uint32_t slot = order[d];
  const bool forward = slot > anchor;
  const uint32_t partner = forward ? slot - 1 : slot + 1;
  const PosAttr::value_t expected =
    forward ? expected_steps[slot - 1] : expected_steps[slot];
  const PosAttr::value_t pv = chain[partner];

  const PosAttr::value_t budget = slop - cost_so_far;
  const PosAttr::value_t span = budget + 1;

  PosAttr::value_t lo;
  PosAttr::value_t hi;
  if (forward) {
    const PosAttr::value_t center =
      (pv > kMax - expected) ? kMax : pv + expected;
    lo = (center > span) ? static_cast<PosAttr::value_t>(center - span) : 0;
    hi = (center > kMax - span) ? kMax
                                : static_cast<PosAttr::value_t>(center + span);
  } else {
    const PosAttr::value_t center =
      (pv > expected) ? static_cast<PosAttr::value_t>(pv - expected) : 0;
    lo = (center > span) ? static_cast<PosAttr::value_t>(center - span) : 0;
    hi = (center > kMax - span) ? kMax
                                : static_cast<PosAttr::value_t>(center + span);
  }

  const auto& sp = slots[slot];
  auto begin = std::lower_bound(sp.begin(), sp.end(), lo);
  auto end = std::upper_bound(sp.begin(), sp.end(), hi);
  for (auto it = begin; it != end; ++it) {
    const PosAttr::value_t p = *it;
    if (enforce_uniqueness) {
      bool dup = false;
      for (size_t k = 0; k < d; ++k) {
        if (chain[order[k]] == p &&
            (groups.empty() || groups[order[k]] == groups[slot])) {
          dup = true;
          break;
        }
      }
      if (dup) {
        continue;
      }
    }
    const int64_t delta =
      forward ? static_cast<int64_t>(p) - static_cast<int64_t>(pv)
              : static_cast<int64_t>(pv) - static_cast<int64_t>(p);
    const PosAttr::value_t step = StepCost(delta, expected);
    if (cost_so_far + step > slop) {
      continue;
    }
    chain[slot] = p;
    CountFromAnchor(slots, slop, expected_steps, chain, order, anchor, d + 1,
                    static_cast<PosAttr::value_t>(cost_so_far + step), res,
                    early_exit, groups, enforce_uniqueness, out);
    if (early_exit && res.any) {
      return;
    }
  }
}

inline MatchResult Run(
  const std::vector<std::vector<PosAttr::value_t>>& slot_pos,
  PosAttr::value_t slop, const std::vector<PosAttr::value_t>& expected_steps,
  MatchScratch& scratch, bool early_exit,
  const std::vector<uint32_t>& groups = {},
  std::vector<EnumeratedMatch>* out = nullptr) {
  SDB_ASSERT(!(early_exit && out));
  if (out) {
    out->clear();
  }
  MatchResult res{};
  const size_t n = slot_pos.size();
  if (n < 2) {
    return res;
  }
  for (const auto& sp : slot_pos) {
    if (sp.empty()) {
      return res;
    }
  }
  SDB_ASSERT(expected_steps.size() == n - 1);
  SDB_ASSERT(groups.empty() || groups.size() == n);

  const bool enforce_uniqueness = EnforceUniqueness(groups);

  uint32_t anchor = 0;
  for (uint32_t i = 1; i < n; ++i) {
    if (slot_pos[i].size() < slot_pos[anchor].size()) {
      anchor = i;
    }
  }

  auto& order = scratch.order;
  order.resize(n);
  size_t idx = 0;
  order[idx++] = anchor;
  for (uint32_t s = anchor + 1; s < n; ++s) {
    order[idx++] = s;
  }
  for (uint32_t s = anchor; s-- > 0;) {
    order[idx++] = s;
  }
  SDB_ASSERT(idx == n);

  auto& chain = scratch.chain;
  chain.resize(n);
  for (PosAttr::value_t pa : slot_pos[anchor]) {
    chain[anchor] = pa;
    CountFromAnchor(slot_pos, slop, expected_steps, chain, order, anchor, 1, 0,
                    res, early_exit, groups, enforce_uniqueness, out);
    if (early_exit && res.any) {
      return res;
    }
  }

  if (out) {
    absl::c_sort(
      *out, [](const EnumeratedMatch& a, const EnumeratedMatch& b) noexcept {
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

template<typename TermPositionT, bool Offs, bool HasFreq, size_t N = 0>
class SlopPhrase {
 public:
  using TermPosition = TermPositionT;
  using Traits = TermPositionTraits<TermPosition>;
  using Positions = search::RunOf<TermPosition, N>;

  static constexpr bool kHasBoost = false;
  static constexpr bool kHasFreq = HasFreq;
  static constexpr bool kOffsets = Offs;

  SlopPhrase(size_t size, PosAttr::value_t max_slop,
             std::vector<PosAttr::value_t>&& expected_steps)
    : _pos{size},
      _max_slop{max_slop},
      _expected_steps{std::move(expected_steps)} {}

  TermPosition& Position(size_t i) noexcept {
    SDB_ASSERT(i < _pos.size());
    return _pos[i];
  }

  void Finish() {
    SDB_ASSERT(_pos.size() >= 2);
    SDB_ASSERT(_max_slop > 0);
    SDB_ASSERT(_expected_steps.size() == _pos.size() - 1);
  }

  IRS_FORCE_INLINE bool Match() {
    _phrase_freq = 0;
    if constexpr (Offs) {
      _start_offset = 0;
      _end_offset = 0;
      _matches.clear();
      _match_idx = 0;
    }

    const size_t n = _pos.size();
    _slot_pos.resize(n);

    if (_term_groups.size() != n) {
      _term_groups.clear();
      _term_groups.reserve(n);
      for (const auto& p : _pos) {
        _term_groups.push_back(p.second.term_group);
      }
    }

    if (n == 2 && !detail::slop::PairJoinDisabled()) {
      return MatchPair();
    }

    if constexpr (Offs) {
      _slot_offs_start.resize(n);
      _slot_offs_end.resize(n);
    }

    const auto gather_all = [&](size_t i) {
      auto& it = *_pos[i].first;
      auto& positions = _slot_pos[i];
      if constexpr (!Offs) {
        positions.resize(it.DocFreq());
        const auto count = it.ReadAll(positions.data());
        SDB_ASSERT(count == positions.size());
      } else {
        auto& starts = _slot_offs_start[i];
        auto& ends = _slot_offs_end[i];
        if (detail::slop::OffsBulkGatherDisabled()) [[unlikely]] {
          positions.clear();
          starts.Clear();
          ends.Clear();
          while (it.next()) {
            positions.push_back(it.value());
            const auto& offs = Traits::Offsets(_pos[i]);
            starts.PushBack(offs.start);
            ends.PushBack(offs.end);
          }
        } else {
          positions.resize(it.DocFreq());
          starts.ResizeUninit(positions.size());
          ends.ResizeUninit(positions.size());
          const auto count =
            it.ReadAll(positions.data(), starts.Data(), ends.Data());
          SDB_ASSERT(count == positions.size());
        }
      }
    };

    for (size_t i = 0; i < n; ++i) {
      gather_all(i);
      if (_slot_pos[i].empty()) {
        return false;
      }
    }

    std::vector<detail::slop::EnumeratedMatch>* collect = nullptr;
    if constexpr (Offs && HasFreq) {
      collect = &_enumerated;
    }
    auto res =
      detail::slop::Run(_slot_pos, _max_slop, _expected_steps, _match_scratch,
                        !HasFreq, _term_groups, collect);
    if (!res.any) {
      return false;
    }

    if constexpr (HasFreq) {
      _phrase_freq = static_cast<uint32_t>(res.freq);
      if constexpr (Offs) {
        BuildMatches();
      }
    } else {
      _phrase_freq = 1;
    }
    return true;
  }

  uint32_t GetFreq() const noexcept { return _phrase_freq; }

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
  struct OffsetPair {
    uint32_t start;
    uint32_t end;
  };

  uint32_t NextPosition() {
    if constexpr (!Offs || !HasFreq) {
      return 0;
    } else {
      if (_match_idx >= _matches.size()) {
        return 0;
      }
      _start_offset = _matches[_match_idx].start;
      _end_offset = _matches[_match_idx].end;
      ++_match_idx;
      return 1;
    }
  }

  bool MatchPair() {
    const bool anchor_is_slot0 =
      _pos[0].first->DocFreq() <= _pos[1].first->DocFreq();
    const size_t ai = anchor_is_slot0 ? 0 : 1;
    const size_t pi = anchor_is_slot0 ? 1 : 0;
    auto& anchor = *_pos[ai].first;
    auto& partner = *_pos[pi].first;

    const OffsAttr* anchor_offs = nullptr;
    const OffsAttr* partner_offs = nullptr;
    if constexpr (Offs) {
      anchor_offs = &Traits::Offsets(_pos[ai]);
      partner_offs = &Traits::Offsets(_pos[pi]);
    }

    std::vector<detail::slop::PairMatch>* collect = nullptr;
    if constexpr (Offs && HasFreq) {
      collect = &_pair_matches;
    }

    const auto res = detail::slop::JoinPair<Offs, HasFreq>(
      anchor, partner, anchor_offs, partner_offs, anchor_is_slot0, _max_slop,
      _expected_steps[0], detail::slop::EnforceUniqueness(_term_groups),
      _pair_scratch, collect);
    if (!res.any) {
      return false;
    }

    if constexpr (HasFreq) {
      _phrase_freq = static_cast<uint32_t>(res.freq);
      if constexpr (Offs) {
        SDB_ASSERT(static_cast<uint32_t>(_pair_matches.size()) == _phrase_freq);
        _matches.reserve(_pair_matches.size());
        for (const auto& m : _pair_matches) {
          _matches.push_back({m.start_offset, m.end_offset});
        }
        if (!_matches.empty()) {
          _start_offset = _matches[0].start;
          _end_offset = _matches[0].end;
          _match_idx = 1;
        }
      }
    } else {
      _phrase_freq = 1;
    }
    return true;
  }

  void BuildMatches() {
    if constexpr (!Offs || !HasFreq) {
      return;
    }
    SDB_ASSERT(static_cast<uint32_t>(_enumerated.size()) == _phrase_freq);
    _matches.clear();
    _matches.reserve(_enumerated.size());
    auto find_index = [&](size_t slot, PosAttr::value_t p) -> size_t {
      const auto& sp = _slot_pos[slot];
      auto it = std::lower_bound(sp.begin(), sp.end(), p);
      SDB_ASSERT(it != sp.end() && *it == p);
      return static_cast<size_t>(it - sp.begin());
    };
    for (const auto& m : _enumerated) {
      const size_t li = find_index(m.leftmost_slot, m.leftmost);
      const size_t ri = find_index(m.rightmost_slot, m.rightmost);
      _matches.push_back({_slot_offs_start[m.leftmost_slot][li],
                          _slot_offs_end[m.rightmost_slot][ri]});
    }
    if (!_matches.empty()) {
      _start_offset = _matches[0].start;
      _end_offset = _matches[0].end;
      _match_idx = 1;
    }
  }

  Positions _pos;
  PosAttr::value_t _max_slop;
  std::vector<PosAttr::value_t> _expected_steps;
  std::vector<std::vector<PosAttr::value_t>> _slot_pos;
  std::vector<detail::slop::UninitU32Buf> _slot_offs_start;
  std::vector<detail::slop::UninitU32Buf> _slot_offs_end;
  detail::slop::MatchScratch _match_scratch;
  std::vector<detail::slop::EnumeratedMatch> _enumerated;
  detail::slop::PairScratch _pair_scratch;
  std::vector<detail::slop::PairMatch> _pair_matches;
  std::vector<OffsetPair> _matches;
  size_t _match_idx = 0;
  uint32_t _phrase_freq = 0;
  uint32_t _start_offset{0};
  uint32_t _end_offset{0};

  std::vector<uint32_t> _term_groups;
};

template<bool Offs, bool HasFreq, size_t N = 0>
using SlopPhraseFrequency =
  SlopPhrase<FixedTermPosition<Offs>, Offs, HasFreq, N>;

}  // namespace irs
