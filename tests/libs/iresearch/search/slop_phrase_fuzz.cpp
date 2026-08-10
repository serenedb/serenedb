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

// Property-based oracle for the sloppy-phrase matcher (irs::detail::slop),
// on in-memory position vectors. Each random case is checked against a
// brute-force reference (full Cartesian product, no windows or pruning):
// Run's freq/best_distance and early-exit, the groups-aware collector,
// JoinPair for n == 2, and the variadic n == 2 path - MergedPosStream
// duplicate-collapsing merge plus JoinPair over the merged streams. New
// matcher paths go here before any timing.

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <tuple>
#include <vector>

#include "filter_test_case_base.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/phrase_iterator.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/slop_phrase.hpp"
#include "tests_shared.hpp"

namespace {

namespace spm = irs::detail::slop;
using value_t = irs::PosAttr::value_t;

struct Case {
  // per-slot sorted positions, may repeat (delta-0 tokens)
  std::vector<std::vector<value_t>> slots;
  // size == slots.size() - 1
  std::vector<value_t> expected_steps;
  // empty, or size == slots.size()
  std::vector<uint32_t> groups;
  value_t slop{0};
};

// Per-group duplicate rule (Lucene, ES-verified): a shared position is
// illegal only between slots of the same group; empty groups mean
// globally strict. Reimplemented (not calling into spm::) so it can't
// inherit a bug.
bool GroupDistinct(const std::vector<value_t>& chain,
                   const std::vector<uint32_t>& groups) {
  for (size_t a = 0; a < chain.size(); ++a) {
    for (size_t b = a + 1; b < chain.size(); ++b) {
      if (chain[a] == chain[b] && (groups.empty() || groups[a] == groups[b])) {
        return false;
      }
    }
  }
  return true;
}

// Independently re-derived step cost, deliberately not calling
// spm::StepCost so a cost-model bug cannot cancel out between the
// matcher and the oracle: plain |delta - expected| distance, plus one
// extra move for a reversal (delta < 0) - except at expected == 1,
// where |delta - 1| already absorbs it. StepCostSpec pins the two
// formulations against each other, including expected == 0.
uint64_t BruteStepCost(int64_t delta, value_t expected) {
  const int64_t e = static_cast<int64_t>(expected);
  const int64_t dist = delta > e ? delta - e : e - delta;
  const int64_t reversal = (expected != 1 && delta < 0) ? 1 : 0;
  return static_cast<uint64_t>(dist + reversal);
}

// Counts valid tuples; cost is computed only at a full tuple, so none of
// Run's window/pruning leaks in.
spm::MatchResult BruteRun(const Case& c) {
  spm::MatchResult res{};
  const size_t n = c.slots.size();
  if (n < 2) {
    return res;
  }
  for (const auto& sp : c.slots) {
    if (sp.empty()) {
      return res;
    }
  }

  std::vector<value_t> chain(n);
  auto rec = [&](auto&& self, size_t i) -> void {
    if (i == n) {
      if (!GroupDistinct(chain, c.groups)) {
        return;
      }
      // 64-bit so it never wraps; counts iff total cost <= slop.
      uint64_t cost = 0;
      for (size_t k = 1; k < n; ++k) {
        const int64_t delta =
          static_cast<int64_t>(chain[k]) - static_cast<int64_t>(chain[k - 1]);
        cost += BruteStepCost(delta, c.expected_steps[k - 1]);
      }
      if (cost > c.slop) {
        return;
      }
      const value_t cost32 = static_cast<value_t>(cost);
      ++res.freq;
      if (!res.any || cost32 < res.best_distance) {
        res.best_distance = cost32;
      }
      res.any = true;
      return;
    }
    for (const value_t p : c.slots[i]) {
      chain[i] = p;
      self(self, i + 1);
    }
  };
  rec(rec, 0);
  return res;
}

std::string Show(const Case& c) {
  std::string s = "slop=" + std::to_string(c.slop) + " expected=[";
  for (size_t i = 0; i < c.expected_steps.size(); ++i) {
    s += std::to_string(c.expected_steps[i]);
    if (i + 1 < c.expected_steps.size()) {
      s += ",";
    }
  }
  s += "] groups=";
  if (c.groups.empty()) {
    s += "(none)";
  } else {
    s += "[";
    for (size_t i = 0; i < c.groups.size(); ++i) {
      s += std::to_string(c.groups[i]);
      if (i + 1 < c.groups.size()) {
        s += ",";
      }
    }
    s += "]";
  }
  s += " slots=";
  for (const auto& sp : c.slots) {
    s += "{";
    for (size_t i = 0; i < sp.size(); ++i) {
      s += std::to_string(sp[i]);
      if (i + 1 < sp.size()) {
        s += ",";
      }
    }
    s += "}";
  }
  return s;
}

std::string Show(const spm::MatchResult& r) {
  return "{any=" + std::string(r.any ? "1" : "0") +
         " freq=" + std::to_string(r.freq) +
         " best=" + std::to_string(r.best_distance) + "}";
}

// PosAttr contract JoinPair relies on: value() invalid before first next();
// next() past the end -> false, value() = eof; seek(t) -> first pos >= t or
// eof; offsets valid only while positioned; reset() rewinds to before the
// first position (MergedPosStream::Add relies on it).
struct MockPos {
  std::vector<value_t> pos;
  std::vector<irs::OffsAttr> offs;  // parallel to pos
  size_t i{static_cast<size_t>(-1)};
  value_t val{irs::pos_limits::invalid()};
  irs::OffsAttr attr;

  void reset() {
    i = static_cast<size_t>(-1);
    val = irs::pos_limits::invalid();
  }

  bool next() {
    const size_t n = (i == static_cast<size_t>(-1)) ? 0 : i + 1;
    if (n >= pos.size()) {
      i = pos.size();
      val = irs::pos_limits::eof();
      return false;
    }
    i = n;
    val = pos[i];
    attr = offs[i];
    return true;
  }
  value_t value() const { return val; }
  value_t seek(value_t t) {
    while (val < t) {
      if (!next()) {
        break;
      }
    }
    return val;
  }
};

// Deterministic offset per (slot, position), so resolved offsets can be
// recomputed at verification time.
irs::OffsAttr OffsFor(uint32_t slot, value_t p) {
  irs::OffsAttr o;
  o.start = p * 8 + slot;
  o.end = o.start + 3;
  return o;
}

MockPos MakeMock(const Case& c, uint32_t slot) {
  MockPos m;
  m.pos = c.slots[slot];
  m.offs.reserve(m.pos.size());
  for (const value_t p : m.pos) {
    m.offs.push_back(OffsFor(slot, p));
  }
  return m;
}

// JoinPair must match the brute reference and Run's collector on every
// n == 2 case, in every instantiation and for either anchor choice.
bool CheckJoin(const Case& c, const spm::MatchResult& ref,
               const std::vector<spm::EnumeratedMatch>& run_out) {
  SDB_ASSERT(c.slots.size() == 2);
  const bool enforce = spm::EnforceUniqueness(c.groups);
  const value_t expected = c.expected_steps[0];
  bool ok = true;

  for (const bool anchor_is_slot0 : {true, false}) {
    const uint32_t a = anchor_is_slot0 ? 0u : 1u;
    const uint32_t p = a ^ 1u;

    // full count + collector (Offs && HasFreq)
    spm::PairScratch scratch;
    std::vector<spm::PairMatch> out;
    MockPos anchor = MakeMock(c, a);
    MockPos partner = MakeMock(c, p);
    const spm::MatchResult join = spm::JoinPair<true, true>(
      anchor, partner, &anchor.attr, &partner.attr, anchor_is_slot0, c.slop,
      expected, enforce, scratch, &out);

    if (join.any != ref.any || join.freq != ref.freq ||
        (ref.any && join.best_distance != ref.best_distance)) {
      std::printf(
        "MISMATCH JoinPair(full) vs Brute (anchor_slot0=%d)\n"
        "  case: %s\n  join: %s\n  ref : %s\n",
        static_cast<int>(anchor_is_slot0), Show(c).c_str(), Show(join).c_str(),
        Show(ref).c_str());
      ok = false;
    }

    // same comparator both sides, so tuples match elementwise; offsets
    // must resolve to the (slot, position) mapping.
    if (out.size() != run_out.size()) {
      std::printf(
        "MISMATCH JoinPair collector size (anchor_slot0=%d)\n"
        "  case: %s\n  join=%zu run=%zu\n",
        static_cast<int>(anchor_is_slot0), Show(c).c_str(), out.size(),
        run_out.size());
      ok = false;
    } else {
      for (size_t k = 0; k < out.size(); ++k) {
        const auto& j = out[k];
        const auto& r = run_out[k];
        const bool tuple_eq =
          std::tie(j.leftmost, j.rightmost, j.leftmost_slot,
                   j.rightmost_slot) ==
          std::tie(r.leftmost, r.rightmost, r.leftmost_slot, r.rightmost_slot);
        const irs::OffsAttr lo = OffsFor(j.leftmost_slot, j.leftmost);
        const irs::OffsAttr ro = OffsFor(j.rightmost_slot, j.rightmost);
        const bool offs_eq =
          j.start_offset == lo.start && j.end_offset == ro.end;
        if (!tuple_eq || !offs_eq) {
          std::printf(
            "MISMATCH JoinPair match %zu (anchor_slot0=%d)\n"
            "  case: %s\n",
            k, static_cast<int>(anchor_is_slot0), Show(c).c_str());
          ok = false;
          break;
        }
      }
    }

    // filter path (early-exit) on fresh iterators
    spm::PairScratch scratch2;
    MockPos anchor2 = MakeMock(c, a);
    MockPos partner2 = MakeMock(c, p);
    const spm::MatchResult join_exit = spm::JoinPair<false, false>(
      anchor2, partner2, nullptr, nullptr, anchor_is_slot0, c.slop, expected,
      enforce, scratch2, nullptr);
    if (join_exit.any != ref.any) {
      std::printf(
        "MISMATCH JoinPair(filter).any (anchor_slot0=%d)\n"
        "  case: %s\n",
        static_cast<int>(anchor_is_slot0), Show(c).c_str());
      ok = false;
    }
  }
  return ok;
}

bool Check(const Case& c) {
  spm::MatchScratch scratch;

  // run_full also collects: one DFS pass counts and emits.
  std::vector<spm::EnumeratedMatch> out;
  const spm::MatchResult run_full =
    spm::Run(c.slots, c.slop, c.expected_steps, scratch, /*early_exit=*/false,
             c.groups, &out);
  // early_exit cannot collect (Run asserts !(early_exit && out)).
  const spm::MatchResult run_exit = spm::Run(
    c.slots, c.slop, c.expected_steps, scratch, /*early_exit=*/true, c.groups);
  const spm::MatchResult ref = BruteRun(c);

  bool ok = true;

  if (run_full.any != ref.any || run_full.freq != ref.freq ||
      run_full.best_distance != ref.best_distance) {
    std::printf(
      "MISMATCH Run(full) vs Brute\n  case: %s\n  run : %s\n  ref : %s\n",
      Show(c).c_str(), Show(run_full).c_str(), Show(ref).c_str());
    ok = false;
  }

  if (run_exit.any != (ref.freq > 0)) {
    std::printf(
      "MISMATCH Run(early_exit).any\n  case: %s\n  run.any=%d ref.freq=%llu\n",
      Show(c).c_str(), static_cast<int>(run_exit.any),
      static_cast<unsigned long long>(ref.freq));
    ok = false;
  }

  // one tuple per counted match, so size == freq (what BuildMatches
  // asserts); groups-aware, so it also equals the groups-aware brute count.
  if (out.size() != run_full.freq) {
    std::printf(
      "MISMATCH collector size vs freq\n  case: %s\n  out=%zu freq=%llu\n",
      Show(c).c_str(), out.size(),
      static_cast<unsigned long long>(run_full.freq));
    ok = false;
  }

  // Emitted matches must be sorted ascending by (leftmost, rightmost, slots).
  for (size_t i = 1; i < out.size(); ++i) {
    const auto& a = out[i - 1];
    const auto& b = out[i];
    const bool ordered =
      std::tie(a.leftmost, a.rightmost, a.leftmost_slot, a.rightmost_slot) <=
      std::tie(b.leftmost, b.rightmost, b.leftmost_slot, b.rightmost_slot);
    if (!ordered) {
      std::printf("MISMATCH collector not sorted\n  case: %s at index %zu\n",
                  Show(c).c_str(), i);
      ok = false;
      break;
    }
  }

  // n == 2 fast path: hold JoinPair to the same references, reusing
  // run_full's matches as the tuple oracle.
  if (c.slots.size() == 2) {
    ok &= CheckJoin(c, ref, out);
  }

  return ok;
}

// A variadic n == 2 case: each slot is a set of per-term sub position
// lists. The engine merges each slot with duplicates collapsed
// (MergedPosStream, mirroring gather's sort + unique). same_group models
// whether the two slots' query term sets intersect: one component
// (enforced pair uniqueness) or two (a shared position is legal).
struct MergedCase {
  std::array<std::vector<std::vector<value_t>>, 2> subs;
  value_t expected{1};
  value_t slop{0};
  bool same_group{true};
};

std::string Show(const MergedCase& c) {
  std::string s = "slop=" + std::to_string(c.slop) +
                  " expected=" + std::to_string(c.expected) +
                  " same_group=" + std::string(c.same_group ? "1" : "0") +
                  " subs=";
  for (const auto& slot : c.subs) {
    s += "[";
    for (const auto& sub : slot) {
      s += "{";
      for (size_t i = 0; i < sub.size(); ++i) {
        s += std::to_string(sub[i]);
        if (i + 1 < sub.size()) {
          s += ",";
        }
      }
      s += "}";
    }
    s += "]";
  }
  return s;
}

// Offsets keyed by (slot, sub, position) so the stream's
// first-registered-sub duplicate rule is observable.
irs::OffsAttr OffsVarFor(uint32_t slot, uint32_t sub, value_t p) {
  irs::OffsAttr o;
  o.start = p * 32 + slot * 8 + sub;
  o.end = o.start + 3;
  return o;
}

// Sorted duplicate-free union of one slot's sub lists - what gather's
// finalize_slot materializes and what the merged stream must enumerate.
std::vector<value_t> MergedUnion(
  const std::vector<std::vector<value_t>>& subs) {
  std::vector<value_t> all;
  for (const auto& s : subs) {
    all.insert(all.end(), s.begin(), s.end());
  }
  std::sort(all.begin(), all.end());
  all.erase(std::unique(all.begin(), all.end()), all.end());
  return all;
}

// The offsets the stream must emit for position p: the first registered
// sub containing p wins.
irs::OffsAttr ExpectedMergedOffs(const MergedCase& c, uint32_t slot,
                                 value_t p) {
  const auto& subs = c.subs[slot];
  for (uint32_t k = 0; k < subs.size(); ++k) {
    if (std::find(subs[k].begin(), subs[k].end(), p) != subs[k].end()) {
      return OffsVarFor(slot, k, p);
    }
  }
  SDB_ASSERT(false);
  return OffsVarFor(slot, 0, p);
}

using MockMergedStream = spm::MergedPosStream<true, MockPos>;

// Builds one mock per sub (owned by the caller, which must keep them alive
// for the stream's lifetime) and registers them in sub index order.
void BindMocks(const MergedCase& c, uint32_t slot, std::vector<MockPos>& mocks,
               MockMergedStream& stream) {
  const auto& subs = c.subs[slot];
  mocks.clear();
  mocks.reserve(subs.size());
  for (uint32_t k = 0; k < subs.size(); ++k) {
    MockPos m;
    m.pos = subs[k];
    m.offs.reserve(m.pos.size());
    for (const value_t p : m.pos) {
      m.offs.push_back(OffsVarFor(slot, k, p));
    }
    mocks.push_back(std::move(m));
  }
  stream.Clear();
  for (auto& m : mocks) {
    stream.Add(&m, &m.attr);
  }
}

// The stream must enumerate exactly the slot's duplicate-free union in
// ascending order, with the first-registered sub's offsets on every
// position.
bool CheckMergedStreamEnumeration(const MergedCase& c, uint32_t slot) {
  std::vector<MockPos> mocks;
  MockMergedStream stream;
  BindMocks(c, slot, mocks, stream);

  const auto expected = MergedUnion(c.subs[slot]);
  if (stream.Empty() != expected.empty()) {
    std::printf("MISMATCH MergedPosStream.Empty (slot=%u)\n  case: %s\n", slot,
                Show(c).c_str());
    return false;
  }

  std::vector<value_t> got;
  bool offs_ok = true;
  while (stream.next()) {
    got.push_back(stream.value());
    const irs::OffsAttr want = ExpectedMergedOffs(c, slot, stream.value());
    const irs::OffsAttr* have = stream.GetOffs();
    if (have->start != want.start || have->end != want.end) {
      offs_ok = false;
    }
  }
  if (got != expected || !offs_ok) {
    std::printf(
      "MISMATCH MergedPosStream enumeration (slot=%u, offs_ok=%d)\n"
      "  case: %s\n",
      slot, static_cast<int>(offs_ok), Show(c).c_str());
    return false;
  }
  return true;
}

// The variadic n == 2 path: JoinPair over two merged streams must match the
// brute reference and Run's collector over the merged-dedup slot lists.
// same_group mirrors the production mapping (EnforceUniqueness over the two
// slots' group ids). Also runs the full plain battery over the merged
// lists, which the production gather would have produced.
bool CheckMergedJoin(const MergedCase& c) {
  const std::vector<uint32_t> groups =
    c.same_group ? std::vector<uint32_t>{0, 0} : std::vector<uint32_t>{0, 1};
  Case merged{.slots = {MergedUnion(c.subs[0]), MergedUnion(c.subs[1])},
              .expected_steps = {c.expected},
              .groups = groups,
              .slop = c.slop};

  bool ok = Check(merged);

  spm::MatchScratch scratch;
  std::vector<spm::EnumeratedMatch> run_out;
  const spm::MatchResult ref =
    spm::Run(merged.slots, merged.slop, merged.expected_steps, scratch,
             /*early_exit=*/false, groups, &run_out);

  ok &= CheckMergedStreamEnumeration(c, 0);
  ok &= CheckMergedStreamEnumeration(c, 1);

  for (const bool anchor_is_slot0 : {true, false}) {
    const uint32_t a = anchor_is_slot0 ? 0u : 1u;
    const uint32_t p = a ^ 1u;

    std::vector<MockPos> anchor_mocks;
    std::vector<MockPos> partner_mocks;
    MockMergedStream anchor;
    MockMergedStream partner;
    BindMocks(c, a, anchor_mocks, anchor);
    BindMocks(c, p, partner_mocks, partner);

    spm::PairScratch pair_scratch;
    std::vector<spm::PairMatch> out;
    const spm::MatchResult join = spm::JoinPair<true, true>(
      anchor, partner, anchor.GetOffs(), partner.GetOffs(), anchor_is_slot0,
      c.slop, c.expected, c.same_group, pair_scratch, &out);

    if (join.any != ref.any || join.freq != ref.freq ||
        (ref.any && join.best_distance != ref.best_distance)) {
      std::printf(
        "MISMATCH MergedJoin(full) vs Brute (anchor_slot0=%d)\n"
        "  case: %s\n  join: %s\n  ref : %s\n",
        static_cast<int>(anchor_is_slot0), Show(c).c_str(), Show(join).c_str(),
        Show(ref).c_str());
      ok = false;
    }

    if (out.size() != run_out.size()) {
      std::printf(
        "MISMATCH MergedJoin collector size (anchor_slot0=%d)\n"
        "  case: %s\n  join=%zu run=%zu\n",
        static_cast<int>(anchor_is_slot0), Show(c).c_str(), out.size(),
        run_out.size());
      ok = false;
    } else {
      for (size_t k = 0; k < out.size(); ++k) {
        const auto& j = out[k];
        const auto& r = run_out[k];
        const bool tuple_eq =
          std::tie(j.leftmost, j.rightmost, j.leftmost_slot,
                   j.rightmost_slot) ==
          std::tie(r.leftmost, r.rightmost, r.leftmost_slot, r.rightmost_slot);
        const irs::OffsAttr lo =
          ExpectedMergedOffs(c, j.leftmost_slot, j.leftmost);
        const irs::OffsAttr ro =
          ExpectedMergedOffs(c, j.rightmost_slot, j.rightmost);
        const bool offs_eq =
          j.start_offset == lo.start && j.end_offset == ro.end;
        if (!tuple_eq || !offs_eq) {
          std::printf(
            "MISMATCH MergedJoin match %zu (anchor_slot0=%d)\n"
            "  case: %s\n",
            k, static_cast<int>(anchor_is_slot0), Show(c).c_str());
          ok = false;
          break;
        }
      }
    }

    // filter path (early-exit) on fresh streams
    std::vector<MockPos> anchor_mocks2;
    std::vector<MockPos> partner_mocks2;
    MockMergedStream anchor2;
    MockMergedStream partner2;
    BindMocks(c, a, anchor_mocks2, anchor2);
    BindMocks(c, p, partner_mocks2, partner2);

    spm::PairScratch pair_scratch2;
    const spm::MatchResult join_exit = spm::JoinPair<false, false>(
      anchor2, partner2, nullptr, nullptr, anchor_is_slot0, c.slop, c.expected,
      c.same_group, pair_scratch2, nullptr);
    if (join_exit.any != ref.any) {
      std::printf(
        "MISMATCH MergedJoin(filter).any (anchor_slot0=%d)\n"
        "  case: %s\n",
        static_cast<int>(anchor_is_slot0), Show(c).c_str());
      ok = false;
    }
  }
  return ok;
}

std::vector<value_t> RandomSlot(std::mt19937_64& rng, value_t universe,
                                bool allow_empty) {
  // random sorted-unique subset of [1, universe], usually non-empty
  std::vector<value_t> all;
  all.reserve(universe);
  for (value_t v = 1; v <= universe; ++v) {
    all.push_back(v);
  }
  std::shuffle(all.begin(), all.end(), rng);
  size_t lo = allow_empty ? 0 : 1;
  std::uniform_int_distribution<size_t> sz_dist(lo, all.size());
  size_t sz = sz_dist(rng);
  all.resize(sz);
  // occasionally duplicate a position (increment-0 tokens)
  if (!all.empty() && rng() % 8 == 0) {
    all.push_back(all[rng() % all.size()]);
  }
  std::sort(all.begin(), all.end());
  return all;
}

Case RandomCase(std::mt19937_64& rng) {
  Case c;
  std::uniform_int_distribution<size_t> n_dist(2, 4);
  // occasionally n=5 with a tiny universe to bound the product
  const size_t n = (rng() % 8 == 0) ? 5 : n_dist(rng);

  std::uniform_int_distribution<value_t> universe_dist(static_cast<value_t>(n),
                                                       n == 5 ? 8 : 12);
  const value_t universe = universe_dist(rng);

  const bool allow_empty = (rng() % 16 == 0);  // exercise the empty-slot return
  c.slots.resize(n);
  for (size_t i = 0; i < n; ++i) {
    c.slots[i] = RandomSlot(rng, universe, allow_empty);
  }

  c.expected_steps.resize(n - 1);
  for (auto& e : c.expected_steps) {
    // weight 1 heavily, sometimes 2/3, occasionally 0 (increment-0 parts)
    const uint32_t roll = rng() % 6;
    e = (roll < 3) ? 1u : (roll == 3 ? 2u : (roll == 4 ? 3u : 0u));
  }

  std::uniform_int_distribution<value_t> slop_dist(0, 6);
  c.slop = slop_dist(rng);

  // Groups: 50% none, 25% all-distinct, 25% with a deliberate collision.
  const uint32_t mode = rng() % 4;
  if (mode == 0 || mode == 1) {
    c.groups.clear();
  } else if (mode == 2) {
    c.groups.resize(n);
    for (size_t i = 0; i < n; ++i) {
      c.groups[i] = static_cast<uint32_t>(i);  // all distinct
    }
  } else {
    c.groups.resize(n);
    for (size_t i = 0; i < n; ++i) {
      c.groups[i] = static_cast<uint32_t>(rng() % n);  // collisions likely
    }
  }

  return c;
}

// Hand-checked edge cases: reference value verified against ES semantics.
int RunEdgeCases() {
  int failures = 0;
  auto expect = [&](const Case& c, bool want_any, uint64_t want_freq,
                    value_t want_best, const char* name) {
    const spm::MatchResult ref = BruteRun(c);
    if (!Check(c)) {
      std::printf("  (in edge case '%s')\n", name);
      ++failures;
    }
    if (ref.any != want_any || ref.freq != want_freq ||
        (want_any && ref.best_distance != want_best)) {
      std::printf(
        "EDGE '%s' reference disagrees with hand value: %s want{any=%d "
        "freq=%llu best=%u}\n",
        name, Show(ref).c_str(), want_any,
        static_cast<unsigned long long>(want_freq), want_best);
      ++failures;
    }
  };

  // Adjacent forward pair, exact.
  expect({.slots = {{1}, {2}}, .expected_steps = {1}, .groups = {}, .slop = 0},
         true, 1, 0, "adjacent_exact");
  // Reversed pair needs slop>=2 (StepCost(-1,1)=2).
  expect({.slots = {{2}, {1}}, .expected_steps = {1}, .groups = {}, .slop = 1},
         false, 0, 0, "reversed_slop1_miss");
  expect({.slots = {{2}, {1}}, .expected_steps = {1}, .groups = {}, .slop = 2},
         true, 1, 2, "reversed_slop2_hit");
  // Same position, no groups -> strict -> dropped.
  expect({.slots = {{1}, {1}}, .expected_steps = {1}, .groups = {}, .slop = 1},
         false, 0, 0, "samepos_nogroups");
  // Same position, distinct groups -> allowed, StepCost(0,1)=1.
  expect(
    {.slots = {{1}, {1}}, .expected_steps = {1}, .groups = {0, 1}, .slop = 1},
    true, 1, 1, "samepos_distinct_groups");
  // ES-verified (b): repeat + a third term on the repeat's position.
  // Groups {0,1,0}: slot 1 may share position 2 with slot 2 (different
  // groups); the two group-0 slots sit on distinct positions. One tuple
  // (1,2,2), cost StepCost(1)+StepCost(0) = 1.
  expect({.slots = {{1}, {2}, {2}},
          .expected_steps = {1, 1},
          .groups = {0, 1, 0},
          .slop = 0},
         false, 0, 0, "samepos_repeat_third_slop0");
  expect({.slots = {{1}, {2}, {2}},
          .expected_steps = {1, 1},
          .groups = {0, 1, 0},
          .slop = 1},
         true, 1, 1, "samepos_repeat_third_slop1");
  // Same-group slots forced onto one position stay barred at any slop.
  expect({.slots = {{2}, {1}, {2}},
          .expected_steps = {1, 1},
          .groups = {0, 1, 0},
          .slop = 5},
         false, 0, 0, "samepos_same_group_barred");
  // Increment-0 pair (expected 0): the same position costs 0, so it hits at
  // slop 0 across groups; within one group it stays barred.
  expect(
    {.slots = {{3}, {3}}, .expected_steps = {0}, .groups = {0, 1}, .slop = 0},
    true, 1, 0, "increment0_distinct_groups");
  expect(
    {.slots = {{3}, {3}}, .expected_steps = {0}, .groups = {0, 0}, .slop = 0},
    false, 0, 0, "increment0_same_group_barred");
  // Adjacent (delta 1) under expected 0 costs 1.
  expect({.slots = {{3}, {4}}, .expected_steps = {0}, .groups = {}, .slop = 0},
         false, 0, 0, "increment0_adjacent_slop0_miss");
  expect({.slots = {{3}, {4}}, .expected_steps = {0}, .groups = {}, .slop = 1},
         true, 1, 1, "increment0_adjacent_slop1_hit");
  // Empty slot -> no match.
  expect({.slots = {{}, {1}}, .expected_steps = {1}, .groups = {}, .slop = 5},
         false, 0, 0, "empty_slot");
  // Dense 3-term, small slop -- just exercise the n>=3 path against brute.
  if (!Check({.slots = {{1, 2, 3}, {1, 2, 3}, {1, 2, 3}},
              .expected_steps = {1, 1},
              .groups = {},
              .slop = 2})) {
    std::printf("  (in edge case 'dense3_small_slop')\n");
    ++failures;
  }

  if (failures == 0) {
    std::printf("edge cases: OK\n");
  }
  return failures;
}

// Small universe plus up to three subs per slot makes cross-sub duplicate
// positions frequent; RandomSlot's occasional in-list repeat also feeds
// the within-sub duplicate path.
MergedCase RandomMergedCase(std::mt19937_64& rng) {
  MergedCase c;
  std::uniform_int_distribution<value_t> universe_dist(2, 10);
  const value_t universe = universe_dist(rng);
  for (auto& slot : c.subs) {
    const size_t nsubs = 1 + rng() % 3;
    slot.resize(nsubs);
    for (auto& sub : slot) {
      sub = RandomSlot(rng, universe, /*allow_empty=*/rng() % 8 == 0);
    }
  }
  // weight 1 heavily, sometimes 2/3, occasionally 0 (increment-0 parts)
  const uint32_t roll = rng() % 6;
  c.expected = (roll < 3) ? 1u : (roll == 3 ? 2u : (roll == 4 ? 3u : 0u));
  std::uniform_int_distribution<value_t> slop_dist(0, 6);
  c.slop = slop_dist(rng);
  c.same_group = (rng() % 2) == 0;
  return c;
}

// Hand-picked merged-stream shapes; the semantic reference is the merged
// union under strict uniqueness, so correctness is pinned by CheckMergedJoin
// itself rather than hand freq values.
int RunMergedEdgeCases() {
  int failures = 0;
  auto check = [&](const MergedCase& c, const char* name) {
    if (!CheckMergedJoin(c)) {
      std::printf("  (in merged edge case '%s')\n", name);
      ++failures;
    }
  };

  // Identical subs collapse to one list; freq must not double.
  check({.subs = {{{{1, 3}, {1, 3}}, {{2}}}}, .expected = 1, .slop = 1},
        "identical_subs");
  // Partial overlap across subs at one position.
  check({.subs = {{{{1, 2}, {2, 5}}, {{3}}}}, .expected = 1, .slop = 1},
        "overlap_one_position");
  // Duplicate inside a single sub still emits the position once: the
  // adjacent (2, 3) pair must be counted once, not per copy of 3.
  check({.subs = {{{{2}}, {{3, 3}}}}, .expected = 1, .slop = 0},
        "within_sub_duplicate");
  // Single sub degenerates to a passthrough.
  check({.subs = {{{{1, 4}}, {{2}}}}, .expected = 1, .slop = 1}, "single_sub");
  // Empty sub next to a live one.
  check({.subs = {{{{}, {4}}, {{5}}}}, .expected = 1, .slop = 0}, "empty_sub");
  // All subs of a slot empty: no match, stream must report Empty.
  check({.subs = {{{{}}, {{5}}}}, .expected = 1, .slop = 5}, "empty_slot");
  // Cross-slot same position: strict uniqueness drops the (1, 1) pair.
  check({.subs = {{{{1}}, {{1}}}}, .expected = 1, .slop = 3},
        "cross_slot_same_position");
  // Same shape, disjoint term sets: the (1, 1) pair is legal at cost 1
  // (ES-verified a2 corner).
  check(
    {.subs = {{{{1}}, {{1}}}}, .expected = 1, .slop = 3, .same_group = false},
    "cross_slot_same_position_disjoint");

  if (failures == 0) {
    std::printf("merged edge cases: OK\n");
  }
  return failures;
}

// Env overrides for iteration count / seed: SLOP_FUZZ_ITERS, SLOP_FUZZ_SEED.
uint64_t EnvU64(const char* name, uint64_t fallback) {
  const char* v = std::getenv(name);
  return v ? std::strtoull(v, nullptr, 10) : fallback;
}

}  // namespace

TEST(SlopMatcherFuzz, EdgeCases) { EXPECT_EQ(0, RunEdgeCases()); }

TEST(SlopMatcherFuzz, RandomCases) {
  // modest default so the debug build stays cheap; crank via SLOP_FUZZ_ITERS
  const uint64_t iterations = EnvU64("SLOP_FUZZ_ITERS", 200000ull);
  const uint64_t seed = EnvU64("SLOP_FUZZ_SEED", 0xC0FFEEull);

  std::mt19937_64 rng{seed};
  for (uint64_t i = 0; i < iterations; ++i) {
    const Case c = RandomCase(rng);
    ASSERT_TRUE(Check(c))
      << "iteration " << i << " seed " << seed
      << " (set SLOP_FUZZ_SEED to this value to reproduce the stream)";
  }
}

TEST(SlopMatcherFuzz, MergedEdgeCases) { EXPECT_EQ(0, RunMergedEdgeCases()); }

TEST(SlopMatcherFuzz, RandomMergedPairCases) {
  // modest default so the debug build stays cheap; crank via SLOP_FUZZ_ITERS
  const uint64_t iterations = EnvU64("SLOP_FUZZ_ITERS", 200000ull);
  const uint64_t seed = EnvU64("SLOP_FUZZ_SEED", 0xC0FFEEull);

  std::mt19937_64 rng{seed};
  for (uint64_t i = 0; i < iterations; ++i) {
    const MergedCase c = RandomMergedCase(rng);
    ASSERT_TRUE(CheckMergedJoin(c))
      << "iteration " << i << " seed " << seed
      << " (set SLOP_FUZZ_SEED to this value to reproduce the stream)";
  }
}

// The production case ladder and the oracle's |delta - expected| form
// must agree everywhere; exhaustive over a domain wider than any fuzz
// case generates.
TEST(SlopMatcherFuzz, StepCostSpec) {
  for (value_t expected = 0; expected <= 16; ++expected) {
    for (int64_t delta = -256; delta <= 256; ++delta) {
      ASSERT_EQ(BruteStepCost(delta, expected),
                static_cast<uint64_t>(spm::StepCost(delta, expected)))
        << "delta=" << delta << " expected=" << expected;
    }
  }
}
