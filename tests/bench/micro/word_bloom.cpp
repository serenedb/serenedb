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

// Which structure should back the per-token word probes of stopword masks and
// synonym maps -- a hash-free radix, or a well-tested vendored hash table?
//
// SETTLED 2026-08-31: the vendored hash table. `dict::WordTable`, the two-level
// radix that used to front every stopword set and synonym map, was DELETED on
// the strength of the FINDINGS below; the three consumers now probe their
// `absl::flat_hash_{set,map}` directly. Its arms are gone with it -- `Radix`,
// which is self-contained here and tracked WordTable within 0.65-0.86x across
// the whole range, stands in for the shape so the comparison stays runnable.
//
// Every arm answers exact membership over the SAME canonical 16-byte inline
// handles, across three dimensions:
//
//   n    -- 40 (a small stopword mask) .. 150000 (WordNet-scale synonym map).
//           This is the dimension the pre-2026-08-31 version of this bench was
//           missing: it measured 40 keys only, where a radix's leaf runs are
//           1-3 entries and its linear scan is free by construction. At 150k
//           keys a (length, first byte) dispatch has 3327 reachable slots, of
//           which only ~260 are populated by real words -- so the scan is over
//           runs of ~600.
//   hit   -- 0% (pure miss), 16%, 40% (prose-like). Probes are miss-dominated
//           in production, which is why a structure that answers a miss in one
//           predictable branch was preferred in the first place.
//   keys  -- all-inline (<=12B, the natural-language common case) vs a 30%
//           tail of 13..24B keys, which a radix has to route to a backing
//           container anyway.
//
// ARMS
//   Swiss        absl::flat_hash_set<__uint128_t>, absl::HashOf. THE ADOPTED
//                STRUCTURE (as the set/map the consumers already owned).
//   SwissCheap   same table, one folded 64x64 multiply instead of absl::HashOf.
//                The earlier round concluded "the hash was the whole cost";
//                this arm tests that directly instead of inferring it.
//   BoostFlat    boost::unordered_flat_set<__uint128_t> (+ Cheap variant).
//   Bloom        register-blocked bloom (2 bits in one u64 word, 16 bits/key)
//                in front of the swiss table -- the pre-radix in-tree form.
//   BoostBlock64 / BoostFast32
//                boost::bloom subfilters + swiss confirm.
//   Radix        idealized flat radix: the two levels collapsed into one
//                dispatch + leaf compares, an upper bound on any node-hopping
//                ART.
//
// Keys are generated, not hand-listed, so that n can move: lengths follow the
// English word-length distribution (2..12, mode 4-6) and first bytes follow
// English initial-letter frequency, because those two are exactly what feed a
// (length, first byte) slot index -- a uniform generator would understate the
// leaf-run skew. Misses are drawn from the same distribution, so they land in
// populated slots rather than being rejected by an empty one.
//
// The probe stream is SHUFFLED (fixed seed), so hit/miss order is
// unpredictable. The pre-rewrite version interleaved hits deterministically,
// which the branch predictor learns; absolute numbers here are therefore NOT
// comparable with the older ones, only ratios within one run are.
//
// boost is a bench-side experiment only -- it is deliberately not linked into
// libs/iresearch. Adopting boost::unordered in dict/ would need a CMake change.
//
// Pin to one core on a quiet box:
//   taskset -c N ./build_perf/bin/serenedb-bench-micro-word_bloom \
//     --benchmark_min_time=0.3s --benchmark_repetitions=9 \
//     --benchmark_report_aggregates_only=true
//
// FINDINGS (2026-08-31, build_perf, taskset -c 30, medians of 9 at
// min_time=0.3s; the n<=200 rows re-run at 15 reps / 0.5s because they were the
// only unstable ones). ns per probe:
//
//   Contains, all-inline keys      WordTable   Swiss   BoostFlat   Bloom
//     n=40    hit=0                     0.83    1.72        1.50    2.00
//     n=200   hit=0                     7.51    4.46        1.64    2.08
//     n=5000  hit=0                    50.37    1.61        1.55    1.21
//     n=50000 hit=0                   415.20    4.04        1.91    1.33
//     n=150000 hit=0                 1886.84    2.12        1.75    1.36
//
//   Find (the synonym path)        WordTable   Swiss
//     n=40    hit=0                     1.82    6.45
//     n=5000  hit=0                    42.42    3.71
//     n=150000 hit=0                 1376.31    4.00
//
// 1. THE RADIX DOES NOT SCALE, and the cliff arrives far earlier than the
//    40-key bench that chose it could show. Against absl it is 31x slower at
//    n=5000, 103x at n=50000 and 890x at n=150000 on Contains; 11x / 45x / 344x
//    on Find. Cause is structural, not WordTable's own bookkeeping: the
//    idealized `Radix` arm -- no length branch, no `_has_long`, no slot mask --
//    tracks it at 0.65-0.86x the whole way. With ~26 initials x ~10 lengths
//    actually populated, 150k keys land in ~260 reachable slots, so `FindLeaf`
//    linearly scans runs of ~600.
// 2. Its win is real but narrow: only at n<=40 AND only when every key is
//    inline. At n=200 -- a real English stopword list -- it is already beaten
//    by every arm (0.22-0.94x). In the mixed-key family (30% of keys >12B) even
//    the n=40 advantage disappears into parity (7.54 vs 7.12 BoostFlat / 7.79
//    Swiss), because those probes take the container fallback anyway.
// 3. The n=40/hit=0 cell is a FLAPPER: 1.48 ns in run 1, 0.83 ns in run 2, CV
//    29-36% for WordTable and Swiss alike. Do not decide anything on it.
// 4. boost::unordered_flat_set is the best table at every size and by far the
//    steadiest (CV 0-4%, against absl's 16-36% at small n). It is not linked
//    into iresearch-static today, so adopting it needs a CMake change; absl is
//    already there and is enormously better than the radix everywhere it
//    counts.
// 5. REFUTED: "the hash was the whole cost". The cheap folded-multiply arms are
//    not consistently better than their absl::HashOf twins -- SwissCheap is
//    *worse* at n=40 (3.01 vs 2.42) and level elsewhere. The earlier round's
//    inference from bloom-vs-swiss did not hold up when tested directly.
// 6. A bloom prefilter still earns its keep on miss-dominated streams at large
//    n (1.36 vs 2.12 for absl at n=150000/hit=0) because it answers a miss
//    without touching the table -- but it loses to a bare boost table at small
//    n and costs a second structure.
//
// 7. LENGTH PREFILTER REFUTED (a 256-bit bitset of the lengths present, tested
//    before the table). It never won a single cell:
//      - pure overhead, filter never rejects: 1.08-1.30x slower;
//      - narrow mask (members length 3-5, probes full distribution, so it
//        rejects ~54% -- its BEST case): boost+len loses in all 9 cells
//        (1.12-2.95x), absl+len loses in 6 of 9 and its 3 nominal wins
//        (0.91-0.96x) sit on absl baselines with 14-32% CV, i.e. noise.
//    Same mechanism as the first-byte bitmap before it: a ~50/50 filter
//    mispredicts, and one mispredict (~15-20 cycles) costs far more than the
//    u128 hash it avoids. The shape only pays when it answers one way almost
//    always -- which is exactly what the bloom arm does, and why the bloom wins
//    the miss-dominated cells (1.18-1.25 ns on the narrow mask at hit=0,
//    against 6.09 for WordTable at n=200) while the length filter loses them.
//    Guarded arms measure at 0-1% CV, so this verdict is not a noise artifact.
//
// 8. KEY TYPE MATTERS, and arms 1-7 do NOT measure it. Every handle-keyed arm
//    above is handed a PRECOMPUTED u128, so it prices the hash and the probe
//    but not building the key. Reading `Swiss` vs `StrSwiss` as "integer hash
//    beats string hash, 1.56-4.00x" was therefore wrong, and a fix built on it
//    (keep std::string keys, swap in a hasher that derives the handle from a
//    string_view) REGRESSED the stopword arms a further 1.53x -> 1.96x: it paid
//    MakeTermView's branchy reconstruction AND still memcmp'd strings on a hit.
//    The `Key*` family is the production-faithful comparison -- both arms start
//    from a `duckdb::string_t` and build their own key in the timed loop -- and
//    it puts u128 keys at 0.42-0.66x of string keys. Landed that way.
// 9. Even so, a hash table does NOT recover the radix's tiny-mask advantage:
//    stopword arms sit at 1.22-1.51x of the WordTable baseline. The reason is
//    structural and worth remembering: the radix's rejection was FUSED into the
//    lookup (an empty (length, first byte) slot means count==0, so the compare
//    loop simply never runs) rather than bolted on as a filter branch. That is
//    why it beats a hash table on a 4-word mask and why finding 7's separate
//    prefilter could not reproduce the effect. The trade taken: ~+1 ns/token on
//    stopword masks, against 12x on real synonym dictionaries.
//
// End to end (tokenizer_fill, same 4096-value probe stream, dictionary size the
// only variable): solr_synonyms 0.029 -> solr_synonyms_large 0.496 ms (17x),
// wordnet_synonyms 0.025 -> 0.558 ms (22x). After the deletion both large arms
// drop to 0.039/0.046 ms -- 12.7x and 12.1x faster.

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/hash/hash.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <bit>
#include <boost/bloom/block.hpp>
#include <boost/bloom/fast_multiblock32.hpp>
#include <boost/bloom/filter.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/analysis/text/term_view.hpp"

namespace {

using irs::InlineTermHandle;
using irs::MakeTermView;

// ============================== key generation ==============================

// English initial-letter frequency (percent, ITA-style), used to reproduce the
// first-byte skew of WordTable's slot index.
constexpr double kInitialFreq[26] = {
  11.6, 4.4, 5.2, 3.2, 2.8, 4.0, 1.6,  4.2, 7.3, 0.5, 0.9, 2.4, 3.8,
  2.3,  7.6, 4.3, 0.2, 2.8, 6.7, 16.7, 1.2, 0.8, 6.8, 0.1, 0.2, 0.1};

// English word-length distribution over 2..12 (percent), mode at 4-6.
constexpr double kLenFreq[11] = {17.0, 20.0, 15.0, 11.0, 9.0, 8.0,
                                 7.0,  5.0,  4.0,  2.5,  1.5};

class KeyGen {
 public:
  // A set of n distinct inline keys cannot be drawn from lengths whose alphabet
  // space is smaller than n: at n=150000, sampling length 2 (676 words) would
  // spin forever looking for a fresh one. `MinLength` raises the floor to the
  // shortest length whose space comfortably exceeds n, and members, misses and
  // probes all share it so that misses keep landing in populated slots.
  KeyGen(uint64_t seed, size_t n) : _rng{seed}, _min_len{MinLength(n)} {}

  // `long_tail` = fraction of keys drawn from 13..24 bytes instead of 2..12.
  // `narrow` restricts the draw to lengths 3..5, modelling a small hand-written
  // stopword mask ({"the","and","over","under"}) rather than a full lexicon --
  // the only shape where a length prefilter has anything to reject.
  std::string Next(double long_tail, bool narrow = false) {
    const size_t len = narrow                    ? 3 + (_rng() % 3)
                       : _real(_rng) < long_tail ? 13 + (_rng() % 12)
                                                 : PickLength();
    std::string out;
    out.reserve(len);
    out.push_back(static_cast<char>('a' + PickInitial()));
    for (size_t i = 1; i < len; ++i) {
      out.push_back(static_cast<char>('a' + (_rng() % 26)));
    }
    return out;
  }

 private:
  static size_t MinLength(size_t n) {
    double space = 26.0;
    for (size_t len = 2; len <= duckdb::string_t::INLINE_LENGTH; ++len) {
      space *= 26.0;
      if (space >= 8.0 * static_cast<double>(n)) {
        return len;
      }
    }
    return duckdb::string_t::INLINE_LENGTH;
  }

  size_t PickLength() {
    double r = _real(_rng) * 100.0;
    for (size_t i = 0; i < std::size(kLenFreq); ++i) {
      r -= kLenFreq[i];
      if (r <= 0.0) {
        return std::max(i + 2, _min_len);
      }
    }
    return 12;
  }

  size_t PickInitial() {
    double r = _real(_rng) * 100.0;
    for (size_t i = 0; i < std::size(kInitialFreq); ++i) {
      r -= kInitialFreq[i];
      if (r <= 0.0) {
        return i;
      }
    }
    return 4;
  }

  std::mt19937_64 _rng;
  std::uniform_real_distribution<double> _real{0.0, 1.0};
  size_t _min_len;
};

constexpr size_t kProbes = 65536;

struct Corpus {
  std::vector<std::string> members;
  // Probe stream: shuffled mix of members (hits) and non-members (misses).
  std::vector<std::string> probes;
  std::vector<__uint128_t> handles;  // inline probes only; 0 for long keys
};

// Corpora are cached because the same (n, hit, mix) is rebuilt for every arm,
// and building the 150k ones is slower than the measurement. Values are held
// behind a unique_ptr so a later insert's rehash cannot move a Corpus a caller
// still holds a reference to.
// `narrow` gives the members only lengths 3..5 while the probe stream keeps the
// full distribution, so a length prefilter has ~54% of probes to reject. That
// is the best case for such a filter, and also the branch-prediction worst
// case.
const Corpus& GetCorpus(size_t n, int hit_percent, double long_tail,
                        bool narrow = false) {
  static absl::flat_hash_map<uint64_t, std::unique_ptr<Corpus>> cache;
  const uint64_t key = (static_cast<uint64_t>(n) << 16) |
                       (static_cast<uint64_t>(hit_percent) << 4) |
                       (static_cast<uint64_t>(long_tail > 0.0) << 1) |
                       static_cast<uint64_t>(narrow);
  if (const auto it = cache.find(key); it != cache.end()) {
    return *it->second;
  }
  KeyGen gen{0x5eed1234u, narrow ? size_t{2} : n};
  Corpus corpus;
  absl::flat_hash_set<std::string> seen;
  corpus.members.reserve(n);
  while (corpus.members.size() < n) {
    auto word = gen.Next(long_tail, narrow);
    if (seen.insert(word).second) {
      corpus.members.push_back(std::move(word));
    }
  }
  std::vector<std::string> misses;
  misses.reserve(kProbes);
  while (misses.size() < kProbes) {
    auto word = gen.Next(long_tail);
    if (!seen.contains(word)) {
      misses.push_back(std::move(word));
    }
  }
  const size_t hits = kProbes * static_cast<size_t>(hit_percent) / size_t{100};
  corpus.probes.reserve(kProbes);
  std::mt19937_64 pick{0xc0ffeeull};
  for (size_t i = 0; i < hits; ++i) {
    corpus.probes.push_back(corpus.members[pick() % corpus.members.size()]);
  }
  for (size_t i = hits; i < kProbes; ++i) {
    corpus.probes.push_back(misses[i % misses.size()]);
  }
  std::shuffle(corpus.probes.begin(), corpus.probes.end(), pick);
  corpus.handles.reserve(kProbes);
  for (const auto& probe : corpus.probes) {
    corpus.handles.push_back(probe.size() <= duckdb::string_t::INLINE_LENGTH
                               ? InlineTermHandle(std::string_view{probe})
                               : __uint128_t{0});
  }
  return *cache.emplace(key, std::make_unique<Corpus>(std::move(corpus)))
            .first->second;
}

// ================================== hashes ==================================

IRS_FORCE_INLINE inline uint64_t FoldMul(uint64_t a, uint64_t b) noexcept {
  const auto m = static_cast<__uint128_t>(a) * b;
  return static_cast<uint64_t>(m) ^ static_cast<uint64_t>(m >> 64);
}

// One folded 64x64 multiply -- the cheapest hash that still avalanches.
struct CheapHash {
  using is_avalanching = void;

  IRS_FORCE_INLINE size_t operator()(__uint128_t v) const noexcept {
    return FoldMul(static_cast<uint64_t>(v) ^ 0x9e3779b97f4a7c15ull,
                   static_cast<uint64_t>(v >> 64) ^ 0xc2b2ae3d27d4eb4full);
  }
};

struct AbslHash {
  using is_avalanching = void;

  IRS_FORCE_INLINE size_t operator()(__uint128_t v) const noexcept {
    return absl::HashOf(v);
  }
};

// =================================== arms ===================================

template<typename Set>
Set MakeSet(const std::vector<std::string>& words) {
  Set set;
  set.reserve(words.size());
  for (const auto& word : words) {
    if (word.size() <= duckdb::string_t::INLINE_LENGTH) {
      set.insert(InlineTermHandle(std::string_view{word}));
    }
  }
  return set;
}

using SwissSet = absl::flat_hash_set<__uint128_t>;

struct Swiss {
  explicit Swiss(const std::vector<std::string>& w)
    : set{MakeSet<SwissSet>(w)} {}

  IRS_FORCE_INLINE bool Contains(__uint128_t h) const noexcept {
    return set.contains(h);
  }

  SwissSet set;
};

struct SwissCheap {
  using Set = absl::flat_hash_set<__uint128_t, CheapHash>;

  explicit SwissCheap(const std::vector<std::string>& w)
    : set{MakeSet<Set>(w)} {}

  IRS_FORCE_INLINE bool Contains(__uint128_t h) const noexcept {
    return set.contains(h);
  }

  Set set;
};

struct BoostFlat {
  using Set = boost::unordered_flat_set<__uint128_t, AbslHash>;

  explicit BoostFlat(const std::vector<std::string>& w)
    : set{MakeSet<Set>(w)} {}

  IRS_FORCE_INLINE bool Contains(__uint128_t h) const noexcept {
    return set.contains(h);
  }

  Set set;
};

struct BoostFlatCheap {
  using Set = boost::unordered_flat_set<__uint128_t, CheapHash>;

  explicit BoostFlatCheap(const std::vector<std::string>& w)
    : set{MakeSet<Set>(w)} {}

  IRS_FORCE_INLINE bool Contains(__uint128_t h) const noexcept {
    return set.contains(h);
  }

  Set set;
};

// Register-blocked bloom: 16 bits/key, 2 bits set in a single u64 word, in
// front of a swiss confirm. The in-tree form before the radix replaced it.
struct Bloom {
  explicit Bloom(const std::vector<std::string>& w)
    : set{MakeSet<SwissSet>(w)} {
    filter.assign(std::bit_ceil(std::max<size_t>(w.size() / 4 + 1, 2)), 0);
    mask = static_cast<uint32_t>(filter.size() - 1);
    for (const auto& word : w) {
      if (word.size() > duckdb::string_t::INLINE_LENGTH) {
        continue;
      }
      const uint64_t h = absl::HashOf(InlineTermHandle(std::string_view{word}));
      filter[(h >> 12) & mask] |= Bits(h);
    }
  }

  IRS_FORCE_INLINE static uint64_t Bits(uint64_t h) noexcept {
    return (uint64_t{1} << (h & 63)) | (uint64_t{1} << ((h >> 6) & 63));
  }

  IRS_FORCE_INLINE bool Contains(__uint128_t handle) const noexcept {
    const uint64_t h = absl::HashOf(handle);
    const uint64_t bits = Bits(h);
    if ((filter[(h >> 12) & mask] & bits) != bits) [[likely]] {
      return false;
    }
    return set.contains(handle);
  }

  std::vector<uint64_t> filter;
  uint32_t mask = 0;
  SwissSet set;
};

template<typename Sub>
struct Boosted {
  using Filter = boost::bloom::filter<__uint128_t, 1, Sub, 0, AbslHash>;

  explicit Boosted(const std::vector<std::string>& w)
    : filter{16 * std::max<size_t>(w.size(), 1)}, set{MakeSet<SwissSet>(w)} {
    for (const auto& word : w) {
      if (word.size() > duckdb::string_t::INLINE_LENGTH) {
        continue;
      }
      filter.insert(InlineTermHandle(std::string_view{word}));
    }
  }

  IRS_FORCE_INLINE bool Contains(__uint128_t h) const noexcept {
    if (!filter.may_contain(h)) [[likely]] {
      return false;
    }
    return set.contains(h);
  }

  Filter filter;
  SwissSet set;
};

// Idealized flat radix: WordTable's two levels collapsed into one dispatch plus
// leaf compares, with no length branch, no `_has_long` and no slot mask. An
// upper bound on any real node-hopping ART -- and, against the WordTable arm,
// the price of WordTable's own bookkeeping.
struct Radix {
  struct Slot {
    uint32_t off;
    uint32_t count;
  };

  explicit Radix(const std::vector<std::string>& w) {
    std::vector<__uint128_t> handles;
    handles.reserve(w.size());
    for (const auto& word : w) {
      if (word.size() <= duckdb::string_t::INLINE_LENGTH) {
        handles.push_back(InlineTermHandle(std::string_view{word}));
      }
    }
    std::sort(handles.begin(), handles.end(), [](auto a, auto b) {
      return Key(a) < Key(b) || (Key(a) == Key(b) && a < b);
    });
    slots.assign(13 * 256, {0, 0});
    for (uint32_t i = 0; i < handles.size();) {
      uint32_t j = i;
      while (j < handles.size() && Key(handles[j]) == Key(handles[i])) {
        ++j;
      }
      slots[Key(handles[i])] = {i, j - i};
      i = j;
    }
    leaves = std::move(handles);
  }

  IRS_FORCE_INLINE static uint32_t Key(__uint128_t h) noexcept {
    const auto len = static_cast<uint8_t>(h);
    const auto c0 = static_cast<uint8_t>(h >> 32);
    return len * 256u + c0;
  }

  IRS_FORCE_INLINE bool Contains(__uint128_t h) const noexcept {
    const auto [off, count] = slots[Key(h)];
    for (uint32_t i = 0; i < count; ++i) {
      if (leaves[off + i] == h) {
        return true;
      }
    }
    return false;
  }

  std::vector<Slot> slots;
  std::vector<__uint128_t> leaves;
};

// A 256-bit bitset of the key lengths present in the set, indexed by
// min(size, 255) so it is sound for any key size. A probe whose length no key
// has is rejected with one shift, one load and one test -- no hash, no table
// line touched. Sound in one direction only (never a false negative), so it is
// a filter, not an answer.
//
// Whether it pays is a branch-prediction question, not an instruction-count
// one: the earlier first-byte-bitmap attempt lost because a ~60/40 filter
// mispredicts, and the mispredict costs more than the hash it saves. A length
// filter is near-useless on a real stopword list (every length is present) and
// near-coin-flip on a tiny mask -- but on long keys it skips a string hash plus
// memcmp, which is a much bigger prize than skipping a u128 hash.
struct LenFilter {
  void Add(size_t size) noexcept {
    const size_t i = size < 256 ? size : 255;
    bits[i >> 6] |= uint64_t{1} << (i & 63);
  }

  IRS_FORCE_INLINE bool Maybe(size_t size) const noexcept {
    const size_t i = size < 256 ? size : 255;
    return ((bits[i >> 6] >> (i & 63)) & 1) != 0;
  }

  std::array<uint64_t, 4> bits{};
};

// Length bitset in front of an inline-handle table. The handle's low 4 bytes
// are the string_t length field, so the probe needs no extra input.
template<typename Base>
struct LenGuarded {
  explicit LenGuarded(const std::vector<std::string>& w) : inner{w} {
    for (const auto& word : w) {
      if (word.size() <= duckdb::string_t::INLINE_LENGTH) {
        len.Add(word.size());
      }
    }
  }

  IRS_FORCE_INLINE bool Contains(__uint128_t h) const noexcept {
    if (!len.Maybe(static_cast<uint32_t>(h))) {
      return false;
    }
    return inner.Contains(h);
  }

  LenFilter len;
  Base inner;
};

// Same guard on the string_view path, where a rejected probe skips a string
// hash and a memcmp rather than a u128 hash.
template<typename Base>
struct LenGuardedMixed {
  explicit LenGuardedMixed(const std::vector<std::string>& w) : inner{w} {
    for (const auto& word : w) {
      len.Add(word.size());
    }
  }

  IRS_FORCE_INLINE bool Contains(std::string_view v) const noexcept {
    if (!len.Maybe(v.size())) {
      return false;
    }
    return inner.Contains(v);
  }

  LenFilter len;
  Base inner;
};

using BoostBlock64 = Boosted<boost::bloom::block<uint64_t, 2>>;
using BoostFast32 = Boosted<boost::bloom::fast_multiblock32<8>>;

// ================================== drivers =================================

// Inline-key family: every arm probes the same precomputed u128 handles.
template<typename F>
void RunInline(benchmark::State& state) {
  const auto n = static_cast<size_t>(state.range(0));
  const auto hit = static_cast<int>(state.range(1));
  const auto& corpus = GetCorpus(n, hit, 0.0);
  const F probe{corpus.members};
  for (auto _ : state) {
    size_t found = 0;
    for (const auto h : corpus.handles) {
      found += probe.Contains(h);
    }
    benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(corpus.handles.size()));
}

#define WORD_BENCH(name, type)                                        \
  void BM_##name(benchmark::State& state) { RunInline<type>(state); } \
  BENCHMARK(BM_##name)                                                \
    ->ArgsProduct({{40, 200, 5000, 50000, 150000}, {0, 16, 40}})      \
    ->ArgNames({"n", "hit"})

WORD_BENCH(Radix, Radix);
WORD_BENCH(Swiss, Swiss);
WORD_BENCH(SwissCheap, SwissCheap);
WORD_BENCH(BoostFlat, BoostFlat);
WORD_BENCH(BoostFlatCheap, BoostFlatCheap);
WORD_BENCH(Bloom, Bloom);
WORD_BENCH(BoostBlock64, BoostBlock64);
WORD_BENCH(BoostFast32, BoostFast32);
WORD_BENCH(LenSwiss, LenGuarded<Swiss>);
WORD_BENCH(LenBoostFlat, LenGuarded<BoostFlat>);

#undef WORD_BENCH

// Same all-inline corpus as the family above, but probed with the raw
// string_view instead of the precomputed u128 handle -- so `StrSwiss` against
// `Swiss` isolates ONE thing: the cost of running absl's string hasher over the
// bytes versus hashing the 16-byte canonical handle. Production probes the
// string form, which the handle-keyed arms above do not model.
struct StrSwiss {
  explicit StrSwiss(const std::vector<std::string>& w)
    : set{w.begin(), w.end()} {}

  IRS_FORCE_INLINE bool Contains(std::string_view v) const noexcept {
    return set.contains(v);
  }

  absl::flat_hash_set<std::string> set;
};

template<typename F>
void RunInlineStr(benchmark::State& state) {
  const auto n = static_cast<size_t>(state.range(0));
  const auto hit = static_cast<int>(state.range(1));
  const auto& corpus = GetCorpus(n, hit, 0.0);
  const F probe{corpus.members};
  for (auto _ : state) {
    size_t found = 0;
    for (const auto& value : corpus.probes) {
      found += probe.Contains(std::string_view{value});
    }
    benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(corpus.probes.size()));
}

void BM_StrSwiss(benchmark::State& state) { RunInlineStr<StrSwiss>(state); }
BENCHMARK(BM_StrSwiss)
  ->ArgsProduct({{40, 200, 5000, 50000, 150000}, {0, 16, 40}})
  ->ArgNames({"n", "hit"});

// PRODUCTION-FAITHFUL family. The handle-keyed arms above receive a
// PRECOMPUTED u128, so they price the hash and the probe but not the cost of
// building the key -- which flattered them and led to a wrong conclusion once
// already. Here both arms start from a `duckdb::string_t`, exactly what a
// tokenizer holds, and pay their own key construction inside the timed loop:
//   KeyStr     -- absl keyed by std::string, hashing the bytes (what ships)
//   KeyHandle  -- absl keyed by __uint128_t, memcpy'ing the string_t image
struct KeyStr {
  explicit KeyStr(const std::vector<std::string>& w)
    : set{w.begin(), w.end()} {}

  IRS_FORCE_INLINE bool Contains(const duckdb::string_t& v) const noexcept {
    return set.contains(std::string_view{v.GetData(), v.GetSize()});
  }

  absl::flat_hash_set<std::string> set;
};

struct KeyHandle {
  explicit KeyHandle(const std::vector<std::string>& w)
    : set{MakeSet<SwissSet>(w)} {}

  IRS_FORCE_INLINE bool Contains(const duckdb::string_t& v) const noexcept {
    return set.contains(InlineTermHandle(v));
  }

  SwissSet set;
};

template<typename F>
void RunFromView(benchmark::State& state) {
  const auto n = static_cast<size_t>(state.range(0));
  const auto hit = static_cast<int>(state.range(1));
  const auto& corpus = GetCorpus(n, hit, 0.0);
  const F probe{corpus.members};
  std::vector<duckdb::string_t> views;
  views.reserve(corpus.probes.size());
  for (const auto& value : corpus.probes) {
    views.push_back(
      MakeTermView(value.data(), static_cast<uint32_t>(value.size())));
  }
  for (auto _ : state) {
    size_t found = 0;
    for (const auto& view : views) {
      found += probe.Contains(view);
    }
    benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(views.size()));
}

#define KEY_BENCH(name, type)                                              \
  void BM_Key##name(benchmark::State& state) { RunFromView<type>(state); } \
  BENCHMARK(BM_Key##name)                                                  \
    ->ArgsProduct({{40, 200, 5000, 150000}, {0, 16, 40}})                  \
    ->ArgNames({"n", "hit"})

KEY_BENCH(Str, KeyStr);
KEY_BENCH(Handle, KeyHandle);

#undef KEY_BENCH

// Narrow-mask family: members occupy only lengths 3..5, probes keep the full
// distribution. This is the ONLY regime where a length prefilter has real work
// to do -- and it is simultaneously the regime where its branch is closest to a
// coin flip, which is what sank the earlier first-byte-bitmap attempt.
template<typename F>
void RunNarrow(benchmark::State& state) {
  const auto n = static_cast<size_t>(state.range(0));
  const auto hit = static_cast<int>(state.range(1));
  const auto& corpus = GetCorpus(n, hit, 0.0, true);
  const F probe{corpus.members};
  for (auto _ : state) {
    size_t found = 0;
    for (const auto h : corpus.handles) {
      found += probe.Contains(h);
    }
    benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(corpus.handles.size()));
}

#define NARROW_BENCH(name, type)                                            \
  void BM_Narrow##name(benchmark::State& state) { RunNarrow<type>(state); } \
  BENCHMARK(BM_Narrow##name)                                                \
    ->ArgsProduct({{4, 40, 200}, {0, 16, 40}})                              \
    ->ArgNames({"n", "hit"})

NARROW_BENCH(Swiss, Swiss);
NARROW_BENCH(LenSwiss, LenGuarded<Swiss>);
NARROW_BENCH(BoostFlat, BoostFlat);
NARROW_BENCH(LenBoostFlat, LenGuarded<BoostFlat>);
NARROW_BENCH(Bloom, Bloom);

#undef NARROW_BENCH

// Mixed-key family: 30% of the keys are 13..24B. Every arm takes a string_view,
// so this prices hashing a real string rather than a 16-byte handle.
struct SwissMixed {
  explicit SwissMixed(const std::vector<std::string>& w)
    : set{w.begin(), w.end()} {}

  IRS_FORCE_INLINE bool Contains(std::string_view v) const noexcept {
    return set.contains(v);
  }

  absl::flat_hash_set<std::string> set;
};

// boost::unordered only takes a heterogeneous key when BOTH the hash and the
// equality are transparent; absl::flat_hash_set<std::string> gets that from
// absl's own defaults.
struct TransparentHash {
  using is_transparent = void;

  size_t operator()(std::string_view v) const noexcept {
    return absl::HashOf(v);
  }
};

struct BoostFlatMixed {
  explicit BoostFlatMixed(const std::vector<std::string>& w)
    : set{w.begin(), w.end()} {}

  IRS_FORCE_INLINE bool Contains(std::string_view v) const noexcept {
    return set.contains(v);
  }

  boost::unordered_flat_set<std::string, TransparentHash, std::equal_to<>> set;
};

template<typename F>
void RunMixed(benchmark::State& state) {
  const auto n = static_cast<size_t>(state.range(0));
  const auto hit = static_cast<int>(state.range(1));
  const auto& corpus = GetCorpus(n, hit, 0.3);
  const F probe{corpus.members};
  for (auto _ : state) {
    size_t found = 0;
    for (const auto& value : corpus.probes) {
      found += probe.Contains(std::string_view{value});
    }
    benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(corpus.probes.size()));
}

#define MIXED_BENCH(name, type)                                           \
  void BM_Mixed##name(benchmark::State& state) { RunMixed<type>(state); } \
  BENCHMARK(BM_Mixed##name)                                               \
    ->ArgsProduct({{40, 5000, 150000}, {0, 40}})                          \
    ->ArgNames({"n", "hit"})

MIXED_BENCH(Swiss, SwissMixed);
MIXED_BENCH(BoostFlat, BoostFlatMixed);
MIXED_BENCH(LenSwiss, LenGuardedMixed<SwissMixed>);
MIXED_BENCH(LenBoostFlat, LenGuardedMixed<BoostFlatMixed>);

#undef MIXED_BENCH

// Map family: the synonym path, which resolves to a value rather than a bool.
using SynMap = absl::flat_hash_map<std::string, uint32_t>;

SynMap MakeMap(const std::vector<std::string>& w) {
  SynMap map;
  map.reserve(w.size());
  uint32_t i = 0;
  for (const auto& word : w) {
    map.emplace(word, i++);
  }
  return map;
}

struct SwissFind {
  explicit SwissFind(const std::vector<std::string>& w) : map{MakeMap(w)} {}

  IRS_FORCE_INLINE const uint32_t* Find(
    const duckdb::string_t& v) const noexcept {
    const auto it = map.find(std::string_view{v.GetData(), v.GetSize()});
    return it == map.end() ? nullptr : &it->second;
  }

  SynMap map;
};

template<typename F>
void RunFind(benchmark::State& state) {
  const auto n = static_cast<size_t>(state.range(0));
  const auto hit = static_cast<int>(state.range(1));
  const auto& corpus = GetCorpus(n, hit, 0.0);
  const F probe{corpus.members};
  std::vector<duckdb::string_t> views;
  views.reserve(corpus.probes.size());
  for (const auto& value : corpus.probes) {
    views.push_back(
      MakeTermView(value.data(), static_cast<uint32_t>(value.size())));
  }
  for (auto _ : state) {
    size_t found = 0;
    for (const auto& view : views) {
      found += probe.Find(view) != nullptr;
    }
    benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(views.size()));
}

#define FIND_BENCH(name, type)                                          \
  void BM_Find##name(benchmark::State& state) { RunFind<type>(state); } \
  BENCHMARK(BM_Find##name)                                              \
    ->ArgsProduct({{40, 5000, 50000, 150000}, {0, 16, 40}})             \
    ->ArgNames({"n", "hit"})

FIND_BENCH(Swiss, SwissFind);

#undef FIND_BENCH

}  // namespace

BENCHMARK_MAIN();
