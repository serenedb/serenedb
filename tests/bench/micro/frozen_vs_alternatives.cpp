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

// Measures the exact frozen::set use-cases in vendored boost::text, to decide
// whether frozen can be replaced by something we already vendor. boost::text
// uses frozen::set in two shapes:
//
//   1. Interval lookup (grapheme/word/sentence/line_break.hpp): a small
//      constexpr frozen::set<prop_interval, N> (N = 6/24/28/49) is the
//      fall-back for codepoints missing from the bulk absl::flat_hash_map,
//      queried by frozen::bits::lower_bound over sorted [lo,hi) intervals.
//   2. Membership (detail/case_mapping_tables.hpp): frozen::set<uint32_t, 46>
//      SOFT_DOTTED_CPS, a plain .find() membership test.
//
// The arms deliberately separate ALGORITHM from STORAGE:
//   - "fzstore/*"  runs an algorithm over frozen's own contiguous constexpr
//     array (set.begin()/end() are const T*), so it shares frozen's storage
//     and only the search differs. This is the fair "could a plain
//     constexpr std::array + our search replace frozen?" comparison.
//   - "vec/*", "btree_set", "std_set", "*_hash_set" use runtime-sized
//     containers, showing the cost of losing compile-time contiguous storage.
//
// Reported items/s is per-lookup throughput (SetItemsProcessed).
//
// FINDINGS (build_perf, clang -O3 -mavx2, medians over repeated runs; the dev
// box is shared/noisy so absolute ns drift, but the relative order is stable):
//   - Break tables (N=6..49): frozen::bits::lower_bound<N> is fastest. Neither
//     a 4-byte SoA hi-bound layout, an AVX2 SIMD leaf, branchless-over-frozen-
//     storage, nor ordered sets beat it. The tables are L1-resident, so the
//     game is instruction throughput and frozen's fully-unrolled compile-time-N
//     branchless search already wins.
//   - Postings block (N=128): irs::branchless_lower_bound (== probablydance's
//     bit_floor form) is fastest. sb_lower_bound, galloping, Eytzinger (+the
//     O(n) transform it can't amortize on a search-once block), the SIMD-leaf
//     hybrid, frozen::bits<128>, and std::lower_bound all lose.
// Conclusion: keep BOTH current implementations; neither should replace the
// other, and the exotic structures (Eytzinger, S+tree) only pay off on large
// out-of-cache arrays, which these use-cases are not. This benchmark is kept
// as the record of that investigation.

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_set.h>
#include <benchmark/benchmark.h>
#include <frozen/bits/algorithms.h>
#include <frozen/set.h>
#include <immintrin.h>

#include <algorithm>
#include <array>
#include <boost/text/detail/case_mapping_tables.hpp>
#include <boost/text/grapheme_break.hpp>
#include <boost/text/line_break.hpp>
#include <boost/text/sentence_break.hpp>
#include <boost/text/word_break.hpp>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "iresearch/formats/posting/common.hpp"

namespace {

namespace bt = boost::text::detail;

// Deterministic pseudo-random codepoints spanning the whole Unicode range, so
// each arm pays the same mix of hits and misses.
std::vector<uint32_t> MakeQueries(size_t n) {
  std::vector<uint32_t> q;
  q.reserve(n);
  uint32_t x = 0x9E3779B9u;
  for (size_t i = 0; i < n; ++i) {
    x = x * 2654435761u + 1013904223u;
    q.push_back(x % 0x110000u);
  }
  return q;
}

const std::vector<uint32_t> kQueries = MakeQueries(4096);

// Wrap a checksum-returning callable into a registered benchmark that reports
// per-lookup throughput.
template<class F>
void Add(const std::string& name, F f) {
  benchmark::RegisterBenchmark(name, [f](benchmark::State& s) {
    for (auto _ : s) {
      benchmark::DoNotOptimize(f());
    }
    s.SetItemsProcessed(static_cast<int64_t>(s.iterations()) *
                        static_cast<int64_t>(kQueries.size()));
  });
}

// ---------------------------------------------------------------------------
// Generic uint32 lower_bound primitives (index of first element >= x, or n).
// Transform-free candidates suitable for build-once-search-once (postings).
// ---------------------------------------------------------------------------

// mh-dm branchless binary search, forced branchless via arithmetic (clang
// re-branches the `if` form, defeating the point).
size_t sb_lower_bound(const uint32_t* a, size_t n, uint32_t x) {
  const uint32_t* base = a;
  size_t length = n;
  while (length > 0) {
    const size_t half = length / 2;
    base += static_cast<size_t>(base[half] < x) * (length - half);
    length = half;
  }
  return static_cast<size_t>(base - a);
}

// Exponential (galloping) probe, then branchless binary in the window.
size_t galloping_lower_bound(const uint32_t* a, size_t n, uint32_t x) {
  size_t lo = 0;
  size_t step = 1;
  while (lo + step < n && a[lo + step] < x) {
    lo += step;
    step <<= 1;
  }
  size_t hi = lo + step + 1;
  if (hi > n) {
    hi = n;
  }
  const uint32_t* base = a + lo;
  size_t len = hi - lo;
  while (len > 0) {
    const size_t half = len / 2;
    if (base[half] < x) {
      base += len - half;
    }
    len = half;
  }
  return static_cast<size_t>(base - a);
}

// AVX2 SIMD scan for n <= 16 (unsigned-correct via max_epu32 + cmpeq).
size_t simd_lower_bound_le16(const uint32_t* a, size_t n, uint32_t x) {
  const __m256i lane0 = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
  const __m256i lane1 = _mm256_setr_epi32(8, 9, 10, 11, 12, 13, 14, 15);
  const __m256i vn = _mm256_set1_epi32(static_cast<int>(n));
  const __m256i vx = _mm256_set1_epi32(static_cast<int>(x));
  const __m256i lm0 = _mm256_cmpgt_epi32(vn, lane0);
  const __m256i lm1 = _mm256_cmpgt_epi32(vn, lane1);
  const __m256i a0 =
    _mm256_maskload_epi32(reinterpret_cast<const int*>(a), lm0);
  const __m256i a1 =
    _mm256_maskload_epi32(reinterpret_cast<const int*>(a) + 8, lm1);
  const __m256i ge0 = _mm256_cmpeq_epi32(_mm256_max_epu32(a0, vx), a0);
  const __m256i ge1 = _mm256_cmpeq_epi32(_mm256_max_epu32(a1, vx), a1);
  const unsigned m0 =
    static_cast<unsigned>(_mm256_movemask_ps(_mm256_castsi256_ps(ge0)));
  const unsigned m1 =
    static_cast<unsigned>(_mm256_movemask_ps(_mm256_castsi256_ps(ge1)));
  const unsigned m = (m0 | (m1 << 8)) & ((1u << n) - 1);
  return m ? static_cast<size_t>(__builtin_ctz(m)) : n;
}

// Branchless binary down to <= 16, then a branchy linear scan of the block.
size_t hybrid_lower_bound(const uint32_t* a, size_t n, uint32_t x) {
  constexpr size_t kBlock = 16;
  size_t lo = 0;
  size_t len = n;
  while (len > kBlock) {
    const size_t half = len / 2;
    if (a[lo + half] < x) {
      lo += len - half;
    }
    len = half;
  }
  const size_t end = lo + len;
  for (size_t i = lo; i < end; ++i) {
    if (a[i] >= x) {
      return i;
    }
  }
  return end;
}

// Branchless (arithmetic) binary down to <= 16, then a branchless SIMD leaf.
size_t hybrid_simd_lower_bound(const uint32_t* a, size_t n, uint32_t x) {
  constexpr size_t kBlock = 16;
  size_t lo = 0;
  size_t len = n;
  while (len > kBlock) {
    const size_t half = len / 2;
    lo += static_cast<size_t>(a[lo + half] < x) * (len - half);
    len = half;
  }
  return lo + simd_lower_bound_le16(a + lo, len, x);
}

// Compile-time-N (unrolled) variants -- the fair comparison against the current
// branchless arm, which inlines with a compile-time size (like doc_id_t[128]).
template<size_t N>
size_t sb_lower_bound_n(const uint32_t* a, uint32_t x) {
  const uint32_t* base = a;
  size_t length = N;
  while (length > 0) {
    const size_t half = length / 2;
    base += static_cast<size_t>(base[half] < x) * (length - half);
    length = half;
  }
  return static_cast<size_t>(base - a);
}

template<size_t N>
size_t hybrid_simd_lower_bound_n(const uint32_t* a, uint32_t x) {
  constexpr size_t kBlock = 16;
  size_t lo = 0;
  size_t len = N;
  while (len > kBlock) {
    const size_t half = len / 2;
    lo += static_cast<size_t>(a[lo + half] < x) * (len - half);
    len = half;
  }
  return lo + simd_lower_bound_le16(a + lo, len, x);
}

// probablydance/irs branchless as a (ptr, n, x) function; when n is a
// compile-time constant at the call site it unrolls like the postings path.
size_t pd_lower_bound(const uint32_t* a, size_t n, uint32_t x) {
  return static_cast<size_t>(irs::branchless_lower_bound(a, a + n, x) - a);
}

// ---------------------------------------------------------------------------
// Interval lookup
// ---------------------------------------------------------------------------

template<class Interval>
bool Contains(const Interval& iv, uint32_t cp) {
  return iv.lo_ <= cp && cp < iv.hi_;
}

template<class Interval>
size_t Weight(const Interval& iv) {
  return static_cast<size_t>(iv.prop_) + 1;
}

// frozen::bits::lower_bound over frozen's storage (what boost::text does).
template<class T, std::size_t N>
size_t FrozenBits(const frozen::set<T, N>& set) {
  size_t sum = 0;
  for (uint32_t cp : kQueries) {
    const T query{cp, cp + 1};
    const auto it =
      frozen::bits::lower_bound<N>(set.begin(), query, set.key_comp());
    if (it != set.end() && Contains(*it, cp)) {
      sum += Weight(*it);
    }
  }
  return sum;
}

// Same contiguous frozen storage, but std::lower_bound.
template<class T, std::size_t N>
size_t FzStoreStd(const frozen::set<T, N>& set) {
  size_t sum = 0;
  for (uint32_t cp : kQueries) {
    const T query{cp, cp + 1};
    const auto it = std::lower_bound(set.begin(), set.end(), query);
    if (it != set.end() && Contains(*it, cp)) {
      sum += Weight(*it);
    }
  }
  return sum;
}

// Same contiguous frozen storage, but our irs::branchless_lower_bound.
template<class T, std::size_t N>
size_t FzStoreBranchless(const frozen::set<T, N>& set) {
  size_t sum = 0;
  for (uint32_t cp : kQueries) {
    const T query{cp, cp + 1};
    const auto it = irs::branchless_lower_bound(set.begin(), set.end(), query);
    if (it != set.end() && Contains(*it, cp)) {
      sum += Weight(*it);
    }
  }
  return sum;
}

// Runtime-sized std::vector + our branchless search (storage cost reference).
template<class T>
size_t VecBranchless(const std::vector<T>& v) {
  size_t sum = 0;
  for (uint32_t cp : kQueries) {
    const T query{cp, cp + 1};
    const auto it = irs::branchless_lower_bound(v.begin(), v.end(), query);
    if (it != v.end() && Contains(*it, cp)) {
      sum += Weight(*it);
    }
  }
  return sum;
}

template<class Set>
size_t OrderedSet(const Set& set) {
  using T = typename Set::key_type;
  size_t sum = 0;
  for (uint32_t cp : kQueries) {
    const T query{cp, cp + 1};
    const auto it = set.lower_bound(query);
    if (it != set.end() && Contains(*it, cp)) {
      sum += Weight(*it);
    }
  }
  return sum;
}

// Structure-of-arrays view of the intervals: 4-byte hi/lo/prop arrays instead
// of frozen's 12-byte AoS structs. The containing-interval query becomes:
// first hi > cp (== lower_bound(cp+1) over hi), then confirm lo <= cp.
// hi is padded to a multiple of 16 with UINT32_MAX so SIMD leaves are safe.
template<std::size_t N>
struct IntervalSoA {
  static constexpr std::size_t kPad = ((N + 15) / 16) * 16;
  std::array<uint32_t, kPad> hi{};
  std::array<uint32_t, kPad> lo{};
  std::array<uint32_t, kPad> prop{};

  template<class T>
  explicit IntervalSoA(const frozen::set<T, N>& set) {
    std::size_t i = 0;
    for (const auto& iv : set) {
      lo[i] = iv.lo_;
      hi[i] = iv.hi_;
      prop[i] = static_cast<uint32_t>(iv.prop_);
      ++i;
    }
    for (; i < kPad; ++i) {
      hi[i] = UINT32_MAX;
      lo[i] = UINT32_MAX;
      prop[i] = 0;
    }
  }

  template<class LB>
  size_t Sum(LB lb) const {
    size_t sum = 0;
    for (uint32_t cp : kQueries) {
      const size_t idx = lb(hi.data(), cp + 1);
      if (idx < N && lo[idx] <= cp) {
        sum += static_cast<size_t>(prop[idx]) + 1;
      }
    }
    return sum;
  }
};

template<class T, std::size_t N>
void RegisterInterval(const std::string& name, const frozen::set<T, N>& set) {
  auto vec = std::make_shared<std::vector<T>>(set.begin(), set.end());
  std::sort(vec->begin(), vec->end());
  auto bset = std::make_shared<absl::btree_set<T>>(set.begin(), set.end());
  auto sset = std::make_shared<std::set<T>>(set.begin(), set.end());
  auto soa = std::make_shared<IntervalSoA<N>>(set);

  // Startup correctness check: SoA arms must match the frozen result.
  const size_t ref = FrozenBits(set);
  const size_t soa_pd = soa->Sum(
    [](const uint32_t* a, uint32_t x) { return pd_lower_bound(a, N, x); });
  const size_t soa_sd = soa->Sum([](const uint32_t* a, uint32_t x) {
    return hybrid_simd_lower_bound_n<N>(a, x);
  });
  if (ref != soa_pd || ref != soa_sd) {
    std::fprintf(stderr, "%s SoA VALIDATION FAILED: ref=%zu pd=%zu sd=%zu\n",
                 name.c_str(), ref, soa_pd, soa_sd);
    std::abort();
  }

  Add(name + "/frozen_bits", [&set] { return FrozenBits(set); });
  Add(name + "/fzstore_branchless", [&set] { return FzStoreBranchless(set); });
  Add(name + "/soa_branchless", [soa] {
    return soa->Sum(
      [](const uint32_t* a, uint32_t x) { return pd_lower_bound(a, N, x); });
  });
  Add(name + "/soa_hybrid_simd", [soa] {
    return soa->Sum([](const uint32_t* a, uint32_t x) {
      return hybrid_simd_lower_bound_n<N>(a, x);
    });
  });
  Add(name + "/vec_branchless", [vec] { return VecBranchless(*vec); });
  Add(name + "/btree_set", [bset] { return OrderedSet(*bset); });
  Add(name + "/std_set", [sset] { return OrderedSet(*sset); });
}

// ---------------------------------------------------------------------------
// Membership (SOFT_DOTTED_CPS)
// ---------------------------------------------------------------------------

void RegisterMembership() {
  const auto& soft = bt::SOFT_DOTTED_CPS;
  auto fhs =
    std::make_shared<absl::flat_hash_set<uint32_t>>(soft.begin(), soft.end());
  auto bts =
    std::make_shared<absl::btree_set<uint32_t>>(soft.begin(), soft.end());
  auto sst = std::make_shared<std::set<uint32_t>>(soft.begin(), soft.end());
  auto vec = std::make_shared<std::vector<uint32_t>>(soft.begin(), soft.end());
  std::sort(vec->begin(), vec->end());

  Add("soft_dotted/frozen", [] {
    size_t sum = 0;
    for (uint32_t cp : kQueries) {
      sum += bt::SOFT_DOTTED_CPS.find(cp) != bt::SOFT_DOTTED_CPS.end();
    }
    return sum;
  });
  // Frozen's contiguous storage, our branchless search.
  Add("soft_dotted/fzstore_branchless", [] {
    size_t sum = 0;
    const auto b = bt::SOFT_DOTTED_CPS.begin();
    const auto e = bt::SOFT_DOTTED_CPS.end();
    for (uint32_t cp : kQueries) {
      const auto it = irs::branchless_lower_bound(b, e, cp);
      sum += it != e && *it == cp;
    }
    return sum;
  });
  // Frozen's contiguous storage, std::binary_search.
  Add("soft_dotted/fzstore_binary_search", [] {
    size_t sum = 0;
    const auto b = bt::SOFT_DOTTED_CPS.begin();
    const auto e = bt::SOFT_DOTTED_CPS.end();
    for (uint32_t cp : kQueries) {
      sum += std::binary_search(b, e, cp);
    }
    return sum;
  });
  Add("soft_dotted/flat_hash_set", [fhs] {
    size_t sum = 0;
    for (uint32_t cp : kQueries) {
      sum += fhs->contains(cp);
    }
    return sum;
  });
  Add("soft_dotted/vec_branchless", [vec] {
    size_t sum = 0;
    for (uint32_t cp : kQueries) {
      const auto it = irs::branchless_lower_bound(vec->begin(), vec->end(), cp);
      sum += it != vec->end() && *it == cp;
    }
    return sum;
  });
  Add("soft_dotted/btree_set", [bts] {
    size_t sum = 0;
    for (uint32_t cp : kQueries) {
      sum += bts->contains(cp);
    }
    return sum;
  });
  Add("soft_dotted/std_set", [sst] {
    size_t sum = 0;
    for (uint32_t cp : kQueries) {
      sum += sst->contains(cp);
    }
    return sum;
  });
}

// ---------------------------------------------------------------------------
// Posting block: the real irs::branchless_lower_bound hot path
// (iterator_doc.hpp searches a full sorted doc_id_t[kBlockSize] per seek).
// This is the inverse question: should the postings code drop our runtime
// branchless_lower_bound for frozen::bits::lower_bound<kBlockSize> (which is
// N-templated and unrolls)?
// ---------------------------------------------------------------------------

constexpr std::size_t kBlockSize = 128;  // == doc_limits::kBlockSize

std::array<uint32_t, kBlockSize> MakeDocBlock() {
  std::array<uint32_t, kBlockSize> a{};
  uint32_t doc = 0;
  uint32_t x = 0x01234567u;
  for (size_t i = 0; i < kBlockSize; ++i) {
    x = x * 2654435761u + 1013904223u;
    doc += 1 + (x % 8u);  // monotonically increasing doc-ids, gaps 1..8
    a[i] = doc;
  }
  return a;
}

const std::array<uint32_t, kBlockSize> kDocBlock = MakeDocBlock();

std::vector<uint32_t> MakeTargets(size_t n, uint32_t max_doc) {
  std::vector<uint32_t> t;
  t.reserve(n);
  uint32_t x = 0x00ABCDEFu;
  for (size_t i = 0; i < n; ++i) {
    x = x * 2654435761u + 1013904223u;
    t.push_back(x % (max_doc + 2));
  }
  return t;
}

const std::vector<uint32_t> kTargets = MakeTargets(4096, kDocBlock.back());

// Eytzinger (BFS/heap) layout: branchless k = 2k + (e[k] < x), cache-friendly.
// Built once from the sorted block; 1-indexed (e[0] unused).
struct Eytzinger {
  std::array<uint32_t, kBlockSize + 1> e{};

  explicit Eytzinger(const std::array<uint32_t, kBlockSize>& a) {
    int idx = 0;
    Build(a, 1, idx);
  }

  void Build(const std::array<uint32_t, kBlockSize>& a, int k, int& idx) {
    if (k <= static_cast<int>(kBlockSize)) {
      Build(a, 2 * k, idx);
      e[k] = a[idx++];
      Build(a, 2 * k + 1, idx);
    }
  }

  // Value of first element >= x, or 0 if none (matches the other arms' sum).
  template<bool Prefetch>
  uint32_t LowerBound(uint32_t x) const {
    int k = 1;
    while (k <= static_cast<int>(kBlockSize)) {
      if constexpr (Prefetch) {
        __builtin_prefetch(e.data() + k * 16);
      }
      k = 2 * k + (e[k] < x);
    }
    k >>= __builtin_ffs(~k);
    return k ? e[k] : 0;
  }
};

const Eytzinger kEyt(kDocBlock);

void RegisterPostingBlock() {
  Add("posting_block/branchless", [] {
    size_t sum = 0;
    for (uint32_t tgt : kTargets) {
      const auto it =
        irs::branchless_lower_bound(kDocBlock.begin(), kDocBlock.end(), tgt);
      sum += it != kDocBlock.end() ? *it : 0;
    }
    return sum;
  });
  Add("posting_block/frozen_bits", [] {
    size_t sum = 0;
    for (uint32_t tgt : kTargets) {
      const auto it = frozen::bits::lower_bound<kBlockSize>(
        kDocBlock.begin(), tgt, std::less<uint32_t>{});
      sum += it != kDocBlock.end() ? *it : 0;
    }
    return sum;
  });
  Add("posting_block/std_lower_bound", [] {
    size_t sum = 0;
    for (uint32_t tgt : kTargets) {
      const auto it = std::lower_bound(kDocBlock.begin(), kDocBlock.end(), tgt);
      sum += it != kDocBlock.end() ? *it : 0;
    }
    return sum;
  });
  Add("posting_block/eytzinger", [] {
    size_t sum = 0;
    for (uint32_t tgt : kTargets) {
      sum += kEyt.LowerBound<false>(tgt);
    }
    return sum;
  });
  Add("posting_block/eytzinger_prefetch", [] {
    size_t sum = 0;
    for (uint32_t tgt : kTargets) {
      sum += kEyt.LowerBound<true>(tgt);
    }
    return sum;
  });
  const auto arm = [](auto fn) {
    return [fn](benchmark::State& s) {
      for (auto _ : s) {
        size_t sum = 0;
        for (uint32_t tgt : kTargets) {
          const size_t i = fn(kDocBlock.data(), kBlockSize, tgt);
          sum += i < kBlockSize ? kDocBlock[i] : 0;
        }
        benchmark::DoNotOptimize(sum);
      }
      s.SetItemsProcessed(static_cast<int64_t>(s.iterations()) *
                          static_cast<int64_t>(kTargets.size()));
    };
  };
  benchmark::RegisterBenchmark("posting_block/sb_runtime_n",
                               arm(sb_lower_bound));
  benchmark::RegisterBenchmark("posting_block/galloping",
                               arm(galloping_lower_bound));
  benchmark::RegisterBenchmark("posting_block/hybrid_simd_runtime_n",
                               arm(hybrid_simd_lower_bound));
  // Compile-time-N, inlined (fair vs the branchless arm).
  benchmark::RegisterBenchmark("posting_block/sb_n", [](benchmark::State& s) {
    for (auto _ : s) {
      size_t sum = 0;
      for (uint32_t tgt : kTargets) {
        const size_t i = sb_lower_bound_n<kBlockSize>(kDocBlock.data(), tgt);
        sum += i < kBlockSize ? kDocBlock[i] : 0;
      }
      benchmark::DoNotOptimize(sum);
    }
    s.SetItemsProcessed(static_cast<int64_t>(s.iterations()) *
                        static_cast<int64_t>(kTargets.size()));
  });
  benchmark::RegisterBenchmark(
    "posting_block/hybrid_simd_n", [](benchmark::State& s) {
      for (auto _ : s) {
        size_t sum = 0;
        for (uint32_t tgt : kTargets) {
          const size_t i =
            hybrid_simd_lower_bound_n<kBlockSize>(kDocBlock.data(), tgt);
          sum += i < kBlockSize ? kDocBlock[i] : 0;
        }
        benchmark::DoNotOptimize(sum);
      }
      s.SetItemsProcessed(static_cast<int64_t>(s.iterations()) *
                          static_cast<int64_t>(kTargets.size()));
    });
}

// Every posting-block candidate must return the identical checksum as the
// std::lower_bound reference, else the technique is buggy (a fast wrong search
// is useless). Aborts loudly on mismatch.
void ValidatePostingBlock() {
  size_t ref = 0;
  size_t eyt = 0;
  size_t sb = 0;
  size_t gal = 0;
  size_t hyl = 0;
  size_t hys = 0;
  const auto idx_val = [](size_t i) {
    return i < kBlockSize ? kDocBlock[i] : 0;
  };
  for (uint32_t tgt : kTargets) {
    const auto it = std::lower_bound(kDocBlock.begin(), kDocBlock.end(), tgt);
    ref += it != kDocBlock.end() ? *it : 0;
    eyt += kEyt.LowerBound<false>(tgt);
    sb += idx_val(sb_lower_bound_n<kBlockSize>(kDocBlock.data(), tgt));
    gal += idx_val(galloping_lower_bound(kDocBlock.data(), kBlockSize, tgt));
    hyl += idx_val(hybrid_lower_bound(kDocBlock.data(), kBlockSize, tgt));
    hys +=
      idx_val(hybrid_simd_lower_bound_n<kBlockSize>(kDocBlock.data(), tgt));
  }
  if (ref != eyt || ref != sb || ref != gal || ref != hyl || ref != hys) {
    std::fprintf(stderr,
                 "posting_block VALIDATION FAILED: ref=%zu eyt=%zu sb=%zu "
                 "gal=%zu hyl=%zu hys=%zu\n",
                 ref, eyt, sb, gal, hyl, hys);
    std::abort();
  }
}

}  // namespace

int main(int argc, char** argv) {
  ValidatePostingBlock();

  RegisterInterval("grapheme", bt::GRAPHEME_INTERVALS);
  RegisterInterval("word", bt::WORD_INTERVALS);
  RegisterInterval("sentence", bt::SENTENCE_INTERVALS);
  RegisterInterval("line", bt::LINE_INTERVALS);
  RegisterMembership();
  RegisterPostingBlock();

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
