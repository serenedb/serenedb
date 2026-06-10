////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

// CI-hash candidate microbenchmark.
//
// Current DuckDB CIHash is byte-by-byte Jenkins (32-bit, serial) over a
// per-byte lowercase -- it is the entire cost of case-insensitive hash-set
// lookups (proven by the "bad CI" experiment: native hash + CIEquals ~= CS
// speed). This compares faster CI hashes both in isolation (ns/hash) and
// end-to-end as the hash of a std::unordered_set / absl::flat_hash_set CI
// lookup.
//
//   A: current StringUtil::CIHash       (Jenkins byte-by-byte, 32-bit)
//   B: per-byte lowercase + absl::Hash   (LowLevelHash, 64-bit) -- isolates the
//   hash C: SWAR (word-at-a-time) lowercase + absl::Hash D: SWAR lowercase +
//   XXH3_64bits
//
// Run pinned on a quiet box:
//   taskset -c N ./serenedb-bench-micro-ci_hash --benchmark_min_time=0.3s \
//     --benchmark_repetitions=12 --benchmark_report_aggregates_only=true

#define XXH_INLINE_ALL
#include <absl/container/flat_hash_set.h>
#include <absl/hash/hash.h>
#include <absl/strings/ascii.h>
#include <benchmark/benchmark.h>
#include <xxhash.h>  // lz4 header-only, no XXH_NAMESPACE

#include <cstdint>
#include <cstring>
#include <map>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_util.hpp"

namespace {

using duckdb::CaseInsensitiveStringEquality;

// ---------- SWAR branchless ASCII lowercase (folly String.cpp, file-local)
// ----------
inline void ToLowerAscii8(char& c) {
  auto rotated = uint8_t(c & 0x7f);
  rotated += 0x25;
  rotated &= 0x7f;
  rotated += 0x1a;
  rotated &= ~c;
  rotated >>= 2;
  rotated &= 0x20;
  c += char(rotated);
}
inline void ToLowerAscii32(uint32_t& c) {
  uint32_t rotated = c & uint32_t(0x7f7f7f7fUL);
  rotated += uint32_t(0x25252525UL);
  rotated &= uint32_t(0x7f7f7f7fUL);
  rotated += uint32_t(0x1a1a1a1aUL);
  rotated &= ~c;
  rotated >>= 2;
  rotated &= uint32_t(0x20202020UL);
  c += rotated;
}
inline void ToLowerAscii64(uint64_t& c) {
  uint64_t rotated = c & uint64_t(0x7f7f7f7f7f7f7f7fULL);
  rotated += uint64_t(0x2525252525252525ULL);
  rotated &= uint64_t(0x7f7f7f7f7f7f7f7fULL);
  rotated += uint64_t(0x1a1a1a1a1a1a1a1aULL);
  rotated &= ~c;
  rotated >>= 2;
  rotated &= uint64_t(0x2020202020202020ULL);
  c += rotated;
}

// memcpy-based load/store -> no alignment requirement on `out`.
inline void SwarLowerInto(const char* data, size_t len, char* out) {
  size_t i = 0;
  for (; i + 8 <= len; i += 8) {
    uint64_t w;
    std::memcpy(&w, data + i, 8);
    ToLowerAscii64(w);
    std::memcpy(out + i, &w, 8);
  }
  if (i + 4 <= len) {
    uint32_t w;
    std::memcpy(&w, data + i, 4);
    ToLowerAscii32(w);
    std::memcpy(out + i, &w, 4);
    i += 4;
  }
  for (; i < len; ++i) {
    char c = data[i];
    ToLowerAscii8(c);
    out[i] = c;
  }
}
inline void ByteLowerInto(const char* data, size_t len, char* out) {
  for (size_t i = 0; i < len; ++i) {
    out[i] = duckdb::StringUtil::CharacterToLower(data[i]);
  }
}

constexpr size_t kMaxLen = 4096;
thread_local char g_scratch[kMaxLen];

// abseil's Mix primitive (hash.h:1012): 128-bit multiply fold. 64-bit only.
inline uint64_t Mix(uint64_t lhs, uint64_t rhs) {
  __uint128_t m = static_cast<__uint128_t>(lhs) * rhs;
  return static_cast<uint64_t>(m) ^ static_cast<uint64_t>(m >> 64);
}

// ---------- candidate CI hashes ----------
inline uint64_t HashA(const char* d, size_t n) {
  return duckdb::StringUtil::CIHash(d, n);
}
inline uint64_t HashB(const char* d, size_t n) {
  ByteLowerInto(d, n, g_scratch);
  return absl::HashOf(std::string_view(g_scratch, n));
}
inline uint64_t HashC(const char* d, size_t n) {
  SwarLowerInto(d, n, g_scratch);
  return absl::HashOf(std::string_view(g_scratch, n));
}
inline uint64_t HashD(const char* d, size_t n) {
  SwarLowerInto(d, n, g_scratch);
  return XXH64(
    g_scratch, n,
    0);  // lz4 ships xxhash 0.6.5 (pre-XXH3); XXH64 is the fast 64-bit hash
}
// E: per-char absl::ascii_tolower (256-byte table lookup) + absl::Hash
inline uint64_t HashE(const char* d, size_t n) {
  for (size_t i = 0; i < n; ++i) {
    g_scratch[i] =
      static_cast<char>(absl::ascii_tolower(static_cast<unsigned char>(d[i])));
  }
  return absl::HashOf(std::string_view(g_scratch, n));
}
// F: abseil's own bulk string lowercase -- AsciiStrToLower (branchless, SIMD
// for >=16), the impl behind public absl::AsciiStrToLower(string_view), called
// into our scratch buffer (no std::string materialization, same as every other
// candidate) + absl::Hash.
inline uint64_t HashF(const char* d, size_t n) {
  absl::ascii_internal::AsciiStrToLower(g_scratch, d, n);
  return absl::HashOf(std::string_view(g_scratch, n));
}
// H: FUSED single pass -- SWAR-lowercase each 8-byte word inline, mix straight
// into abseil's Mix. No scratch buffer, no second read of the data.
inline uint64_t HashH(const char* d, size_t n) {
  constexpr uint64_t k0 = 0x2d358dccaa6c78a5ull, k1 = 0x8bb84b93962eacc9ull,
                     k2 = 0x4b33a62ed433d4a3ull;
  uint64_t state = k0 ^ n;
  size_t i = 0;
  for (; i + 8 <= n; i += 8) {
    uint64_t w;
    std::memcpy(&w, d + i, 8);
    ToLowerAscii64(w);
    state = Mix(w ^ k1, state);
  }
  if (i < n) {  // tail 1..7 bytes, zero-padded (zeros are unaffected by
                // lowercasing)
    uint64_t w = 0;
    std::memcpy(&w, d + i, n - i);
    ToLowerAscii64(w);
    state = Mix(w ^ k1, state);
  }
  return Mix(state, k2 ^ n);
}
inline uint32_t Load32(const char* p) {
  uint32_t v;
  std::memcpy(&v, p, 4);
  return v;
}
inline uint64_t Load64(const char* p) {
  uint64_t v;
  std::memcpy(&v, p, 8);
  return v;
}
// I: FUSED, fixed-width tail -- no variable-length memcpy. n>8: full words + an
// OVERLAPPING last-8 read; n<=8: branchless duplicating read (abseil-style). No
// buffer.
inline uint64_t HashI(const char* d, size_t n) {
  constexpr uint64_t k0 = 0x2d358dccaa6c78a5ull, k1 = 0x8bb84b93962eacc9ull,
                     k2 = 0x4b33a62ed433d4a3ull;
  uint64_t state = k0 ^ n;
  if (n <= 8) {
    if (n == 0) {
      return Mix(state, k2);
    }
    uint64_t w =
      n >= 4
        ? (static_cast<uint64_t>(Load32(d)) << 32) | Load32(d + n - 4)
        : (static_cast<uint64_t>(static_cast<unsigned char>(d[0])) << 16) |
            (static_cast<uint64_t>(static_cast<unsigned char>(d[n - 1])) << 8) |
            static_cast<unsigned char>(d[n / 2]);
    ToLowerAscii64(w);
    return Mix(w ^ k1, Mix(state, k2 ^ n));
  }
  size_t i = 0;
  for (; i + 8 < n; i += 8) {
    uint64_t w = Load64(d + i);
    ToLowerAscii64(w);
    state = Mix(w ^ k1, state);
  }
  uint64_t last =
    Load64(d + n - 8);  // overlapping last 8 bytes -- fixed width, no branch
  ToLowerAscii64(last);
  state = Mix(last ^ k1, state);
  return Mix(state, k2 ^ n);
}

// ===================== part 1: isolated hash cost (ns/hash)
// =====================
std::string TestString(size_t len) {
  static const char alphabet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_";
  std::string s;
  s.reserve(len);
  for (size_t i = 0; i < len; ++i) {
    s.push_back(alphabet[(i * 7 + 3) % (sizeof(alphabet) - 1)]);
  }
  return s;
}

template<uint64_t (*Fn)(const char*, size_t)>
void BmHash(benchmark::State& state) {
  const size_t len = static_cast<size_t>(state.range(0));
  std::string data = TestString(len);
  // correctness: case variants must hash equal
  std::string up = data, lo = data;
  for (auto& c : up) {
    c = duckdb::StringUtil::CharacterToUpper(c);
  }
  for (auto& c : lo) {
    c = duckdb::StringUtil::CharacterToLower(c);
  }
  if (Fn(up.data(), up.size()) != Fn(lo.data(), lo.size())) {
    state.SkipWithError("CI hash not case-insensitive");
    return;
  }
  for (auto _ : state) {
    benchmark::DoNotOptimize(data);
    benchmark::DoNotOptimize(Fn(data.data(), data.size()));
  }
}
BENCHMARK_TEMPLATE(BmHash, HashA)
  ->Arg(4)
  ->Arg(8)
  ->Arg(12)
  ->Arg(16)
  ->Arg(32)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(1024)
  ->Name("hashA_jenkins");
BENCHMARK_TEMPLATE(BmHash, HashB)
  ->Arg(4)
  ->Arg(8)
  ->Arg(12)
  ->Arg(16)
  ->Arg(32)
  ->Arg(64)
  ->Name("hashB_bytelower_absl");
BENCHMARK_TEMPLATE(BmHash, HashC)
  ->Arg(4)
  ->Arg(8)
  ->Arg(12)
  ->Arg(16)
  ->Arg(32)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(1024)
  ->Name("hashC_swar_absl");
BENCHMARK_TEMPLATE(BmHash, HashD)
  ->Arg(4)
  ->Arg(8)
  ->Arg(12)
  ->Arg(16)
  ->Arg(32)
  ->Arg(64)
  ->Name("hashD_swar_xxh64");
BENCHMARK_TEMPLATE(BmHash, HashE)
  ->Arg(4)
  ->Arg(8)
  ->Arg(12)
  ->Arg(16)
  ->Arg(32)
  ->Arg(64)
  ->Name("hashE_asciilower_absl");
BENCHMARK_TEMPLATE(BmHash, HashF)
  ->Arg(4)
  ->Arg(8)
  ->Arg(12)
  ->Arg(16)
  ->Arg(32)
  ->Arg(64)
  ->Name("hashF_abslstrlower_absl");
BENCHMARK_TEMPLATE(BmHash, HashH)
  ->Arg(4)
  ->Arg(8)
  ->Arg(12)
  ->Arg(16)
  ->Arg(32)
  ->Arg(64)
  ->Name("hashH_fused_swar_mix");
BENCHMARK_TEMPLATE(BmHash, HashI)
  ->Arg(4)
  ->Arg(8)
  ->Arg(12)
  ->Arg(16)
  ->Arg(32)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(1024)
  ->Name("hashI_fused_fixedtail");

// ===================== part 2: end-to-end CI set lookup =====================
// N=100 case-insensitive set; 50/50 hit/miss runtime queries (mixed case).
struct Data {
  std::vector<std::string> keys;
  std::vector<std::string> queries;  // 256, power of 2
};
const Data& GetData(size_t n) {
  static std::map<size_t, Data> cache;
  auto it = cache.find(n);
  if (it != cache.end()) {
    return it->second;
  }
  Data r;
  uint64_t st = 0x9e3779b97f4a7c15ull + n;
  auto next = [&] {
    st = st * 6364136223846793005ull + 1442695040888963407ull;
    return st;
  };
  auto make = [&](bool upper) {
    size_t len = 4 + (next() >> 58) % 17;
    std::string s;
    for (size_t i = 0; i < len; ++i) {
      char c = char('a' + (next() >> 56) % 26);
      s.push_back(
        upper && (next() & 1) ? duckdb::StringUtil::CharacterToUpper(c) : c);
    }
    return s;
  };
  for (size_t i = 0; i < n; ++i) {
    r.keys.push_back(make(false));  // lowercase keys
  }
  for (int i = 0; i < 128; ++i) {
    std::string up = r.keys[next() % r.keys.size()];  // mixed-case hit
    for (auto& c : up) {
      if (next() & 1) {
        c = duckdb::StringUtil::CharacterToUpper(c);
      }
    }
    r.queries.push_back(up);
  }
  for (int i = 0; i < 128; ++i) {
    r.queries.push_back(
      make(true));  // mixed-case miss (overwhelmingly not a key)
  }
  return cache.emplace(n, std::move(r)).first->second;
}

template<uint64_t (*Fn)(const char*, size_t)>
struct HashFn {
  using is_transparent = void;
  size_t operator()(std::string_view s) const { return Fn(s.data(), s.size()); }
};

template<template<class...> class Set, uint64_t (*Fn)(const char*, size_t)>
void BmSet(benchmark::State& state) {
  const auto& d = GetData(static_cast<size_t>(state.range(0)));
  Set<std::string, HashFn<Fn>, CaseInsensitiveStringEquality> set(
    d.keys.begin(), d.keys.end());
  if (set.count(d.queries[0]) == 0 && set.count(d.keys[0]) == 0) {
    state.SkipWithError("set lookup broken");
    return;
  }
  size_t i = 0, m = d.queries.size() - 1;
  for (auto _ : state) {
    i = (i + 1) & m;
    benchmark::DoNotOptimize(set.count(d.queries[i]));
  }
}
template<class K, class H, class E>
using StdSet = std::unordered_set<K, H, E>;
template<class K, class H, class E>
using AbslSet = absl::flat_hash_set<K, H, E>;

// Quality at scale: current Jenkins (A) vs abseil-of-lowered gold (C) vs fused
// (I). A clustering (poor-distribution) hash shows up as SLOW lookups as N
// grows.
BENCHMARK_TEMPLATE(BmSet, AbslSet, HashA)
  ->Arg(100)
  ->Arg(1000)
  ->Arg(10000)
  ->Name("set_absl_A_jenkins");
BENCHMARK_TEMPLATE(BmSet, AbslSet, HashC)
  ->Arg(100)
  ->Arg(1000)
  ->Arg(10000)
  ->Name("set_absl_C_abslOfLowered");
BENCHMARK_TEMPLATE(BmSet, AbslSet, HashI)
  ->Arg(100)
  ->Arg(1000)
  ->Arg(10000)
  ->Name("set_absl_I_fused");
BENCHMARK_TEMPLATE(BmSet, StdSet, HashI)
  ->Arg(100)
  ->Arg(1000)
  ->Arg(10000)
  ->Name("set_std_I_fused");

}  // namespace

BENCHMARK_MAIN();
