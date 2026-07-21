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

// Case-insensitive compare: CIEquals (a==b) and CILessThan (a<b,
// lexicographic). Existing (absl, folly) vs our own SWAR / SIMD. Each candidate
// is correctness-checked against the current impl over adversarial inputs
// before timing.
//
//   taskset -c N ./serenedb-bench-micro-ci_compare --benchmark_min_time=0.3s \
//     --benchmark_repetitions=12 --benchmark_report_aggregates_only=true

#include <absl/strings/internal/memutil.h>
#include <absl/strings/match.h>
#include <benchmark/benchmark.h>
#ifdef SDB_ENABLE_FOLLY
#include <folly/Range.h>
#endif
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <vector>

namespace {

inline uint8_t Lower8(uint8_t c) {
  return (c >= 'A' && c <= 'Z') ? uint8_t(c + 32) : c;
}
inline uint64_t Load64(const char* p) {
  uint64_t v;
  std::memcpy(&v, p, 8);
  return v;
}
inline uint64_t Lower64(uint64_t c) {
  uint64_t r = c & 0x7f7f7f7f7f7f7f7full;
  r += 0x2525252525252525ull;
  r &= 0x7f7f7f7f7f7f7f7full;
  r += 0x1a1a1a1a1a1a1a1aull;
  r &= ~c;
  r >>= 2;
  r &= 0x2020202020202020ull;
  return c + r;
}
#if defined(__x86_64__) || defined(__i386__)
inline __m128i Lower128(__m128i v) {
  __m128i biased = _mm_add_epi8(v, _mm_set1_epi8(int8_t(0x80 - 'A')));
  __m128i is_up =
    _mm_cmpgt_epi8(_mm_set1_epi8(int8_t('Z' + 0x80 - 'A' + 1)), biased);
  return _mm_or_si128(v, _mm_and_si128(is_up, _mm_set1_epi8(0x20)));
}
#endif

// independent scalar oracle (shares no code with the abseil impls): used to
// transitively validate the patched EqualsIgnoreCase / memcasecmp via the
// harness.
bool EqNaive(const char* a, size_t an, const char* b, size_t bn) {
  if (an != bn) {
    return false;
  }
  for (size_t i = 0; i < an; ++i) {
    if (Lower8(uint8_t(a[i])) != Lower8(uint8_t(b[i]))) {
      return false;
    }
  }
  return true;
}
bool LtNaive(const char* a, size_t an, const char* b, size_t bn) {
  size_t n = std::min(an, bn);
  for (size_t i = 0; i < n; ++i) {
    uint8_t ca = Lower8(uint8_t(a[i])), cb = Lower8(uint8_t(b[i]));
    if (ca != cb) {
      return ca < cb;
    }
  }
  return an < bn;
}

// ===================== CIEquals candidates =====================
bool EqAbsl(const char* a, size_t an, const char* b, size_t bn) {
  return absl::EqualsIgnoreCase({a, an}, {b, bn});
}
#ifdef SDB_ENABLE_FOLLY
bool EqFolly(const char* a, size_t an, const char* b, size_t bn) {
  if (an != bn) {
    return false;
  }
  return std::equal(a, a + an, b, folly::AsciiCaseInsensitive{});
}
#endif
bool EqSwar(const char* a, size_t an, const char* b, size_t bn) {
  if (an != bn) {
    return false;
  }
  size_t n = an, i = 0;
  for (; i + 8 <= n; i += 8) {
    if (Lower64(Load64(a + i)) != Lower64(Load64(b + i))) {
      return false;
    }
  }
  if (i < n) {
    if (n >= 8) {  // overlapping last 8
      size_t t = n - 8;
      if (Lower64(Load64(a + t)) != Lower64(Load64(b + t))) {
        return false;
      }
    } else {
      for (; i < n; ++i) {
        if (Lower8(uint8_t(a[i])) != Lower8(uint8_t(b[i]))) {
          return false;
        }
      }
    }
  }
  return true;
}
#if defined(__x86_64__) || defined(__i386__)
bool EqSimd(const char* a, size_t an, const char* b, size_t bn) {
  if (an != bn) {
    return false;
  }
  size_t n = an, i = 0;
  for (; i + 16 <= n; i += 16) {
    __m128i va =
      Lower128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(a + i)));
    __m128i vb =
      Lower128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(b + i)));
    if (_mm_movemask_epi8(_mm_cmpeq_epi8(va, vb)) != 0xFFFF) {
      return false;
    }
  }
  return EqSwar(a + i, n - i, b + i, n - i);
}
#endif

// ===================== CILessThan candidates =====================
// current: absl memcasecmp + length tiebreak
bool LtAbsl(const char* a, size_t an, const char* b, size_t bn) {
  size_t n = std::min(an, bn);
  int r = absl::strings_internal::memcasecmp(a, b, n);
  return r != 0 ? r < 0 : an < bn;
}
bool LtSwar(const char* a, size_t an, const char* b, size_t bn) {
  size_t n = std::min(an, bn), i = 0;
  for (; i + 8 <= n; i += 8) {
    uint64_t wa = Lower64(Load64(a + i));
    uint64_t wb = Lower64(Load64(b + i));
    if (wa != wb) {
      // lexicographic = big-endian integer compare of the lowered bytes
      return __builtin_bswap64(wa) < __builtin_bswap64(wb);
    }
  }
  for (; i < n; ++i) {
    uint8_t ca = Lower8(uint8_t(a[i])), cb = Lower8(uint8_t(b[i]));
    if (ca != cb) {
      return ca < cb;
    }
  }
  return an < bn;
}
#if defined(__x86_64__) || defined(__i386__)
bool LtSimd(const char* a, size_t an, const char* b, size_t bn) {
  size_t n = std::min(an, bn), i = 0;
  for (; i + 16 <= n; i += 16) {
    __m128i va =
      Lower128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(a + i)));
    __m128i vb =
      Lower128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(b + i)));
    unsigned m = unsigned(_mm_movemask_epi8(_mm_cmpeq_epi8(va, vb)));
    if (m != 0xFFFFu) {
      unsigned d =
        unsigned(__builtin_ctz(~m & 0xFFFFu));  // first differing byte
      uint8_t ca = Lower8(uint8_t(a[i + d])), cb = Lower8(uint8_t(b[i + d]));
      return ca < cb;
    }
  }
  return LtSwar(a + i, an - i, b + i, bn - i);
}
#endif

// ===================== correctness harness =====================
std::vector<std::string> Corpus() {
  std::vector<std::string> v;
  std::mt19937 rng(99);
  const char* al =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_@[`{ ";
  for (int t = 0; t < 400; ++t) {
    size_t len = rng() % 40;  // 0..39, hits all paths incl >16
    std::string s;
    for (size_t i = 0; i < len; ++i) {
      s.push_back(al[rng() % 67]);
    }
    v.push_back(s);
  }
  // adversarial exact pairs around the +-32 traps + high bytes
  v.push_back("@");
  v.push_back("`");
  v.push_back("[");
  v.push_back("{");
  v.push_back(std::string(1, char(0x80)));
  v.push_back(std::string(1, char(0xE0)));
  return v;
}
bool CheckEq(bool (*fn)(const char*, size_t, const char*, size_t),
             const std::vector<std::string>& c) {
  for (auto& a : c) {
    for (auto& b : c) {
      if (fn(a.data(), a.size(), b.data(), b.size()) !=
          EqAbsl(a.data(), a.size(), b.data(), b.size())) {
        return false;
      }
    }
  }
  return true;
}
bool CheckLt(bool (*fn)(const char*, size_t, const char*, size_t),
             const std::vector<std::string>& c) {
  for (auto& a : c) {
    for (auto& b : c) {
      if (fn(a.data(), a.size(), b.data(), b.size()) !=
          LtAbsl(a.data(), a.size(), b.data(), b.size())) {
        return false;
      }
    }
  }
  return true;
}

// realistic query pairs: ~half equal-CI (hit), half same-len-differ / diff-len
// (miss), short (the hash/tree regime). runtime strings, power-of-2 pool.
struct Pairs {
  std::vector<std::string> a, b;
};
const Pairs& GetPairs() {
  static const Pairs p = [] {
    Pairs r;
    std::mt19937 rng(7);
    const char* lo = "abcdefghijklmnopqrstuvwxyz0123456789_";
    auto mk = [&](size_t n) {
      std::string s;
      for (size_t i = 0; i < n; ++i) {
        s.push_back(lo[rng() % 37]);
      }
      return s;
    };
    auto mixed = [&](std::string s) {
      for (auto& ch : s) {
        if (ch >= 'a' && ch <= 'z' && (rng() & 1)) {
          ch = char(ch - 32);
        }
      }
      return s;
    };
    for (int i = 0; i < 256; ++i) {
      std::string key = mk(3 + rng() % 24);  // 3..26 bytes
      r.a.push_back(key);
      int kind = i % 4;
      if (kind == 0) {
        r.b.push_back(mixed(key));  // equal CI (hit)
      } else if (kind == 1) {
        std::string m = key;  // same length, differ near the end
        if (!m.empty()) {
          m.back() = lo[(m.back() + 1) % 37 < 37 ? rng() % 37 : 0];
        }
        r.b.push_back(mixed(m));
      } else if (kind == 2) {
        r.b.push_back(
          mixed(key.substr(0, key.size() / 2)));  // prefix (diff length)
      } else {
        r.b.push_back(mixed(mk(3 + rng() % 24)));  // unrelated
      }
    }
    return r;
  }();
  return p;
}

template<bool (*Fn)(const char*, size_t, const char*, size_t), bool IsLt>
void Bm(benchmark::State& s) {
  const auto& c = Corpus();
  bool ok = IsLt ? CheckLt(Fn, c) : CheckEq(Fn, c);
  if (!ok) {
    s.SkipWithError("incorrect vs reference");
    return;
  }
  const auto& p = GetPairs();
  size_t i = 0, m = p.a.size() - 1;
  for (auto _ : s) {
    i = (i + 1) & m;
    benchmark::DoNotOptimize(
      Fn(p.a[i].data(), p.a[i].size(), p.b[i].data(), p.b[i].size()));
  }
}

}  // namespace

BENCHMARK_TEMPLATE(Bm, EqNaive, false)->Name("eq_naive_ref");
BENCHMARK_TEMPLATE(Bm, LtNaive, true)->Name("lt_naive_ref");
BENCHMARK_TEMPLATE(Bm, EqAbsl, false)->Name("eq_absl_current");
#ifdef SDB_ENABLE_FOLLY
BENCHMARK_TEMPLATE(Bm, EqFolly, false)->Name("eq_folly");
#endif
BENCHMARK_TEMPLATE(Bm, EqSwar, false)->Name("eq_swar");
#if defined(__x86_64__) || defined(__i386__)
BENCHMARK_TEMPLATE(Bm, EqSimd, false)->Name("eq_simd");
#endif
BENCHMARK_TEMPLATE(Bm, LtAbsl, true)->Name("lt_absl_current");
BENCHMARK_TEMPLATE(Bm, LtSwar, true)->Name("lt_swar");
#if defined(__x86_64__) || defined(__i386__)
BENCHMARK_TEMPLATE(Bm, LtSimd, true)->Name("lt_simd");
#endif

BENCHMARK_MAIN();
