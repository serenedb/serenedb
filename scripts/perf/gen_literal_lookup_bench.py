#!/usr/bin/env python3
"""Generate tests/bench/micro/literal_lookup.cpp.

Benchmarks the best data structure for a *compile-time set of string literals*
across size x length-distribution x {case-sensitive, case-insensitive}, with a
50/50 hit/miss runtime query stream (keys are runtime values so the optimizer
cannot constant-fold the lookup -- the pitfall that makes naive TrivialSet
microbenchmarks lie).

Structures: linear scan, sorted+binary, std::unordered_set, absl::flat_hash_set,
absl::node_hash_set (DuckDB's case_insensitive_set_t), userver TrivialSet,
frozen perfect-hash set.
"""
import random
from pathlib import Path

SIZES = [10, 32, 100, 1000]
DISTS = ["varied", "same"]
POOL = 256  # query pool size (power of 2)

rng = random.Random(1234567)
ALPHA = "abcdefghijklmnopqrstuvwxyz0123456789_"


def gen_unique(n, dist):
    """n unique lowercase keys; 'varied' = lengths 3..24, 'same' = length 12."""
    out, seen = [], set()
    while len(out) < n:
        ln = 12 if dist == "same" else rng.randint(3, 24)
        s = "".join(rng.choice(ALPHA) for _ in range(ln))
        if s[0].isdigit():
            s = "k" + s[1:]
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def gen_misses(n, dist, member_set):
    out, seen = [], set()
    while len(out) < n:
        ln = 12 if dist == "same" else rng.randint(3, 24)
        s = "".join(rng.choice(ALPHA) for _ in range(ln))
        if s[0].isdigit():
            s = "k" + s[1:]
        if s not in member_set and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def mixed_case(s, salt):
    # deterministic per-char case flip so CI lookups actually fold case
    r = random.Random(hash(s) ^ salt ^ 99)
    return "".join(c.upper() if (c.isalpha() and r.random() < 0.5) else c for c in s)


def cpp_str(s):
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def build():
    parts = []
    bench_lines = []
    query_globals = []

    for size in SIZES:
        for dist in DISTS:
            keys = gen_unique(size, dist)
            tag = f"{size}_{dist}"

            # compile-time literal array (lowercase keys)
            arr = ",\n    ".join(cpp_str(k) for k in keys)
            parts.append(f"constexpr std::string_view kLit_{tag}[] = {{\n    {arr}}};")
            # TrivialSet (one object; CS via Contains, CI via ContainsICase)
            parts.append(f"constexpr auto kTriv_{tag} = duckdb::MakeTrivialSet<kLit_{tag}>();")
            # frozen CS perfect-hash
            fz = ",\n    ".join(f"frozen::string({cpp_str(k)})" for k in keys)
            parts.append(f"constexpr frozen::string kFz_{tag}[] = {{\n    {fz}}};")
            parts.append(f"constexpr auto kFrozCs_{tag} = frozen::make_unordered_set(kFz_{tag});")
            # frozen CI perfect-hash (CI hash + CI equal; keys already lowercase)
            parts.append(
                f"constexpr auto kFrozCi_{tag} = frozen::make_unordered_set(kFz_{tag}, CiElsa{{}}, CiEq{{}});")

            # runtime query pools (CS exact-case, CI mixed-case)
            half = POOL // 2
            hit_keys = [rng.choice(keys) for _ in range(half)]
            misses = gen_misses(half, dist, set(keys))
            cs_q = hit_keys + misses
            rng.shuffle(cs_q)
            ci_q = [mixed_case(k, i) for i, k in enumerate(hit_keys)] + \
                   [mixed_case(m, i + 7) for i, m in enumerate(misses)]
            rng.shuffle(ci_q)

            cs_init = ",\n    ".join(cpp_str(s) for s in cs_q)
            ci_init = ",\n    ".join(cpp_str(s) for s in ci_q)
            query_globals.append(f"static const std::vector<std::string> qCs_{tag} = {{\n    {cs_init}}};")
            query_globals.append(f"static const std::vector<std::string> qCi_{tag} = {{\n    {ci_init}}};")

    # ---- emit cells ----
    def cell(fn, helper, struct_expr, q):
        nm = f"{fn}"
        bench_lines.append(
            f"static void {nm}(benchmark::State& s) {{ {helper}(s, {struct_expr}, {q}); }}\n"
            f"BENCHMARK({nm});")

    for size in SIZES:
        for dist in DISTS:
            tag = f"{size}_{dist}"
            qcs, qci = f"qCs_{tag}", f"qCi_{tag}"
            # build runtime structures lazily via Get* (defined in template helpers)
            arr = f"kLit_{tag}"
            # --- CS ---
            cell(f"cs_linear_{tag}", "RunLinearCS", arr, qcs)
            cell(f"cs_sorted_{tag}", "RunSortedCS", f"GetSorted({arr})", qcs)
            cell(f"cs_std_{tag}", "RunHash", f"GetStdCs({arr})", qcs)
            cell(f"cs_abslflat_{tag}", "RunHash", f"GetAbslFlatCs({arr})", qcs)
            cell(f"cs_abslnode_{tag}", "RunHash", f"GetAbslNodeCs({arr})", qcs)
            cell(f"cs_trivial_{tag}", "RunTrivCs", f"kTriv_{tag}", qcs)
            cell(f"cs_frozen_{tag}", "RunFrozen", f"kFrozCs_{tag}", qcs)
            # --- CI ---
            cell(f"ci_linear_{tag}", "RunLinearCI", arr, qci)
            cell(f"ci_sorted_{tag}", "RunSortedCI", f"GetSortedCI({arr})", qci)
            cell(f"ci_std_{tag}", "RunHash", f"GetStdCi({arr})", qci)
            cell(f"ci_abslflat_{tag}", "RunHash", f"GetAbslFlatCi({arr})", qci)
            cell(f"ci_abslnode_{tag}", "RunHash", f"GetAbslNodeCi({arr})", qci)
            cell(f"ci_stdbad_{tag}", "RunHash", f"GetStdCiBad({arr})", qci)
            cell(f"ci_abslflatbad_{tag}", "RunHash", f"GetAbslFlatCiBad({arr})", qci)
            cell(f"ci_abslnodebad_{tag}", "RunHash", f"GetAbslNodeCiBad({arr})", qci)
            cell(f"ci_trivial_{tag}", "RunTrivCi", f"kTriv_{tag}", qci)
            cell(f"ci_frozen_{tag}", "RunFrozen", f"kFrozCi_{tag}", qci)

    return "\n\n".join(parts), "\n".join(query_globals), "\n\n".join(bench_lines)


LITERALS, QUERIES, BENCHES = build()

CPP = f'''// GENERATED by scripts/perf/gen_literal_lookup_bench.py -- do not edit by hand.
//
// Which data structure is fastest for a COMPILE-TIME set of string literals?
// Dimensions: size {{10,32,100,1000}} x length {{varied,same}} x {{CS,CI}}, 50/50
// hit/miss. Query keys are RUNTIME std::string (no constant-folding). Pin to one
// core on a quiet box: taskset -c N ... --benchmark_repetitions=10 \\
//   --benchmark_report_aggregates_only=true --benchmark_min_time=0.3s

#include <benchmark/benchmark.h>

#include <algorithm>
#include <array>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <absl/container/flat_hash_set.h>
#include <absl/hash/hash.h>
#include <frozen/string.h>
#include <frozen/unordered_set.h>

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/trivial_map.hpp"

namespace {{

using duckdb::CaseInsensitiveStringEquality;
using duckdb::CaseInsensitiveStringHashFunction;
using duckdb::StringUtil;

// --- frozen CI hash/equal (constexpr, fold ASCII case) ---
constexpr char Lower(char c) {{ return (c >= 'A' && c <= 'Z') ? char(c + 32) : c; }}
struct CiElsa {{
  constexpr std::size_t operator()(const frozen::string& v, std::size_t seed) const {{
    std::size_t h = seed ^ 0xcbf29ce484222325ull;
    for (std::size_t i = 0; i < v.size(); ++i) {{
      h = (h ^ static_cast<unsigned char>(Lower(v[i]))) * 0x100000001b3ull;
    }}
    return h;
  }}
}};
struct CiEq {{
  constexpr bool operator()(const frozen::string& a, const frozen::string& b) const {{
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i) {{
      if (Lower(a[i]) != Lower(b[i])) return false;
    }}
    return true;
  }}
}};

// ============================ literal sets ============================
{LITERALS}

// ============================ query pools =============================
{QUERIES}

// ===================== runtime structure builders =====================
template <std::size_t N>
const std::vector<std::string_view>& GetSorted(const std::string_view (&a)[N]) {{
  static const std::vector<std::string_view> v = [&] {{
    std::vector<std::string_view> r(a, a + N);
    std::sort(r.begin(), r.end());
    return r;
  }}();
  return v;
}}
template <std::size_t N>
const std::vector<std::string_view>& GetSortedCI(const std::string_view (&a)[N]) {{
  static const std::vector<std::string_view> v = [&] {{
    std::vector<std::string_view> r(a, a + N);
    std::sort(r.begin(), r.end(), [](std::string_view x, std::string_view y) {{ return StringUtil::CILessThan(x, y); }});
    return r;
  }}();
  return v;
}}
template <std::size_t N>
const std::unordered_set<std::string>& GetStdCs(const std::string_view (&a)[N]) {{
  static const std::unordered_set<std::string> s(a, a + N);
  return s;
}}
template <std::size_t N>
const std::unordered_set<std::string, CaseInsensitiveStringHashFunction, CaseInsensitiveStringEquality>&
GetStdCi(const std::string_view (&a)[N]) {{
  static const std::unordered_set<std::string, CaseInsensitiveStringHashFunction, CaseInsensitiveStringEquality>
      s(a, a + N);
  return s;
}}
template <std::size_t N>
const absl::flat_hash_set<std::string>& GetAbslFlatCs(const std::string_view (&a)[N]) {{
  static const absl::flat_hash_set<std::string> s(a, a + N);
  return s;
}}
template <std::size_t N>
const absl::flat_hash_set<std::string, CaseInsensitiveStringHashFunction, CaseInsensitiveStringEquality>&
GetAbslFlatCi(const std::string_view (&a)[N]) {{
  static const absl::flat_hash_set<std::string, CaseInsensitiveStringHashFunction, CaseInsensitiveStringEquality>
      s(a, a + N);
  return s;
}}
template <std::size_t N>
const duckdb::unordered_set<std::string>& GetAbslNodeCs(const std::string_view (&a)[N]) {{
  static const duckdb::unordered_set<std::string> s(a, a + N);
  return s;
}}
template <std::size_t N>
const duckdb::case_insensitive_set_t& GetAbslNodeCi(const std::string_view (&a)[N]) {{
  static const duckdb::case_insensitive_set_t s(a, a + N);
  return s;
}}
// "bad CI": NATIVE (case-sensitive) hash + CIEquals. Intentionally inconsistent
// (equal keys hash to different buckets), so mixed-case hits MISS -- this is NOT a
// correct CI set. It exists only to isolate the CIHash cost: its speed ~= native
// hash + miss path, so (real-CI - bad-CI) ~= the per-lookup CIHash penalty.
template <std::size_t N>
const std::unordered_set<std::string, std::hash<std::string>, CaseInsensitiveStringEquality>&
GetStdCiBad(const std::string_view (&a)[N]) {{
  static const std::unordered_set<std::string, std::hash<std::string>, CaseInsensitiveStringEquality> s(a, a + N);
  return s;
}}
template <std::size_t N>
const absl::flat_hash_set<std::string, absl::Hash<std::string>, CaseInsensitiveStringEquality>&
GetAbslFlatCiBad(const std::string_view (&a)[N]) {{
  static const absl::flat_hash_set<std::string, absl::Hash<std::string>, CaseInsensitiveStringEquality> s(a, a + N);
  return s;
}}
template <std::size_t N>
const duckdb::unordered_set<std::string, absl::Hash<std::string>, CaseInsensitiveStringEquality>&
GetAbslNodeCiBad(const std::string_view (&a)[N]) {{
  static const duckdb::unordered_set<std::string, absl::Hash<std::string>, CaseInsensitiveStringEquality> s(a, a + N);
  return s;
}}

// ============================ lookup loops ============================
// One lookup per benchmark iteration -> reported Time is ns/lookup (+ a constant
// index/mask shared by every structure, so comparisons are fair).
template <std::size_t N>
void RunLinearCS(benchmark::State& s, const std::string_view (&a)[N], const std::vector<std::string>& q) {{
  std::size_t i = 0, m = q.size() - 1;
  for (auto _ : s) {{
    i = (i + 1) & m;
    std::string_view key = q[i];
    bool found = false;
    for (std::size_t j = 0; j < N; ++j) {{ if (a[j] == key) {{ found = true; break; }} }}
    benchmark::DoNotOptimize(found);
  }}
}}
template <std::size_t N>
void RunLinearCI(benchmark::State& s, const std::string_view (&a)[N], const std::vector<std::string>& q) {{
  std::size_t i = 0, m = q.size() - 1;
  for (auto _ : s) {{
    i = (i + 1) & m;
    std::string_view key = q[i];
    bool found = false;
    for (std::size_t j = 0; j < N; ++j) {{ if (StringUtil::CIEquals(a[j], key)) {{ found = true; break; }} }}
    benchmark::DoNotOptimize(found);
  }}
}}
void RunSortedCS(benchmark::State& s, const std::vector<std::string_view>& v, const std::vector<std::string>& q) {{
  std::size_t i = 0, m = q.size() - 1;
  for (auto _ : s) {{
    i = (i + 1) & m;
    bool found = std::binary_search(v.begin(), v.end(), std::string_view(q[i]));
    benchmark::DoNotOptimize(found);
  }}
}}
void RunSortedCI(benchmark::State& s, const std::vector<std::string_view>& v, const std::vector<std::string>& q) {{
  std::size_t i = 0, m = q.size() - 1;
  for (auto _ : s) {{
    i = (i + 1) & m;
    std::string_view key = q[i];
    auto it = std::lower_bound(v.begin(), v.end(), key,
                               [](std::string_view x, std::string_view y) {{ return StringUtil::CILessThan(x, y); }});
    bool found = it != v.end() && StringUtil::CIEquals(*it, key);
    benchmark::DoNotOptimize(found);
  }}
}}
// Look up the std::string key directly: each hash set uses ITS OWN default
// hash/eq for CS, and DuckDB's CIHash/CIEquals for CI. count(const string&) is
// exact-key (no transparency needed), zero-copy.
template <class SetT>
void RunHash(benchmark::State& s, const SetT& set, const std::vector<std::string>& q) {{
  std::size_t i = 0, m = q.size() - 1;
  for (auto _ : s) {{
    i = (i + 1) & m;
    benchmark::DoNotOptimize(set.count(q[i]));
  }}
}}
template <class SetT>
void RunTrivCs(benchmark::State& s, const SetT& set, const std::vector<std::string>& q) {{
  std::size_t i = 0, m = q.size() - 1;
  for (auto _ : s) {{
    i = (i + 1) & m;
    benchmark::DoNotOptimize(set.Contains(std::string_view(q[i])));
  }}
}}
template <class SetT>
void RunTrivCi(benchmark::State& s, const SetT& set, const std::vector<std::string>& q) {{
  std::size_t i = 0, m = q.size() - 1;
  for (auto _ : s) {{
    i = (i + 1) & m;
    benchmark::DoNotOptimize(set.ContainsICase(std::string_view(q[i])));
  }}
}}
template <class SetT>
void RunFrozen(benchmark::State& s, const SetT& set, const std::vector<std::string>& q) {{
  std::size_t i = 0, m = q.size() - 1;
  for (auto _ : s) {{
    i = (i + 1) & m;
    benchmark::DoNotOptimize(set.count(frozen::string(q[i])));
  }}
}}

// ================================ cells ===============================
{BENCHES}

}}  // namespace

BENCHMARK_MAIN();
'''

out = Path("tests/bench/micro/literal_lookup.cpp")
out.write_text(CPP)
print(f"wrote {out} ({CPP.count(chr(10))} lines)")
