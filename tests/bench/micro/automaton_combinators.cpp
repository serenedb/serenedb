#include <benchmark/benchmark.h>

#include "iresearch/utils/automaton_combinators.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace {

constexpr std::string_view kWildcard1 = "pro%";
constexpr std::string_view kWildcard2 = "%ing";
constexpr std::string_view kRegexp1 = "^[a-z]{4,8}$";
constexpr std::string_view kRegexp2 = "^[aeiou].*";

void BmBuildWildcard(benchmark::State& state) {
  for (auto _ : state) {
    auto a = irs::FromWildcard(kWildcard1);
    benchmark::DoNotOptimize(a);
  }
}

void BmBuildRegexp(benchmark::State& state) {
  for (auto _ : state) {
    auto a = irs::FromRegexp(kRegexp1);
    benchmark::DoNotOptimize(a);
  }
}

void BmIntersectWildcardWildcard(benchmark::State& state) {
  const auto a1 = irs::FromWildcard(kWildcard1);
  const auto a2 = irs::FromWildcard(kWildcard2);
  for (auto _ : state) {
    auto r = irs::IntersectAutomatons(a1, a2);
    benchmark::DoNotOptimize(r);
  }
}

void BmIntersectRegexpRegexp(benchmark::State& state) {
  const auto a1 = irs::FromRegexp(kRegexp1);
  const auto a2 = irs::FromRegexp(kRegexp2);
  for (auto _ : state) {
    auto r = irs::IntersectAutomatons(a1, a2);
    benchmark::DoNotOptimize(r);
  }
}

void BmIntersectWildcardRegexp(benchmark::State& state) {
  const auto a1 = irs::FromWildcard(kWildcard1);
  const auto a2 = irs::FromRegexp(kRegexp1);
  for (auto _ : state) {
    auto r = irs::IntersectAutomatons(a1, a2);
    benchmark::DoNotOptimize(r);
  }
}

void BmUnionWildcardWildcard(benchmark::State& state) {
  const auto a1 = irs::FromWildcard(kWildcard1);
  const auto a2 = irs::FromWildcard(kWildcard2);
  for (auto _ : state) {
    auto r = irs::UnionAutomatons(a1, a2);
    benchmark::DoNotOptimize(r);
  }
}

void BmUnionRegexpRegexp(benchmark::State& state) {
  const auto a1 = irs::FromRegexp(kRegexp1);
  const auto a2 = irs::FromRegexp(kRegexp2);
  for (auto _ : state) {
    auto r = irs::UnionAutomatons(a1, a2);
    benchmark::DoNotOptimize(r);
  }
}

void BmUnionWildcardRegexp(benchmark::State& state) {
  const auto a1 = irs::FromWildcard(kWildcard1);
  const auto a2 = irs::FromRegexp(kRegexp1);
  for (auto _ : state) {
    auto r = irs::UnionAutomatons(a1, a2);
    benchmark::DoNotOptimize(r);
  }
}

void BmUnionDeMorganWildcardWildcard(benchmark::State& state) {
  const auto a1 = irs::FromWildcard(kWildcard1);
  const auto a2 = irs::FromWildcard(kWildcard2);
  for (auto _ : state) {
    auto r = irs::UnionAutomatonsDeMorgan(a1, a2);
    benchmark::DoNotOptimize(r);
  }
}

void BmUnionDeMorganRegexpRegexp(benchmark::State& state) {
  const auto a1 = irs::FromRegexp(kRegexp1);
  const auto a2 = irs::FromRegexp(kRegexp2);
  for (auto _ : state) {
    auto r = irs::UnionAutomatonsDeMorgan(a1, a2);
    benchmark::DoNotOptimize(r);
  }
}

void BmIntersectChain(benchmark::State& state) {
  const int n = state.range(0);
  std::vector<irs::automaton> inputs;
  inputs.reserve(n);
  for (int i = 0; i < n; ++i) {
    inputs.push_back((i & 1) == 0 ? irs::FromWildcard(kWildcard1)
                                   : irs::FromWildcard(kWildcard2));
  }
  for (auto _ : state) {
    irs::automaton acc = inputs[0];
    for (int i = 1; i < n; ++i) {
      acc = irs::IntersectAutomatons(acc, inputs[i]);
    }
    benchmark::DoNotOptimize(acc);
  }
}

void BmUnionChain(benchmark::State& state) {
  const int n = state.range(0);
  std::vector<irs::automaton> inputs;
  inputs.reserve(n);
  for (int i = 0; i < n; ++i) {
    inputs.push_back((i & 1) == 0 ? irs::FromWildcard(kWildcard1)
                                   : irs::FromWildcard(kWildcard2));
  }
  for (auto _ : state) {
    irs::automaton acc = inputs[0];
    for (int i = 1; i < n; ++i) {
      acc = irs::UnionAutomatons(acc, inputs[i]);
    }
    benchmark::DoNotOptimize(acc);
  }
}

BENCHMARK(BmBuildWildcard);
BENCHMARK(BmBuildRegexp);

BENCHMARK(BmIntersectWildcardWildcard);
BENCHMARK(BmIntersectRegexpRegexp);
BENCHMARK(BmIntersectWildcardRegexp);

BENCHMARK(BmUnionWildcardWildcard);
BENCHMARK(BmUnionRegexpRegexp);
BENCHMARK(BmUnionWildcardRegexp);

BENCHMARK(BmUnionDeMorganWildcardWildcard);
BENCHMARK(BmUnionDeMorganRegexpRegexp);

BENCHMARK(BmIntersectChain)->RangeMultiplier(2)->Range(2, 16);
BENCHMARK(BmUnionChain)->RangeMultiplier(2)->Range(2, 16);

}  // namespace

BENCHMARK_MAIN();
