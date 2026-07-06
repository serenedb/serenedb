
#include <benchmark/benchmark.h>

#include <string_view>

#include "fst/arcsort.h"
#include "fst/union.h"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace {

void RegexpBuild(benchmark::State& state) {
  for (auto _ : state) {
    auto a = irs::FromRegexp(std::string_view{"a.*[02468ace]x?y+z"});
    benchmark::DoNotOptimize(a.NumStates());
  }
  state.SetItemsProcessed(state.iterations());
}

void WildcardBuild(benchmark::State& state) {
  for (auto _ : state) {
    auto a = irs::FromWildcard(std::string_view{"a%b_c%d"});
    benchmark::DoNotOptimize(a.NumStates());
  }
  state.SetItemsProcessed(state.iterations());
}

void UnionBuild(benchmark::State& state) {
  for (auto _ : state) {
    auto u = irs::FromRegexp(std::string_view{"(?:.*x.*)|(?:a.*e)"});
    benchmark::DoNotOptimize(u.NumStates());
  }
  state.SetItemsProcessed(state.iterations());
}

void UnionNfaPrepOnly(benchmark::State& state) {
  const auto lhs = irs::FromRegexp(std::string_view{".*x.*"});
  const auto rhs = irs::FromRegexp(std::string_view{"a.*e"});
  for (auto _ : state) {
    irs::automaton nfa{lhs};
    fst::Union(&nfa, rhs);
    fst::ArcSort(&nfa, fst::ILabelCompare<irs::automaton::Arc>{});
    benchmark::DoNotOptimize(nfa.NumStates());
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(UnionNfaPrepOnly)->Unit(benchmark::kMicrosecond);
BENCHMARK(RegexpBuild)->Unit(benchmark::kMicrosecond);
BENCHMARK(WildcardBuild)->Unit(benchmark::kMicrosecond);
BENCHMARK(UnionBuild)->Unit(benchmark::kMicrosecond);

}  // namespace

BENCHMARK_MAIN();
