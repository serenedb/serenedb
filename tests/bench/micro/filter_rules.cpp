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

#include <benchmark/benchmark.h>

#include <memory>
#include <string_view>

#include "iresearch/search/filter_rules.hpp"

namespace {

auto MakeByTerm(std::string_view field,
                std::string_view term) -> std::unique_ptr<irs::ByTerm> {
  auto t = std::make_unique<irs::ByTerm>();
  *t->mutable_field() = field;
  auto bv = irs::ViewCast<irs::byte_type>(term);
  t->mutable_options()->term.assign(bv.data(), bv.size());
  return t;
}

auto BuildFlatAndOfByTerms(size_t n) -> irs::Filter::ptr {
  auto root = std::make_unique<irs::And>();
  for (size_t i = 0; i < n; ++i) {
    root->add(MakeByTerm("field", "term"));
  }
  return root;
}

auto BuildFlatAndOfEmpty(size_t n) -> irs::Filter::ptr {
  auto root = std::make_unique<irs::And>();
  for (size_t i = 0; i < n; ++i) {
    root->add<irs::Empty>();
  }
  return root;
}

auto BuildMixedFlatAnd(size_t n) -> irs::Filter::ptr {
  auto root = std::make_unique<irs::And>();
  for (size_t i = 0; i < n; ++i) {
    if ((i & 1u) == 0u) {
      root->add(MakeByTerm("field", "term"));
    } else {
      root->add<irs::Empty>();
    }
  }
  return root;
}

auto BuildNestedAnd(size_t depth) -> irs::Filter::ptr {
  auto root = std::make_unique<irs::And>();
  auto* current = root.get();
  for (size_t i = 0; i + 1 < depth; ++i) {
    current = &current->add<irs::And>();
    current->add<irs::Empty>();
  }
  return root;
}

auto BuildNotChain(size_t depth) -> irs::Filter::ptr {
  if (depth == 0) {
    return std::make_unique<irs::Empty>();
  }
  auto root = std::make_unique<irs::Not>();
  auto* current = root.get();
  for (size_t i = 1; i < depth; ++i) {
    current = &current->filter<irs::Not>();
  }
  current->filter<irs::Empty>();
  return root;
}

void BmBuildFlatAndOfByTerms(benchmark::State& state) {
  for (auto _ : state) {
    auto tree = BuildFlatAndOfByTerms(state.range(0));
    benchmark::DoNotOptimize(tree);
  }
}

void BmApplyEmptyPipeline(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  for (auto _ : state) {
    auto tree = BuildFlatAndOfByTerms(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmApplyNonMatchingRule(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::ByTermsFilterRule>();
  for (auto _ : state) {
    auto tree = BuildFlatAndOfEmpty(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmApplyByTermsFlat(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::ByTermsFilterRule>();
  for (auto _ : state) {
    auto tree = BuildFlatAndOfByTerms(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmApplyByTermsMixed(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::ByTermsFilterRule>();
  for (auto _ : state) {
    auto tree = BuildMixedFlatAnd(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmApplyAndFlatteningNested(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AndFlatteningFilterRule>();
  for (auto _ : state) {
    auto tree = BuildNestedAnd(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmApplyNotChain(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::NotFilterRule>();
  for (auto _ : state) {
    auto tree = BuildNotChain(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmApplyChainedRules(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::NotFilterRule>();
  constructor.Add<irs::AndFlatteningFilterRule>();
  constructor.Add<irs::OrFlatteningFilterRule>();
  constructor.Add<irs::ByTermsFilterRule>();
  for (auto _ : state) {
    auto tree = BuildMixedFlatAnd(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

BENCHMARK(BmBuildFlatAndOfByTerms)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyEmptyPipeline)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyNonMatchingRule)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyByTermsFlat)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyByTermsMixed)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyAndFlatteningNested)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyNotChain)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyChainedRules)->RangeMultiplier(4)->Range(1, 1024);

}  // namespace

BENCHMARK_MAIN();
