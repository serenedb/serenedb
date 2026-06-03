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

#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/term_filter.hpp"

namespace {

irs::bytes_view AsBytes(std::string_view s) noexcept {
  return irs::ViewCast<irs::byte_type>(s);
}

irs::Filter::ptr MakeTerm() {
  auto term = std::make_unique<irs::ByTerm>();
  *term->mutable_field() = "kw";
  term->mutable_options()->term = AsBytes("term_0042");
  return term;
}

void AppendTerm(irs::BooleanFilter& root, std::string_view value) {
  auto& term = root.add<irs::ByTerm>();
  *term.mutable_field() = "kw";
  term.mutable_options()->term = AsBytes(value);
}

irs::Filter::ptr MakeNotChain(size_t depth) {
  auto root = std::make_unique<irs::Not>();
  auto* current = root.get();
  for (size_t i = 1; i < depth; ++i) {
    current = &current->filter<irs::Not>();
  }
  auto& term = current->filter<irs::ByTerm>();
  *term.mutable_field() = "kw";
  term.mutable_options()->term = AsBytes("term_0042");
  return root;
}

irs::Filter::ptr MakeNestedAnd() {
  auto root = std::make_unique<irs::And>();
  auto& inner = root->add<irs::And>();
  AppendTerm(inner, "term_0042");
  AppendTerm(inner, "term_0100");
  AppendTerm(*root, "term_0250");
  return root;
}

irs::Filter::ptr MakeNestedOr() {
  auto root = std::make_unique<irs::Or>();
  auto& inner = root->add<irs::Or>();
  AppendTerm(inner, "term_0042");
  AppendTerm(inner, "term_0100");
  AppendTerm(*root, "term_0250");
  return root;
}

void BmBuildLeaf(benchmark::State& state) {
  for (auto _ : state) {
    auto root = MakeTerm();
    benchmark::DoNotOptimize(root);
  }
}

void BmOptimizeLeaf(benchmark::State& state) {
  for (auto _ : state) {
    auto root = MakeTerm();
    irs::Optimize(root);
    benchmark::DoNotOptimize(root);
  }
}

void BmBuildNot(benchmark::State& state) {
  for (auto _ : state) {
    auto root = MakeNotChain(1);
    benchmark::DoNotOptimize(root);
  }
}

void BmOptimizeNot(benchmark::State& state) {
  for (auto _ : state) {
    auto root = MakeNotChain(1);
    irs::Optimize(root);
    benchmark::DoNotOptimize(root);
  }
}

void BmOptimizeNotChain8(benchmark::State& state) {
  for (auto _ : state) {
    auto root = MakeNotChain(8);
    irs::Optimize(root);
    benchmark::DoNotOptimize(root);
  }
}

void BmBuildNestedAnd(benchmark::State& state) {
  for (auto _ : state) {
    auto root = MakeNestedAnd();
    benchmark::DoNotOptimize(root);
  }
}

void BmOptimizeNestedAnd(benchmark::State& state) {
  for (auto _ : state) {
    auto root = MakeNestedAnd();
    irs::Optimize(root);
    benchmark::DoNotOptimize(root);
  }
}

void BmOptimizeNestedOr(benchmark::State& state) {
  for (auto _ : state) {
    auto root = MakeNestedOr();
    irs::Optimize(root);
    benchmark::DoNotOptimize(root);
  }
}

BENCHMARK(BmBuildLeaf);
BENCHMARK(BmOptimizeLeaf);
BENCHMARK(BmBuildNot);
BENCHMARK(BmOptimizeNot);
BENCHMARK(BmOptimizeNotChain8);
BENCHMARK(BmBuildNestedAnd);
BENCHMARK(BmOptimizeNestedAnd);
BENCHMARK(BmOptimizeNestedOr);

}  // namespace

BENCHMARK_MAIN();
