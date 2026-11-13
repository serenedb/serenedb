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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <benchmark/benchmark.h>

#include <cstdint>
#include <iresearch/formats/index/term_dict_prefixes.hpp>
#include <iresearch/utils/string.hpp>
#include <random>
#include <string>
#include <vector>

namespace {

constexpr uint64_t kTerms = uint64_t{1} << 20;

std::string Hex(uint64_t id) {
  std::mt19937_64 rng{id};
  return absl::StrFormat("%016x%016x", rng(), rng());
}

std::vector<irs::bstring> Sorted(std::vector<std::string> terms) {
  absl::c_sort(terms);
  std::vector<irs::bstring> out;
  out.reserve(terms.size());
  for (const auto& term : terms) {
    out.emplace_back(irs::ViewCast<irs::byte_type>(std::string_view{term}));
  }
  return out;
}

const std::vector<irs::bstring>& Urls() {
  static const auto kUrls = [] {
    std::vector<std::string> terms;
    terms.reserve(kTerms);
    for (uint64_t id = 0; id != kTerms; ++id) {
      terms.push_back(absl::StrCat("https://host", id % 1000,
                                   ".example.com/path", id % 500, "/", id,
                                   "?q=", Hex(id)));
    }
    return Sorted(std::move(terms));
  }();
  return kUrls;
}

const std::vector<irs::bstring>& Hexes() {
  static const auto kHexes = [] {
    std::vector<std::string> terms;
    terms.reserve(kTerms);
    for (uint64_t id = 0; id != kTerms; ++id) {
      terms.push_back(Hex(id));
    }
    return Sorted(std::move(terms));
  }();
  return kHexes;
}

void Build(benchmark::State& state, const std::vector<irs::bstring>& terms) {
  irs::term_dict::PrefixTrieBuilder builder;
  for (auto _ : state) {
    for (const auto& term : terms) {
      builder.Add(term);
    }
    benchmark::DoNotOptimize(builder.Finish());
    builder.Clear();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(terms.size()));
}

void BmBuildUrls(benchmark::State& state) { Build(state, Urls()); }
void BmBuildHexes(benchmark::State& state) { Build(state, Hexes()); }

BENCHMARK(BmBuildUrls)->Unit(benchmark::kMillisecond);
BENCHMARK(BmBuildHexes)->Unit(benchmark::kMillisecond);

}  // namespace
