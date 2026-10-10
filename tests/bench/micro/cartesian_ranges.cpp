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

#include <spatial/modules/boost/boost_geometry.hpp>
#include <unordered_map>

#include "connector/cartesian_covering.h"

namespace {

void CartesianRanges(benchmark::State& state) {
  const auto budget = static_cast<uint32_t>(state.range(0));
  const bool hilbert = state.range(1) != 0;
  const irs::curve::Options options{
    .max_cells = std::max(1u, budget), .hilbert = hilbert, .cartesian = true};
  std::vector<duckdb::BoostGeometry> shapes;
  std::unordered_map<std::string, std::vector<uint32_t>> postings;
  for (uint32_t id = 0; id < 256; ++id) {
    const auto x = static_cast<double>(id * 4);
    shapes.emplace_back(duckdb::BoostLinestring{{x, 0}, {x + 1024, 1024}});
    if (budget != 0) {
      const auto covering =
        sdb::connector::CoverCartesianGeometry(shapes.back(), options);
      for (const auto& term : irs::curve::Terms(covering, options, false)) {
        postings[term].push_back(id);
      }
    }
  }
  const duckdb::BoostGeometry query{duckdb::BoostPoint{512, 512}};
  uint64_t reads = 0;
  uint64_t candidates = 0;
  uint64_t returned = 0;
  for (auto _ : state) {
    reads = candidates = returned = 0;
    std::vector<bool> selected(shapes.size());
    if (budget == 0) {
      for (uint32_t id = 0; id <= 128; ++id) {
        selected[id] = true;
        ++reads;
      }
    } else {
      const auto covering =
        sdb::connector::CoverCartesianGeometry(query, options);
      for (const auto& term : irs::curve::Terms(covering, options, true)) {
        const auto it = postings.find(term);
        if (it == postings.end()) {
          continue;
        }
        for (auto id : it->second) {
          selected[id] = true;
          ++reads;
        }
      }
    }
    for (size_t id = 0; id < shapes.size(); ++id) {
      if (selected[id]) {
        ++candidates;
        returned += duckdb::EvalPredicate(duckdb::BoostPredicate::INTERSECTS,
                                          shapes[id], query);
      }
    }
    benchmark::DoNotOptimize(returned);
  }
  if (returned != 1) {
    state.SkipWithError(
      "Cartesian candidate filter lost the intersecting line");
  }
  state.counters["postings_read"] = reads;
  state.counters["candidates"] = candidates;
  state.counters["returned"] = returned;
}

BENCHMARK(CartesianRanges)
  ->Args({0, 0})
  ->Args({8, 0})
  ->Args({64, 0})
  ->Args({256, 0})
  ->Args({64, 1});

}  // namespace
