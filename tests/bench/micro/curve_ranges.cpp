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

#include <iresearch/utils/space_filling_curve.hpp>
#include <map>
#include <numeric>
#include <random>
#include <unordered_map>

namespace {

using namespace irs::curve;
using Posting = std::vector<uint32_t>;
using Dictionary = std::unordered_map<std::string, Posting>;

struct Sample {
  std::vector<Point> points;
  std::array<std::array<std::map<uint64_t, Posting>, 4>, kMaxDimensions>
    numeric;
  Dictionary curve;
  Options options;
  Box query;
  uint32_t constrained;

  Sample(uint32_t dimensions, bool correlated, bool partial, uint32_t budget,
         uint32_t method)
    : options{.dimensions = dimensions,
              .max_cells = budget,
              .hilbert = method == 2,
              .level_step = DefaultLevelStep(dimensions)},
      constrained{partial ? 1u : dimensions} {
    std::mt19937_64 random{1099};
    std::uniform_real_distribution<double> values{0, 1000};
    for (uint32_t id = 0; id < 8192; ++id) {
      Point point{};
      const auto shared = values(random);
      for (uint32_t axis = 0; axis < dimensions; ++axis) {
        const auto value =
          correlated ? shared + values(random) * 0.01 : values(random);
        point[axis] = EncodeDouble(value);
        if (method == 0) {
          for (uint32_t level = 0; level < 4; ++level) {
            const auto shift = level * 16;
            numeric[axis][level][(point[axis] >> shift) << shift].push_back(id);
          }
        }
      }
      points.push_back(point);
      if (method != 0) {
        PointTerms(point, options, [&](std::span<const uint8_t> term) {
          curve[std::string{term.begin(), term.end()}].push_back(id);
        });
      }
    }
    for (uint32_t axis = 0; axis < dimensions; ++axis) {
      const auto center = DecodeDouble(points[0][axis]);
      query.min[axis] = axis < constrained ? EncodeDouble(center - 25) : 0;
      query.max[axis] =
        axis < constrained ? EncodeDouble(center + 25) : UINT64_MAX;
    }
  }

  void Numeric(uint32_t axis, uint64_t min, uint64_t max, uint32_t level,
               std::vector<uint32_t>& counts, uint64_t& reads) const {
    const auto shift = level * 16;
    const auto& terms = numeric[axis][level];
    auto it = terms.lower_bound((min >> shift) << shift);
    for (; it != terms.end() && it->first <= max; ++it) {
      const auto end = it->first | ((uint64_t{1} << shift) - 1);
      if (it->first >= min && end <= max) {
        for (auto doc : it->second) {
          ++counts[doc];
          ++reads;
        }
      } else if (level != 0) {
        Numeric(axis, std::max(min, it->first), std::min(max, end), level - 1,
                counts, reads);
      }
    }
  }

  std::array<uint64_t, 4> Run(uint32_t method) const {
    uint64_t reads = 0;
    uint64_t candidates = 0;
    uint64_t returned = 0;
    uint64_t cells = 0;
    std::vector<uint32_t> counts(points.size());
    if (method == 0) {
      for (uint32_t axis = 0; axis < constrained; ++axis) {
        Numeric(axis, query.min[axis], query.max[axis], 3, counts, reads);
      }
    } else {
      const auto cover = CoverBox(query, options);
      cells = cover.size();
      for (const auto& term : PointQueryTerms(cover, query, options)) {
        const auto it = curve.find(term);
        if (it == curve.end()) {
          continue;
        }
        for (auto doc : it->second) {
          counts[doc] = constrained;
          ++reads;
        }
      }
    }
    for (size_t id = 0; id < points.size(); ++id) {
      if (counts[id] != constrained) {
        continue;
      }
      ++candidates;
      bool match = true;
      for (uint32_t axis = 0; axis < options.dimensions; ++axis) {
        match &= points[id][axis] >= query.min[axis] &&
                 points[id][axis] <= query.max[axis];
      }
      returned += match;
    }
    return {reads, candidates, returned, cells};
  }
};

void CurveRanges(benchmark::State& state) {
  const auto method = static_cast<uint32_t>(state.range(4));
  const Sample sample{static_cast<uint32_t>(state.range(0)),
                      state.range(1) != 0, state.range(2) != 0,
                      static_cast<uint32_t>(state.range(3)), method};
  const auto expected = std::count_if(
    sample.points.begin(), sample.points.end(), [&](const Point& p) {
      for (uint32_t axis = 0; axis < sample.options.dimensions; ++axis) {
        if (p[axis] < sample.query.min[axis] ||
            p[axis] > sample.query.max[axis]) {
          return false;
        }
      }
      return true;
    });
  std::array<uint64_t, 4> result{};
  for (auto _ : state) {
    result = sample.Run(method);
    benchmark::DoNotOptimize(result);
  }
  if (result[2] != static_cast<uint64_t>(expected)) {
    state.SkipWithError(
      "curve result differs from exact per-column intersection");
  }
  state.counters["postings_read"] = result[0];
  state.counters["candidates"] = result[1];
  state.counters["returned"] = result[2];
  state.counters["cells"] = result[3];
  state.counters["index_terms"] = static_cast<double>(sample.curve.size());
}

void Arguments(benchmark::Benchmark* bench) {
  for (int64_t dimensions : {2, 3, 4}) {
    for (int64_t correlated : {0, 1}) {
      for (int64_t partial : {0, 1}) {
        bench->Args({dimensions, correlated, partial, 64, 0});
        for (int64_t budget : {8, 64, 256}) {
          for (int64_t method : {1, 2}) {
            bench->Args({dimensions, correlated, partial, budget, method});
          }
        }
      }
    }
  }
}

BENCHMARK(CurveRanges)
  ->Apply(Arguments)
  ->ArgNames({"d", "correlated", "partial", "budget", "method"});

}  // namespace
