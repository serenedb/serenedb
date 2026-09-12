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

// Prices reading a vector through the typed accessor against the hand-written
// unified-format walk, so a conversion of an ingest or index-build loop can be
// decided on measurement rather than taste. Both arms flatten once per
// invocation and then read every row, which is the shape of the loops that
// remain on the unified form.
//
//   perf stat -e instructions ./serenedb-bench-micro-vector_access \
//     --benchmark_filter='^BM_Strings/' --benchmark_min_time=2000x

#include <benchmark/benchmark.h>

#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <random>
#include <string>
#include <vector>

namespace {

constexpr duckdb::idx_t kRows = 2048;

enum class Shape {
  Flat,
  FlatNulls,
  Dictionary,
};

const std::vector<std::string>& Corpus() {
  static const auto corpus = [] {
    std::mt19937_64 rng{7};
    std::vector<std::string> out;
    out.reserve(kRows);
    for (duckdb::idx_t i = 0; i < kRows; ++i) {
      std::string v;
      const auto len = 4 + rng() % 28;
      for (size_t j = 0; j < len; ++j) {
        v += static_cast<char>('a' + rng() % 26);
      }
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

template<class T, bool UseValues>
uint64_t Read(duckdb::Vector& vec, duckdb::idx_t count);

template<>
uint64_t Read<duckdb::string_t, true>(duckdb::Vector& vec,
                                      duckdb::idx_t count) {
  uint64_t acc = 0;
  auto values = vec.Values<duckdb::string_t>();
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto value = values[i];
    if (!value.IsValid()) {
      continue;
    }
    acc += value.GetValue().GetSize();
  }
  return acc;
}

template<>
uint64_t Read<duckdb::string_t, false>(duckdb::Vector& vec,
                                       duckdb::idx_t count) {
  uint64_t acc = 0;
  duckdb::UnifiedVectorFormat fmt;
  vec.ToUnifiedFormat(count, fmt);
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto idx = fmt.sel->get_index(i);
    if (!fmt.validity.RowIsValid(idx)) {
      continue;
    }
    acc += data[idx].GetSize();
  }
  return acc;
}

template<>
uint64_t Read<int64_t, true>(duckdb::Vector& vec, duckdb::idx_t count) {
  uint64_t acc = 0;
  auto values = vec.Values<int64_t>();
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto value = values[i];
    if (!value.IsValid()) {
      continue;
    }
    acc += static_cast<uint64_t>(value.GetValue());
  }
  return acc;
}

template<>
uint64_t Read<int64_t, false>(duckdb::Vector& vec, duckdb::idx_t count) {
  uint64_t acc = 0;
  duckdb::UnifiedVectorFormat fmt;
  vec.ToUnifiedFormat(count, fmt);
  const auto* data = duckdb::UnifiedVectorFormat::GetData<int64_t>(fmt);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto idx = fmt.sel->get_index(i);
    if (!fmt.validity.RowIsValid(idx)) {
      continue;
    }
    acc += static_cast<uint64_t>(data[idx]);
  }
  return acc;
}

struct Source {
  duckdb::Vector base;
  duckdb::SelectionVector sel;
  duckdb::Vector view;

  Source(const duckdb::LogicalType& type, Shape shape)
    : base(type, kRows), sel(kRows), view(type) {
    if (type.id() == duckdb::LogicalTypeId::VARCHAR) {
      auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(base);
      const auto& corpus = Corpus();
      for (duckdb::idx_t i = 0; i < kRows; ++i) {
        data[i] = duckdb::StringVector::AddString(base, corpus[i]);
      }
    } else {
      auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(base);
      for (duckdb::idx_t i = 0; i < kRows; ++i) {
        data[i] = static_cast<int64_t>(i) * 7;
      }
    }
    if (shape == Shape::FlatNulls) {
      auto& validity = duckdb::FlatVector::ValidityMutable(base);
      for (duckdb::idx_t i = 0; i < kRows; i += 8) {
        validity.SetInvalid(i);
      }
    }
    for (duckdb::idx_t i = 0; i < kRows; ++i) {
      sel.set_index(i, (i * 13 + 5) % kRows);
    }
    if (shape == Shape::Dictionary) {
      view.Slice(base, sel, kRows);
    } else {
      view.Reference(base);
    }
  }
};

template<class T, bool UseValues>
void Run(benchmark::State& state, Shape shape) {
  const auto type = std::is_same_v<T, duckdb::string_t>
                      ? duckdb::LogicalType::VARCHAR
                      : duckdb::LogicalType::BIGINT;
  Source source{type, shape};
  for (auto _ : state) {
    benchmark::DoNotOptimize(Read<T, UseValues>(source.view, kRows));
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kRows);
}

void BM_Strings(benchmark::State& state, Shape shape, bool use_values) {
  if (use_values) {
    Run<duckdb::string_t, true>(state, shape);
  } else {
    Run<duckdb::string_t, false>(state, shape);
  }
}

void BM_Rowids(benchmark::State& state, Shape shape, bool use_values) {
  if (use_values) {
    Run<int64_t, true>(state, shape);
  } else {
    Run<int64_t, false>(state, shape);
  }
}

}  // namespace

BENCHMARK_CAPTURE(BM_Strings, flat_unified, Shape::Flat, false);
BENCHMARK_CAPTURE(BM_Strings, flat_values, Shape::Flat, true);
BENCHMARK_CAPTURE(BM_Strings, nulls_unified, Shape::FlatNulls, false);
BENCHMARK_CAPTURE(BM_Strings, nulls_values, Shape::FlatNulls, true);
BENCHMARK_CAPTURE(BM_Strings, dict_unified, Shape::Dictionary, false);
BENCHMARK_CAPTURE(BM_Strings, dict_values, Shape::Dictionary, true);

BENCHMARK_CAPTURE(BM_Rowids, flat_unified, Shape::Flat, false);
BENCHMARK_CAPTURE(BM_Rowids, flat_values, Shape::Flat, true);
BENCHMARK_CAPTURE(BM_Rowids, dict_unified, Shape::Dictionary, false);
BENCHMARK_CAPTURE(BM_Rowids, dict_values, Shape::Dictionary, true);

BENCHMARK_MAIN();
