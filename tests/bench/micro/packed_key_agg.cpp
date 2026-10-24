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

#include <cstdint>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/types/hash.hpp>
#include <duckdb/execution/aggregate_hashtable.hpp>
#include <duckdb/main/client_context.hpp>
#include <string>
#include <vector>

namespace {

constexpr size_t kRows = size_t{1} << 21;

uint64_t Mix(uint64_t x) {
  x ^= x >> 33;
  x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33;
  return x;
}

std::vector<int64_t> Keys(uint64_t distinct, uint64_t run) {
  std::vector<int64_t> keys(kRows);
  uint64_t key = 0;
  for (size_t i = 0; i < kRows; i++) {
    if (i % run == 0) {
      key = Mix(i * 0x9e3779b97f4a7c15ULL) % distinct;
    }
    keys[i] = static_cast<int64_t>(Mix(key + 1));
  }
  return keys;
}

struct Dataset {
  std::string name;
  std::vector<int64_t> keys;
};

const std::vector<Dataset>& Datasets() {
  static const std::vector<Dataset> datasets = {
    {"distinct_1k", Keys(1024, 1)},
    {"distinct_64k", Keys(65536, 1)},
    {"distinct_2m", Keys(kRows, 1)},
    {"runs_userid", Keys(kRows / 4, 5)},
  };
  return datasets;
}

duckdb::DuckDB& Db() {
  static duckdb::DuckDB db(nullptr);
  return db;
}

void DuckDBTable(benchmark::State& state, const Dataset& data) {
  duckdb::Connection con(Db());
  auto& context = *con.context;
  auto& allocator = duckdb::Allocator::DefaultAllocator();
  duckdb::DataChunk groups;
  groups.Initialize(allocator, {duckdb::LogicalType::BIGINT});
  duckdb::DataChunk payload;
  const duckdb::unsafe_vector<duckdb::idx_t> filter;
  size_t total = 0;
  for (auto _ : state) {
    duckdb::GroupedAggregateHashTable ht(context, allocator,
                                         {duckdb::LogicalType::BIGINT});
    for (size_t row = 0; row < kRows; row += STANDARD_VECTOR_SIZE) {
      const size_t count = std::min<size_t>(STANDARD_VECTOR_SIZE, kRows - row);
      groups.Reset();
      std::memcpy(duckdb::FlatVector::GetDataMutable<int64_t>(groups.data[0]),
                  data.keys.data() + row, count * sizeof(int64_t));
      groups.SetChildCardinality(count);
      ht.AddChunk(groups, payload, filter);
    }
    total = ht.Count();
    benchmark::DoNotOptimize(total);
  }
  state.counters["groups"] = static_cast<double>(total);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kRows));
}

void DuckDBSplitTable(benchmark::State& state, const Dataset& data,
                      size_t split_bits) {
  duckdb::Connection con(Db());
  auto& context = *con.context;
  auto& allocator = duckdb::Allocator::DefaultAllocator();
  duckdb::DataChunk groups;
  groups.Initialize(allocator, {duckdb::LogicalType::BIGINT});
  duckdb::DataChunk payload;
  const duckdb::unsafe_vector<duckdb::idx_t> filter;
  const size_t splits = size_t{1} << split_bits;
  std::vector<std::vector<int64_t>> slices(splits);
  size_t total = 0;
  for (auto _ : state) {
    for (auto& slice : slices) {
      slice.clear();
    }
    for (const int64_t key : data.keys) {
      slices[(duckdb::Hash(key) >> 40) & (splits - 1)].push_back(key);
    }
    total = 0;
    for (const auto& slice : slices) {
      duckdb::GroupedAggregateHashTable ht(context, allocator,
                                           {duckdb::LogicalType::BIGINT});
      for (size_t row = 0; row < slice.size(); row += STANDARD_VECTOR_SIZE) {
        const size_t count =
          std::min<size_t>(STANDARD_VECTOR_SIZE, slice.size() - row);
        groups.Reset();
        std::memcpy(duckdb::FlatVector::GetDataMutable<int64_t>(groups.data[0]),
                    slice.data() + row, count * sizeof(int64_t));
        groups.SetChildCardinality(count);
        ht.AddChunk(groups, payload, filter);
      }
      total += ht.Count();
    }
    benchmark::DoNotOptimize(total);
  }
  state.counters["groups"] = static_cast<double>(total);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kRows));
}

class PackedTable {
 public:
  PackedTable() { Resize(1024); }

  void AddChunk(const int64_t* keys, size_t count) {
    uint64_t hashes[STANDARD_VECTOR_SIZE];
    for (size_t i = 0; i < count; i++) {
      hashes[i] = duckdb::Hash(keys[i]);
    }
    for (size_t i = 0; i < count; i++) {
      if (2 * (size_ + 1) > slots_.size()) {
        Resize(2 * slots_.size());
      }
      const uint64_t key = static_cast<uint64_t>(keys[i]);
      size_t at = hashes[i] & mask_;
      for (;;) {
        Slot& slot = slots_[at];
        if (slot.count == 0) {
          slot.key = key;
          slot.count = 1;
          size_++;
          break;
        }
        if (slot.key == key) {
          slot.count++;
          break;
        }
        at = (at + 1) & mask_;
      }
    }
  }

  size_t Count() const { return size_; }

 private:
  struct Slot {
    uint64_t key;
    uint64_t count;
  };

  void Resize(size_t capacity) {
    std::vector<Slot> old = std::move(slots_);
    slots_.assign(capacity, Slot{0, 0});
    mask_ = capacity - 1;
    for (const Slot& slot : old) {
      if (slot.count == 0) {
        continue;
      }
      size_t at = duckdb::Hash(static_cast<int64_t>(slot.key)) & mask_;
      while (slots_[at].count != 0) {
        at = (at + 1) & mask_;
      }
      slots_[at] = slot;
    }
  }

  std::vector<Slot> slots_;
  size_t mask_ = 0;
  size_t size_ = 0;
};

void Packed(benchmark::State& state, const Dataset& data) {
  size_t total = 0;
  for (auto _ : state) {
    PackedTable ht;
    for (size_t row = 0; row < kRows; row += STANDARD_VECTOR_SIZE) {
      const size_t count = std::min<size_t>(STANDARD_VECTOR_SIZE, kRows - row);
      ht.AddChunk(data.keys.data() + row, count);
    }
    total = ht.Count();
    benchmark::DoNotOptimize(total);
  }
  state.counters["groups"] = static_cast<double>(total);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kRows));
}

class PackedRowTable {
 public:
  PackedRowTable() { Resize(1024); }

  void AddChunk(const int64_t* keys, size_t count) {
    uint32_t starts[STANDARD_VECTOR_SIZE];
    uint32_t lengths[STANDARD_VECTOR_SIZE];
    size_t runs = 0;
    for (size_t i = 0; i < count; i++) {
      if (i > 0 && keys[i] == keys[i - 1]) {
        lengths[runs - 1]++;
        continue;
      }
      starts[runs] = static_cast<uint32_t>(i);
      lengths[runs] = 1;
      runs++;
    }
    uint64_t hashes[STANDARD_VECTOR_SIZE];
    for (size_t r = 0; r < runs; r++) {
      hashes[r] = duckdb::Hash(keys[starts[r]]);
    }
    for (size_t r = 0; r < runs; r++) {
      if (2 * (rows_.size() + 1) > keys_.size()) {
        Resize(2 * keys_.size());
      }
      const uint64_t key = static_cast<uint64_t>(keys[starts[r]]);
      size_t at = hashes[r] & mask_;
      for (;;) {
        const uint32_t row = row_of_[at];
        if (row == 0) {
          keys_[at] = key;
          rows_.push_back(Row{key, lengths[r]});
          row_of_[at] = static_cast<uint32_t>(rows_.size());
          break;
        }
        if (keys_[at] == key) {
          rows_[row - 1].count += lengths[r];
          break;
        }
        at = (at + 1) & mask_;
      }
    }
  }

  size_t Count() const { return rows_.size(); }

 private:
  struct Row {
    uint64_t key;
    uint64_t count;
  };

  void Resize(size_t capacity) {
    keys_.assign(capacity, 0);
    row_of_.assign(capacity, 0);
    mask_ = capacity - 1;
    for (size_t i = 0; i < rows_.size(); i++) {
      size_t at = duckdb::Hash(static_cast<int64_t>(rows_[i].key)) & mask_;
      while (row_of_[at] != 0) {
        at = (at + 1) & mask_;
      }
      keys_[at] = rows_[i].key;
      row_of_[at] = static_cast<uint32_t>(i + 1);
    }
  }

  std::vector<uint64_t> keys_;
  std::vector<uint32_t> row_of_;
  std::vector<Row> rows_;
  size_t mask_ = 0;
};

void PackedRows(benchmark::State& state, const Dataset& data) {
  size_t total = 0;
  for (auto _ : state) {
    PackedRowTable ht;
    for (size_t row = 0; row < kRows; row += STANDARD_VECTOR_SIZE) {
      const size_t count = std::min<size_t>(STANDARD_VECTOR_SIZE, kRows - row);
      ht.AddChunk(data.keys.data() + row, count);
    }
    total = ht.Count();
    benchmark::DoNotOptimize(total);
  }
  state.counters["groups"] = static_cast<double>(total);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kRows));
}

int Main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  for (const Dataset& data : Datasets()) {
    benchmark::RegisterBenchmark(
      data.name + "/duckdb",
      [&data](benchmark::State& state) { DuckDBTable(state, data); })
      ->Unit(benchmark::kMillisecond);
    for (size_t split_bits : {1, 2, 3, 4, 5}) {
      benchmark::RegisterBenchmark(
        data.name + "/duckdb_split" + std::to_string(size_t{1} << split_bits),
        [&data, split_bits](benchmark::State& state) {
          DuckDBSplitTable(state, data, split_bits);
        })
        ->Unit(benchmark::kMillisecond);
    }
    benchmark::RegisterBenchmark(
      data.name + "/packed_rows",
      [&data](benchmark::State& state) { PackedRows(state, data); })
      ->Unit(benchmark::kMillisecond);
    benchmark::RegisterBenchmark(
      data.name + "/packed",
      [&data](benchmark::State& state) { Packed(state, data); })
      ->Unit(benchmark::kMillisecond);
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}

}  // namespace

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
