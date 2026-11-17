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

#include <algorithm>
#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/common/sorting/sort_key.hpp>
#include <duckdb/common/types/string_heap.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/function/create_sort_key.hpp>
#include <random>
#include <string>
#include <vector>

namespace {

constexpr size_t kRows = size_t{1} << 16;
constexpr size_t kLimit = 10000;

struct BlobEntry {
  duckdb::string_t key;
  uint64_t index;

  bool operator<(const BlobEntry& other) const { return key < other.key; }
};

struct Dataset {
  std::string name;
  duckdb::StringHeap heap;
  std::vector<duckdb::string_t> blobs;
  size_t max_size = 0;
};

using Fill = void (*)(std::mt19937_64& rng, duckdb::DataChunk& chunk,
                      size_t count);

void Build(Dataset& data, const duckdb::vector<duckdb::LogicalType>& types,
           const duckdb::vector<duckdb::OrderModifiers>& modifiers, Fill fill) {
  std::mt19937_64 rng{42};
  auto& allocator = duckdb::Allocator::DefaultAllocator();
  duckdb::DataChunk chunk;
  chunk.Initialize(allocator, types);
  duckdb::Vector keys{duckdb::LogicalType::BLOB};
  for (size_t row = 0; row < kRows; row += STANDARD_VECTOR_SIZE) {
    const size_t count = std::min<size_t>(STANDARD_VECTOR_SIZE, kRows - row);
    chunk.Reset();
    fill(rng, chunk, count);
    chunk.SetCardinality(count);
    duckdb::CreateSortKeyHelpers::CreateSortKey(chunk, modifiers, keys);
    const auto* blobs = duckdb::FlatVector::GetData<duckdb::string_t>(keys);
    for (size_t i = 0; i < count; i++) {
      data.blobs.push_back(data.heap.AddBlob(blobs[i]));
      data.max_size = std::max<size_t>(data.max_size, blobs[i].GetSize());
    }
  }
}

void FillTimestamp(std::mt19937_64& rng, duckdb::DataChunk& chunk,
                   size_t count) {
  auto* ts = duckdb::FlatVector::GetDataMutable<int64_t>(chunk.data[0]);
  for (size_t i = 0; i < count; i++) {
    ts[i] = int64_t{1372636800} * 1000000 +
            static_cast<int64_t>(rng() % (uint64_t{31} * 86400 * 1000000));
  }
}

void FillTwoBigints(std::mt19937_64& rng, duckdb::DataChunk& chunk,
                    size_t count) {
  auto* user = duckdb::FlatVector::GetDataMutable<int64_t>(chunk.data[0]);
  auto* time = duckdb::FlatVector::GetDataMutable<int64_t>(chunk.data[1]);
  for (size_t i = 0; i < count; i++) {
    user[i] = static_cast<int64_t>(rng() >> 1);
    time[i] = static_cast<int64_t>(rng() % 2678400);
  }
}

const std::vector<std::string>& Phrases() {
  static const std::vector<std::string> phrases = [] {
    std::mt19937_64 rng{7};
    std::vector<std::string> words;
    for (size_t i = 0; i < 2000; i++) {
      std::string word;
      const size_t size = 3 + rng() % 8;
      for (size_t c = 0; c < size; c++) {
        word.push_back(static_cast<char>('a' + rng() % 26));
      }
      words.push_back(std::move(word));
    }
    std::vector<std::string> result;
    for (size_t i = 0; i < 50000; i++) {
      std::string phrase = words[rng() % 64];
      const size_t extra = rng() % 4;
      for (size_t w = 0; w < extra; w++) {
        phrase += ' ';
        phrase += words[rng() % words.size()];
      }
      result.push_back(std::move(phrase));
    }
    return result;
  }();
  return phrases;
}

void FillPhrase(std::mt19937_64& rng, duckdb::DataChunk& chunk, size_t count) {
  const auto& phrases = Phrases();
  auto* out =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(chunk.data[0]);
  for (size_t i = 0; i < count; i++) {
    const auto& phrase = phrases[rng() % phrases.size()];
    out[i] = duckdb::StringVector::AddString(chunk.data[0], phrase);
  }
}

std::vector<Dataset>& Datasets() {
  static std::vector<Dataset> datasets = [] {
    std::vector<Dataset> result(3);
    const duckdb::OrderModifiers asc{duckdb::OrderType::ASCENDING,
                                     duckdb::OrderByNullType::NULLS_LAST};
    const duckdb::OrderModifiers desc{duckdb::OrderType::DESCENDING,
                                      duckdb::OrderByNullType::NULLS_LAST};
    result[0].name = "timestamp";
    Build(result[0], {duckdb::LogicalType::TIMESTAMP}, {asc}, FillTimestamp);
    result[1].name = "bigint_desc_bigint";
    Build(result[1], {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT},
          {desc, asc}, FillTwoBigints);
    result[2].name = "varchar";
    Build(result[2], {duckdb::LogicalType::VARCHAR}, {asc}, FillPhrase);
    return result;
  }();
  return datasets;
}

std::vector<BlobEntry> BlobEntries(const Dataset& data) {
  std::vector<BlobEntry> entries;
  entries.reserve(data.blobs.size());
  for (size_t i = 0; i < data.blobs.size(); i++) {
    entries.push_back({.key = data.blobs[i], .index = i});
  }
  return entries;
}

template<duckdb::SortKeyType kType>
struct WordEntries {
  using Key = duckdb::SortKey<kType>;

  explicit WordEntries(const Dataset& data) {
    size_t heap_size = 0;
    if constexpr (!Key::CONSTANT_SIZE) {
      for (const auto& blob : data.blobs) {
        heap_size += blob.GetSize();
      }
    }
    heap.resize(heap_size);
    Construct(data);
  }

  void Construct(const Dataset& data) {
    keys.resize(data.blobs.size());
    auto* heap_ptr = heap.data();
    for (size_t i = 0; i < data.blobs.size(); i++) {
      keys[i].Construct(data.blobs[i], heap_ptr);
      keys[i].payload.u.pad = i;
    }
  }

  std::vector<Key> keys;
  std::vector<duckdb::data_t> heap;
};

template<typename Entry>
void SortEntries(benchmark::State& state, const std::vector<Entry>& entries) {
  std::vector<Entry> copy;
  for (auto _ : state) {
    copy = entries;
    std::sort(copy.begin(), copy.end());
    benchmark::DoNotOptimize(copy.data());
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kRows));
}

template<typename Entry>
void TopEntries(benchmark::State& state, const std::vector<Entry>& entries) {
  std::vector<Entry> heap;
  heap.reserve(kLimit);
  for (auto _ : state) {
    heap.clear();
    for (const auto& entry : entries) {
      if (heap.size() < kLimit) {
        heap.push_back(entry);
        std::push_heap(heap.begin(), heap.end());
      } else if (entry < heap.front()) {
        std::pop_heap(heap.begin(), heap.end());
        heap.back() = entry;
        std::push_heap(heap.begin(), heap.end());
      }
    }
    benchmark::DoNotOptimize(heap.data());
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kRows));
}

void StoreBlobs(benchmark::State& state, const Dataset& data) {
  for (auto _ : state) {
    duckdb::StringHeap heap;
    std::vector<BlobEntry> entries;
    entries.reserve(data.blobs.size());
    for (size_t i = 0; i < data.blobs.size(); i++) {
      entries.push_back({.key = heap.AddBlob(data.blobs[i]), .index = i});
    }
    benchmark::DoNotOptimize(entries.data());
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kRows));
}

template<duckdb::SortKeyType kType>
void StoreWords(benchmark::State& state, const Dataset& data) {
  WordEntries<kType> entries{data};
  for (auto _ : state) {
    entries.Construct(data);
    benchmark::DoNotOptimize(entries.keys.data());
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kRows));
}

template<duckdb::SortKeyType kType>
void RegisterWords(const Dataset& data, const std::string& label) {
  benchmark::RegisterBenchmark(data.name + "/sort/" + label,
                               [&data](benchmark::State& state) {
                                 SortEntries(state,
                                             WordEntries<kType>{data}.keys);
                               })
    ->Unit(benchmark::kMicrosecond);
  benchmark::RegisterBenchmark(data.name + "/top10000/" + label,
                               [&data](benchmark::State& state) {
                                 TopEntries(state,
                                            WordEntries<kType>{data}.keys);
                               })
    ->Unit(benchmark::kMicrosecond);
  benchmark::RegisterBenchmark(
    data.name + "/store/" + label,
    [&data](benchmark::State& state) { StoreWords<kType>(state, data); })
    ->Unit(benchmark::kMicrosecond);
}

int Main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  for (const Dataset& data : Datasets()) {
    benchmark::RegisterBenchmark(data.name + "/sort/blob",
                                 [&data](benchmark::State& state) {
                                   SortEntries(state, BlobEntries(data));
                                 })
      ->Unit(benchmark::kMicrosecond);
    benchmark::RegisterBenchmark(data.name + "/top10000/blob",
                                 [&data](benchmark::State& state) {
                                   TopEntries(state, BlobEntries(data));
                                 })
      ->Unit(benchmark::kMicrosecond);
    benchmark::RegisterBenchmark(
      data.name + "/store/blob",
      [&data](benchmark::State& state) { StoreBlobs(state, data); })
      ->Unit(benchmark::kMicrosecond);
    if (data.max_size <= 8) {
      RegisterWords<duckdb::SortKeyType::PAYLOAD_FIXED_16>(data, "fixed16");
    } else if (data.max_size <= 16) {
      RegisterWords<duckdb::SortKeyType::PAYLOAD_FIXED_24>(data, "fixed24");
    } else if (data.max_size <= 24) {
      RegisterWords<duckdb::SortKeyType::PAYLOAD_FIXED_32>(data, "fixed32");
    }
    RegisterWords<duckdb::SortKeyType::PAYLOAD_VARIABLE_32>(data, "variable32");
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}

}  // namespace

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
