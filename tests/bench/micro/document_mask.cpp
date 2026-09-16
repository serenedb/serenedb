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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#include <absl/container/flat_hash_set.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include "iresearch/index/document_mask.hpp"

namespace {

constexpr irs::doc_id_t kDocs = 1'000'000;
constexpr irs::doc_id_t kTailDocs = 10'000'000;
constexpr irs::doc_id_t kTailVisible = 2'000'000;

std::vector<irs::doc_id_t> MakeDeleted(int64_t per_mille) {
  std::vector<irs::doc_id_t> all(kDocs);
  std::iota(all.begin(), all.end(), irs::doc_limits::min());

  std::vector<irs::doc_id_t> deleted;
  const auto count = static_cast<size_t>(kDocs * per_mille / 1000);
  deleted.reserve(count);
  std::mt19937 rng{42};
  std::sample(all.begin(), all.end(), std::back_inserter(deleted), count, rng);
  return deleted;
}

template<typename Mask>
size_t ScanAscending(const Mask& mask, irs::doc_id_t end) {
  size_t live = 0;
  for (auto doc = irs::doc_limits::min(); doc < end; ++doc) {
    live += static_cast<size_t>(!mask.Contains(doc));
  }
  return live;
}

void BmProbeRoaring(benchmark::State& state) {
  const auto mask = [&] {
    irs::DocumentMaskBuilder builder;
    builder.Add(MakeDeleted(state.range(0)));
    return std::move(builder).Build();
  }();

  for (auto _ : state) {
    benchmark::DoNotOptimize(
      ScanAscending(mask, kDocs + irs::doc_limits::min()));
  }

  state.counters["serialized_bytes"] = static_cast<double>(mask.ByteSize());
  state.SetItemsProcessed(state.iterations() * kDocs);
}

BENCHMARK(BmProbeRoaring)->Arg(1)->Arg(10)->Arg(100)->Arg(500);

size_t ScanWithIterator(const irs::DocumentMask& mask, irs::doc_id_t end) {
  auto it_mask = mask.Begin();
  auto next = it_mask.Value();
  size_t live = 0;
  for (auto doc = irs::doc_limits::min(); doc < end; ++doc) {
    if (doc < next) {
      ++live;
      continue;
    }
    next = it_mask.Next();
  }
  return live;
}

void BmProbeRoaringIterator(benchmark::State& state) {
  const auto mask = [&] {
    irs::DocumentMaskBuilder builder;
    builder.Add(MakeDeleted(state.range(0)));
    return std::move(builder).Build();
  }();

  for (auto _ : state) {
    benchmark::DoNotOptimize(
      ScanWithIterator(mask, kDocs + irs::doc_limits::min()));
  }

  state.counters["serialized_bytes"] = static_cast<double>(mask.ByteSize());
  state.SetItemsProcessed(state.iterations() * kDocs);
}

BENCHMARK(BmProbeRoaringIterator)->Arg(1)->Arg(10)->Arg(100)->Arg(500);

void BmScanTailAsBitsIterator(benchmark::State& state) {
  const auto mask = [] {
    irs::DocumentMaskBuilder builder;
    builder.AddRange(irs::doc_limits::min() + kTailVisible,
                     irs::doc_limits::min() + kTailDocs);
    return std::move(builder).Build();
  }();

  for (auto _ : state) {
    benchmark::DoNotOptimize(
      ScanWithIterator(mask, kTailDocs + irs::doc_limits::min()));
  }

  state.counters["serialized_bytes"] = static_cast<double>(mask.ByteSize());
}

BENCHMARK(BmScanTailAsBitsIterator);

void BmProbeHashSet(benchmark::State& state) {
  const auto deleted = MakeDeleted(state.range(0));
  absl::flat_hash_set<irs::doc_id_t> mask{deleted.begin(), deleted.end()};

  struct Probe {
    bool Contains(irs::doc_id_t doc) const noexcept {
      return set->contains(doc);
    }
    const absl::flat_hash_set<irs::doc_id_t>* set;
  };

  for (auto _ : state) {
    benchmark::DoNotOptimize(
      ScanAscending(Probe{&mask}, kDocs + irs::doc_limits::min()));
  }

  state.counters["serialized_bytes"] =
    static_cast<double>(mask.capacity() * sizeof(irs::doc_id_t));
  state.SetItemsProcessed(state.iterations() * kDocs);
}

BENCHMARK(BmProbeHashSet)->Arg(1)->Arg(10)->Arg(100)->Arg(500);

void BmLoadRoaring(benchmark::State& state) {
  const auto deleted = MakeDeleted(state.range(0));

  for (auto _ : state) {
    irs::DocumentMaskBuilder builder;
    builder.Add(deleted);
    const auto mask = std::move(builder).Build();
    benchmark::DoNotOptimize(mask.Count());
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(deleted.size()));
}

BENCHMARK(BmLoadRoaring)->Arg(1)->Arg(10)->Arg(100)->Arg(500);

void BmLoadHashSet(benchmark::State& state) {
  const auto deleted = MakeDeleted(state.range(0));

  for (auto _ : state) {
    absl::flat_hash_set<irs::doc_id_t> mask;
    mask.reserve(deleted.size());
    mask.insert(deleted.begin(), deleted.end());
    benchmark::DoNotOptimize(mask.size());
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(deleted.size()));
}

BENCHMARK(BmLoadHashSet)->Arg(1)->Arg(10)->Arg(100)->Arg(500);

// The uncommitted suffix is a bound, not bits: nothing is stored for it and a
// scan simply stops at `uncommitted_begin`.
void BmScanTailAsBound(benchmark::State& state) {
  const irs::DocumentMask mask;
  constexpr auto kUncommitted = irs::doc_limits::min() + kTailVisible;

  for (auto _ : state) {
    benchmark::DoNotOptimize(ScanAscending(mask, kUncommitted));
  }

  state.counters["serialized_bytes"] = static_cast<double>(mask.ByteSize());
}

BENCHMARK(BmScanTailAsBound);

void BmScanTailAsBits(benchmark::State& state) {
  const auto mask = [] {
    irs::DocumentMaskBuilder builder;
    builder.AddRange(irs::doc_limits::min() + kTailVisible,
                     irs::doc_limits::min() + kTailDocs);
    return std::move(builder).Build();
  }();

  for (auto _ : state) {
    benchmark::DoNotOptimize(
      ScanAscending(mask, irs::doc_limits::min() + kTailDocs));
  }

  state.counters["serialized_bytes"] = static_cast<double>(mask.ByteSize());
}

BENCHMARK(BmScanTailAsBits);

}  // namespace

BENCHMARK_MAIN();
