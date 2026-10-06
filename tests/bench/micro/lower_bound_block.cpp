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
#include <bit>
#include <cstdint>
#include <limits>
#include <map>
#include <random>
#include <utility>
#include <vector>

#include "iresearch/formats/posting/common.hpp"

namespace {

constexpr uint32_t kDocs = irs::doc_limits::kBlockSize;
constexpr uint32_t kSlack = 32;
constexpr uint32_t kStride = kDocs + kSlack;
constexpr uint32_t kBlocks = 2048;
constexpr uint32_t kGroup = 32;
constexpr uint32_t kGroups = kDocs / kGroup;

enum class Kernel : int {
  Copy,
  Halving,
  Heads,
  Strided,
  Near,
  All,
};

struct Fixture {
  std::vector<uint32_t> docs;
  std::vector<uint32_t> heads;
  std::vector<uint32_t> targets;
  std::vector<uint32_t> begin;
};

Fixture Make(uint32_t gap, uint32_t probes) {
  std::mt19937_64 rng{uint64_t{gap} * 7919 + probes};
  std::geometric_distribution<uint32_t> step{1.0 / gap};
  Fixture f;
  f.docs.assign(size_t{kBlocks} * kStride,
                std::numeric_limits<uint32_t>::max());
  f.heads.resize(size_t{kBlocks} * kGroups);
  uint32_t doc = 0;
  for (uint32_t b = 0; b != kBlocks; ++b) {
    auto* block = f.docs.data() + size_t{b} * kStride;
    for (uint32_t i = 0; i != kDocs; ++i) {
      doc += 1 + step(rng);
      block[i] = doc;
    }
    for (uint32_t g = 0; g != kGroups; ++g) {
      f.heads[size_t{b} * kGroups + g] = block[g * kGroup + kGroup - 1];
    }
    f.begin.push_back(static_cast<uint32_t>(f.targets.size()));
    std::uniform_int_distribution<uint32_t> pick{block[0], block[kDocs - 1]};
    std::vector<uint32_t> t(probes);
    for (auto& v : t) {
      v = pick(rng);
    }
    std::sort(t.begin(), t.end());
    f.targets.insert(f.targets.end(), t.begin(), t.end());
  }
  f.begin.push_back(static_cast<uint32_t>(f.targets.size()));
  return f;
}

const Fixture& Get(uint32_t gap, uint32_t probes) {
  static std::map<std::pair<uint32_t, uint32_t>, Fixture> cache;
  auto [it, added] = cache.try_emplace({gap, probes});
  if (added) {
    it->second = Make(gap, probes);
  }
  return it->second;
}

IRS_FORCE_INLINE uint32_t CountHeads(const uint32_t* heads,
                                     uint32_t target) noexcept {
  uint32_t count = 0;
  for (uint32_t g = 0; g != kGroups; ++g) {
    count += static_cast<uint32_t>(heads[g] < target);
  }
  return count;
}

IRS_FORCE_INLINE uint32_t CountStrided(const uint32_t* block,
                                       uint32_t target) noexcept {
  uint32_t count = 0;
  for (uint32_t g = 0; g != kGroups; ++g) {
    count += static_cast<uint32_t>(block[g * kGroup + kGroup - 1] < target);
  }
  return count;
}

IRS_FORCE_INLINE uint32_t Heads(const uint32_t* block, const uint32_t* heads,
                                uint32_t target) noexcept {
  const auto g = std::min(CountHeads(heads, target), kGroups - 1);
  return g * kGroup +
         irs::CountLess<kGroup>(block + size_t{g} * kGroup, target);
}

IRS_FORCE_INLINE uint32_t Strided(const uint32_t* block,
                                  uint32_t target) noexcept {
  const auto g = std::min(CountStrided(block, target), kGroups - 1);
  return g * kGroup +
         irs::CountLess<kGroup>(block + size_t{g} * kGroup, target);
}

template<Kernel K>
void Bench(benchmark::State& state) {
  const auto& f = Get(static_cast<uint32_t>(state.range(0)),
                      static_cast<uint32_t>(state.range(1)));
  uint64_t sink = 0;
  uint64_t probes = 0;
  alignas(64) uint32_t block[kStride];
  uint32_t heads[kGroups];
  for (auto _ : state) {
    for (uint32_t b = 0; b != kBlocks; ++b) {
      std::copy_n(f.docs.data() + size_t{b} * kStride, kStride, block);
      if constexpr (K == Kernel::Heads) {
        for (uint32_t g = 0; g != kGroups; ++g) {
          heads[g] = block[g * kGroup + kGroup - 1];
        }
      }
      benchmark::ClobberMemory();
      uint32_t at = 0;
      for (auto t = f.begin[b], end = f.begin[b + 1]; t != end; ++t) {
        const auto target = f.targets[t];
        if constexpr (K == Kernel::Copy) {
          at = target;
        } else if constexpr (K == Kernel::Halving) {
          at = static_cast<uint32_t>(
            irs::BranchlessLowerBound<kDocs>(block, target) - block);
        } else if constexpr (K == Kernel::Heads) {
          at = Heads(block, heads, target);
        } else if constexpr (K == Kernel::Strided) {
          at = Strided(block, target);
        } else if constexpr (K == Kernel::Near) {
          if (block[at + kGroup - 1] >= target) {
            at += irs::CountLess<kGroup>(block + at, target);
          } else {
            at = Strided(block, target);
          }
        } else {
          at = irs::CountLess<kDocs>(block, target);
        }
        sink += at;
      }
      probes += f.begin[b + 1] - f.begin[b];
    }
  }
  benchmark::DoNotOptimize(sink);
  state.SetItemsProcessed(static_cast<int64_t>(probes));
}

bool Check() {
  for (uint32_t gap : {1u, 8u, 1000u}) {
    for (uint32_t probes : {1u, 16u, 256u}) {
      const auto& f = Get(gap, probes);
      for (uint32_t b = 0; b < kBlocks; b += 37) {
        const auto* block = f.docs.data() + size_t{b} * kStride;
        const auto* heads = f.heads.data() + size_t{b} * kGroups;
        uint32_t near = 0;
        for (auto t = f.begin[b], end = f.begin[b + 1]; t != end; ++t) {
          const auto target = f.targets[t];
          const auto expected = static_cast<uint32_t>(
            std::lower_bound(block, block + kDocs, target) - block);
          if (block[near + kGroup - 1] >= target) {
            near += irs::CountLess<kGroup>(block + near, target);
          } else {
            near = Strided(block, target);
          }
          if (static_cast<uint32_t>(
                irs::BranchlessLowerBound<kDocs>(block, target) - block) !=
                expected ||
              Heads(block, heads, target) != expected ||
              Strided(block, target) != expected || near != expected ||
              irs::CountLess<kDocs>(block, target) != expected) {
            return false;
          }
        }
      }
    }
  }
  return true;
}

void BmCheck(benchmark::State& state) {
  if (!Check()) {
    state.SkipWithError("mismatch");
    return;
  }
  for (auto _ : state) {
  }
}

void Args(benchmark::internal::Benchmark* b) {
  for (int64_t gap : {1, 8, 64, 1000}) {
    for (int64_t probes : {1, 4, 16, 64, 256}) {
      b->Args({gap, probes});
    }
  }
  b->ArgNames({"gap", "probes"});
}

BENCHMARK(BmCheck)->Iterations(1);
BENCHMARK(Bench<Kernel::Copy>)->Apply(Args);
BENCHMARK(Bench<Kernel::Halving>)->Apply(Args);
BENCHMARK(Bench<Kernel::Heads>)->Apply(Args);
BENCHMARK(Bench<Kernel::Strided>)->Apply(Args);
BENCHMARK(Bench<Kernel::Near>)->Apply(Args);
BENCHMARK(Bench<Kernel::All>)->Apply(Args);

}  // namespace
