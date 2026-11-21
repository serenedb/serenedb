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
#include <immintrin.h>
#include <roaring/bitset_util.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <random>
#include <vector>

#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/lower_bound.hpp"

namespace {

constexpr size_t kSlack = 64;
constexpr uint32_t kBlock = 256;
constexpr size_t kTargets = 1024;
constexpr size_t kSegmentWords = 65536;
constexpr size_t kWindowWords = 32;

std::vector<uint64_t> MakeWords(size_t n, uint32_t bits, uint64_t seed) {
  std::mt19937_64 rng{seed};
  std::vector<uint64_t> words(n + kSlack, 0);
  for (size_t w = 0; w != n; ++w) {
    const auto want = bits > 64 ? static_cast<uint32_t>(rng() % 65) : bits;
    uint64_t word = 0;
    while (static_cast<uint32_t>(std::popcount(word)) < want) {
      word |= uint64_t{1} << (rng() % 64);
    }
    words[w] = word;
  }
  return words;
}

std::vector<uint64_t> MakeBernoulli(size_t n, uint32_t per1024, uint64_t seed) {
  std::mt19937_64 rng{seed};
  std::bernoulli_distribution bit{per1024 / 1024.0};
  std::vector<uint64_t> words(n + kSlack, 0);
  for (size_t w = 0; w != n; ++w) {
    for (uint32_t b = 0; b != 64; ++b) {
      words[w] |= static_cast<uint64_t>(bit(rng)) << b;
    }
  }
  return words;
}

std::vector<uint64_t> MakeGapped(size_t n, size_t gap) {
  std::vector<uint64_t> words(n + kSlack, 0);
  for (size_t w = gap - 1; w < n; w += gap) {
    words[w] = uint64_t{1} << (w % 64);
  }
  return words;
}

uint64_t CountLoop(const uint64_t* words, size_t n) {
  uint64_t total = 0;
  for (size_t w = 0; w != n; ++w) {
    total += static_cast<uint64_t>(std::popcount(words[w]));
  }
  return total;
}

uint64_t CountHarleySeal(const uint64_t* words, size_t n) {
  return roaring::internal::avx2_harley_seal_popcount256(
    reinterpret_cast<const __m256i*>(words), n / 4);
}

uint64_t CountBits(const uint64_t* words, size_t n) {
  return irs::CountBits(words, n);
}

template<uint64_t (*Count)(const uint64_t*, size_t)>
void BmCount(benchmark::State& state) {
  const auto n = static_cast<size_t>(state.range(0));
  const auto words = MakeWords(n, 32, 1);
  for (auto _ : state) {
    benchmark::DoNotOptimize(words.data());
    benchmark::DoNotOptimize(Count(words.data(), n));
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(n));
}

BENCHMARK_TEMPLATE(BmCount, CountLoop)
  ->Arg(32)
  ->Arg(64)
  ->Arg(256)
  ->Arg(1024)
  ->Arg(81920);
BENCHMARK_TEMPLATE(BmCount, CountHarleySeal)
  ->Arg(32)
  ->Arg(64)
  ->Arg(256)
  ->Arg(1024)
  ->Arg(81920);
BENCHMARK_TEMPLATE(BmCount, CountBits)
  ->Arg(32)
  ->Arg(64)
  ->Arg(256)
  ->Arg(1024)
  ->Arg(81920);

size_t ExtractLoop(const uint64_t* words, size_t n, uint32_t offset,
                   uint32_t* out) {
  auto* p = out;
  for (size_t w = 0; w != n; ++w) {
    for (auto word = words[w]; word != 0; word &= word - 1) {
      *p++ = static_cast<uint32_t>(offset + w * 64 + std::countr_zero(word));
    }
  }
  return static_cast<size_t>(p - out);
}

size_t ExtractMaterialize(const uint64_t* words, size_t n, uint32_t offset,
                          uint32_t* out) {
  auto* p = out;
  for (size_t w = 0; w != n; ++w) {
    p =
      irs::MaterializeWord(static_cast<uint32_t>(offset + w * 64), words[w], p);
  }
  return static_cast<size_t>(p - out);
}

template<uint32_t Sparse>
size_t ExtractHybrid(const uint64_t* words, size_t n, uint32_t offset,
                     uint32_t* out) {
  auto* p = out;
  for (size_t w = 0; w != n; ++w) {
    const auto base = static_cast<uint32_t>(offset + w * 64);
    if (static_cast<uint32_t>(std::popcount(words[w])) <= Sparse) {
      irs::ForEachSetBit(words[w], [&](uint32_t bit) { *p++ = base + bit; });
    } else {
      p = irs::MaterializeWord(base, words[w], p);
    }
  }
  return static_cast<size_t>(p - out);
}

size_t ExtractRoaring(const uint64_t* words, size_t n, uint32_t offset,
                      uint32_t* out) {
  return roaring::internal::bitset_extract_setbits_avx2(words, n, out, n * 64,
                                                        offset);
}

size_t ExtractRoaring512(const uint64_t* words, size_t n, uint32_t offset,
                         uint32_t* out) {
  return roaring::internal::bitset_extract_setbits_avx512(words, n, out, n * 64,
                                                          offset);
}

IRS_AVX512 size_t ExtractCompress(const uint64_t* words, size_t n,
                                  uint32_t offset, uint32_t* out) {
  auto* p = out;
  for (size_t w = 0; w != n; ++w) {
    p = irs::MaterializeWord16(static_cast<uint32_t>(offset + w * 64), words[w],
                               p);
  }
  return static_cast<size_t>(p - out);
}

size_t ExtractWords(const uint64_t* words, size_t n, uint32_t offset,
                    uint32_t* out) {
  return static_cast<size_t>(irs::MaterializeRange(offset, words, n, out) -
                             out);
}

template<size_t (*Extract)(const uint64_t*, size_t, uint32_t, uint32_t*)>
void RunExtract(benchmark::State& state, const std::vector<uint64_t>& words,
                size_t n, size_t window) {
  if (Extract == ExtractRoaring512 && !irs::HasRoaringAvx512()) {
    state.SkipWithError("needs AVX-512 VBMI2");
    return;
  }
  std::vector<uint32_t> out(n * 64 + kSlack);
  int64_t total = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(words.data());
    size_t produced = 0;
    for (size_t w = 0; w < n; w += window) {
      produced +=
        Extract(words.data() + w, std::min(window, n - w),
                static_cast<uint32_t>(1 + w * 64), out.data() + produced);
    }
    total += static_cast<int64_t>(produced);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(total);
}

template<size_t (*Extract)(const uint64_t*, size_t, uint32_t, uint32_t*)>
void BmExtract(benchmark::State& state) {
  const auto n = static_cast<size_t>(state.range(0));
  const auto bits = static_cast<uint32_t>(state.range(1));
  RunExtract<Extract>(state, MakeWords(n, bits, 3), n, n);
}

template<size_t (*Extract)(const uint64_t*, size_t, uint32_t, uint32_t*)>
void BmExtractBernoulli(benchmark::State& state) {
  const auto per1024 = static_cast<uint32_t>(state.range(0));
  RunExtract<Extract>(state, MakeBernoulli(kSegmentWords, per1024, 3),
                      kSegmentWords, kWindowWords);
}

void ExtractArgs(benchmark::internal::Benchmark* b) {
  for (int64_t n : {32, 64}) {
    for (int64_t bits : {1, 4, 8, 12, 16, 32, 63, 65}) {
      b->Args({n, bits});
    }
  }
}

void BernoulliArgs(benchmark::internal::Benchmark* b) {
  for (int64_t per1024 : {1, 4, 16, 64, 128, 256, 512}) {
    b->Arg(per1024);
  }
}

BENCHMARK_TEMPLATE(BmExtract, ExtractLoop)->Apply(ExtractArgs);
BENCHMARK_TEMPLATE(BmExtract, ExtractMaterialize)->Apply(ExtractArgs);
BENCHMARK_TEMPLATE(BmExtract, ExtractHybrid<12>)->Apply(ExtractArgs);
BENCHMARK_TEMPLATE(BmExtract, ExtractRoaring)->Apply(ExtractArgs);
BENCHMARK_TEMPLATE(BmExtract, ExtractRoaring512)->Apply(ExtractArgs);
BENCHMARK_TEMPLATE(BmExtract, ExtractCompress)->Apply(ExtractArgs);
BENCHMARK_TEMPLATE(BmExtract, ExtractWords)->Apply(ExtractArgs);
BENCHMARK_TEMPLATE(BmExtractBernoulli, ExtractLoop)->Apply(BernoulliArgs);
BENCHMARK_TEMPLATE(BmExtractBernoulli, ExtractMaterialize)
  ->Apply(BernoulliArgs);
BENCHMARK_TEMPLATE(BmExtractBernoulli, ExtractHybrid<12>)->Apply(BernoulliArgs);
BENCHMARK_TEMPLATE(BmExtractBernoulli, ExtractRoaring)->Apply(BernoulliArgs);
BENCHMARK_TEMPLATE(BmExtractBernoulli, ExtractRoaring512)->Apply(BernoulliArgs);
BENCHMARK_TEMPLATE(BmExtractBernoulli, ExtractCompress)->Apply(BernoulliArgs);
BENCHMARK_TEMPLATE(BmExtractBernoulli, ExtractWords)->Apply(BernoulliArgs);

uint64_t NextSetBit(const uint64_t* words, size_t n, size_t from) {
  return irs::NextSetBit(words, from, n);
}

uint64_t NextSkip(const uint64_t* words, size_t n, size_t from) {
  auto w = from / 64;
  if (const auto bits = words[w] & (~uint64_t{0} << (from % 64)); bits != 0) {
    return w * 64 + static_cast<uint64_t>(std::countr_zero(bits));
  }
  ++w;
  for (; w + 4 <= n; w += 4) {
    const auto v =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(words + w));
    if (!_mm256_testz_si256(v, v)) {
      break;
    }
  }
  for (; w != n; ++w) {
    if (words[w] != 0) {
      return w * 64 + static_cast<uint64_t>(std::countr_zero(words[w]));
    }
  }
  return n * 64;
}

template<uint64_t (*Next)(const uint64_t*, size_t, size_t)>
void BmNext(benchmark::State& state) {
  constexpr size_t kWords = 4096;
  const auto gap = static_cast<size_t>(state.range(0));
  const auto words = MakeGapped(kWords, gap);
  for (auto _ : state) {
    benchmark::DoNotOptimize(words.data());
    for (size_t at = Next(words.data(), kWords, 0); at + 1 < kWords * 64;
         at = Next(words.data(), kWords, at + 1)) {
      benchmark::DoNotOptimize(at);
    }
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(kWords));
}

BENCHMARK_TEMPLATE(BmNext, NextSetBit)->Arg(1)->Arg(4)->Arg(32)->Arg(256);
BENCHMARK_TEMPLATE(BmNext, NextSkip)->Arg(1)->Arg(4)->Arg(32)->Arg(256);

struct Block {
  std::vector<uint32_t> docs;
  std::vector<uint32_t> targets;
};

Block MakeBlock(uint64_t seed) {
  std::mt19937_64 rng{seed};
  Block block;
  block.docs.resize(kBlock + kSlack);
  uint32_t doc = 1;
  for (uint32_t i = 0; i != kBlock; ++i) {
    doc += 1 + static_cast<uint32_t>(rng() % 64);
    block.docs[i] = doc;
  }
  for (size_t i = kBlock; i != block.docs.size(); ++i) {
    block.docs[i] = block.docs[kBlock - 1];
  }
  block.targets.resize(kTargets);
  for (auto& target : block.targets) {
    target =
      block.docs[0] + static_cast<uint32_t>(
                        rng() % (block.docs[kBlock - 1] - block.docs[0] + 1));
  }
  return block;
}

uint32_t SeekBranchless(const uint32_t* docs, uint32_t, uint32_t target) {
  return static_cast<uint32_t>(irs::BranchlessLowerBound<kBlock>(docs, target) -
                               docs);
}

uint32_t SeekStd(const uint32_t* docs, uint32_t at, uint32_t target) {
  return static_cast<uint32_t>(
    std::lower_bound(docs + at, docs + kBlock, target) - docs);
}

uint32_t SeekScan(const uint32_t* docs, uint32_t at, uint32_t target) {
  return static_cast<uint32_t>(irs::ScanNotBelow(docs + at, target) - docs);
}

uint32_t SeekScanBounded(const uint32_t* docs, uint32_t at, uint32_t target) {
  return static_cast<uint32_t>(
    irs::ScanNotBelow(docs + at, docs + kBlock, target) - docs);
}

uint32_t SeekVector(const uint32_t* docs, uint32_t at, uint32_t target) {
  const auto key = _mm256_set1_epi32(static_cast<int32_t>(target));
  for (;; at += 8) {
    if (const auto found = irs::NotBelowLanes(docs + at, key); found != 0) {
      return at + static_cast<uint32_t>(std::countr_zero(found));
    }
  }
}

template<uint32_t (*Seek)(const uint32_t*, uint32_t, uint32_t)>
void BmSeekRandom(benchmark::State& state) {
  const auto block = MakeBlock(4);
  uint64_t sum = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(block.docs.data());
    for (const auto target : block.targets) {
      sum += Seek(block.docs.data(), 0, target);
    }
  }
  benchmark::DoNotOptimize(sum);
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(kTargets));
}

constexpr size_t kSeekBlocks = 2048;

struct Seeks {
  std::vector<uint32_t> docs;
  std::vector<uint32_t> targets;
  std::vector<uint32_t> counts;
};

Seeks MakeSeeks(uint32_t mean, uint32_t zeros, uint64_t seed) {
  std::mt19937_64 rng{seed};
  std::geometric_distribution<uint32_t> extra{1.0 / mean};
  std::bernoulli_distribution again{zeros / 100.0};
  Seeks seeks;
  seeks.docs.resize(kSeekBlocks * kBlock + kSlack);
  uint32_t doc = 1;
  for (size_t i = 0; i != kSeekBlocks * kBlock; ++i) {
    doc += 1 + static_cast<uint32_t>(rng() % 64);
    seeks.docs[i] = doc;
  }
  for (size_t i = kSeekBlocks * kBlock; i != seeks.docs.size(); ++i) {
    seeks.docs[i] = doc;
  }
  seeks.counts.resize(kSeekBlocks);
  for (size_t b = 0; b != kSeekBlocks; ++b) {
    const auto* docs = seeks.docs.data() + b * kBlock;
    for (uint32_t next = 1 + extra(rng); next < kBlock;
         next += 1 + extra(rng)) {
      do {
        seeks.targets.push_back(docs[next - 1] + 1);
        ++seeks.counts[b];
      } while (again(rng));
    }
  }
  return seeks;
}

template<uint32_t (*Seek)(const uint32_t*, uint32_t, uint32_t)>
void BmSeekGeometric(benchmark::State& state) {
  const auto seeks = MakeSeeks(static_cast<uint32_t>(state.range(0)),
                               static_cast<uint32_t>(state.range(1)), 6);
  uint64_t sum = 0;
  for (auto _ : state) {
    const auto* target = seeks.targets.data();
    for (size_t b = 0; b != kSeekBlocks; ++b) {
      const auto* docs = seeks.docs.data() + b * kBlock;
      uint32_t at = 0;
      for (auto n = seeks.counts[b]; n != 0; --n) {
        benchmark::DoNotOptimize(at);
        at = Seek(docs, at, *target++ + (at >> 31));
      }
      sum += at;
    }
  }
  benchmark::DoNotOptimize(sum);
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(seeks.targets.size()));
}

void SeekArgs(benchmark::internal::Benchmark* bench) {
  bench->ArgNames({"mean", "zeros"});
  for (const auto zeros : {0, 50, 90}) {
    for (const auto mean : {2, 8, 32}) {
      bench->Args({mean, zeros});
    }
  }
}

BENCHMARK_TEMPLATE(BmSeekRandom, SeekBranchless);
BENCHMARK_TEMPLATE(BmSeekRandom, SeekStd);
BENCHMARK_TEMPLATE(BmSeekRandom, SeekScan);
BENCHMARK_TEMPLATE(BmSeekGeometric, SeekBranchless)->Apply(SeekArgs);
BENCHMARK_TEMPLATE(BmSeekGeometric, SeekStd)->Apply(SeekArgs);
BENCHMARK_TEMPLATE(BmSeekGeometric, SeekScan)->Apply(SeekArgs);
BENCHMARK_TEMPLATE(BmSeekGeometric, SeekScanBounded)->Apply(SeekArgs);
BENCHMARK_TEMPLATE(BmSeekGeometric, SeekVector)->Apply(SeekArgs);

}  // namespace
