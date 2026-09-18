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

#include "iresearch/index/document_mask.hpp"

#include <absl/container/flat_hash_set.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <map>
#include <numeric>
#include <random>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/search/detail/bitset_storage.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/fill/docs_mask.hpp"
#include "iresearch/search/probe/bitset_docs.hpp"
#include "iresearch/search/probe/docs_mask.hpp"
#include "iresearch/utils/containers/bitset.hpp"

namespace {

using irs::doc_id_t;

constexpr doc_id_t kDocs = 1'000'000;
constexpr doc_id_t kBegin = irs::doc_limits::min();
constexpr doc_id_t kEnd = kBegin + kDocs;
constexpr doc_id_t kTailDocs = 10'000'000;
constexpr doc_id_t kTailVisible = 2'000'000;
constexpr doc_id_t kRunLength = 512;
constexpr uint32_t kBlock = 128;
constexpr size_t kChainLinks = 8;
constexpr size_t kBits = 64;

constexpr int64_t kUniform = 0;
constexpr int64_t kClustered = 1;
constexpr int64_t kTail = 2;

constexpr doc_id_t kWindows =
  (kDocs + irs::detail::kWindowDocs - 1) / irs::detail::kWindowDocs;
constexpr size_t kPaddedBits = size_t{kWindows} * irs::detail::kWindowDocs;

std::vector<doc_id_t> MakeDeleted(int64_t per_mille, int64_t shape) {
  const auto count = static_cast<size_t>(
    uint64_t{kDocs} * static_cast<uint64_t>(per_mille) / 1000);
  std::vector<doc_id_t> deleted;
  deleted.reserve(count);
  std::mt19937 rng{42};

  if (shape == kTail) {
    for (auto doc = kEnd - static_cast<doc_id_t>(count); doc != kEnd; ++doc) {
      deleted.push_back(doc);
    }
    return deleted;
  }

  if (shape == kClustered) {
    const auto slots = static_cast<size_t>(kDocs / kRunLength);
    const auto runs = std::min(slots, (count + kRunLength - 1) / kRunLength);
    std::vector<doc_id_t> all(slots);
    std::iota(all.begin(), all.end(), doc_id_t{0});
    std::vector<doc_id_t> picked;
    picked.reserve(runs);
    std::sample(all.begin(), all.end(), std::back_inserter(picked), runs, rng);
    std::sort(picked.begin(), picked.end());
    for (const auto slot : picked) {
      const auto first = kBegin + slot * kRunLength;
      for (doc_id_t i = 0; i != kRunLength && deleted.size() != count; ++i) {
        deleted.push_back(first + i);
      }
    }
    return deleted;
  }

  std::vector<doc_id_t> all(kDocs);
  std::iota(all.begin(), all.end(), kBegin);
  std::sample(all.begin(), all.end(), std::back_inserter(deleted), count, rng);
  return deleted;
}

const std::vector<doc_id_t>& Deleted(int64_t per_mille, int64_t shape) {
  static std::map<std::pair<int64_t, int64_t>, std::vector<doc_id_t>> gCache;
  const std::pair key{per_mille, shape};
  if (const auto it = gCache.find(key); it != gCache.end()) {
    return it->second;
  }
  return gCache.emplace(key, MakeDeleted(per_mille, shape)).first->second;
}

const std::vector<doc_id_t>& Candidates(int64_t stride) {
  static std::map<int64_t, std::vector<doc_id_t>> gCache;
  if (const auto it = gCache.find(stride); it != gCache.end()) {
    return it->second;
  }
  std::vector<doc_id_t> out;
  out.reserve(static_cast<size_t>(kDocs / stride) + 1);
  for (auto doc = kBegin; doc < kEnd; doc += static_cast<doc_id_t>(stride)) {
    out.push_back(doc);
  }
  return gCache.emplace(stride, std::move(out)).first->second;
}

class RoaringMask {
 public:
  explicit RoaringMask(std::span<const doc_id_t> deleted) {
    irs::DocumentMaskBuilder builder;
    builder.Add(deleted);
    _mask = std::move(builder).Build();
  }

  irs::probe::DocsMask Probes() const noexcept {
    return irs::probe::DocsMask{&_mask, irs::doc_limits::eof()};
  }

  irs::fill::DocsMask Fills() const noexcept {
    return irs::fill::DocsMask{&_mask, irs::doc_limits::eof()};
  }

  bool Test(doc_id_t doc) const noexcept { return _mask.Contains(doc); }

  size_t Bytes() const noexcept { return _mask.ByteSize(); }

  const irs::DocumentMask& Set() const noexcept { return _mask; }

 private:
  irs::DocumentMask _mask;
};

class HashSetMask {
 public:
  explicit HashSetMask(std::span<const doc_id_t> deleted) {
    _set.reserve(deleted.size());
    _set.insert(deleted.begin(), deleted.end());
  }

  class Cursor {
   public:
    explicit Cursor(const HashSetMask& mask) noexcept : _set{&mask._set} {}

    doc_id_t Probe(doc_id_t doc) const noexcept {
      return _set->contains(doc) ? doc : doc + 1;
    }

    doc_id_t FillOr(doc_id_t min, doc_id_t max,
                    uint64_t* IRS_RESTRICT words) const noexcept {
      for (auto doc = min; doc < max; ++doc) {
        if (_set->contains(doc)) {
          const auto offset = static_cast<size_t>(doc - min);
          words[offset / kBits] |= uint64_t{1} << (offset % kBits);
        }
      }
      return max;
    }

   private:
    const absl::flat_hash_set<doc_id_t>* _set;
  };

  Cursor Probes() const noexcept { return Cursor{*this}; }
  Cursor Fills() const noexcept { return Cursor{*this}; }

  bool Test(doc_id_t doc) const noexcept { return _set.contains(doc); }

  size_t Bytes() const noexcept {
    return _set.capacity() * (sizeof(doc_id_t) + 1);
  }

 private:
  absl::flat_hash_set<doc_id_t> _set;
};

irs::detail::BitsetStorage MakeStorage(std::span<const doc_id_t> deleted) {
  irs::detail::BitsetStorage set{kDocs};
  auto* const words = set.Words();
  for (const auto doc : deleted) {
    const auto offset =
      static_cast<size_t>(doc - irs::detail::BitsetStorage::kMin);
    words[offset / kBits] |= uint64_t{1} << (offset % kBits);
  }
  return set;
}

class BitsetProbeMask {
 public:
  explicit BitsetProbeMask(std::span<const doc_id_t> deleted)
    : _bytes{size_t{(kDocs + kBits - 1) / kBits} * sizeof(uint64_t)},
      _docs{MakeStorage(deleted)} {}

  class Cursor {
   public:
    explicit Cursor(const BitsetProbeMask& mask) noexcept
      : _docs{&mask._docs} {}

    doc_id_t Probe(doc_id_t doc) noexcept { return _docs->Probe(doc); }

    doc_id_t FillOr(doc_id_t min, doc_id_t max,
                    uint64_t* IRS_RESTRICT words) noexcept {
      for (auto doc = min; doc < max;) {
        const auto probe = _docs->Probe(doc);
        if (probe != doc) {
          doc = probe;
          continue;
        }
        const auto offset = static_cast<size_t>(doc - min);
        words[offset / kBits] |= uint64_t{1} << (offset % kBits);
        ++doc;
      }
      return max;
    }

   private:
    irs::probe::BitsetDocs* _docs;
  };

  Cursor Probes() const noexcept { return Cursor{*this}; }
  Cursor Fills() const noexcept { return Cursor{*this}; }

  bool Test(doc_id_t doc) const noexcept { return _docs.Test(doc); }

  size_t Bytes() const noexcept { return _bytes; }

 private:
  size_t _bytes;
  mutable irs::probe::BitsetDocs _docs;
};

class BitsetExactMask {
 public:
  explicit BitsetExactMask(std::span<const doc_id_t> deleted)
    : _bits{kPaddedBits} {
    for (const auto doc : deleted) {
      _bits.set(static_cast<size_t>(doc - kBegin));
    }
  }

  class Cursor {
   public:
    explicit Cursor(const BitsetExactMask& mask) noexcept
      : _words{mask._bits.data()}, _count{mask._bits.words()} {}

    doc_id_t Probe(doc_id_t doc) const noexcept {
      auto word = static_cast<size_t>(doc - kBegin) / kBits;
      if (word >= _count) {
        return irs::doc_limits::eof();
      }
      auto rest = static_cast<uint64_t>(_words[word]) &
                  (~uint64_t{0} << (static_cast<size_t>(doc - kBegin) % kBits));
      while (rest == 0) {
        if (++word == _count) {
          return irs::doc_limits::eof();
        }
        rest = static_cast<uint64_t>(_words[word]);
      }
      return kBegin +
             static_cast<doc_id_t>(word * kBits +
                                   static_cast<size_t>(std::countr_zero(rest)));
    }

    doc_id_t FillOr(doc_id_t min, doc_id_t max,
                    uint64_t* IRS_RESTRICT words) const noexcept {
      const auto base = static_cast<size_t>(min - kBegin) / kBits;
      const auto n = irs::detail::WindowWords(min, max);
      for (size_t w = 0; w != n; ++w) {
        words[w] |= static_cast<uint64_t>(_words[base + w]);
      }
      return max;
    }

   private:
    const irs::bitset::word_t* _words;
    size_t _count;
  };

  Cursor Probes() const noexcept { return Cursor{*this}; }
  Cursor Fills() const noexcept { return Cursor{*this}; }

  bool Test(doc_id_t doc) const noexcept {
    return _bits.test(static_cast<size_t>(doc - kBegin));
  }

  size_t Bytes() const noexcept {
    return _bits.words() * sizeof(irs::bitset::word_t);
  }

  const irs::bitset& Bits() const noexcept { return _bits; }

 private:
  irs::bitset _bits;
};

template<typename Cursor>
size_t CountLive(Cursor& cursor) {
  size_t live = 0;
  for (auto doc = kBegin; doc < kEnd;) {
    const auto probe = cursor.Probe(doc);
    if (probe == doc) {
      ++doc;
      continue;
    }
    const auto stop = std::min(probe, kEnd);
    live += static_cast<size_t>(stop - doc);
    doc = stop;
  }
  return live;
}

template<typename Cursor>
size_t CountLiveBlocks(Cursor& cursor, std::span<const doc_id_t> candidates) {
  size_t live = 0;
  for (size_t at = 0; at < candidates.size(); at += kBlock) {
    const auto len = std::min<size_t>(kBlock, candidates.size() - at);
    const auto block = candidates.subspan(at, len);
    if (cursor.Probe(block.front()) > block.back()) {
      live += len;
      continue;
    }
    for (const auto doc : block) {
      live += static_cast<size_t>(cursor.Probe(doc) != doc);
    }
  }
  return live;
}

template<typename Cursor>
size_t FillWindows(Cursor& cursor, uint64_t* IRS_RESTRICT dst,
                   uint64_t* IRS_RESTRICT own) {
  size_t live = 0;
  for (auto min = kBegin; min < kEnd; min += irs::detail::kWindowDocs) {
    const auto max = std::min(min + irs::detail::kWindowDocs, kEnd);
    const auto words = irs::detail::WindowWords(min, max);
    for (size_t w = 0; w != words; ++w) {
      dst[w] = ~uint64_t{0};
    }
    irs::detail::Clear(own, words);
    cursor.FillOr(min, max, own);
    live += irs::detail::FoldAndNot(dst, own, words);
  }
  return live;
}

void RatioShape(benchmark::internal::Benchmark* b) {
  b->ArgsProduct({{10, 50, 200, 500, 990}, {kUniform, kClustered, kTail}});
}

void RatioShapeStride(benchmark::internal::Benchmark* b) {
  b->ArgsProduct(
    {{10, 50, 200, 500, 990}, {kUniform, kClustered, kTail}, {16, 256}});
}

void RatioStride(benchmark::internal::Benchmark* b) {
  b->ArgsProduct({{10, 50, 200, 500, 990},
                  {kUniform, kClustered, kTail},
                  {1, 16, 256, 4096, 65536, 262144}});
}

template<typename Mask>
void BmBuild(benchmark::State& state) {
  const auto& deleted = Deleted(state.range(0), state.range(1));

  for (auto _ : state) {
    Mask mask{deleted};
    benchmark::DoNotOptimize(mask.Bytes());
  }

  const Mask mask{deleted};
  state.counters["bytes"] = static_cast<double>(mask.Bytes());
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(deleted.size()));
}

BENCHMARK_TEMPLATE(BmBuild, RoaringMask)
  ->Name("Build/roaring")
  ->Apply(RatioShape);
BENCHMARK_TEMPLATE(BmBuild, HashSetMask)
  ->Name("Build/hashset")
  ->Apply(RatioShape);
BENCHMARK_TEMPLATE(BmBuild, BitsetProbeMask)
  ->Name("Build/bitset_probe")
  ->Apply(RatioShape);
BENCHMARK_TEMPLATE(BmBuild, BitsetExactMask)
  ->Name("Build/bitset_exact")
  ->Apply(RatioShape);

template<typename Mask>
void BmSeekDense(benchmark::State& state) {
  const Mask mask{Deleted(state.range(0), state.range(1))};
  size_t live = 0;

  for (auto _ : state) {
    auto cursor = mask.Probes();
    live = CountLive(cursor);
    benchmark::DoNotOptimize(live);
  }

  state.counters["live"] = static_cast<double>(live);
  state.counters["bytes"] = static_cast<double>(mask.Bytes());
  state.SetItemsProcessed(state.iterations() * kDocs);
}

BENCHMARK_TEMPLATE(BmSeekDense, RoaringMask)
  ->Name("SeekDense/roaring")
  ->Apply(RatioShape);
BENCHMARK_TEMPLATE(BmSeekDense, HashSetMask)
  ->Name("SeekDense/hashset")
  ->Apply(RatioShape);
BENCHMARK_TEMPLATE(BmSeekDense, BitsetProbeMask)
  ->Name("SeekDense/bitset_probe")
  ->Apply(RatioShape);
BENCHMARK_TEMPLATE(BmSeekDense, BitsetExactMask)
  ->Name("SeekDense/bitset_exact")
  ->Apply(RatioShape);

template<typename Mask>
void BmSeekBlocks(benchmark::State& state) {
  const Mask mask{Deleted(state.range(0), state.range(1))};
  const auto& candidates = Candidates(state.range(2));
  size_t live = 0;

  for (auto _ : state) {
    auto cursor = mask.Probes();
    live = CountLiveBlocks(cursor, candidates);
    benchmark::DoNotOptimize(live);
  }

  state.counters["live"] = static_cast<double>(live);
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(candidates.size()));
}

BENCHMARK_TEMPLATE(BmSeekBlocks, RoaringMask)
  ->Name("SeekBlocks/roaring")
  ->Apply(RatioShapeStride);
BENCHMARK_TEMPLATE(BmSeekBlocks, HashSetMask)
  ->Name("SeekBlocks/hashset")
  ->Apply(RatioShapeStride);
BENCHMARK_TEMPLATE(BmSeekBlocks, BitsetProbeMask)
  ->Name("SeekBlocks/bitset_probe")
  ->Apply(RatioShapeStride);
BENCHMARK_TEMPLATE(BmSeekBlocks, BitsetExactMask)
  ->Name("SeekBlocks/bitset_exact")
  ->Apply(RatioShapeStride);

template<typename Mask>
void BmFillWindow(benchmark::State& state) {
  const Mask mask{Deleted(state.range(0), state.range(1))};
  std::vector<uint64_t> dst(irs::detail::kWindowWords);
  std::vector<uint64_t> own(irs::detail::kWindowWords);
  size_t live = 0;

  for (auto _ : state) {
    auto cursor = mask.Fills();
    live = FillWindows(cursor, dst.data(), own.data());
    benchmark::DoNotOptimize(live);
  }

  state.counters["live"] = static_cast<double>(live);
  state.SetItemsProcessed(state.iterations() * kDocs);
}

BENCHMARK_TEMPLATE(BmFillWindow, RoaringMask)
  ->Name("FillWindow/roaring")
  ->Apply(RatioShape);
BENCHMARK_TEMPLATE(BmFillWindow, HashSetMask)
  ->Name("FillWindow/hashset")
  ->Apply(RatioShape);
BENCHMARK_TEMPLATE(BmFillWindow, BitsetProbeMask)
  ->Name("FillWindow/bitset_probe")
  ->Apply(RatioShape);
BENCHMARK_TEMPLATE(BmFillWindow, BitsetExactMask)
  ->Name("FillWindow/bitset_exact")
  ->Apply(RatioShape);

std::vector<std::vector<doc_id_t>> SplitChain(
  const std::vector<doc_id_t>& deleted) {
  std::vector<std::vector<doc_id_t>> parts(kChainLinks);
  for (size_t i = 0; i != deleted.size(); ++i) {
    parts[i % kChainLinks].push_back(deleted[i]);
  }
  return parts;
}

void BmMergeRoaring(benchmark::State& state) {
  const auto parts = SplitChain(Deleted(state.range(0), state.range(1)));
  std::vector<irs::DocumentMask> links;
  links.reserve(kChainLinks);
  for (const auto& part : parts) {
    irs::DocumentMaskBuilder builder;
    builder.Add(part);
    links.emplace_back(std::move(builder).Build());
  }

  for (auto _ : state) {
    irs::DocumentMaskBuilder builder;
    for (const auto& link : links) {
      builder.Merge(link);
    }
    const auto mask = std::move(builder).Build();
    benchmark::DoNotOptimize(mask.Count());
  }
}

BENCHMARK(BmMergeRoaring)->Name("Merge/roaring")->Apply(RatioShape);

void BmMergeHashSet(benchmark::State& state) {
  const auto parts = SplitChain(Deleted(state.range(0), state.range(1)));
  std::vector<absl::flat_hash_set<doc_id_t>> links;
  links.reserve(kChainLinks);
  for (const auto& part : parts) {
    links.emplace_back(part.begin(), part.end());
  }

  for (auto _ : state) {
    absl::flat_hash_set<doc_id_t> mask;
    for (const auto& link : links) {
      mask.insert(link.begin(), link.end());
    }
    benchmark::DoNotOptimize(mask.size());
  }
}

BENCHMARK(BmMergeHashSet)->Name("Merge/hashset")->Apply(RatioShape);

void BmMergeBitset(benchmark::State& state) {
  const auto parts = SplitChain(Deleted(state.range(0), state.range(1)));
  std::vector<irs::detail::BitsetStorage> links;
  links.reserve(kChainLinks);
  for (const auto& part : parts) {
    links.emplace_back(MakeStorage(part));
  }

  for (auto _ : state) {
    irs::detail::BitsetStorage mask{kDocs};
    auto* const words = mask.Words();
    for (const auto& link : links) {
      const auto* const src = link.Words();
      for (uint32_t w = 0, n = mask.WordCount(); w != n; ++w) {
        words[w] |= src[w];
      }
    }
    benchmark::DoNotOptimize(irs::detail::CountBits(mask));
  }
}

BENCHMARK(BmMergeBitset)->Name("Merge/bitset")->Apply(RatioShape);

template<typename Mask>
void BmLookupTest(benchmark::State& state) {
  const Mask mask{Deleted(state.range(0), state.range(1))};
  const auto& candidates = Candidates(state.range(2));
  size_t hits = 0;

  for (auto _ : state) {
    hits = 0;
    for (const auto doc : candidates) {
      hits += static_cast<size_t>(mask.Test(doc));
    }
    benchmark::DoNotOptimize(hits);
  }

  state.counters["hits"] = static_cast<double>(hits);
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(candidates.size()));
}

BENCHMARK_TEMPLATE(BmLookupTest, RoaringMask)
  ->Name("LookupTest/roaring")
  ->Apply(RatioStride);

void BmRoaringContainsBulk(benchmark::State& state) {
  const auto& deleted = Deleted(state.range(0), state.range(1));
  roaring::Roaring set;
  set.addMany(deleted.size(), deleted.data());
  set.runOptimize();
  set.shrinkToFit();
  const auto& candidates = Candidates(state.range(2));
  size_t hits = 0;

  for (auto _ : state) {
    roaring::BulkContext ctx;
    hits = 0;
    for (const auto doc : candidates) {
      hits += static_cast<size_t>(set.containsBulk(ctx, doc));
    }
    benchmark::DoNotOptimize(hits);
  }

  state.counters["hits"] = static_cast<double>(hits);
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(candidates.size()));
}

BENCHMARK(BmRoaringContainsBulk)
  ->Name("LookupTest/roaring_bulk")
  ->Apply(RatioStride);
BENCHMARK_TEMPLATE(BmLookupTest, HashSetMask)
  ->Name("LookupTest/hashset")
  ->Apply(RatioStride);
BENCHMARK_TEMPLATE(BmLookupTest, BitsetProbeMask)
  ->Name("LookupTest/bitset_probe")
  ->Apply(RatioStride);
BENCHMARK_TEMPLATE(BmLookupTest, BitsetExactMask)
  ->Name("LookupTest/bitset_exact")
  ->Apply(RatioStride);

template<typename Mask>
void BmLookupProbe(benchmark::State& state) {
  const Mask mask{Deleted(state.range(0), state.range(1))};
  const auto& candidates = Candidates(state.range(2));
  size_t hits = 0;

  for (auto _ : state) {
    auto cursor = mask.Probes();
    hits = 0;
    for (const auto doc : candidates) {
      hits += static_cast<size_t>(cursor.Probe(doc) == doc);
    }
    benchmark::DoNotOptimize(hits);
  }

  state.counters["hits"] = static_cast<double>(hits);
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(candidates.size()));
}

BENCHMARK_TEMPLATE(BmLookupProbe, RoaringMask)
  ->Name("LookupProbe/roaring")
  ->Apply(RatioStride);
BENCHMARK_TEMPLATE(BmLookupProbe, HashSetMask)
  ->Name("LookupProbe/hashset")
  ->Apply(RatioStride);
BENCHMARK_TEMPLATE(BmLookupProbe, BitsetProbeMask)
  ->Name("LookupProbe/bitset_probe")
  ->Apply(RatioStride);
BENCHMARK_TEMPLATE(BmLookupProbe, BitsetExactMask)
  ->Name("LookupProbe/bitset_exact")
  ->Apply(RatioStride);

void BmRoaringSeekRaw(benchmark::State& state) {
  const RoaringMask mask{Deleted(state.range(0), state.range(1))};
  const auto& candidates = Candidates(state.range(2));
  size_t hits = 0;

  for (auto _ : state) {
    auto it = mask.Set().Begin();
    hits = 0;
    for (const auto doc : candidates) {
      hits += static_cast<size_t>(it.Seek(doc) == doc);
    }
    benchmark::DoNotOptimize(hits);
  }

  state.counters["hits"] = static_cast<double>(hits);
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(candidates.size()));
}

BENCHMARK(BmRoaringSeekRaw)
  ->Name("LookupProbe/roaring_raw")
  ->Apply(RatioStride);

void BmRoaringIteratorInit(benchmark::State& state) {
  const RoaringMask mask{Deleted(state.range(0), state.range(1))};

  for (auto _ : state) {
    auto it = mask.Set().Begin();
    benchmark::DoNotOptimize(it.Value());
  }
}

BENCHMARK(BmRoaringIteratorInit)->Name("LookupInit/roaring")->Apply(RatioShape);

constexpr size_t kScaleProbes = 65536;

struct ScaleData {
  doc_id_t docs = 0;
  roaring::Roaring set;
  absl::flat_hash_set<doc_id_t> hash;
  std::vector<uint64_t> bits;
  std::vector<doc_id_t> ordered;
  std::vector<doc_id_t> shuffled;
};

const ScaleData& Scale(int64_t docs_m, int64_t per_mille, int64_t shape) {
  static ScaleData gCache;
  static std::array<int64_t, 3> gKey{-1, -1, -1};
  const std::array<int64_t, 3> key{docs_m, per_mille, shape};
  if (gKey == key) {
    return gCache;
  }

  const auto docs = static_cast<doc_id_t>(docs_m * 1'000'000);
  const auto count = static_cast<size_t>(
    uint64_t{docs} * static_cast<uint64_t>(per_mille) / 1000);
  std::mt19937_64 rng{42};

  std::vector<doc_id_t> deleted;
  deleted.reserve(count);
  if (shape == kClustered) {
    const auto runs = (count + kRunLength - 1) / kRunLength;
    const auto slots = static_cast<size_t>(docs / kRunLength);
    absl::flat_hash_set<size_t> picked;
    picked.reserve(runs);
    while (picked.size() < std::min(runs, slots)) {
      picked.insert(rng() % slots);
    }
    std::vector<size_t> sorted{picked.begin(), picked.end()};
    std::sort(sorted.begin(), sorted.end());
    for (const auto slot : sorted) {
      const auto first = kBegin + static_cast<doc_id_t>(slot * kRunLength);
      for (doc_id_t i = 0; i != kRunLength && deleted.size() != count; ++i) {
        deleted.push_back(first + i);
      }
    }
  } else {
    absl::flat_hash_set<doc_id_t> picked;
    picked.reserve(count);
    while (picked.size() < count) {
      picked.insert(kBegin + static_cast<doc_id_t>(rng() % docs));
    }
    deleted.assign(picked.begin(), picked.end());
    std::sort(deleted.begin(), deleted.end());
  }

  gCache = ScaleData{};
  gCache.docs = docs;
  gCache.set.addMany(deleted.size(), deleted.data());
  gCache.set.runOptimize();
  gCache.set.shrinkToFit();
  gCache.hash.reserve(deleted.size());
  gCache.hash.insert(deleted.begin(), deleted.end());
  gCache.bits.assign((docs + kBits - 1) / kBits, 0);
  for (const auto doc : deleted) {
    const auto off = static_cast<size_t>(doc - kBegin);
    gCache.bits[off / kBits] |= uint64_t{1} << (off % kBits);
  }

  gCache.ordered.reserve(kScaleProbes);
  const auto stride = std::max<doc_id_t>(1, docs / kScaleProbes);
  for (size_t i = 0; i != kScaleProbes; ++i) {
    gCache.ordered.push_back(kBegin + static_cast<doc_id_t>(i) * stride);
  }
  gCache.shuffled = gCache.ordered;
  std::shuffle(gCache.shuffled.begin(), gCache.shuffled.end(), rng);

  gKey = key;
  return gCache;
}

void ScaleArgs(benchmark::internal::Benchmark* b) {
  b->ArgsProduct({{1, 8, 64}, {10, 200}, {kUniform, kClustered}, {0, 1}});
}

template<int Arm>
void BmLookupScale(benchmark::State& state) {
  const auto& data = Scale(state.range(0), state.range(1), state.range(2));
  const auto& probes = state.range(3) == 0 ? data.ordered : data.shuffled;
  size_t hits = 0;

  for (auto _ : state) {
    hits = 0;
    if constexpr (Arm == 0) {
      for (const auto doc : probes) {
        hits += static_cast<size_t>(data.set.contains(doc));
      }
    } else if constexpr (Arm == 1) {
      roaring::BulkContext ctx;
      for (const auto doc : probes) {
        hits += static_cast<size_t>(data.set.containsBulk(ctx, doc));
      }
    } else if constexpr (Arm == 2) {
      for (const auto doc : probes) {
        hits += static_cast<size_t>(data.hash.contains(doc));
      }
    } else {
      for (const auto doc : probes) {
        const auto off = static_cast<size_t>(doc - kBegin);
        hits +=
          static_cast<size_t>((data.bits[off / kBits] >> (off % kBits)) & 1);
      }
    }
    benchmark::DoNotOptimize(hits);
  }

  state.counters["hits"] = static_cast<double>(hits);
  state.counters["roaring_bytes"] =
    static_cast<double>(data.set.getSizeInBytes());
  state.counters["bitset_bytes"] =
    static_cast<double>(data.bits.size() * sizeof(uint64_t));
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(probes.size()));
}

BENCHMARK_TEMPLATE(BmLookupScale, 0)->Name("Scale/roaring")->Apply(ScaleArgs);
BENCHMARK_TEMPLATE(BmLookupScale, 1)
  ->Name("Scale/roaring_bulk")
  ->Apply(ScaleArgs);
BENCHMARK_TEMPLATE(BmLookupScale, 2)->Name("Scale/hashset")->Apply(ScaleArgs);
BENCHMARK_TEMPLATE(BmLookupScale, 3)->Name("Scale/bitset")->Apply(ScaleArgs);

size_t ScanWithIterator(const irs::DocumentMask& mask, doc_id_t end) {
  auto it_mask = mask.Begin();
  auto next = it_mask.Value();
  size_t live = 0;
  for (auto doc = kBegin; doc < end; ++doc) {
    if (doc < next) {
      ++live;
      continue;
    }
    next = it_mask.Next();
  }
  return live;
}

void BmScanTailAsBound(benchmark::State& state) {
  const irs::DocumentMask mask;
  constexpr auto kUncommitted = kBegin + kTailVisible;

  for (auto _ : state) {
    size_t live = 0;
    for (auto doc = kBegin; doc < kUncommitted; ++doc) {
      live += static_cast<size_t>(!mask.Contains(doc));
    }
    benchmark::DoNotOptimize(live);
  }

  state.counters["serialized_bytes"] = static_cast<double>(mask.ByteSize());
}

BENCHMARK(BmScanTailAsBound);

void BmScanTailAsBits(benchmark::State& state) {
  const auto mask = [] {
    irs::DocumentMaskBuilder builder;
    builder.AddRange(kBegin + kTailVisible, kBegin + kTailDocs);
    return std::move(builder).Build();
  }();

  for (auto _ : state) {
    size_t live = 0;
    for (auto doc = kBegin; doc < kBegin + kTailDocs; ++doc) {
      live += static_cast<size_t>(!mask.Contains(doc));
    }
    benchmark::DoNotOptimize(live);
  }

  state.counters["serialized_bytes"] = static_cast<double>(mask.ByteSize());
}

BENCHMARK(BmScanTailAsBits);

void BmScanTailAsBitsIterator(benchmark::State& state) {
  const auto mask = [] {
    irs::DocumentMaskBuilder builder;
    builder.AddRange(kBegin + kTailVisible, kBegin + kTailDocs);
    return std::move(builder).Build();
  }();

  for (auto _ : state) {
    benchmark::DoNotOptimize(ScanWithIterator(mask, kBegin + kTailDocs));
  }

  state.counters["serialized_bytes"] = static_cast<double>(mask.ByteSize());
}

BENCHMARK(BmScanTailAsBitsIterator);

}  // namespace

BENCHMARK_MAIN();
