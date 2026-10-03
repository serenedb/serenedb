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

#include "iresearch/formats/posting/block_index.hpp"

#include <absl/base/internal/endian.h>
#include <benchmark/benchmark.h>
#include <immintrin.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <random>
#include <tuple>
#include <utility>
#include <vector>

namespace {

constexpr uint32_t kDocs = irs::doc_limits::kBlockSize;
constexpr uint32_t kSeeks = 1 << 16;

struct Term {
  std::vector<uint32_t> last;
  std::vector<uint32_t> end;
  std::vector<uint32_t> pos_group;
  std::vector<uint16_t> pos_index;
  std::vector<uint16_t> freq;
  std::vector<uint16_t> norm;
};

Term MakeTerm(uint32_t blocks, uint32_t gap, uint64_t seed) {
  std::mt19937_64 rng{seed};
  std::geometric_distribution<uint32_t> step{1.0 / gap};
  std::uniform_int_distribution<uint32_t> small{1, 4000};
  Term t;
  uint32_t doc = 0;
  uint32_t end = 0;
  uint32_t group = 0;
  uint32_t index = 0;
  const uint32_t block_bytes = kDocs * (std::bit_width(gap) + 2) / 8 + 40;
  for (uint32_t b = 0; b != blocks; ++b) {
    for (uint32_t d = 0; d != kDocs; ++d) {
      doc += 1 + (gap == 1 ? 0 : step(rng));
    }
    t.last.push_back(doc);
    end += block_bytes + small(rng) % 64;
    t.end.push_back(end);
    index += 700 + small(rng) % 300;
    group += (index / 4096) * 3000;
    index %= 4096;
    t.pos_group.push_back(group);
    t.pos_index.push_back(static_cast<uint16_t>(index));
    t.freq.push_back(static_cast<uint16_t>(small(rng)));
    t.norm.push_back(static_cast<uint16_t>(small(rng)));
  }
  return t;
}

class Flat {
 public:
  void Build(const Term& t) {
    const auto n = static_cast<uint32_t>(t.last.size());
    const auto r = irs::BlockIndex::Runs(n);
    const irs::BlockIndexShape shape{
      .pos = true, .offs = false, .bounds = true};
    const uint8_t flags = t.end[n - 2] > 65535 ? irs::BlockIndex::kWideEnd : 0;
    _bytes.assign(irs::BlockIndex::Bytes(n, shape, flags) + 64, 0);
    auto* p = _bytes.data();
    std::memset(p, 0, 8);
    p += 8;
    for (uint32_t k = 0; k != n; ++k) {
      absl::little_endian::Store32(p, t.last[k]);
      p += 4;
    }
    for (uint32_t i = 0; i != r; ++i) {
      absl::little_endian::Store32(
        p, t.last[std::min(n, (i + 1) * irs::BlockIndex::kRun) - 1]);
      p += 4;
    }
    for (uint32_t k = 0; k + 1 != n; ++k) {
      if (flags & irs::BlockIndex::kWideEnd) {
        absl::little_endian::Store32(p, t.end[k]);
        p += 4;
      } else {
        absl::little_endian::Store16(p, static_cast<uint16_t>(t.end[k]));
        p += 2;
      }
      absl::little_endian::Store32(p, t.pos_group[k]);
      p += 4;
      absl::little_endian::Store16(p, t.pos_index[k]);
      p += 2;
    }
    for (uint32_t k = 0; k != n; ++k) {
      absl::little_endian::Store32(p, t.freq[k]);
      absl::little_endian::Store32(p + 4, t.norm[k]);
      p += 8;
    }
    for (uint32_t i = 0; i != r; ++i) {
      p += 8;
    }
    _index.Reset(_bytes.data(), n, shape, flags);
    _size = irs::BlockIndex::Bytes(n, shape, flags);
  }

  uint32_t Find(uint32_t from, uint32_t target) const noexcept {
    return _index.Find(from, target);
  }

  uint64_t Land(uint32_t b) const noexcept {
    if (b == 0 || b >= _index.Size()) {
      return 0;
    }
    const auto k = b - 1;
    return _index.End(k) + _index.PosGroup(k) + _index.PosIndex(k) +
           _index.Last(k);
  }

  size_t Size() const noexcept { return _size; }

 private:
  std::vector<uint8_t> _bytes;
  irs::BlockIndex _index;
  size_t _size = 0;
};

IRS_FORCE_INLINE uint32_t CountLess16(const uint16_t* begin,
                                      uint32_t value) noexcept {
  const __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(begin));
  const __m256i t = _mm256_set1_epi16(static_cast<int16_t>(value));
  const __m256i ge = _mm256_cmpeq_epi16(_mm256_max_epu16(v, t), v);
  return 16 - static_cast<uint32_t>(std::popcount(
                static_cast<uint32_t>(_mm256_movemask_epi8(ge)))) /
                2;
}

template<bool Narrow>
class Chunked {
 public:
  static constexpr uint32_t kRun = Narrow ? 16 : 32;
  using Entry = std::conditional_t<Narrow, uint16_t, uint32_t>;

  struct Chunk {
    Entry last[kRun];
    Entry end[kRun];
    uint32_t pos_group[kRun];
    uint16_t pos_index[kRun];
    Entry freq[kRun];
    Entry norm[kRun];
  };

  void Build(const Term& t) {
    _n = static_cast<uint32_t>(t.last.size());
    const auto runs = (_n + kRun - 1) / kRun;
    _run_last.assign(runs + 64, std::numeric_limits<uint32_t>::max());
    _run_base.assign(runs, 0);
    _run_end.assign(runs, 0);
    _chunks.assign(runs, Chunk{});
    for (uint32_t r = 0; r != runs; ++r) {
      const uint32_t base = r == 0 ? 0 : t.last[r * kRun - 1];
      const uint32_t end_base = r == 0 ? 0 : t.end[r * kRun - 1];
      _run_base[r] = base;
      _run_end[r] = end_base;
      auto& c = _chunks[r];
      std::fill(std::begin(c.last), std::end(c.last),
                std::numeric_limits<Entry>::max());
      for (uint32_t j = 0; j != kRun && r * kRun + j < _n; ++j) {
        const auto k = r * kRun + j;
        c.last[j] = static_cast<Entry>(t.last[k] - (Narrow ? base : 0));
        c.end[j] = static_cast<Entry>(t.end[k] - (Narrow ? end_base : 0));
        c.pos_group[j] = t.pos_group[k];
        c.pos_index[j] = t.pos_index[k];
        c.freq[j] = t.freq[k];
        c.norm[j] = t.norm[k];
      }
      _run_last[r] = t.last[std::min(_n, (r + 1) * kRun) - 1];
    }
    _size = runs * (sizeof(Chunk) + 12);
  }

  bool Fits(const Term& t) const noexcept {
    if (!Narrow) {
      return true;
    }
    for (uint32_t r = 0; r * kRun < _n; ++r) {
      const uint32_t base = r == 0 ? 0 : t.last[r * kRun - 1];
      const uint32_t end_base = r == 0 ? 0 : t.end[r * kRun - 1];
      const auto k = std::min(_n, (r + 1) * kRun) - 1;
      if (t.last[k] - base > 65534 || t.end[k] - end_base > 65535) {
        return false;
      }
    }
    return true;
  }

  IRS_FORCE_INLINE uint32_t Last(uint32_t k) const noexcept {
    const auto r = k / kRun;
    return (Narrow ? _run_base[r] : 0) + _chunks[r].last[k % kRun];
  }

  IRS_FORCE_INLINE uint32_t InRun(uint32_t r, uint32_t target) const noexcept {
    const auto& c = _chunks[r];
    if constexpr (Narrow) {
      return r * kRun + CountLess16(c.last, target - _run_base[r]);
    } else {
      return r * kRun + irs::CountLess<32>(c.last, target);
    }
  }

  uint32_t Find(uint32_t from, uint32_t target) const noexcept {
    if (from >= _n || Last(from) >= target) {
      return from;
    }
    auto r = from / kRun;
    if (_run_last[r] >= target) {
      return std::max(from, InRun(r, target));
    }
    ++r;
    const auto runs = (_n + kRun - 1) / kRun;
    while (r + 64 <= runs && _run_last[r + 63] < target) {
      r += 64;
    }
    if (r + 64 <= runs) {
      r += irs::CountLess<64>(_run_last.data() + r, target);
    } else {
      while (r != runs && _run_last[r] < target) {
        ++r;
      }
      if (r == runs) {
        return _n;
      }
    }
    return std::min(_n, InRun(r, target));
  }

  uint64_t Land(uint32_t b) const noexcept {
    if (b == 0 || b >= _n) {
      return 0;
    }
    const auto k = b - 1;
    const auto r = k / kRun;
    const auto& c = _chunks[r];
    const auto j = k % kRun;
    return (Narrow ? _run_end[r] : 0) + c.end[j] + c.pos_group[j] +
           c.pos_index[j] + Last(k);
  }

  size_t Size() const noexcept { return _size; }

 private:
  std::vector<uint32_t> _run_last;
  std::vector<uint32_t> _run_base;
  std::vector<uint32_t> _run_end;
  std::vector<Chunk> _chunks;
  uint32_t _n = 0;
  size_t _size = 0;
};

struct Fixture {
  std::vector<Term> terms;
  std::vector<Flat> flat;
  std::vector<Chunked<true>> narrow;
  std::vector<Chunked<false>> wide;
  std::vector<std::vector<uint32_t>> targets;
  bool narrow_fits = true;
};

Fixture& GetFixture(uint32_t gap, uint32_t step, uint32_t terms) {
  static std::map<std::tuple<uint32_t, uint32_t, uint32_t>,
                  std::unique_ptr<Fixture>>
    cache;
  auto& f = cache[{gap, step, terms}];
  if (f) {
    return *f;
  }
  f = std::make_unique<Fixture>();
  constexpr uint32_t kBlocks = 2048;
  std::mt19937_64 rng{uint64_t{gap} * 31 + step};
  for (uint32_t i = 0; i != terms; ++i) {
    f->terms.push_back(MakeTerm(kBlocks, gap, i * 7 + gap));
    auto& t = f->terms.back();
    f->flat.emplace_back().Build(t);
    f->narrow.emplace_back().Build(t);
    f->narrow_fits &= f->narrow.back().Fits(t);
    f->wide.emplace_back().Build(t);
    std::vector<uint32_t> targets;
    std::uniform_int_distribution<uint32_t> jitter{0, kDocs - 1};
    uint64_t pos = 0;
    const uint64_t total = uint64_t{kBlocks} * kDocs;
    while (targets.size() != kSeeks / terms) {
      pos += uint64_t{step} * kDocs / 2 + jitter(rng) % (step * kDocs) + 1;
      if (pos >= total) {
        pos = jitter(rng);
        targets.push_back(0);
        continue;
      }
      const auto b = static_cast<uint32_t>(pos / kDocs);
      const uint32_t lo = b == 0 ? 0 : t.last[b - 1];
      targets.push_back(lo + 1 + (t.last[b] - lo) / 2);
    }
    f->targets.push_back(std::move(targets));
  }
  return *f;
}

template<typename Index>
void Run(benchmark::State& state, const std::vector<Index>& indexes,
         const Fixture& f) {
  uint64_t sink = 0;
  size_t term = 0;
  size_t i = 0;
  uint32_t block = 0;
  for (auto _ : state) {
    const auto& targets = f.targets[term];
    const auto target = targets[i];
    if (target == 0) {
      block = 0;
    } else {
      block = indexes[term].Find(block, target);
      sink += indexes[term].Land(block);
    }
    if (++i == targets.size()) {
      i = 0;
      block = 0;
      if (++term == indexes.size()) {
        term = 0;
      }
    }
  }
  benchmark::DoNotOptimize(sink);
  size_t bytes = 0;
  for (const auto& index : indexes) {
    bytes += index.Size();
  }
  state.counters["bytes_per_block"] =
    static_cast<double>(bytes) / static_cast<double>(indexes.size() * 2048);
}

template<int L>
void BmFind(benchmark::State& state) {
  const auto gap = static_cast<uint32_t>(state.range(0));
  const auto step = static_cast<uint32_t>(state.range(1));
  const auto terms = static_cast<uint32_t>(state.range(2));
  auto& f = GetFixture(gap, step, terms);
  if constexpr (L == 0) {
    Run(state, f.flat, f);
  } else if constexpr (L == 1) {
    if (!f.narrow_fits) {
      state.SkipWithError("narrow does not fit");
      return;
    }
    Run(state, f.narrow, f);
  } else {
    Run(state, f.wide, f);
  }
}

void Args(benchmark::internal::Benchmark* b) {
  for (int64_t gap : {1, 4, 16, 256}) {
    for (int64_t step : {1, 4, 16, 64, 1024}) {
      for (int64_t terms : {1, 512}) {
        b->Args({gap, step, terms});
      }
    }
  }
  b->ArgNames({"gap", "step", "terms"});
}

BENCHMARK(BmFind<0>)->Name("Flat")->Apply(Args);
BENCHMARK(BmFind<1>)->Name("Narrow16")->Apply(Args);
BENCHMARK(BmFind<2>)->Name("Wide32")->Apply(Args);

}  // namespace

BENCHMARK_MAIN();
