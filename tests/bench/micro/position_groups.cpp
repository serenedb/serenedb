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

#include <absl/base/internal/endian.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <random>
#include <tuple>
#include <vector>

#include "iresearch/formats/posting/block_codec.hpp"

namespace {

using Codec = irs::block_codec::Codec256;

constexpr uint32_t kBlock = Codec::kBlock;
constexpr uint32_t kDocBlock = 256;
constexpr uint32_t kDocs = 1 << 22;
constexpr size_t kPad = 2048;

enum class Shape : int {
  Titles,
  Articles,
  Books,
};

struct Term {
  std::vector<uint32_t> freq;
  std::vector<uint32_t> prefix;
  std::vector<uint32_t> gaps;
  std::vector<uint64_t> doc_block_first;
};

Term MakeTerm(Shape shape) {
  std::mt19937_64 rng{static_cast<uint64_t>(shape) + 11};
  Term t;
  t.freq.resize(kDocs);
  t.prefix.resize(kDocs);
  const double freq_mean =
    shape == Shape::Titles ? 1.1 : (shape == Shape::Articles ? 3.0 : 20.0);
  const double gap_mean =
    shape == Shape::Titles ? 3.0 : (shape == Shape::Articles ? 40.0 : 300.0);
  std::geometric_distribution<uint32_t> freq{1.0 / freq_mean};
  std::geometric_distribution<uint32_t> gap{1.0 / gap_mean};
  uint64_t total = 0;
  for (uint32_t d = 0; d != kDocs; ++d) {
    if (d % kDocBlock == 0) {
      t.doc_block_first.push_back(total);
    }
    t.prefix[d] = static_cast<uint32_t>(total - t.doc_block_first.back());
    t.freq[d] = 1 + freq(rng);
    for (uint32_t i = 0; i != t.freq[d]; ++i) {
      t.gaps.push_back(1 + gap(rng));
    }
    total += t.freq[d];
  }
  return t;
}

uint32_t EncodeBlock(const uint32_t* values, uint32_t len, uint8_t* out) {
  return len == kBlock ? Codec::EncodeValuesBlock(values, out)
                       : Codec::EncodeValuesTail(values, len, out);
}

class FixedGroups {
 public:
  explicit FixedGroups(uint32_t blocks) : _blocks{blocks} {}

  void Build(const Term& t) {
    const auto total = t.gaps.size();
    std::vector<uint32_t> padded(t.gaps);
    padded.resize((total + kBlock - 1) / kBlock * kBlock, 0);
    const size_t nblocks = padded.size() / kBlock;
    std::vector<uint8_t> enc(Codec::kMaxBlockBytes + 64);
    for (size_t g = 0; g * _blocks < nblocks; ++g) {
      const size_t header = _data.size();
      _data.resize(header + 2 * _blocks, 0);
      uint32_t end = 0;
      for (uint32_t j = 0; j != _blocks; ++j) {
        const size_t b = g * _blocks + j;
        if (b < nblocks) {
          const auto size =
            EncodeBlock(padded.data() + b * kBlock, kBlock, enc.data());
          _data.insert(_data.end(), enc.begin(), enc.begin() + size);
          end += size;
        }
        absl::little_endian::Store16(_data.data() + header + 2 * j,
                                     static_cast<uint16_t>(end));
      }
    }
    _bytes = _data.size();
    _data.resize(_data.size() + kPad, 0);
    uint64_t group = 0;
    uint64_t group_first = 0;
    for (const auto first : t.doc_block_first) {
      while (first - group_first >= uint64_t{_blocks} * kBlock) {
        group_first += uint64_t{_blocks} * kBlock;
        group = Next(group);
      }
      _landing.push_back({group, static_cast<uint32_t>(first - group_first)});
    }
  }

  uint64_t Next(uint64_t group) const noexcept {
    return group + 2 * _blocks +
           absl::little_endian::Load16(_data.data() + group +
                                       2 * (_blocks - 1));
  }

  uint32_t Start(uint64_t group, uint32_t block) const noexcept {
    return block == 0 ? 0
                      : absl::little_endian::Load16(_data.data() + group +
                                                    2 * (block - 1));
  }

  IRS_FORCE_INLINE uint64_t Fetch(uint32_t doc_block, uint32_t p, uint32_t f,
                                  uint32_t* buf) noexcept {
    auto [group, index] = _landing[doc_block];
    uint64_t target = uint64_t{index} + p;
    while (target >= uint64_t{_blocks} * kBlock) {
      target -= uint64_t{_blocks} * kBlock;
      group = Next(group);
    }
    uint64_t sum = 0;
    uint32_t pos = 0;
    while (f != 0) {
      const auto block = static_cast<uint32_t>(target / kBlock);
      const auto slot = static_cast<uint32_t>(target % kBlock);
      if (group != _group || block != _block) {
        Codec::DecodeValuesBlock(
          _data.data() + group + 2 * _blocks + Start(group, block), buf);
        _group = group;
        _block = block;
      }
      const auto take = std::min(f, kBlock - slot);
      for (uint32_t i = 0; i != take; ++i) {
        pos += buf[slot + i];
        sum += pos;
      }
      f -= take;
      target += take;
      if (target == uint64_t{_blocks} * kBlock) {
        target = 0;
        group = Next(group);
      }
    }
    return sum;
  }

  void Reset() noexcept {
    _group = ~uint64_t{0};
    _block = ~uint32_t{0};
  }

  size_t Bytes() const noexcept { return _bytes; }

 private:
  struct Landing {
    uint64_t group;
    uint32_t index;
  };

  std::vector<uint8_t> _data;
  std::vector<Landing> _landing;
  size_t _bytes = 0;
  uint64_t _group = ~uint64_t{0};
  uint32_t _block = ~uint32_t{0};
  uint32_t _blocks;
};

class DocBlockGroups {
 public:
  void Build(const Term& t) {
    std::vector<uint8_t> enc(Codec::kMaxBlockBytes + 64);
    const auto docs_blocks = t.doc_block_first.size();
    for (size_t b = 0; b != docs_blocks; ++b) {
      const uint64_t first = t.doc_block_first[b];
      const uint64_t last =
        b + 1 == docs_blocks ? t.gaps.size() : t.doc_block_first[b + 1];
      const auto count = static_cast<uint32_t>(last - first);
      const uint32_t blocks = (count + kBlock - 1) / kBlock;
      _landing.push_back({_data.size(), count});
      const size_t header = _data.size();
      _data.resize(header + 2 * (blocks - 1), 0);
      uint32_t end = 0;
      for (uint32_t j = 0; j != blocks; ++j) {
        const auto len = std::min(kBlock, count - j * kBlock);
        const auto size =
          EncodeBlock(t.gaps.data() + first + j * kBlock, len, enc.data());
        _data.insert(_data.end(), enc.begin(), enc.begin() + size);
        end += size;
        if (j + 1 != blocks) {
          absl::little_endian::Store16(_data.data() + header + 2 * j,
                                       static_cast<uint16_t>(end));
        }
      }
    }
    _bytes = _data.size();
    _data.resize(_data.size() + kPad, 0);
  }

  IRS_FORCE_INLINE uint64_t Fetch(uint32_t doc_block, uint32_t p, uint32_t f,
                                  uint32_t* buf) noexcept {
    const auto [group, count] = _landing[doc_block];
    const uint32_t blocks = (count + kBlock - 1) / kBlock;
    const auto* base = _data.data() + group + 2 * (blocks - 1);
    uint64_t sum = 0;
    uint32_t pos = 0;
    uint32_t target = p;
    while (f != 0) {
      const auto block = target / kBlock;
      const auto slot = target % kBlock;
      if (group != _group || block != _block) {
        const auto start = block == 0
                             ? 0
                             : absl::little_endian::Load16(
                                 _data.data() + group + 2 * (block - 1));
        const auto len = std::min(kBlock, count - block * kBlock);
        if (len == kBlock) {
          Codec::DecodeValuesBlock(base + start, buf);
        } else {
          Codec::DecodeValuesTail(base + start, len, buf);
        }
        _group = group;
        _block = block;
      }
      const auto take = std::min(f, kBlock - slot);
      for (uint32_t i = 0; i != take; ++i) {
        pos += buf[slot + i];
        sum += pos;
      }
      f -= take;
      target += take;
    }
    return sum;
  }

  void Reset() noexcept {
    _group = ~uint64_t{0};
    _block = ~uint32_t{0};
  }

  size_t Bytes() const noexcept { return _bytes; }

 private:
  struct Landing {
    uint64_t group;
    uint32_t count;
  };

  std::vector<uint8_t> _data;
  std::vector<Landing> _landing;
  size_t _bytes = 0;
  uint64_t _group = ~uint64_t{0};
  uint32_t _block = ~uint32_t{0};
};

struct Fixture {
  Term term;
  FixedGroups g16{16};
  FixedGroups g32{32};
  FixedGroups g64{64};
  DocBlockGroups gd;
  std::map<uint32_t, std::vector<uint32_t>> candidates;

  const std::vector<uint32_t>& Candidates(uint32_t every) {
    auto& c = candidates[every];
    if (c.empty()) {
      std::mt19937_64 rng{every};
      std::uniform_int_distribution<uint32_t> pick{0, every - 1};
      for (uint32_t d = 0; d < kDocs; d += every) {
        c.push_back(d + (every == 1 ? 0 : pick(rng)));
      }
    }
    return c;
  }
};

Fixture& GetFixture(Shape shape) {
  static std::map<int, std::unique_ptr<Fixture>> cache;
  auto& f = cache[static_cast<int>(shape)];
  if (!f) {
    f = std::make_unique<Fixture>();
    f->term = MakeTerm(shape);
    f->g16.Build(f->term);
    f->g32.Build(f->term);
    f->g64.Build(f->term);
    f->gd.Build(f->term);
  }
  return *f;
}

template<typename Layout>
uint64_t Expected(const Term& t, const std::vector<uint32_t>& candidates,
                  Layout& layout) {
  alignas(64) uint32_t buf[kBlock + 64];
  layout.Reset();
  uint64_t sum = 0;
  for (const auto d : candidates) {
    sum += layout.Fetch(d / kDocBlock, t.prefix[d], t.freq[d], buf);
  }
  return sum;
}

uint64_t Reference(const Term& t, const std::vector<uint32_t>& candidates) {
  uint64_t sum = 0;
  for (const auto d : candidates) {
    const uint64_t first = t.doc_block_first[d / kDocBlock] + t.prefix[d];
    uint32_t pos = 0;
    for (uint32_t i = 0; i != t.freq[d]; ++i) {
      pos += t.gaps[first + i];
      sum += pos;
    }
  }
  return sum;
}

template<int L>
void BmFetch(benchmark::State& state) {
  const auto shape = static_cast<Shape>(state.range(0));
  const auto every = static_cast<uint32_t>(state.range(1));
  auto& f = GetFixture(shape);
  const auto& candidates = f.Candidates(every);
  auto run = [&](auto& layout) {
    if (Expected(f.term, candidates, layout) != Reference(f.term, candidates)) {
      state.SkipWithError("mismatch");
      return;
    }
    alignas(64) uint32_t buf[kBlock + 64];
    uint64_t sink = 0;
    size_t i = 0;
    layout.Reset();
    for (auto _ : state) {
      const auto d = candidates[i];
      sink +=
        layout.Fetch(d / kDocBlock, f.term.prefix[d], f.term.freq[d], buf);
      if (++i == candidates.size()) {
        i = 0;
        layout.Reset();
      }
    }
    benchmark::DoNotOptimize(sink);
    state.counters["bytes_per_pos"] = static_cast<double>(layout.Bytes()) /
                                      static_cast<double>(f.term.gaps.size());
  };
  if constexpr (L == 16) {
    run(f.g16);
  } else if constexpr (L == 32) {
    run(f.g32);
  } else if constexpr (L == 64) {
    run(f.g64);
  } else {
    run(f.gd);
  }
}

void Args(benchmark::internal::Benchmark* b) {
  for (int shape : {0, 1, 2}) {
    for (int64_t every : {1, 8, 64, 1024}) {
      b->Args({shape, every});
    }
  }
  b->ArgNames({"shape", "every"});
}

BENCHMARK(BmFetch<16>)->Name("Group16")->Apply(Args);
BENCHMARK(BmFetch<32>)->Name("Group32")->Apply(Args);
BENCHMARK(BmFetch<64>)->Name("Group64")->Apply(Args);
BENCHMARK(BmFetch<0>)->Name("GroupPerDocBlock")->Apply(Args);

}  // namespace

BENCHMARK_MAIN();
