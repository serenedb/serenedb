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
#include <sys/mman.h>
#include <unistd.h>

#include <array>
#include <iresearch/formats/posting/block_codec.hpp>
#include <iresearch/formats/posting/block_io.hpp>
#include <iresearch/formats/posting/common.hpp>
#include <iresearch/search/detail/bitset_build.hpp>
#include <map>
#include <random>
#include <utility>
#include <vector>

namespace {

namespace bc = irs::block_codec;

using Codec = bc::BlockCodec;

constexpr uint32_t kBlock = bc::kBlock;
constexpr uint32_t kSlack = 64;
constexpr uint32_t kListDocs = 1024 * kBlock;

class Guarded {
 public:
  explicit Guarded(size_t size) {
    const auto page = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    _size = (size + page - 1) / page * page + page;
    _base =
      static_cast<irs::byte_type*>(mmap(nullptr, _size, PROT_READ | PROT_WRITE,
                                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    SDB_VERIFY(_base != MAP_FAILED, "mmap");
    _end = _base + _size - page;
    SDB_VERIFY(mprotect(_end, page, PROT_NONE) == 0, "mprotect");
  }

  ~Guarded() { munmap(_base, _size); }

  irs::byte_type* Bytes(size_t size) { return _end - size; }

  uint32_t* Values(size_t count) {
    return reinterpret_cast<uint32_t*>(_end - count * sizeof(uint32_t));
  }

 private:
  irs::byte_type* _base;
  irs::byte_type* _end;
  size_t _size;
};

struct Guards {
  Guarded encoded{bc::kMaxBlockBytes};
  Guarded in{bc::kMaxBlockBytes + bc::kInSlack};
  Guarded out{(kBlock + bc::kOutSlack) * sizeof(uint32_t)};
};

void CheckDocsBlock(Guards& guards, const std::vector<irs::doc_id_t>& docs,
                    irs::doc_id_t prev, const bc::EncodeOptions& options) {
  const auto len = static_cast<uint32_t>(docs.size());
  const bool full = len == kBlock;
  auto* encoded = guards.encoded.Bytes(bc::kMaxBlockBytes);
  const auto size =
    full ? Codec::EncodeDeltaBlock(docs.data(), prev, encoded, options)
         : Codec::EncodeDeltaTail(docs.data(), len, prev, encoded, options);
  SDB_VERIFY(size == (full ? Codec::DeltaBlockSize(encoded)
                           : Codec::DeltaTailSize(encoded, len)),
             "docs size, len ", len);
  auto* in = guards.in.Bytes(size + bc::kInSlack);
  std::memcpy(in, encoded, size);
  auto* out = guards.out.Values(len + bc::kOutSlack);
  const auto* end = full ? Codec::DecodeDeltaBlock(in, prev, out)
                         : Codec::DecodeDeltaTail(in, len, prev, out);
  SDB_VERIFY(end == in + size, "docs end, len ", len, " token ",
             uint32_t{encoded[0]});
  SDB_VERIFY(std::equal(docs.begin(), docs.end(), out), "docs, len ", len,
             " token ", uint32_t{encoded[0]});
  std::fill_n(out, len, 0);
  const auto* portable =
    full ? bc::kDeltaBlockDecoders<false>[in[0]](in, prev, out)
         : bc::kDeltaTailDecoders<false>[in[0]](in, len, prev, out);
  SDB_VERIFY(portable == in + size && std::equal(docs.begin(), docs.end(), out),
             "portable docs, len ", len, " token ", uint32_t{encoded[0]});
}

void CheckValuesBlock(Guards& guards, const std::vector<uint32_t>& values,
                      const bc::EncodeOptions& options) {
  const auto len = static_cast<uint32_t>(values.size());
  const bool full = len == kBlock;
  auto* encoded = guards.encoded.Bytes(bc::kMaxBlockBytes);
  const auto size =
    full ? Codec::EncodeValuesBlock(values.data(), encoded, options)
         : Codec::EncodeValuesTail(values.data(), len, encoded, options);
  SDB_VERIFY(size == (full ? Codec::ValuesBlockSize(encoded)
                           : Codec::ValuesTailSize(encoded, len)),
             "values size, len ", len);
  auto* in = guards.in.Bytes(size + bc::kInSlack);
  std::memcpy(in, encoded, size);
  auto* out = guards.out.Values(len + bc::kOutSlack);
  const auto* end = full ? Codec::DecodeValuesBlock(in, out)
                         : Codec::DecodeValuesTail(in, len, out);
  SDB_VERIFY(end == in + size, "values end, len ", len, " token ",
             uint32_t{encoded[0]});
  SDB_VERIFY(std::equal(values.begin(), values.end(), out), "values, len ", len,
             " token ", uint32_t{encoded[0]});
}

std::vector<irs::doc_id_t> RandomDocs(std::mt19937& rng, uint32_t len,
                                      irs::doc_id_t prev, uint32_t max_gap,
                                      uint32_t outliers, uint32_t outlier_gap) {
  std::uniform_int_distribution<uint32_t> gap{1, max_gap};
  std::vector<uint32_t> gaps(len);
  for (auto& g : gaps) {
    g = gap(rng);
  }
  for (uint32_t i = 0; i != outliers; ++i) {
    gaps[rng() % len] = outlier_gap;
  }
  std::vector<irs::doc_id_t> docs(len);
  for (uint32_t i = 0; i != len; ++i) {
    prev += gaps[i];
    docs[i] = prev;
  }
  return docs;
}

std::vector<uint32_t> RandomValues(std::mt19937& rng, uint32_t len,
                                   uint32_t base, uint32_t max,
                                   uint32_t outliers) {
  std::uniform_int_distribution<uint32_t> value{base, max};
  std::vector<uint32_t> values(len);
  for (auto& v : values) {
    v = value(rng);
  }
  for (uint32_t i = 0; i != outliers; ++i) {
    values[rng() % len] = 1'000'000 + value(rng) % 1000;
  }
  return values;
}

std::vector<bc::EncodeOptions> CheckedOptions() {
  return {
    {},
    {.narrow_highs = true},
    {.exception_cost_eighths = 12, .narrow_highs = true},
    {.exception_cost_eighths = 64},
  };
}

std::vector<uint32_t> WidthEdges() {
  std::vector<uint32_t> edges;
  for (uint32_t k = 1; k != 32; ++k) {
    const uint64_t power = uint64_t{1} << k;
    for (const uint64_t v : {power - 2, power - 1, power, power + 1}) {
      if (v <= static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
        edges.push_back(static_cast<uint32_t>(v));
      }
    }
  }
  while (edges.size() % 8 != 0) {
    edges.push_back(std::numeric_limits<int32_t>::max());
  }
  return edges;
}

uint64_t SelfCheck() {
  Guards guards;
  std::mt19937 rng{42};
  uint64_t checked = 0;
  const auto edges = WidthEdges();
  for (size_t i = 0; i != edges.size(); i += 8) {
    const auto widths = bc::BitWidths<true>(edges.data() + i);
    for (uint32_t k = 0; k != 8; ++k) {
      SDB_VERIFY(static_cast<uint32_t>(widths[k]) ==
                   static_cast<uint32_t>(std::bit_width(edges[i + k])),
                 "width of ", edges[i + k]);
      ++checked;
    }
  }
  for (size_t begin = 0; begin < edges.size(); begin += kBlock) {
    const auto end = std::min<size_t>(edges.size(), begin + kBlock);
    CheckValuesBlock(
      guards, std::vector<uint32_t>(edges.begin() + begin, edges.begin() + end),
      {});
    ++checked;
  }
  for (const auto& options : CheckedOptions()) {
    for (uint32_t len = 1; len <= kBlock; ++len) {
      for (const irs::doc_id_t prev : {0U, 1000U, 50'000'000U}) {
        for (const uint32_t max_gap : {1U, 2U, 3U, 64U, 70'000U}) {
          for (const uint32_t outliers : {0U, 1U, 5U}) {
            for (const uint32_t outlier_gap : {3'000'000U, 2'000'000'000U}) {
              if (uint64_t{prev} + uint64_t{len} * max_gap +
                    uint64_t{outliers} * outlier_gap >
                  std::numeric_limits<int32_t>::max()) {
                continue;
              }
              const auto docs =
                RandomDocs(rng, len, prev, max_gap, outliers, outlier_gap);
              CheckDocsBlock(guards, docs, prev, options);
              ++checked;
            }
          }
        }
      }
      for (const uint32_t base : {0U, 1U}) {
        for (const uint32_t max : {1U, 2U, 4U, 300U, 0x7FFFFFFFU}) {
          for (const uint32_t outliers : {0U, 1U, 5U, 40U}) {
            const auto values = RandomValues(rng, len, base, max, outliers);
            CheckValuesBlock(guards, values, options);
            ++checked;
          }
        }
      }
    }
  }
  return checked;
}

void BmSelfCheck(benchmark::State& state) {
  uint64_t checked = 0;
  for (auto _ : state) {
    checked += SelfCheck();
  }
  state.counters["checked"] = static_cast<double>(checked);
}

BENCHMARK(BmSelfCheck)->Iterations(1)->Unit(benchmark::kMillisecond);

enum class DocShape : uint8_t {
  Geometric2,
  Geometric8,
  Geometric64,
  Geometric1024,
  Bursty,
  Dense15of16,
};

enum class FreqShape : uint8_t {
  AllOne,
  OneOrTwo,
  Geometric15,
  Geometric3,
  HeavyTail,
};

std::vector<irs::doc_id_t> MakeDocs(DocShape shape, uint32_t count) {
  std::mt19937 rng{static_cast<uint32_t>(shape) + 1};
  std::geometric_distribution<uint32_t> inside{0.5};
  std::uniform_int_distribution<uint32_t> jump{10'000, 1'000'000};
  const auto gap = [&](uint32_t i) -> uint32_t {
    const auto geometric = [&](double mean) {
      return 1 + std::geometric_distribution<uint32_t>{1.0 / mean}(rng);
    };
    switch (shape) {
      case DocShape::Geometric2:
        return geometric(2);
      case DocShape::Geometric8:
        return geometric(8);
      case DocShape::Geometric64:
        return geometric(64);
      case DocShape::Geometric1024:
        return geometric(1024);
      case DocShape::Bursty:
        return rng() % 200 == 0 ? jump(rng) : 1 + inside(rng);
      case DocShape::Dense15of16:
        return i % 16 == 15 ? 2 : 1;
    }
    return 1;
  };
  std::vector<irs::doc_id_t> docs(count);
  irs::doc_id_t doc = 0;
  for (uint32_t i = 0; i != count; ++i) {
    if (i % kListDocs == 0) {
      doc = 0;
    }
    doc += gap(i);
    docs[i] = doc;
  }
  return docs;
}

std::vector<uint32_t> MakeFreqs(FreqShape shape, uint32_t count) {
  std::mt19937 rng{static_cast<uint32_t>(shape) + 100};
  std::vector<uint32_t> freqs(count, 1);
  switch (shape) {
    case FreqShape::AllOne:
      break;
    case FreqShape::OneOrTwo:
      for (auto& f : freqs) {
        f = 1 + (rng() % 4 == 0);
      }
      break;
    case FreqShape::Geometric15: {
      std::geometric_distribution<uint32_t> extra{1.0 / 1.5};
      for (auto& f : freqs) {
        f = 1 + extra(rng);
      }
    } break;
    case FreqShape::Geometric3: {
      std::geometric_distribution<uint32_t> extra{1.0 / 3.0};
      for (auto& f : freqs) {
        f = 1 + extra(rng);
      }
    } break;
    case FreqShape::HeavyTail:
      for (auto& f : freqs) {
        const auto r = rng() % 1000;
        f = r < 900   ? 1 + rng() % 2
            : r < 990 ? 3 + rng() % 8
                      : 10 + rng() % 1000;
      }
      break;
  }
  return freqs;
}

enum class PosShape : uint8_t {
  Title,
  Article,
  Book,
  Repeats,
  OffsetStarts,
  OffsetLengths,
};

std::vector<uint32_t> MakePositions(PosShape shape, uint32_t count) {
  std::mt19937 rng{static_cast<uint32_t>(shape) + 200};
  std::lognormal_distribution<double> title{std::log(12.0), 0.5};
  std::lognormal_distribution<double> article{std::log(600.0), 0.8};
  std::lognormal_distribution<double> book{std::log(20'000.0), 0.7};
  std::geometric_distribution<uint32_t> extra{1.0 / 3.0};
  std::vector<uint32_t> values;
  values.reserve(count + 4096);
  std::vector<uint32_t> positions;
  while (values.size() < count) {
    double length = 0;
    uint32_t freq = 1;
    switch (shape) {
      case PosShape::Title:
        length = title(rng);
        freq = 1 + (rng() % 8 == 0);
        break;
      case PosShape::Article:
      case PosShape::OffsetStarts:
      case PosShape::OffsetLengths:
        length = article(rng);
        freq = 1 + extra(rng);
        break;
      case PosShape::Book:
        length = book(rng);
        freq = 1 + extra(rng) * 8;
        break;
      case PosShape::Repeats:
        length = article(rng);
        freq = 2 + extra(rng);
        break;
    }
    const auto len = std::max<uint32_t>(1, static_cast<uint32_t>(length));
    freq = std::min(freq, len);
    positions.clear();
    if (shape == PosShape::Repeats) {
      const uint32_t start = 1 + rng() % len;
      for (uint32_t i = 0; i != freq; ++i) {
        positions.push_back(start + i);
      }
    } else {
      while (positions.size() != freq) {
        positions.push_back(1 + rng() % len);
        std::sort(positions.begin(), positions.end());
        positions.erase(std::unique(positions.begin(), positions.end()),
                        positions.end());
      }
    }
    uint32_t last = 0;
    for (const auto pos : positions) {
      switch (shape) {
        case PosShape::OffsetStarts:
          values.push_back((pos - last) * 6 + rng() % 5);
          break;
        case PosShape::OffsetLengths:
          values.push_back(rng() % 10 == 0 ? 4 + rng() % 5 : 3);
          break;
        default:
          values.push_back(pos - last);
          break;
      }
      last = pos;
    }
  }
  values.resize(count);
  return values;
}

irs::doc_id_t ListPrev(uint32_t block, irs::doc_id_t prev) {
  return block * kBlock % kListDocs == 0 ? 0 : prev;
}

irs::bstring EncodeDocs(const std::vector<irs::doc_id_t>& docs) {
  const auto blocks = static_cast<uint32_t>(docs.size() / kBlock);
  irs::bstring bytes;
  std::array<irs::byte_type, bc::kMaxBlockBytes> block;
  irs::doc_id_t prev = 0;
  for (uint32_t b = 0; b != blocks; ++b) {
    const auto* first = docs.data() + b * kBlock;
    prev = ListPrev(b, prev);
    bytes.append(block.data(),
                 Codec::EncodeDeltaBlock(first, prev, block.data()));
    prev = first[kBlock - 1];
  }
  bytes.append(kSlack, 0);
  const auto* p = bytes.data();
  std::array<irs::doc_id_t, kBlock + bc::kOutSlack> out;
  prev = 0;
  for (uint32_t b = 0; b != blocks; ++b) {
    const auto* first = docs.data() + b * kBlock;
    prev = ListPrev(b, prev);
    p = Codec::DecodeDeltaBlock(p, prev, out.data());
    SDB_VERIFY(std::equal(first, first + kBlock, out.data()), "docs, block ",
               b);
    prev = first[kBlock - 1];
  }
  return bytes;
}

template<uint32_t Add = 0>
irs::bstring EncodeValues(const std::vector<uint32_t>& values,
                          const bc::EncodeOptions& options = {}) {
  const auto blocks = static_cast<uint32_t>(values.size() / kBlock);
  irs::bstring bytes;
  std::array<irs::byte_type, bc::kMaxBlockBytes> block;
  std::array<uint32_t, kBlock> stored;
  for (uint32_t b = 0; b != blocks; ++b) {
    for (uint32_t i = 0; i != kBlock; ++i) {
      stored[i] = values[b * kBlock + i] - Add;
    }
    bytes.append(block.data(), Codec::EncodeValuesBlock(stored.data(),
                                                        block.data(), options));
  }
  bytes.append(kSlack, 0);
  const auto* p = bytes.data();
  std::array<uint32_t, kBlock + bc::kOutSlack> out;
  for (uint32_t b = 0; b != blocks; ++b) {
    const auto* first = values.data() + b * kBlock;
    p = Codec::DecodeValuesBlock<Add>(p, out.data());
    SDB_VERIFY(std::equal(first, first + kBlock, out.data()), "values, block ",
               b);
  }
  return bytes;
}

const irs::bstring& CachedDocs(DocShape shape, uint32_t values) {
  static std::map<std::pair<DocShape, uint32_t>, irs::bstring> cache;
  auto it = cache.find({shape, values});
  if (it == cache.end()) {
    it =
      cache
        .emplace(std::pair{shape, values}, EncodeDocs(MakeDocs(shape, values)))
        .first;
  }
  return it->second;
}

const irs::bstring& CachedFreqs(FreqShape shape, uint32_t values) {
  static std::map<std::pair<FreqShape, uint32_t>, irs::bstring> cache;
  auto it = cache.find({shape, values});
  if (it == cache.end()) {
    it = cache
           .emplace(std::pair{shape, values},
                    EncodeValues<irs::block_io::kFreqBias>(
                      MakeFreqs(shape, values), irs::block_io::kFreqOptions))
           .first;
  }
  return it->second;
}

const irs::bstring& CachedPositions(PosShape shape, uint32_t values) {
  static std::map<std::pair<PosShape, uint32_t>, irs::bstring> cache;
  auto it = cache.find({shape, values});
  if (it == cache.end()) {
    it = cache
           .emplace(std::pair{shape, values},
                    EncodeValues(MakePositions(shape, values)))
           .first;
  }
  return it->second;
}

void Report(benchmark::State& state, size_t bytes, uint32_t values) {
  state.SetItemsProcessed(state.iterations() * values);
  state.counters["bytes_per_block"] =
    static_cast<double>(bytes - kSlack) * kBlock / static_cast<double>(values);
}

template<typename Encoding>
void CountPatches(benchmark::State& state, const irs::bstring& bytes,
                  uint32_t blocks, uint32_t (*size)(const irs::byte_type*)) {
  const auto* p = bytes.data();
  uint32_t patched = 0;
  uint32_t exceptions = 0;
  uint32_t bitsets = 0;
  uint32_t bit_highs = 0;
  for (uint32_t b = 0; b != blocks; ++b) {
    const uint32_t token = p[0];
    if (std::is_same_v<Encoding, bc::DeltaEncoding> &&
        bc::IsTokenBitset(token)) {
      ++bitsets;
    } else if (token >= bc::Code(Encoding::PatchByte)) {
      ++patched;
      exceptions += p[1];
      bit_highs += token >= bc::Code(Encoding::PatchBit);
    }
    p += size(p);
  }
  state.counters["patched"] = static_cast<double>(patched) / blocks;
  state.counters["exceptions"] = static_cast<double>(exceptions) / blocks;
  state.counters["bitsets"] = static_cast<double>(bitsets) / blocks;
  state.counters["bit_highs"] = static_cast<double>(bit_highs) / blocks;
}

template<bool Portable>
void DecodeDocs(benchmark::State& state) {
  const auto shape = static_cast<DocShape>(state.range(0));
  const auto values = static_cast<uint32_t>(state.range(1));
  const auto blocks = values / kBlock;
  const auto& encoded = CachedDocs(shape, values);
  alignas(64) std::array<irs::doc_id_t, kBlock + bc::kOutSlack> out;
  for (auto _ : state) {
    const auto* p = encoded.data();
    irs::doc_id_t prev = 0;
    for (uint32_t b = 0; b != blocks; ++b) {
      if constexpr (Portable) {
        p = bc::kDeltaBlockDecoders<false>[p[0]](p, ListPrev(b, prev),
                                                 out.data());
      } else {
        p = Codec::DecodeDeltaBlock(p, ListPrev(b, prev), out.data());
      }
      prev = out[kBlock - 1];
    }
    benchmark::DoNotOptimize(prev);
  }
  Report(state, encoded.size(), values);
  CountPatches<bc::DeltaEncoding>(state, encoded, blocks,
                                  &Codec::DeltaBlockSize);
}

template<uint32_t Add = 0>
void DecodeValueBlocks(benchmark::State& state, const irs::bstring& encoded,
                       uint32_t values) {
  const auto blocks = values / kBlock;
  alignas(64) std::array<uint32_t, kBlock + bc::kOutSlack> out;
  for (auto _ : state) {
    const auto* p = encoded.data();
    for (uint32_t b = 0; b != blocks; ++b) {
      p = Codec::DecodeValuesBlock<Add>(p, out.data());
    }
    benchmark::DoNotOptimize(out);
    benchmark::ClobberMemory();
  }
  Report(state, encoded.size(), values);
  CountPatches<bc::ValueEncoding>(state, encoded, blocks,
                                  &Codec::ValuesBlockSize);
}

void BmDocs(benchmark::State& state) { DecodeDocs<false>(state); }

void BmDocsPortable(benchmark::State& state) { DecodeDocs<true>(state); }

void BmFreqs(benchmark::State& state) {
  const auto values = static_cast<uint32_t>(state.range(1));
  DecodeValueBlocks<irs::block_io::kFreqBias>(
    state, CachedFreqs(static_cast<FreqShape>(state.range(0)), values), values);
}

void BmPositions(benchmark::State& state) {
  const auto values = static_cast<uint32_t>(state.range(1));
  DecodeValueBlocks(
    state, CachedPositions(static_cast<PosShape>(state.range(0)), values),
    values);
}

constexpr std::array<int, 3> kValueCounts = {256, 4096 * 128, 262144 * 128};

void DocShapes(benchmark::internal::Benchmark* b) {
  for (int shape = 0; shape <= static_cast<int>(DocShape::Dense15of16);
       ++shape) {
    for (const int values : kValueCounts) {
      b->Args({shape, values});
    }
  }
}

void FreqShapes(benchmark::internal::Benchmark* b) {
  for (int shape = 0; shape <= static_cast<int>(FreqShape::HeavyTail);
       ++shape) {
    for (const int values : kValueCounts) {
      b->Args({shape, values});
    }
  }
}

void PosShapes(benchmark::internal::Benchmark* b) {
  for (int shape = 0; shape <= static_cast<int>(PosShape::OffsetLengths);
       ++shape) {
    for (const int values : {4096 * 128, 262144 * 128}) {
      b->Args({shape, values});
    }
  }
}

BENCHMARK(BmDocs)->Apply(DocShapes);
BENCHMARK(BmDocsPortable)->Apply(DocShapes);
BENCHMARK(BmFreqs)->Apply(FreqShapes);
BENCHMARK(BmPositions)->Apply(PosShapes);

constexpr uint32_t kEncodeValues = 1024 * kBlock;

void BmEncodeDocs(benchmark::State& state) {
  const auto docs =
    MakeDocs(static_cast<DocShape>(state.range(0)), kEncodeValues);
  std::vector<irs::byte_type> out(bc::kMaxBlockBytes + kSlack);
  for (auto _ : state) {
    irs::doc_id_t prev = 0;
    for (uint32_t b = 0; b != kEncodeValues / kBlock; ++b) {
      const auto* first = docs.data() + b * kBlock;
      benchmark::DoNotOptimize(
        Codec::EncodeDeltaBlock(first, prev, out.data()));
      prev = first[kBlock - 1];
    }
  }
  state.SetItemsProcessed(state.iterations() * kEncodeValues);
}

void EncodeValueBlocks(benchmark::State& state,
                       const std::vector<uint32_t>& values,
                       const bc::EncodeOptions& options = {}) {
  std::vector<irs::byte_type> out(bc::kMaxBlockBytes + kSlack);
  for (auto _ : state) {
    for (uint32_t b = 0; b != kEncodeValues / kBlock; ++b) {
      benchmark::DoNotOptimize(Codec::EncodeValuesBlock(
        values.data() + b * kBlock, out.data(), options));
    }
  }
  state.SetItemsProcessed(state.iterations() * kEncodeValues);
}

void BmEncodeFreqs(benchmark::State& state) {
  auto freqs = MakeFreqs(static_cast<FreqShape>(state.range(0)), kEncodeValues);
  for (auto& f : freqs) {
    f -= irs::block_io::kFreqBias;
  }
  EncodeValueBlocks(state, freqs, irs::block_io::kFreqOptions);
}

void BmEncodePositions(benchmark::State& state) {
  EncodeValueBlocks(
    state, MakePositions(static_cast<PosShape>(state.range(0)), kEncodeValues));
}

BENCHMARK(BmEncodeDocs)->DenseRange(0, 5);
BENCHMARK(BmEncodeFreqs)->DenseRange(0, 4);
BENCHMARK(BmEncodePositions)->DenseRange(0, 5);

constexpr uint32_t kTails = 1024;

std::vector<uint32_t> MakeTails(uint32_t len, uint32_t bits, bool docs) {
  std::mt19937 rng{len * 64 + bits};
  std::vector<uint32_t> values(kTails * len);
  for (uint32_t t = 0; t != kTails; ++t) {
    const uint32_t mask =
      (uint32_t{1} << (bits != 0 ? bits : 1 + rng() % 20)) - 1;
    uint32_t doc = 0;
    for (uint32_t i = 0; i != len; ++i) {
      const uint32_t value = 1 + (static_cast<uint32_t>(rng()) & mask);
      doc += value;
      values[t * len + i] = docs ? doc : value;
    }
  }
  return values;
}

void ReportTails(benchmark::State& state, size_t bytes, uint32_t len) {
  state.SetItemsProcessed(state.iterations() * kTails * len);
  state.counters["bytes_per_value"] =
    static_cast<double>(bytes) / (kTails * static_cast<double>(len));
}

template<bool Portable>
void DecodeTailDocs(benchmark::State& state) {
  const auto len = static_cast<uint32_t>(state.range(0));
  const auto docs = MakeTails(len, static_cast<uint32_t>(state.range(1)), true);
  irs::bstring bytes;
  std::array<irs::byte_type, bc::kMaxBlockBytes> block;
  for (uint32_t t = 0; t != kTails; ++t) {
    bytes.append(block.data(), Codec::EncodeDeltaTail(docs.data() + t * len,
                                                      len, 0, block.data()));
  }
  const auto size = bytes.size();
  bytes.append(kSlack, 0);
  alignas(64) std::array<irs::doc_id_t, kBlock + bc::kOutSlack> out;
  const auto decode = [&](const irs::byte_type* in) {
    if constexpr (Portable) {
      return bc::kDeltaTailDecoders<false>[in[0]](in, len, 0, out.data());
    } else {
      return Codec::DecodeDeltaTail(in, len, 0, out.data());
    }
  };
  const auto* p = bytes.data();
  for (uint32_t t = 0; t != kTails; ++t) {
    p = decode(p);
    SDB_VERIFY(
      std::equal(out.begin(), out.begin() + len, docs.data() + t * len),
      "tail docs ", t);
  }
  for (auto _ : state) {
    p = bytes.data();
    for (uint32_t t = 0; t != kTails; ++t) {
      p = decode(p);
    }
    benchmark::DoNotOptimize(out);
    benchmark::ClobberMemory();
  }
  ReportTails(state, size, len);
}

void BmTailDocs(benchmark::State& state) { DecodeTailDocs<false>(state); }

void BmTailDocsPortable(benchmark::State& state) {
  DecodeTailDocs<true>(state);
}

void BmTailValues(benchmark::State& state) {
  const auto len = static_cast<uint32_t>(state.range(0));
  const auto values =
    MakeTails(len, static_cast<uint32_t>(state.range(1)), false);
  irs::bstring bytes;
  std::array<irs::byte_type, bc::kMaxBlockBytes> block;
  for (uint32_t t = 0; t != kTails; ++t) {
    bytes.append(block.data(), Codec::EncodeValuesTail(values.data() + t * len,
                                                       len, block.data()));
  }
  const auto size = bytes.size();
  bytes.append(kSlack, 0);
  alignas(64) std::array<uint32_t, kBlock + bc::kOutSlack> out;
  const auto* p = bytes.data();
  for (uint32_t t = 0; t != kTails; ++t) {
    p = Codec::DecodeValuesTail(p, len, out.data());
    SDB_VERIFY(
      std::equal(out.begin(), out.begin() + len, values.data() + t * len),
      "tail values ", t);
  }
  for (auto _ : state) {
    p = bytes.data();
    for (uint32_t t = 0; t != kTails; ++t) {
      p = Codec::DecodeValuesTail(p, len, out.data());
    }
    benchmark::DoNotOptimize(out);
    benchmark::ClobberMemory();
  }
  ReportTails(state, size, len);
}

void BmEncodeTailDocs(benchmark::State& state) {
  const auto len = static_cast<uint32_t>(state.range(0));
  const auto docs = MakeTails(len, static_cast<uint32_t>(state.range(1)), true);
  std::array<irs::byte_type, bc::kMaxBlockBytes> block;
  for (auto _ : state) {
    for (uint32_t t = 0; t != kTails; ++t) {
      benchmark::DoNotOptimize(
        Codec::EncodeDeltaTail(docs.data() + t * len, len, 0, block.data()));
    }
  }
  state.SetItemsProcessed(state.iterations() * kTails * len);
}

void BmEncodeTailValues(benchmark::State& state) {
  const auto len = static_cast<uint32_t>(state.range(0));
  const auto values =
    MakeTails(len, static_cast<uint32_t>(state.range(1)), false);
  std::array<irs::byte_type, bc::kMaxBlockBytes> block;
  for (auto _ : state) {
    for (uint32_t t = 0; t != kTails; ++t) {
      benchmark::DoNotOptimize(
        Codec::EncodeValuesTail(values.data() + t * len, len, block.data()));
    }
  }
  state.SetItemsProcessed(state.iterations() * kTails * len);
}

void TailShapes(benchmark::internal::Benchmark* b) {
  for (const int len : {4, 16, 64, 127}) {
    for (const int bits : {2, 7, 16, 0}) {
      b->Args({len, bits});
    }
  }
}

BENCHMARK(BmTailDocs)->Apply(TailShapes);
BENCHMARK(BmTailDocsPortable)->Apply(TailShapes);
BENCHMARK(BmTailValues)->Apply(TailShapes);
BENCHMARK(BmEncodeTailDocs)->Apply(TailShapes);
BENCHMARK(BmEncodeTailValues)->Apply(TailShapes);

enum class GapShape : uint8_t {
  Geometric,
  Mixed,
};

constexpr uint32_t kBiasDocs = 1024 * kBlock;

const std::vector<irs::doc_id_t>& CachedGapDocs(GapShape shape,
                                                uint32_t mean_x10) {
  static std::map<std::pair<GapShape, uint32_t>, std::vector<irs::doc_id_t>>
    cache;
  auto it = cache.find({shape, mean_x10});
  if (it != cache.end()) {
    return it->second;
  }
  std::mt19937 rng{600 + mean_x10 * 2 + static_cast<uint32_t>(shape)};
  const double mean = mean_x10 / 10.0;
  const double near = shape == GapShape::Mixed ? std::max(1.0, mean / 2) : mean;
  const double far = (mean - 0.9 * near) / 0.1;
  const auto draw = [&](double m) {
    return 1 + std::geometric_distribution<uint32_t>{1.0 / m}(rng);
  };
  std::vector<irs::doc_id_t> docs(kBiasDocs);
  irs::doc_id_t doc = 0;
  for (auto& d : docs) {
    doc += shape == GapShape::Mixed && rng() % 10 == 0 ? draw(far) : draw(near);
    d = doc;
  }
  return cache.emplace(std::pair{shape, mean_x10}, std::move(docs))
    .first->second;
}

const irs::bstring& CachedBias(GapShape shape, uint32_t mean_x10) {
  static std::map<std::pair<GapShape, uint32_t>, irs::bstring> cache;
  auto it = cache.find({shape, mean_x10});
  if (it != cache.end()) {
    return it->second;
  }
  const auto& docs = CachedGapDocs(shape, mean_x10);
  irs::bstring bytes;
  std::array<irs::byte_type, bc::kMaxBlockBytes> block;
  irs::doc_id_t prev = 0;
  for (uint32_t b = 0; b != kBiasDocs / kBlock; ++b) {
    bytes.append(block.data(), Codec::EncodeDeltaBlock(docs.data() + b * kBlock,
                                                       prev, block.data()));
    prev = docs[b * kBlock + kBlock - 1];
  }
  bytes.append(kSlack, 0);
  const auto* p = bytes.data();
  std::array<irs::doc_id_t, kBlock + bc::kOutSlack> out;
  prev = 0;
  for (uint32_t b = 0; b != kBiasDocs / kBlock; ++b) {
    p = Codec::DecodeDeltaBlock(p, prev, out.data());
    SDB_VERIFY(
      std::equal(out.begin(), out.begin() + kBlock, docs.data() + b * kBlock),
      "bias block ", b);
    prev = out[kBlock - 1];
  }
  return cache.emplace(std::pair{shape, mean_x10}, std::move(bytes))
    .first->second;
}

void BmBiasDecode(benchmark::State& state) {
  const auto shape = static_cast<GapShape>(state.range(0));
  const auto mean_x10 = static_cast<uint32_t>(state.range(1));
  const auto& encoded = CachedBias(shape, mean_x10);
  alignas(64) std::array<irs::doc_id_t, kBlock + bc::kOutSlack> out;
  for (auto _ : state) {
    const auto* p = encoded.data();
    irs::doc_id_t prev = 0;
    for (uint32_t b = 0; b != kBiasDocs / kBlock; ++b) {
      p = Codec::DecodeDeltaBlock(p, prev, out.data());
      prev = out[kBlock - 1];
    }
    benchmark::DoNotOptimize(prev);
  }
  Report(state, encoded.size(), kBiasDocs);
  CountPatches<bc::DeltaEncoding>(state, encoded, kBiasDocs / kBlock,
                                  &Codec::DeltaBlockSize);
}

template<typename Sink>
void BmBiasSink(benchmark::State& state) {
  constexpr auto kBits = irs::BitsRequired<uint64_t>();
  const auto shape = static_cast<GapShape>(state.range(0));
  const auto mean_x10 = static_cast<uint32_t>(state.range(1));
  const auto& encoded = CachedBias(shape, mean_x10);
  const auto& docs = CachedGapDocs(shape, mean_x10);
  constexpr bool kClear = std::is_same_v<Sink, irs::detail::ClearBits>;
  std::vector<uint64_t> words(docs.back() / kBits + 2 * kBlock,
                              kClear ? ~uint64_t{0} : 0);
  irs::PostingMeta meta;
  meta.docs_count = kBiasDocs;
  irs::detail::HoleBuf holes;
  irs::DocsBuf buf;
  for (auto _ : state) {
    irs::BytesViewInput in{
      irs::bytes_view{encoded.data(), encoded.size() - kSlack}};
    Sink sink{words.data()};
    irs::detail::ReadPosting(meta, in, nullptr, holes.data, buf.data(), false,
                             false, sink);
    benchmark::DoNotOptimize(words.data());
    benchmark::ClobberMemory();
  }
  uint64_t set = 0;
  for (const auto word : words) {
    set += std::popcount(word);
  }
  SDB_VERIFY(set == (kClear ? words.size() * kBits - kBiasDocs : kBiasDocs),
             "bias sink sets ", set);
  Report(state, encoded.size(), kBiasDocs);
}

void BmBiasSet(benchmark::State& state) {
  BmBiasSink<irs::detail::OrBits>(state);
}

void BmBiasClear(benchmark::State& state) {
  BmBiasSink<irs::detail::ClearBits>(state);
}

void BiasArgs(benchmark::internal::Benchmark* b) {
  for (const int shape : {0, 1}) {
    for (const int mean_x10 :
         {12, 15, 20, 25, 30, 40, 50, 60, 80, 100, 120, 160, 200, 250, 320}) {
      b->Args({shape, mean_x10});
    }
  }
}

BENCHMARK(BmBiasDecode)->Apply(BiasArgs);
BENCHMARK(BmBiasSet)->Apply(BiasArgs);
BENCHMARK(BmBiasClear)->Apply(BiasArgs);

}  // namespace
