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
#include <iresearch/formats/posting/format_block_128.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/store_utils.hpp>
#include <iresearch/utils/bytes_output.hpp>
#include <map>
#include <random>
#include <utility>
#include <vector>

namespace {

namespace bc = irs::block_codec;

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

template<typename Codec>
struct Guards {
  Guarded encoded{Codec::kMaxBlockBytes};
  Guarded in{Codec::kMaxBlockBytes + bc::kInSlack};
  Guarded out{(Codec::kBlock + bc::kOutSlack) * sizeof(uint32_t)};
};

template<typename Codec>
void CheckDocsBlock(Guards<Codec>& guards,
                    const std::vector<irs::doc_id_t>& docs, irs::doc_id_t prev,
                    const bc::EncodeOptions& options) {
  const auto len = static_cast<uint32_t>(docs.size());
  const bool full = len == Codec::kBlock;
  auto* encoded = guards.encoded.Bytes(Codec::kMaxBlockBytes);
  const auto size =
    full ? Codec::EncodeDeltaBlock(docs.data(), prev, encoded, options)
         : Codec::EncodeDeltaTail(docs.data(), len, prev, encoded, options);
  SDB_VERIFY(size == (full ? Codec::DeltaBlockSize(encoded)
                           : Codec::DeltaTailSize(encoded, len)),
             "docs size, len ", len);
  auto* in = guards.in.Bytes(size + bc::kInSlack);
  std::memcpy(in, encoded, size);
  auto* out = guards.out.Values(Codec::kBlock + bc::kOutSlack);
  const auto* end = full ? Codec::DecodeDeltaBlock(in, prev, out)
                         : Codec::DecodeDeltaTail(in, len, prev, out);
  SDB_VERIFY(end == in + size, "docs end, len ", len, " token ",
             uint32_t{encoded[0]});
  SDB_VERIFY(std::equal(docs.begin(), docs.end(), out), "docs, len ", len,
             " token ", uint32_t{encoded[0]});
  std::fill_n(out, len, 0);
  const auto* portable =
    full
      ? bc::kDeltaBlockDecoders<Codec::kLanes, false>[in[0]](in, prev, out)
      : bc::kDeltaTailDecoders<Codec::kLanes, false>[in[0]](in, len, prev, out);
  SDB_VERIFY(portable == in + size && std::equal(docs.begin(), docs.end(), out),
             "portable docs, len ", len, " token ", uint32_t{encoded[0]});
}

template<typename Codec>
void CheckValuesBlock(Guards<Codec>& guards,
                      const std::vector<uint32_t>& values,
                      const bc::EncodeOptions& options) {
  const auto len = static_cast<uint32_t>(values.size());
  const bool full = len == Codec::kBlock;
  auto* encoded = guards.encoded.Bytes(Codec::kMaxBlockBytes);
  const auto size =
    full ? Codec::EncodeValuesBlock(values.data(), encoded, options)
         : Codec::EncodeValuesTail(values.data(), len, encoded, options);
  SDB_VERIFY(size == (full ? Codec::ValuesBlockSize(encoded)
                           : Codec::ValuesTailSize(encoded, len)),
             "values size, len ", len);
  auto* in = guards.in.Bytes(size + bc::kInSlack);
  std::memcpy(in, encoded, size);
  auto* out = guards.out.Values(Codec::kBlock);
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
    {.minus_one = false},
    {.patch16 = false, .patch32 = false, .patch_mixed = false},
    {.patch16 = false,
     .patch32 = false,
     .patch_mixed = false,
     .patch_bitmap = false},
    {.patch16 = true, .patch32 = false, .patch_mixed = false},
    {.patch16 = false, .patch32 = true, .patch_mixed = false},
    {.patch16 = false, .patch32 = false, .patch_mixed = true},
    {.patch_bitmap = false},
    {.bitset_margin_percent = 0},
    {.bitset_margin_percent = 1000},
    {.bitset = false},
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

template<typename Codec>
uint64_t SelfCheck() {
  Guards<Codec> guards;
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
  for (size_t begin = 0; begin < edges.size(); begin += Codec::kBlock) {
    const auto end = std::min<size_t>(edges.size(), begin + Codec::kBlock);
    CheckValuesBlock(
      guards, std::vector<uint32_t>(edges.begin() + begin, edges.begin() + end),
      {});
    ++checked;
  }
  for (const auto& options : CheckedOptions()) {
    for (uint32_t len = 1; len <= Codec::kBlock; ++len) {
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
    checked += SelfCheck<bc::Codec128>();
    checked += SelfCheck<bc::Codec256>();
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

template<typename Codec>
irs::doc_id_t ListPrev(uint32_t block, irs::doc_id_t prev) {
  return block * Codec::kBlock % kListDocs == 0 ? 0 : prev;
}

irs::bstring Collect(irs::MemoryOutput& out) {
  out.stream.Flush();
  irs::bstring bytes;
  irs::BytesOutput sink{bytes};
  out.file >> sink;
  bytes.resize(bytes.size() + kSlack);
  return bytes;
}

struct Current {
  static constexpr uint32_t kBlock = bc::kBlock;
};

template<typename Codec>
irs::bstring EncodeDocs(const std::vector<irs::doc_id_t>& docs) {
  constexpr uint32_t kN = Codec::kBlock;
  const auto blocks = static_cast<uint32_t>(docs.size() / kN);
  irs::bstring bytes;
  std::array<irs::byte_type, Codec::kMaxBlockBytes> block;
  irs::doc_id_t prev = 0;
  for (uint32_t b = 0; b != blocks; ++b) {
    const auto* first = docs.data() + b * kN;
    prev = ListPrev<Codec>(b, prev);
    bytes.append(block.data(),
                 Codec::EncodeDeltaBlock(first, prev, block.data()));
    prev = first[kN - 1];
  }
  bytes.append(kSlack, 0);
  const auto* p = bytes.data();
  std::array<irs::doc_id_t, kN + bc::kOutSlack> out;
  prev = 0;
  for (uint32_t b = 0; b != blocks; ++b) {
    const auto* first = docs.data() + b * kN;
    prev = ListPrev<Codec>(b, prev);
    p = Codec::DecodeDeltaBlock(p, prev, out.data());
    SDB_VERIFY(std::equal(first, first + kN, out.data()), "docs, block ", b);
    prev = first[kN - 1];
  }
  return bytes;
}

template<>
irs::bstring EncodeDocs<Current>(const std::vector<irs::doc_id_t>& docs) {
  const auto blocks = static_cast<uint32_t>(docs.size() / kBlock);
  irs::MemoryOutput current{irs::IResourceManager::gNoop};
  alignas(64) std::array<uint32_t, kBlock> block;
  alignas(64) std::array<uint32_t, kBlock> scratch;
  irs::doc_id_t prev = 0;
  for (uint32_t b = 0; b != blocks; ++b) {
    const auto* first = docs.data() + b * kBlock;
    prev = ListPrev<Current>(b, prev);
    std::copy_n(first, kBlock, block.data());
    irs::FormatTraits128::WriteBlockDelta(current.stream, block.data(), prev,
                                          scratch.data());
    prev = first[kBlock - 1];
  }
  auto bytes = Collect(current);
  irs::BytesViewInput in{irs::bytes_view{bytes}};
  irs::DocsBuf out;
  prev = 0;
  for (uint32_t b = 0; b != blocks; ++b) {
    const auto* first = docs.data() + b * kBlock;
    prev = ListPrev<Current>(b, prev);
    irs::FormatTraits128::ReadBlockDelta(in, scratch.data(), out.data(), prev);
    SDB_VERIFY(std::equal(first, first + kBlock, out.data()),
               "current docs, block ", b);
    prev = first[kBlock - 1];
  }
  return bytes;
}

template<typename Codec>
irs::bstring EncodeValues(const std::vector<uint32_t>& values) {
  constexpr uint32_t kN = Codec::kBlock;
  const auto blocks = static_cast<uint32_t>(values.size() / kN);
  irs::bstring bytes;
  std::array<irs::byte_type, Codec::kMaxBlockBytes> block;
  for (uint32_t b = 0; b != blocks; ++b) {
    bytes.append(block.data(), Codec::EncodeValuesBlock(values.data() + b * kN,
                                                        block.data()));
  }
  bytes.append(kSlack, 0);
  const auto* p = bytes.data();
  std::array<uint32_t, kN + bc::kOutSlack> out;
  for (uint32_t b = 0; b != blocks; ++b) {
    const auto* first = values.data() + b * kN;
    p = Codec::DecodeValuesBlock(p, out.data());
    SDB_VERIFY(std::equal(first, first + kN, out.data()), "values, block ", b);
  }
  return bytes;
}

template<>
irs::bstring EncodeValues<Current>(const std::vector<uint32_t>& values) {
  const auto blocks = static_cast<uint32_t>(values.size() / kBlock);
  irs::MemoryOutput current{irs::IResourceManager::gNoop};
  alignas(64) std::array<uint32_t, kBlock> block;
  alignas(64) std::array<uint32_t, kBlock> scratch;
  for (uint32_t b = 0; b != blocks; ++b) {
    std::copy_n(values.data() + b * kBlock, kBlock, block.data());
    irs::FormatTraits128::WriteBlock(current.stream, block.data(),
                                     scratch.data());
  }
  auto bytes = Collect(current);
  irs::BytesViewInput in{irs::bytes_view{bytes}};
  std::array<uint32_t, kBlock> out;
  for (uint32_t b = 0; b != blocks; ++b) {
    const auto* first = values.data() + b * kBlock;
    irs::FormatTraits128::ReadBlock(in, scratch.data(), out.data());
    SDB_VERIFY(std::equal(first, first + kBlock, out.data()),
               "current values, block ", b);
  }
  return bytes;
}

template<typename Codec>
const irs::bstring& CachedDocs(DocShape shape, uint32_t values) {
  static std::map<std::pair<DocShape, uint32_t>, irs::bstring> cache;
  auto it = cache.find({shape, values});
  if (it == cache.end()) {
    it = cache
           .emplace(std::pair{shape, values},
                    EncodeDocs<Codec>(MakeDocs(shape, values)))
           .first;
  }
  return it->second;
}

template<typename Codec>
const irs::bstring& CachedFreqs(FreqShape shape, uint32_t values) {
  static std::map<std::pair<FreqShape, uint32_t>, irs::bstring> cache;
  auto it = cache.find({shape, values});
  if (it == cache.end()) {
    it = cache
           .emplace(std::pair{shape, values},
                    EncodeValues<Codec>(MakeFreqs(shape, values)))
           .first;
  }
  return it->second;
}

template<typename Codec>
const irs::bstring& CachedPositions(PosShape shape, uint32_t values) {
  static std::map<std::pair<PosShape, uint32_t>, irs::bstring> cache;
  auto it = cache.find({shape, values});
  if (it == cache.end()) {
    it = cache
           .emplace(std::pair{shape, values},
                    EncodeValues<Codec>(MakePositions(shape, values)))
           .first;
  }
  return it->second;
}

void Report(benchmark::State& state, size_t bytes, uint32_t values) {
  state.SetItemsProcessed(state.iterations() * values);
  state.counters["bytes_per_128"] =
    static_cast<double>(bytes - kSlack) * kBlock / static_cast<double>(values);
}

template<typename Encoding, typename Codec>
void CountPatches(benchmark::State& state, const irs::bstring& bytes,
                  uint32_t blocks, uint32_t (*size)(const irs::byte_type*)) {
  const auto* p = bytes.data();
  uint32_t patched = 0;
  uint32_t exceptions = 0;
  uint32_t bitsets = 0;
  uint32_t minus_one = 0;
  uint32_t bitmaps = 0;
  for (uint32_t b = 0; b != blocks; ++b) {
    const uint32_t token = p[0];
    if constexpr (std::is_same_v<Encoding, bc::DeltaEncoding>) {
      bitsets += token == static_cast<uint32_t>(bc::DeltaEncoding::Bitset);
    }
    if (token >= static_cast<uint32_t>(Encoding::Pack)) {
      const auto shape = bc::ShapeOf<Encoding>(token);
      minus_one += shape.add;
      if (shape.family == bc::Family::PatchBitmap) {
        ++patched;
        ++bitmaps;
        exceptions += bc::BitmapCount<Codec::kLanes>(
          p + 2 + bc::PackedSize<Codec::kLanes>(Codec::kBlock, shape.bits));
      } else if (shape.family == bc::Family::PatchMixed) {
        ++patched;
        exceptions += p[1] + p[2];
      } else if (shape.family != bc::Family::Pack) {
        ++patched;
        exceptions += p[1];
      }
    }
    p += size(p);
  }
  state.counters["patched"] = static_cast<double>(patched) / blocks;
  state.counters["exceptions"] = static_cast<double>(exceptions) / blocks;
  state.counters["bitsets"] = static_cast<double>(bitsets) / blocks;
  state.counters["bitmaps"] = static_cast<double>(bitmaps) / blocks;
  state.counters["minus_one"] = static_cast<double>(minus_one) / blocks;
}

template<typename Codec, bool Portable = false>
void BmDocs(benchmark::State& state) {
  constexpr uint32_t kN = Codec::kBlock;
  const auto shape = static_cast<DocShape>(state.range(0));
  const auto values = static_cast<uint32_t>(state.range(1));
  const auto blocks = values / kN;
  const auto& encoded = CachedDocs<Codec>(shape, values);
  alignas(64) std::array<irs::doc_id_t, kN + bc::kOutSlack> out;
  for (auto _ : state) {
    const auto* p = encoded.data();
    irs::doc_id_t prev = 0;
    for (uint32_t b = 0; b != blocks; ++b) {
      if constexpr (Portable) {
        p = bc::kDeltaBlockDecoders<Codec::kLanes, false>[p[0]](
          p, ListPrev<Codec>(b, prev), out.data());
      } else {
        p = Codec::DecodeDeltaBlock(p, ListPrev<Codec>(b, prev), out.data());
      }
      prev = out[kN - 1];
    }
    benchmark::DoNotOptimize(prev);
  }
  Report(state, encoded.size(), values);
  CountPatches<bc::DeltaEncoding, Codec>(state, encoded, blocks,
                                         &Codec::DeltaBlockSize);
}

void BmDocsCurrent(benchmark::State& state) {
  const auto shape = static_cast<DocShape>(state.range(0));
  const auto values = static_cast<uint32_t>(state.range(1));
  const auto blocks = values / kBlock;
  const auto& encoded = CachedDocs<Current>(shape, values);
  irs::BytesViewInput in{irs::bytes_view{encoded}};
  irs::DocsBuf out;
  alignas(64) std::array<uint32_t, kBlock> scratch;
  for (auto _ : state) {
    in.Seek(0);
    irs::doc_id_t prev = 0;
    for (uint32_t b = 0; b != blocks; ++b) {
      irs::FormatTraits128::ReadBlockDelta(in, scratch.data(), out.data(),
                                           ListPrev<Current>(b, prev));
      prev = out[kBlock - 1];
    }
    benchmark::DoNotOptimize(prev);
  }
  Report(state, encoded.size(), values);
}

template<typename Codec>
void DecodeValueBlocks(benchmark::State& state, const irs::bstring& encoded,
                       uint32_t values) {
  constexpr uint32_t kN = Codec::kBlock;
  const auto blocks = values / kN;
  alignas(64) std::array<uint32_t, kN + bc::kOutSlack> out;
  for (auto _ : state) {
    const auto* p = encoded.data();
    for (uint32_t b = 0; b != blocks; ++b) {
      p = Codec::DecodeValuesBlock(p, out.data());
    }
    benchmark::DoNotOptimize(out);
    benchmark::ClobberMemory();
  }
  Report(state, encoded.size(), values);
  CountPatches<bc::ValueEncoding, Codec>(state, encoded, blocks,
                                         &Codec::ValuesBlockSize);
}

void DecodeValueBlocksCurrent(benchmark::State& state,
                              const irs::bstring& encoded, uint32_t values) {
  const auto blocks = values / kBlock;
  irs::BytesViewInput in{irs::bytes_view{encoded}};
  alignas(64) std::array<uint32_t, kBlock> out;
  alignas(64) std::array<uint32_t, kBlock> scratch;
  for (auto _ : state) {
    in.Seek(0);
    for (uint32_t b = 0; b != blocks; ++b) {
      irs::FormatTraits128::ReadBlock(in, scratch.data(), out.data());
    }
    benchmark::DoNotOptimize(out);
    benchmark::ClobberMemory();
  }
  Report(state, encoded.size(), values);
}

template<typename Codec>
void BmFreqs(benchmark::State& state) {
  const auto values = static_cast<uint32_t>(state.range(1));
  DecodeValueBlocks<Codec>(
    state, CachedFreqs<Codec>(static_cast<FreqShape>(state.range(0)), values),
    values);
}

void BmFreqsCurrent(benchmark::State& state) {
  const auto values = static_cast<uint32_t>(state.range(1));
  DecodeValueBlocksCurrent(
    state, CachedFreqs<Current>(static_cast<FreqShape>(state.range(0)), values),
    values);
}

template<typename Codec>
void BmPositions(benchmark::State& state) {
  const auto values = static_cast<uint32_t>(state.range(1));
  DecodeValueBlocks<Codec>(
    state,
    CachedPositions<Codec>(static_cast<PosShape>(state.range(0)), values),
    values);
}

void BmPositionsCurrent(benchmark::State& state) {
  const auto values = static_cast<uint32_t>(state.range(1));
  DecodeValueBlocksCurrent(
    state,
    CachedPositions<Current>(static_cast<PosShape>(state.range(0)), values),
    values);
}

void BmDocs128(benchmark::State& state) { BmDocs<bc::Codec128>(state); }
void BmDocs256(benchmark::State& state) { BmDocs<bc::Codec256>(state); }
void BmDocs256Portable(benchmark::State& state) {
  BmDocs<bc::Codec256, true>(state);
}
void BmFreqs128(benchmark::State& state) { BmFreqs<bc::Codec128>(state); }
void BmFreqs256(benchmark::State& state) { BmFreqs<bc::Codec256>(state); }
void BmPositions128(benchmark::State& state) {
  BmPositions<bc::Codec128>(state);
}
void BmPositions256(benchmark::State& state) {
  BmPositions<bc::Codec256>(state);
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

BENCHMARK(BmDocs128)->Apply(DocShapes);
BENCHMARK(BmDocs256)->Apply(DocShapes);
BENCHMARK(BmDocs256Portable)->Apply(DocShapes);
BENCHMARK(BmDocsCurrent)->Apply(DocShapes);
BENCHMARK(BmFreqs128)->Apply(FreqShapes);
BENCHMARK(BmFreqs256)->Apply(FreqShapes);
BENCHMARK(BmFreqsCurrent)->Apply(FreqShapes);
BENCHMARK(BmPositions128)->Apply(PosShapes);
BENCHMARK(BmPositions256)->Apply(PosShapes);
BENCHMARK(BmPositionsCurrent)->Apply(PosShapes);

constexpr uint32_t kEncodeValues = 1024 * kBlock;

template<typename Codec>
void BmEncodeDocs(benchmark::State& state) {
  constexpr uint32_t kN = Codec::kBlock;
  const auto docs =
    MakeDocs(static_cast<DocShape>(state.range(0)), kEncodeValues);
  std::vector<irs::byte_type> out(Codec::kMaxBlockBytes + kSlack);
  for (auto _ : state) {
    irs::doc_id_t prev = 0;
    for (uint32_t b = 0; b != kEncodeValues / kN; ++b) {
      const auto* first = docs.data() + b * kN;
      benchmark::DoNotOptimize(
        Codec::EncodeDeltaBlock(first, prev, out.data()));
      prev = first[kN - 1];
    }
  }
  state.SetItemsProcessed(state.iterations() * kEncodeValues);
}

void BmEncodeDocsCurrent(benchmark::State& state) {
  const auto docs =
    MakeDocs(static_cast<DocShape>(state.range(0)), kEncodeValues);
  alignas(64) std::array<uint32_t, kBlock> block;
  alignas(64) std::array<uint32_t, kBlock> scratch;
  for (auto _ : state) {
    irs::MemoryOutput out{irs::IResourceManager::gNoop};
    irs::doc_id_t prev = 0;
    for (uint32_t b = 0; b != kEncodeValues / kBlock; ++b) {
      const auto* first = docs.data() + b * kBlock;
      std::copy_n(first, kBlock, block.data());
      irs::FormatTraits128::WriteBlockDelta(out.stream, block.data(), prev,
                                            scratch.data());
      prev = first[kBlock - 1];
    }
    benchmark::DoNotOptimize(out.stream.Position());
  }
  state.SetItemsProcessed(state.iterations() * kEncodeValues);
}

template<typename Codec>
void EncodeValueBlocks(benchmark::State& state,
                       const std::vector<uint32_t>& values) {
  constexpr uint32_t kN = Codec::kBlock;
  std::vector<irs::byte_type> out(Codec::kMaxBlockBytes + kSlack);
  for (auto _ : state) {
    for (uint32_t b = 0; b != kEncodeValues / kN; ++b) {
      benchmark::DoNotOptimize(
        Codec::EncodeValuesBlock(values.data() + b * kN, out.data()));
    }
  }
  state.SetItemsProcessed(state.iterations() * kEncodeValues);
}

void EncodeValueBlocksCurrent(benchmark::State& state,
                              const std::vector<uint32_t>& values) {
  alignas(64) std::array<uint32_t, kBlock> block;
  alignas(64) std::array<uint32_t, kBlock> scratch;
  for (auto _ : state) {
    irs::MemoryOutput out{irs::IResourceManager::gNoop};
    for (uint32_t b = 0; b != kEncodeValues / kBlock; ++b) {
      std::copy_n(values.data() + b * kBlock, kBlock, block.data());
      irs::FormatTraits128::WriteBlock(out.stream, block.data(),
                                       scratch.data());
    }
    benchmark::DoNotOptimize(out.stream.Position());
  }
  state.SetItemsProcessed(state.iterations() * kEncodeValues);
}

template<typename Codec>
void BmEncodeFreqs(benchmark::State& state) {
  EncodeValueBlocks<Codec>(
    state, MakeFreqs(static_cast<FreqShape>(state.range(0)), kEncodeValues));
}

void BmEncodeFreqsCurrent(benchmark::State& state) {
  EncodeValueBlocksCurrent(
    state, MakeFreqs(static_cast<FreqShape>(state.range(0)), kEncodeValues));
}

template<typename Codec>
void BmEncodePositions(benchmark::State& state) {
  EncodeValueBlocks<Codec>(
    state, MakePositions(static_cast<PosShape>(state.range(0)), kEncodeValues));
}

void BmEncodePositionsCurrent(benchmark::State& state) {
  EncodeValueBlocksCurrent(
    state, MakePositions(static_cast<PosShape>(state.range(0)), kEncodeValues));
}

void BmEncodeDocs128(benchmark::State& state) {
  BmEncodeDocs<bc::Codec128>(state);
}
void BmEncodeDocs256(benchmark::State& state) {
  BmEncodeDocs<bc::Codec256>(state);
}
void BmEncodeFreqs128(benchmark::State& state) {
  BmEncodeFreqs<bc::Codec128>(state);
}
void BmEncodeFreqs256(benchmark::State& state) {
  BmEncodeFreqs<bc::Codec256>(state);
}
void BmEncodePositions128(benchmark::State& state) {
  BmEncodePositions<bc::Codec128>(state);
}
void BmEncodePositions256(benchmark::State& state) {
  BmEncodePositions<bc::Codec256>(state);
}

BENCHMARK(BmEncodeDocs128)->DenseRange(0, 5);
BENCHMARK(BmEncodeDocs256)->DenseRange(0, 5);
BENCHMARK(BmEncodeDocsCurrent)->DenseRange(0, 5);
BENCHMARK(BmEncodeFreqs128)->DenseRange(0, 4);
BENCHMARK(BmEncodeFreqs256)->DenseRange(0, 4);
BENCHMARK(BmEncodeFreqsCurrent)->DenseRange(0, 4);
BENCHMARK(BmEncodePositions128)->DenseRange(0, 5);
BENCHMARK(BmEncodePositions256)->DenseRange(0, 5);
BENCHMARK(BmEncodePositionsCurrent)->DenseRange(0, 5);

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

template<typename Codec, bool Portable = false>
void BmTailDocs(benchmark::State& state) {
  const auto len = static_cast<uint32_t>(state.range(0));
  const auto docs = MakeTails(len, static_cast<uint32_t>(state.range(1)), true);
  irs::bstring bytes;
  std::array<irs::byte_type, Codec::kMaxBlockBytes> block;
  for (uint32_t t = 0; t != kTails; ++t) {
    bytes.append(block.data(), Codec::EncodeDeltaTail(docs.data() + t * len,
                                                      len, 0, block.data()));
  }
  const auto size = bytes.size();
  bytes.append(kSlack, 0);
  alignas(64) std::array<irs::doc_id_t, Codec::kBlock + bc::kOutSlack> out;
  const auto decode = [&](const irs::byte_type* in) {
    if constexpr (Portable) {
      return bc::kDeltaTailDecoders<Codec::kLanes, false>[in[0]](in, len, 0,
                                                                 out.data());
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

void BmTailDocsCurrent(benchmark::State& state) {
  const auto len = static_cast<uint32_t>(state.range(0));
  const auto docs = MakeTails(len, static_cast<uint32_t>(state.range(1)), true);
  irs::MemoryOutput current{irs::IResourceManager::gNoop};
  alignas(64) std::array<uint32_t, kBlock> block;
  alignas(64) std::array<uint32_t, kBlock> scratch;
  for (uint32_t t = 0; t != kTails; ++t) {
    std::copy_n(docs.data() + t * len, len, block.data());
    irs::FormatTraits128::WriteTailDelta(len, current.stream, block.data(), 0,
                                         scratch.data());
  }
  const auto bytes = Collect(current);
  irs::BytesViewInput in{irs::bytes_view{bytes}};
  irs::DocsBuf out;
  for (uint32_t t = 0; t != kTails; ++t) {
    irs::FormatTraits128::ReadTailDelta(len, in, scratch.data(), out.data(), 0);
    SDB_VERIFY(std::equal(out.begin() + kBlock - len, out.begin() + kBlock,
                          docs.data() + t * len),
               "current tail docs ", t);
  }
  for (auto _ : state) {
    in.Seek(0);
    for (uint32_t t = 0; t != kTails; ++t) {
      irs::FormatTraits128::ReadTailDelta(len, in, scratch.data(), out.data(),
                                          0);
    }
    benchmark::DoNotOptimize(out);
    benchmark::ClobberMemory();
  }
  ReportTails(state, bytes.size() - kSlack, len);
}

template<typename Codec>
void BmTailValues(benchmark::State& state) {
  const auto len = static_cast<uint32_t>(state.range(0));
  const auto values =
    MakeTails(len, static_cast<uint32_t>(state.range(1)), false);
  irs::bstring bytes;
  std::array<irs::byte_type, Codec::kMaxBlockBytes> block;
  for (uint32_t t = 0; t != kTails; ++t) {
    bytes.append(block.data(), Codec::EncodeValuesTail(values.data() + t * len,
                                                       len, block.data()));
  }
  const auto size = bytes.size();
  bytes.append(kSlack, 0);
  alignas(64) std::array<uint32_t, Codec::kBlock + bc::kOutSlack> out;
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

void BmTailValuesCurrent(benchmark::State& state) {
  const auto len = static_cast<uint32_t>(state.range(0));
  const auto values =
    MakeTails(len, static_cast<uint32_t>(state.range(1)), false);
  irs::MemoryOutput current{irs::IResourceManager::gNoop};
  alignas(64) std::array<uint32_t, kBlock> block;
  alignas(64) std::array<uint32_t, kBlock> scratch;
  for (uint32_t t = 0; t != kTails; ++t) {
    std::copy_n(values.data() + t * len, len, block.data());
    irs::FormatTraits128::WriteTail(len, current.stream, block.data(),
                                    scratch.data());
  }
  const auto bytes = Collect(current);
  irs::BytesViewInput in{irs::bytes_view{bytes}};
  alignas(64) std::array<uint32_t, kBlock> out;
  for (uint32_t t = 0; t != kTails; ++t) {
    irs::FormatTraits128::ReadTail(len, in, scratch.data(), out.data());
    SDB_VERIFY(std::equal(out.begin() + kBlock - len, out.begin() + kBlock,
                          values.data() + t * len),
               "current tail values ", t);
  }
  for (auto _ : state) {
    in.Seek(0);
    for (uint32_t t = 0; t != kTails; ++t) {
      irs::FormatTraits128::ReadTail(len, in, scratch.data(), out.data());
    }
    benchmark::DoNotOptimize(out);
    benchmark::ClobberMemory();
  }
  ReportTails(state, bytes.size() - kSlack, len);
}

template<typename Codec>
void BmEncodeTailDocs(benchmark::State& state) {
  const auto len = static_cast<uint32_t>(state.range(0));
  const auto docs = MakeTails(len, static_cast<uint32_t>(state.range(1)), true);
  std::array<irs::byte_type, Codec::kMaxBlockBytes> block;
  for (auto _ : state) {
    for (uint32_t t = 0; t != kTails; ++t) {
      benchmark::DoNotOptimize(
        Codec::EncodeDeltaTail(docs.data() + t * len, len, 0, block.data()));
    }
  }
  state.SetItemsProcessed(state.iterations() * kTails * len);
}

void BmEncodeTailDocsCurrent(benchmark::State& state) {
  const auto len = static_cast<uint32_t>(state.range(0));
  const auto docs = MakeTails(len, static_cast<uint32_t>(state.range(1)), true);
  alignas(64) std::array<uint32_t, kBlock> block;
  alignas(64) std::array<uint32_t, kBlock> scratch;
  for (auto _ : state) {
    irs::MemoryOutput out{irs::IResourceManager::gNoop};
    for (uint32_t t = 0; t != kTails; ++t) {
      std::copy_n(docs.data() + t * len, len, block.data());
      irs::FormatTraits128::WriteTailDelta(len, out.stream, block.data(), 0,
                                           scratch.data());
    }
    benchmark::DoNotOptimize(out.stream.Position());
  }
  state.SetItemsProcessed(state.iterations() * kTails * len);
}

template<typename Codec>
void BmEncodeTailValues(benchmark::State& state) {
  const auto len = static_cast<uint32_t>(state.range(0));
  const auto values =
    MakeTails(len, static_cast<uint32_t>(state.range(1)), false);
  std::array<irs::byte_type, Codec::kMaxBlockBytes> block;
  for (auto _ : state) {
    for (uint32_t t = 0; t != kTails; ++t) {
      benchmark::DoNotOptimize(
        Codec::EncodeValuesTail(values.data() + t * len, len, block.data()));
    }
  }
  state.SetItemsProcessed(state.iterations() * kTails * len);
}

void BmEncodeTailValuesCurrent(benchmark::State& state) {
  const auto len = static_cast<uint32_t>(state.range(0));
  const auto values =
    MakeTails(len, static_cast<uint32_t>(state.range(1)), false);
  alignas(64) std::array<uint32_t, kBlock> block;
  alignas(64) std::array<uint32_t, kBlock> scratch;
  for (auto _ : state) {
    irs::MemoryOutput out{irs::IResourceManager::gNoop};
    for (uint32_t t = 0; t != kTails; ++t) {
      std::copy_n(values.data() + t * len, len, block.data());
      irs::FormatTraits128::WriteTail(len, out.stream, block.data(),
                                      scratch.data());
    }
    benchmark::DoNotOptimize(out.stream.Position());
  }
  state.SetItemsProcessed(state.iterations() * kTails * len);
}

void BmTailDocs256(benchmark::State& state) { BmTailDocs<bc::Codec256>(state); }
void BmTailDocs256Portable(benchmark::State& state) {
  BmTailDocs<bc::Codec256, true>(state);
}
void BmTailValues256(benchmark::State& state) {
  BmTailValues<bc::Codec256>(state);
}
void BmEncodeTailDocs256(benchmark::State& state) {
  BmEncodeTailDocs<bc::Codec256>(state);
}
void BmEncodeTailValues256(benchmark::State& state) {
  BmEncodeTailValues<bc::Codec256>(state);
}

void TailShapes(benchmark::internal::Benchmark* b) {
  for (const int len : {4, 16, 64, 127}) {
    for (const int bits : {2, 7, 16, 0}) {
      b->Args({len, bits});
    }
  }
}

BENCHMARK(BmTailDocs256)->Apply(TailShapes);
BENCHMARK(BmTailDocs256Portable)->Apply(TailShapes);
BENCHMARK(BmTailDocsCurrent)->Apply(TailShapes);
BENCHMARK(BmTailValues256)->Apply(TailShapes);
BENCHMARK(BmTailValuesCurrent)->Apply(TailShapes);
BENCHMARK(BmEncodeTailDocs256)->Apply(TailShapes);
BENCHMARK(BmEncodeTailDocsCurrent)->Apply(TailShapes);
BENCHMARK(BmEncodeTailValues256)->Apply(TailShapes);
BENCHMARK(BmEncodeTailValuesCurrent)->Apply(TailShapes);

constexpr uint32_t kDensityDocs = 4096 * 256;

const std::vector<irs::doc_id_t>& CachedDensityDocs(uint32_t percent) {
  static std::map<uint32_t, std::vector<irs::doc_id_t>> cache;
  auto it = cache.find(percent);
  if (it == cache.end()) {
    std::mt19937 rng{400 + percent};
    std::vector<irs::doc_id_t> docs;
    docs.reserve(kDensityDocs);
    for (irs::doc_id_t id = 1; docs.size() != kDensityDocs; ++id) {
      if (rng() % 100 < percent) {
        docs.push_back(id);
      }
    }
    it = cache.emplace(percent, std::move(docs)).first;
  }
  return it->second;
}

template<typename Codec>
const irs::bstring& CachedDensity(uint32_t percent, bool bitset) {
  constexpr uint32_t kN = Codec::kBlock;
  static std::map<std::pair<uint32_t, bool>, irs::bstring> cache;
  auto it = cache.find({percent, bitset});
  if (it != cache.end()) {
    return it->second;
  }
  const auto& docs = CachedDensityDocs(percent);
  irs::bstring bytes;
  std::array<irs::byte_type, Codec::kMaxBlockBytes> block;
  irs::doc_id_t prev = 0;
  for (uint32_t b = 0; b != kDensityDocs / kN; ++b) {
    bytes.append(block.data(),
                 Codec::EncodeDeltaBlock(docs.data() + b * kN, prev,
                                         block.data(), {.bitset = bitset}));
    prev = docs[b * kN + kN - 1];
  }
  bytes.append(kSlack, 0);
  const auto* p = bytes.data();
  std::array<irs::doc_id_t, kN + bc::kOutSlack> out;
  prev = 0;
  for (uint32_t b = 0; b != kDensityDocs / kN; ++b) {
    p = Codec::DecodeDeltaBlock(p, prev, out.data());
    SDB_VERIFY(std::equal(out.begin(), out.begin() + kN, docs.data() + b * kN),
               "density block ", b);
    prev = out[kN - 1];
  }
  return cache.emplace(std::pair{percent, bitset}, std::move(bytes))
    .first->second;
}

const irs::byte_type* FillZeroBase(const irs::byte_type* p, irs::doc_id_t& prev,
                                   uint64_t* words) {
  constexpr uint32_t kN = bc::Codec256::kBlock;
  const uint32_t token = p[0];
  uint32_t c16 = 0;
  uint32_t c32 = 0;
  const auto* e = p + 1;
  if (token == static_cast<uint32_t>(bc::DeltaEncoding::Patch16)) {
    c16 = *e++;
  } else if (token == static_cast<uint32_t>(bc::DeltaEncoding::Patch32)) {
    c32 = *e++;
  } else {
    c16 = e[0];
    c32 = e[1];
    e += 2;
  }
  const auto* e16 = e;
  const auto* e32 = e + 2 * c16;
  uint64_t next = uint64_t{prev} + 1;
  uint32_t done = 0;
  uint32_t i16 = 0;
  uint32_t i32 = 0;
  while (i16 != c16 || i32 != c32) {
    uint32_t slot;
    uint32_t skip;
    if (i32 == c32 || (i16 != c16 && e16[2 * i16] < e32[4 * i32])) {
      slot = e16[2 * i16];
      skip = e16[2 * i16 + 1];
      ++i16;
    } else {
      slot = e32[4 * i32];
      skip = absl::little_endian::Load32(e32 + 4 * i32) >> 8;
      ++i32;
    }
    if (slot != done) {
      irs::SetBitRange(words, next, next + slot - done);
    }
    next += slot - done + skip;
    done = slot;
  }
  irs::SetBitRange(words, next, next + kN - done);
  prev = static_cast<irs::doc_id_t>(next + kN - done - 1);
  return e32 + 4 * c32;
}

const irs::byte_type* FillZeroBaseWords(const irs::byte_type* p,
                                        irs::doc_id_t& prev, uint64_t* words) {
  constexpr uint32_t kN = bc::Codec256::kBlock;
  constexpr uint32_t kLocal = 8;
  constexpr auto kBits = irs::BitsRequired<uint64_t>();
  const uint32_t token = p[0];
  if (token != static_cast<uint32_t>(bc::DeltaEncoding::Patch16)) {
    return FillZeroBase(p, prev, words);
  }
  const uint32_t count = p[1];
  const auto* e = p + 2;
  uint32_t skipped = 0;
  for (uint32_t i = 0; i != count; ++i) {
    skipped += e[2 * i + 1];
  }
  const uint32_t range = kN + skipped;
  if (range > kLocal * kBits) {
    return FillZeroBase(p, prev, words);
  }
  const uint32_t n = (range + kBits - 1) / kBits;
  uint64_t local[kLocal + 1];
  for (uint32_t w = 0; w != kLocal + 1; ++w) {
    local[w] = w < n ? ~uint64_t{0} : 0;
  }
  local[n - 1] = ~uint64_t{0} >> (n * kBits - range);
  uint32_t running = 0;
  for (uint32_t i = 0; i != count; ++i) {
    const uint32_t at = e[2 * i] + running;
    const uint32_t skip = e[2 * i + 1];
    const auto mask = ((static_cast<unsigned __int128>(1) << skip) - 1)
                      << (at % kBits);
    local[at / kBits] &= ~static_cast<uint64_t>(mask);
    local[at / kBits + 1] &= ~static_cast<uint64_t>(mask >> kBits);
    running += skip;
  }
  irs::OrBitsetAt(words, uint64_t{prev} + 1, local, n);
  prev += range;
  return e + 2 * count;
}

bool IsZeroBasePatch(uint32_t token) {
  return token == static_cast<uint32_t>(bc::DeltaEncoding::Patch16) ||
         token == static_cast<uint32_t>(bc::DeltaEncoding::Patch32) ||
         token == static_cast<uint32_t>(bc::DeltaEncoding::PatchMixed);
}

template<typename Codec>
void BmDensityFill(benchmark::State& state) {
  constexpr uint32_t kN = Codec::kBlock;
  constexpr auto kBits = irs::BitsRequired<uint64_t>();
  const auto percent = static_cast<uint32_t>(state.range(0));
  const bool bitset = state.range(1) != 0;
  const auto zero_base = static_cast<uint32_t>(state.range(2));
  const auto& encoded = CachedDensity<Codec>(percent, bitset);
  const auto& docs_ref = CachedDensityDocs(percent);
  std::vector<uint64_t> words(docs_ref.back() / kBits + 2 * kN);
  alignas(64) std::array<irs::doc_id_t, kN + bc::kOutSlack> out;
  for (auto _ : state) {
    const auto* p = encoded.data();
    irs::doc_id_t prev = 0;
    for (uint32_t b = 0; b != kDensityDocs / kN; ++b) {
      if (p[0] == static_cast<irs::byte_type>(bc::DeltaEncoding::Bitset)) {
        const uint32_t n = p[1];
        const auto* bits = reinterpret_cast<const uint64_t*>(p + 2);
        irs::OrBitsetAt(words.data(), uint64_t{prev} + 1, bits, n);
        prev += 1 + (n - 1) * kBits + (kBits - 1) -
                static_cast<uint32_t>(std::countl_zero(bits[n - 1]));
        p += 2 + n * sizeof(uint64_t);
      } else if (zero_base != 0 && IsZeroBasePatch(p[0])) {
        p = zero_base == 1 ? FillZeroBase(p, prev, words.data())
                           : FillZeroBaseWords(p, prev, words.data());
      } else {
        p = Codec::DecodeDeltaBlock(p, prev, out.data());
        for (uint32_t i = 0; i != kN; ++i) {
          irs::SetBit(words[out[i] / kBits], out[i] % kBits);
        }
        prev = out[kN - 1];
      }
    }
    benchmark::DoNotOptimize(words.data());
    benchmark::ClobberMemory();
  }
  SDB_VERIFY(std::all_of(docs_ref.begin(), docs_ref.end(),
                         [&](irs::doc_id_t doc) {
                           return (words[doc / kBits] >> (doc % kBits)) & 1;
                         }),
             "density fill");
  uint64_t set = 0;
  for (const auto word : words) {
    set += std::popcount(word);
  }
  SDB_VERIFY(set == kDensityDocs, "density fill sets ", set);
  Report(state, encoded.size(), kDensityDocs);
}

void BmDensityFill256(benchmark::State& state) {
  BmDensityFill<bc::Codec256>(state);
}

void DensityFillArgs(benchmark::internal::Benchmark* b) {
  for (const int percent : {50, 70, 80, 85, 88, 90, 92, 94, 96, 98}) {
    b->Args({percent, 1, 0});
    for (const int zero_base : {0, 1, 2}) {
      b->Args({percent, 0, zero_base});
    }
  }
}

BENCHMARK(BmDensityFill256)->Apply(DensityFillArgs);

}  // namespace

BENCHMARK_MAIN();
