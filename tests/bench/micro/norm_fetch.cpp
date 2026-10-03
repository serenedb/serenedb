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
#include <utility>
#include <vector>

#include "iresearch/utils/shared.hpp"

namespace {

constexpr size_t kBlock = 256;
constexpr size_t kBlocks = 4096;
constexpr uint32_t kPageShift = 16;
constexpr uint32_t kPageRows = uint32_t{1} << kPageShift;
constexpr uint32_t kEscape = 255;
constexpr uint32_t kDenseSpan = 4096;
constexpr size_t kSlack = 8;

enum class Shape : int {
  Game,
  Otel,
  OtelTail,
};
enum class Layout : int {
  Raw,
  ByteEscapes,
  Lossy,
  BitPage,
  NibblePage,
  Flat,
};
enum class Kernel : int {
  PerDoc,
  Dense,
  Prefetch,
};
enum class Mode : int {
  Fetch,
  Score,
};

std::vector<uint32_t> MakeValues(Shape shape, size_t rows) {
  std::mt19937_64 rng{static_cast<uint64_t>(shape) * 7919 + rows};
  const bool game = shape == Shape::Game;
  std::lognormal_distribution<double> dist{std::log(game ? 90.0 : 7.0),
                                           game ? 1.443 : 0.854};
  const uint32_t max = game ? 44543 : 137;
  std::vector<uint32_t> values(rows);
  for (auto& v : values) {
    v = std::clamp<uint32_t>(static_cast<uint32_t>(std::lround(dist(rng))), 1,
                             max);
  }
  if (shape == Shape::OtelTail) {
    std::uniform_int_distribution<size_t> pick{0, rows - 1};
    std::uniform_int_distribution<uint32_t> big{255, 469};
    for (size_t i = 0, n = rows * 11 / 10000; i != n; ++i) {
      values[pick(rng)] = big(rng);
    }
  }
  return values;
}

int LongToInt4(uint64_t i) {
  const int bits = 64 - std::countl_zero(i);
  if (bits < 4) {
    return static_cast<int>(i);
  }
  const int shift = bits - 4;
  return static_cast<int>((i >> shift) & 0x07) | ((shift + 1) << 3);
}

uint64_t Int4ToLong(int i) {
  const uint64_t bits = static_cast<uint64_t>(i & 0x07);
  const int shift = (i >> 3) - 1;
  return shift == -1 ? bits : (bits | 0x08) << shift;
}

const int kFreeValues = 255 - LongToInt4(std::numeric_limits<int32_t>::max());

uint8_t IntToByte4(uint32_t i) {
  if (i < static_cast<uint32_t>(kFreeValues)) {
    return static_cast<uint8_t>(i);
  }
  return static_cast<uint8_t>(kFreeValues + LongToInt4(i - kFreeValues));
}

uint32_t Byte4ToInt(uint8_t b) {
  if (b < kFreeValues) {
    return b;
  }
  return static_cast<uint32_t>(
    std::min<uint64_t>(kFreeValues + Int4ToLong(b - kFreeValues),
                       std::numeric_limits<int32_t>::max()));
}

struct Exceptions {
  std::vector<uint32_t> rows;
  std::vector<uint32_t> values;
  std::vector<uint32_t> page_begin;

  void Finish(size_t total_rows) {
    const size_t pages = (total_rows + kPageRows - 1) / kPageRows;
    page_begin.assign(pages + 1, 0);
    size_t i = 0;
    for (size_t p = 0; p != pages; ++p) {
      page_begin[p] = static_cast<uint32_t>(i);
      while (i != rows.size() && (rows[i] >> kPageShift) == p) {
        ++i;
      }
    }
    page_begin[pages] = static_cast<uint32_t>(rows.size());
  }

  IRS_NO_INLINE uint32_t Find(uint32_t row) const noexcept {
    const auto page = row >> kPageShift;
    const auto* begin = rows.data() + page_begin[page];
    const auto* end = rows.data() + page_begin[page + 1];
    const auto* it = std::lower_bound(begin, end, row);
    return values[static_cast<size_t>(it - rows.data())];
  }

  size_t Bytes(size_t entry_bytes) const noexcept {
    return rows.size() * entry_bytes;
  }
};

struct RawColumn {
  std::vector<uint8_t> data;
  uint32_t width = 1;

  void Build(const std::vector<uint32_t>& values) {
    const auto max = *std::max_element(values.begin(), values.end());
    width = max < 256 ? 1 : (max < 65536 ? 2 : 4);
    data.assign(values.size() * width + kSlack, 0);
    for (size_t r = 0; r != values.size(); ++r) {
      std::memcpy(data.data() + r * width, &values[r], width);
    }
  }

  size_t Bytes() const noexcept { return data.size() - kSlack; }

  const uint8_t* Line(uint32_t row) const noexcept {
    return data.data() + size_t{row} * width;
  }

  template<uint32_t W>
  IRS_FORCE_INLINE uint32_t At(uint32_t row) const noexcept {
    if constexpr (W == 1) {
      return data[row];
    } else if constexpr (W == 2) {
      return absl::little_endian::Load16(data.data() + size_t{row} * 2);
    } else {
      return absl::little_endian::Load32(data.data() + size_t{row} * 4);
    }
  }

  template<uint32_t W>
  IRS_FORCE_INLINE void FetchW(const uint32_t* IRS_RESTRICT docs,
                               uint32_t* IRS_RESTRICT out) const noexcept {
    for (size_t i = 0; i != kBlock; ++i) {
      out[i] = At<W>(docs[i]);
    }
  }

  IRS_FORCE_INLINE void Fetch(const uint32_t* IRS_RESTRICT docs,
                              uint32_t* IRS_RESTRICT out) const noexcept {
    switch (width) {
      case 1:
        return FetchW<1>(docs, out);
      case 2:
        return FetchW<2>(docs, out);
      default:
        return FetchW<4>(docs, out);
    }
  }

  IRS_FORCE_INLINE void DecodeRange(uint32_t first, uint32_t count,
                                    uint32_t* IRS_RESTRICT out) const noexcept {
    switch (width) {
      case 1:
        for (uint32_t i = 0; i != count; ++i) {
          out[i] = At<1>(first + i);
        }
        return;
      case 2:
        for (uint32_t i = 0; i != count; ++i) {
          out[i] = At<2>(first + i);
        }
        return;
      default:
        for (uint32_t i = 0; i != count; ++i) {
          out[i] = At<4>(first + i);
        }
    }
  }

  IRS_FORCE_INLINE void Patch(const uint32_t*, uint32_t*) const noexcept {}
};

struct ByteEscapeColumn {
  std::vector<uint8_t> data;
  Exceptions exceptions;

  void Build(const std::vector<uint32_t>& values) {
    data.assign(values.size() + kSlack, 0);
    for (size_t r = 0; r != values.size(); ++r) {
      if (values[r] >= kEscape) {
        data[r] = kEscape;
        exceptions.rows.push_back(static_cast<uint32_t>(r));
        exceptions.values.push_back(values[r]);
      } else {
        data[r] = static_cast<uint8_t>(values[r]);
      }
    }
    exceptions.Finish(values.size());
  }

  size_t Bytes() const noexcept {
    return data.size() - kSlack + exceptions.Bytes(8);
  }

  const uint8_t* Line(uint32_t row) const noexcept { return data.data() + row; }

  IRS_FORCE_INLINE void Fetch(const uint32_t* IRS_RESTRICT docs,
                              uint32_t* IRS_RESTRICT out) const noexcept {
    for (size_t i = 0; i != kBlock; ++i) {
      out[i] = data[docs[i]];
    }
  }

  IRS_FORCE_INLINE void DecodeRange(uint32_t first, uint32_t count,
                                    uint32_t* IRS_RESTRICT out) const noexcept {
    for (uint32_t i = 0; i != count; ++i) {
      out[i] = data[first + i];
    }
  }

  IRS_FORCE_INLINE void Patch(const uint32_t* IRS_RESTRICT docs,
                              uint32_t* IRS_RESTRICT out) const noexcept {
    if (exceptions.rows.empty()) {
      return;
    }
    bool escaped = false;
    for (size_t i = 0; i != kBlock; ++i) {
      escaped |= out[i] == kEscape;
    }
    if (escaped) [[unlikely]] {
      for (size_t i = 0; i != kBlock; ++i) {
        if (out[i] == kEscape) {
          out[i] = exceptions.Find(docs[i]);
        }
      }
    }
  }
};

struct LossyColumn {
  std::vector<uint8_t> data;
  uint32_t table[256];

  void Build(const std::vector<uint32_t>& values) {
    data.assign(values.size() + kSlack, 0);
    for (size_t r = 0; r != values.size(); ++r) {
      data[r] = IntToByte4(values[r]);
    }
    for (uint32_t b = 0; b != 256; ++b) {
      table[b] = Byte4ToInt(static_cast<uint8_t>(b));
    }
  }

  size_t Bytes() const noexcept { return data.size() - kSlack; }

  const uint8_t* Line(uint32_t row) const noexcept { return data.data() + row; }

  IRS_FORCE_INLINE void Fetch(const uint32_t* IRS_RESTRICT docs,
                              uint32_t* IRS_RESTRICT out) const noexcept {
    for (size_t i = 0; i != kBlock; ++i) {
      out[i] = table[data[docs[i]]];
    }
  }

  IRS_FORCE_INLINE void DecodeRange(uint32_t first, uint32_t count,
                                    uint32_t* IRS_RESTRICT out) const noexcept {
    for (uint32_t i = 0; i != count; ++i) {
      out[i] = table[data[first + i]];
    }
  }

  IRS_FORCE_INLINE void Patch(const uint32_t*, uint32_t*) const noexcept {}
};

template<bool Nibbles>
struct BitPageColumn {
  std::vector<uint8_t> data;
  std::vector<uint64_t> offsets;
  std::vector<uint8_t> widths;
  Exceptions exceptions;

  static size_t PageBytes(size_t rows, uint32_t w) noexcept {
    return (rows * w + 7) / 8;
  }

  void Build(const std::vector<uint32_t>& values) {
    const size_t pages = (values.size() + kPageRows - 1) / kPageRows;
    for (size_t p = 0; p != pages; ++p) {
      const size_t first = p * kPageRows;
      const size_t rows = std::min<size_t>(kPageRows, values.size() - first);
      uint32_t best_w = 25;
      for (uint32_t w = 1; w <= 25; ++w) {
        if (Nibbles && w % 4 != 0) {
          continue;
        }
        const uint32_t escape = (uint32_t{1} << w) - 1;
        size_t exc = 0;
        for (size_t r = 0; r != rows; ++r) {
          exc += values[first + r] >= escape;
        }
        if (exc * 256 <= rows) {
          best_w = w;
          break;
        }
      }
      widths.push_back(static_cast<uint8_t>(best_w));
      offsets.push_back(data.size());
      const uint32_t escape = (uint32_t{1} << best_w) - 1;
      std::vector<uint8_t> page(PageBytes(rows, best_w) + kSlack, 0);
      for (size_t r = 0; r != rows; ++r) {
        uint32_t v = values[first + r];
        if (v >= escape) {
          exceptions.rows.push_back(static_cast<uint32_t>(first + r));
          exceptions.values.push_back(v);
          v = escape;
        }
        const uint64_t bit = uint64_t{r} * best_w;
        uint64_t word;
        std::memcpy(&word, page.data() + bit / 8, sizeof(word));
        word |= uint64_t{v} << (bit % 8);
        std::memcpy(page.data() + bit / 8, &word, sizeof(word));
      }
      page.resize(page.size() - kSlack);
      data.insert(data.end(), page.begin(), page.end());
    }
    data.resize(data.size() + kSlack, 0);
    exceptions.Finish(values.size());
  }

  size_t Bytes() const noexcept {
    return data.size() - kSlack + exceptions.Bytes(6) + widths.size() * 8;
  }

  const uint8_t* Line(uint32_t row) const noexcept {
    const auto p = row >> kPageShift;
    return data.data() + offsets[p] +
           (uint64_t{row & (kPageRows - 1)} * widths[p]) / 8;
  }

  IRS_FORCE_INLINE static uint32_t Unpack(const uint8_t* IRS_RESTRICT base,
                                          uint32_t r, uint32_t w,
                                          uint32_t mask) noexcept {
    const uint32_t bit = r * w;
    return (absl::little_endian::Load32(base + bit / 8) >> (bit % 8)) & mask;
  }

  IRS_FORCE_INLINE void Fetch(const uint32_t* IRS_RESTRICT docs,
                              uint32_t* IRS_RESTRICT out) const noexcept {
    const auto p0 = docs[0] >> kPageShift;
    if (p0 == docs[kBlock - 1] >> kPageShift) [[likely]] {
      const uint32_t w = widths[p0];
      const uint32_t mask = (uint32_t{1} << w) - 1;
      const uint8_t* base = data.data() + offsets[p0];
      for (size_t i = 0; i != kBlock; ++i) {
        out[i] = Unpack(base, docs[i] & (kPageRows - 1), w, mask);
      }
      return;
    }
    for (size_t i = 0; i != kBlock; ++i) {
      const auto p = docs[i] >> kPageShift;
      const uint32_t w = widths[p];
      out[i] = Unpack(data.data() + offsets[p], docs[i] & (kPageRows - 1), w,
                      (uint32_t{1} << w) - 1);
    }
  }

  IRS_FORCE_INLINE void DecodeRange(uint32_t first, uint32_t count,
                                    uint32_t* IRS_RESTRICT out) const noexcept {
    for (uint32_t i = 0; i != count;) {
      const auto row = first + i;
      const auto p = row >> kPageShift;
      const uint32_t w = widths[p];
      const uint32_t mask = (uint32_t{1} << w) - 1;
      const uint8_t* base = data.data() + offsets[p];
      const uint32_t end =
        std::min<uint32_t>(count, i + (kPageRows - (row & (kPageRows - 1))));
      uint64_t bit = uint64_t{row & (kPageRows - 1)} * w;
      for (; i != end; ++i, bit += w) {
        out[i] = static_cast<uint32_t>(
                   absl::little_endian::Load64(base + bit / 8) >> (bit % 8)) &
                 mask;
      }
    }
  }

  IRS_FORCE_INLINE void Patch(const uint32_t* IRS_RESTRICT docs,
                              uint32_t* IRS_RESTRICT out) const noexcept {
    if (exceptions.rows.empty()) {
      return;
    }
    const auto p0 = docs[0] >> kPageShift;
    if (p0 == docs[kBlock - 1] >> kPageShift) [[likely]] {
      const uint32_t escape = (uint32_t{1} << widths[p0]) - 1;
      bool escaped = false;
      for (size_t i = 0; i != kBlock; ++i) {
        escaped |= out[i] == escape;
      }
      if (!escaped) [[likely]] {
        return;
      }
    }
    for (size_t i = 0; i != kBlock; ++i) {
      if (out[i] == (uint32_t{1} << widths[docs[i] >> kPageShift]) - 1) {
        out[i] = exceptions.Find(docs[i]);
      }
    }
  }
};

struct FlatColumn {
  RawColumn flat;
  std::vector<uint64_t> decoded;

  void Build(const std::vector<uint32_t>& values) {
    flat.Build(values);
    const size_t pages = (values.size() + kPageRows - 1) / kPageRows;
    decoded.assign((pages + 63) / 64, ~uint64_t{0});
  }

  size_t Bytes() const noexcept { return flat.Bytes(); }

  const uint8_t* Line(uint32_t row) const noexcept { return flat.Line(row); }

  IRS_FORCE_INLINE bool Decoded(uint32_t page) const noexcept {
    return (decoded[page / 64] >> (page % 64)) & 1;
  }

  IRS_NO_INLINE void Decode(uint32_t page) const noexcept {
    benchmark::DoNotOptimize(page);
  }

  IRS_FORCE_INLINE void Fetch(const uint32_t* IRS_RESTRICT docs,
                              uint32_t* IRS_RESTRICT out) const noexcept {
    const auto p0 = docs[0] >> kPageShift;
    if (p0 == docs[kBlock - 1] >> kPageShift) [[likely]] {
      if (!Decoded(p0)) [[unlikely]] {
        Decode(p0);
      }
      return flat.Fetch(docs, out);
    }
    for (size_t i = 0; i != kBlock; ++i) {
      const auto p = docs[i] >> kPageShift;
      if (!Decoded(p)) [[unlikely]] {
        Decode(p);
      }
    }
    flat.Fetch(docs, out);
  }

  IRS_FORCE_INLINE void DecodeRange(uint32_t first, uint32_t count,
                                    uint32_t* IRS_RESTRICT out) const noexcept {
    flat.DecodeRange(first, count, out);
  }

  IRS_FORCE_INLINE void Patch(const uint32_t*, uint32_t*) const noexcept {}
};

struct Fixture {
  std::vector<uint32_t> values;
  RawColumn raw;
  ByteEscapeColumn byte_escapes;
  LossyColumn lossy;
  BitPageColumn<false> bit_page;
  BitPageColumn<true> nibble_page;
  FlatColumn flat;
  std::map<uint32_t, std::vector<uint32_t>> blocks;

  const std::vector<uint32_t>& Blocks(uint32_t span) {
    auto& docs = blocks[span];
    if (!docs.empty()) {
      return docs;
    }
    const auto rows = static_cast<uint32_t>(values.size());
    std::mt19937_64 rng{uint64_t{span} * 104729 + rows};
    std::uniform_int_distribution<uint32_t> start{0, rows - span};
    std::vector<uint32_t> starts(kBlocks);
    for (auto& s : starts) {
      s = start(rng);
    }
    std::sort(starts.begin(), starts.end());
    docs.resize(kBlocks * kBlock);
    std::vector<uint32_t> pool(span);
    for (size_t b = 0; b != kBlocks; ++b) {
      auto* out = docs.data() + b * kBlock;
      if (span == kBlock) {
        for (uint32_t i = 0; i != kBlock; ++i) {
          out[i] = starts[b] + i;
        }
        continue;
      }
      if (span <= 65536) {
        for (uint32_t i = 0; i != span; ++i) {
          pool[i] = i;
        }
        for (uint32_t i = 0; i != kBlock; ++i) {
          std::uniform_int_distribution<uint32_t> pick{i, span - 1};
          std::swap(pool[i], pool[pick(rng)]);
          out[i] = starts[b] + pool[i];
        }
      } else {
        std::uniform_int_distribution<uint32_t> pick{0, span - 1};
        size_t n = 0;
        while (n != kBlock) {
          out[n++] = starts[b] + pick(rng);
          if (n == kBlock) {
            std::sort(out, out + kBlock);
            n = static_cast<size_t>(std::unique(out, out + kBlock) - out);
          }
        }
      }
      std::sort(out, out + kBlock);
    }
    return docs;
  }
};

Fixture& GetFixture(Shape shape, uint32_t rows) {
  static std::map<std::pair<int, uint32_t>, std::unique_ptr<Fixture>> cache;
  auto& f = cache[{static_cast<int>(shape), rows}];
  if (!f) {
    f = std::make_unique<Fixture>();
    f->values = MakeValues(shape, rows);
    f->raw.Build(f->values);
    f->byte_escapes.Build(f->values);
    f->lossy.Build(f->values);
    f->bit_page.Build(f->values);
    f->nibble_page.Build(f->values);
    f->flat.Build(f->values);
  }
  return *f;
}

template<typename Column>
IRS_FORCE_INLINE void PrefetchBlock(
  const Column& column, const uint32_t* IRS_RESTRICT docs) noexcept {
  uintptr_t last = 0;
  for (size_t i = 0; i != kBlock; ++i) {
    const auto line = reinterpret_cast<uintptr_t>(column.Line(docs[i])) >> 6;
    if (line != last) {
      _mm_prefetch(reinterpret_cast<const char*>(line << 6), _MM_HINT_T0);
      last = line;
    }
  }
}

template<Kernel K, typename Column>
IRS_FORCE_INLINE void FetchBlock(const Column& column,
                                 const uint32_t* IRS_RESTRICT docs,
                                 const uint32_t* IRS_RESTRICT next,
                                 uint32_t* IRS_RESTRICT scratch,
                                 uint32_t* IRS_RESTRICT out) noexcept {
  if constexpr (K == Kernel::Prefetch) {
    if (next != nullptr) {
      PrefetchBlock(column, next);
    }
  }
  if constexpr (K == Kernel::Dense) {
    const auto first = docs[0];
    const auto span = docs[kBlock - 1] - first + 1;
    if (span <= kDenseSpan) {
      column.DecodeRange(first, span, scratch);
      for (size_t i = 0; i != kBlock; ++i) {
        out[i] = scratch[docs[i] - first];
      }
      column.Patch(docs, out);
      return;
    }
  }
  column.Fetch(docs, out);
  column.Patch(docs, out);
}

IRS_FORCE_INLINE float Score(const uint32_t* IRS_RESTRICT norms,
                             const float* IRS_RESTRICT freqs,
                             float avg) noexcept {
  constexpr float kK1 = 1.2f;
  constexpr float kB = 0.75f;
  float sum = 0;
  for (size_t i = 0; i != kBlock; ++i) {
    const float f = freqs[i];
    sum += f / (f + kK1 * (1 - kB + kB * static_cast<float>(norms[i]) / avg));
  }
  return sum;
}

template<Kernel K, Mode M, typename Column>
void Run(benchmark::State& state, const Column& column,
         const std::vector<uint32_t>& docs) {
  alignas(64) uint32_t out[kBlock];
  alignas(64) static uint32_t scratch[kDenseSpan + kSlack];
  alignas(64) float freqs[kBlock];
  for (size_t i = 0; i != kBlock; ++i) {
    freqs[i] = static_cast<float>(1 + i % 4);
  }
  size_t b = 0;
  for (auto _ : state) {
    const auto* block = docs.data() + b * kBlock;
    const auto* next = b + 1 == kBlocks ? nullptr : block + kBlock;
    FetchBlock<K>(column, block, next, scratch, out);
    if constexpr (M == Mode::Score) {
      benchmark::DoNotOptimize(Score(out, freqs, 60.f));
    } else {
      benchmark::DoNotOptimize(out);
      benchmark::ClobberMemory();
    }
    if (++b == kBlocks) {
      b = 0;
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kBlock);
}

template<typename Column>
bool Check(const Column& column, const Fixture& f,
           const std::vector<uint32_t>& docs, bool lossy) {
  alignas(64) uint32_t out[kBlock];
  alignas(64) static uint32_t scratch[kDenseSpan + kSlack];
  for (size_t b = 0; b < kBlocks; b += 97) {
    const auto* block = docs.data() + b * kBlock;
    for (int k = 0; k != 2; ++k) {
      if (k == 0) {
        FetchBlock<Kernel::PerDoc>(column, block, nullptr, scratch, out);
      } else {
        FetchBlock<Kernel::Dense>(column, block, nullptr, scratch, out);
      }
      for (size_t i = 0; i != kBlock; ++i) {
        const auto v = f.values[block[i]];
        const auto expected = lossy ? Byte4ToInt(IntToByte4(v)) : v;
        if (out[i] != expected) {
          return false;
        }
      }
    }
  }
  return true;
}

template<Kernel K, Mode M>
void Bench(benchmark::State& state) {
  const auto shape = static_cast<Shape>(state.range(0));
  const auto layout = static_cast<Layout>(state.range(1));
  const auto rows = static_cast<uint32_t>(state.range(2));
  const auto span = static_cast<uint32_t>(state.range(3));
  auto& f = GetFixture(shape, rows);
  const auto& docs = f.Blocks(span);
  const auto dispatch = [&](const auto& column, bool lossy) {
    if (!Check(column, f, docs, lossy)) {
      state.SkipWithError("mismatch");
      return;
    }
    Run<K, M>(state, column, docs);
    state.counters["bits_per_row"] =
      static_cast<double>(column.Bytes()) * 8 / static_cast<double>(rows);
  };
  switch (layout) {
    case Layout::Raw:
      return dispatch(f.raw, false);
    case Layout::ByteEscapes:
      return dispatch(f.byte_escapes, false);
    case Layout::Lossy:
      return dispatch(f.lossy, true);
    case Layout::BitPage:
      return dispatch(f.bit_page, false);
    case Layout::NibblePage:
      return dispatch(f.nibble_page, false);
    case Layout::Flat:
      return dispatch(f.flat, false);
  }
}

void Args(benchmark::internal::Benchmark* b) {
  for (int shape : {0, 1, 2}) {
    for (int layout : {0, 1, 2, 3, 4, 5}) {
      if (layout == 1 && shape == 0) {
        continue;
      }
      for (int64_t rows :
           {int64_t{1} << 20, int64_t{1} << 24, int64_t{1} << 28}) {
        for (int64_t span : {256, 1024, 4096, 32768, 262144}) {
          b->Args({shape, layout, rows, span});
        }
      }
    }
  }
  b->ArgNames({"shape", "layout", "rows", "span"});
}

BENCHMARK(Bench<Kernel::PerDoc, Mode::Fetch>)->Apply(Args);
BENCHMARK(Bench<Kernel::Dense, Mode::Fetch>)->Apply(Args);
BENCHMARK(Bench<Kernel::PerDoc, Mode::Score>)->Apply(Args);
BENCHMARK(Bench<Kernel::Dense, Mode::Score>)->Apply(Args);
BENCHMARK(Bench<Kernel::Prefetch, Mode::Score>)->Apply(Args);

}  // namespace

BENCHMARK_MAIN();
