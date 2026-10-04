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
#include <sys/mman.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <random>
#include <utility>
#include <vector>

#include "iresearch/utils/shared.hpp"

namespace {

constexpr size_t kBlock = 256;
constexpr size_t kBlocks = 4096;
constexpr size_t kSlack = 16;

enum class Kernel : int {
  Scalar,
  ScalarAny,
  Gather,
  GatherAny,
};

constexpr uint32_t Mask(uint32_t width) noexcept {
  return width == 32 ? ~uint32_t{0} : (uint32_t{1} << width) - 1;
}

uint32_t Value(uint64_t row, uint32_t width) noexcept {
  uint64_t x = (row + 1) * 0x9E3779B97F4A7C15ULL;
  x ^= x >> 29;
  x *= 0xBF58476D1CE4E5B9ULL;
  x ^= x >> 32;
  return static_cast<uint32_t>(x) & Mask(width);
}

class Column {
 public:
  Column() = default;
  Column(const Column&) = delete;
  Column& operator=(const Column&) = delete;
  ~Column() { Reset(); }

  void Build(uint32_t rows, uint32_t width) {
    Reset();
    _size = (uint64_t{rows} * width + 7) / 8 + kSlack;
    void* p = mmap(nullptr, _size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED) {
      std::abort();
    }
    madvise(p, _size, MADV_NOHUGEPAGE);
    _data = static_cast<uint8_t*>(p);
    for (uint64_t r = 0; r != rows; ++r) {
      const uint64_t bit = r * width;
      uint8_t* at = _data + (bit >> 3);
      absl::little_endian::Store64(at,
                                   absl::little_endian::Load64(at) |
                                     (uint64_t{Value(r, width)} << (bit & 7)));
    }
    _rows = rows;
    _width = width;
  }

  const uint8_t* Data() const noexcept { return _data; }
  uint32_t Rows() const noexcept { return _rows; }
  uint32_t Width() const noexcept { return _width; }

 private:
  void Reset() noexcept {
    if (_data != nullptr) {
      munmap(_data, _size);
      _data = nullptr;
    }
    _rows = 0;
    _width = 0;
  }

  uint8_t* _data = nullptr;
  size_t _size = 0;
  uint32_t _rows = 0;
  uint32_t _width = 0;
};

using Fetch = void (*)(const uint8_t* IRS_RESTRICT,
                       const uint32_t* IRS_RESTRICT,
                       uint32_t* IRS_RESTRICT) noexcept;

template<uint32_t W>
[[gnu::noinline]] void FetchScalar(const uint8_t* IRS_RESTRICT base,
                                   const uint32_t* IRS_RESTRICT rows,
                                   uint32_t* IRS_RESTRICT out) noexcept {
#pragma clang loop vectorize(disable)
  for (size_t i = 0; i != kBlock; ++i) {
    const uint64_t bit = uint64_t{rows[i]} * W;
    out[i] = static_cast<uint32_t>(
               absl::little_endian::Load64(base + (bit >> 3)) >> (bit & 7)) &
             Mask(W);
  }
}

[[gnu::noinline]] void FetchScalarAny(const uint8_t* IRS_RESTRICT base,
                                      const uint32_t* IRS_RESTRICT rows,
                                      uint32_t* IRS_RESTRICT out,
                                      uint32_t width) noexcept {
  const uint32_t mask = Mask(width);
#pragma clang loop vectorize(disable)
  for (size_t i = 0; i != kBlock; ++i) {
    const uint64_t bit = uint64_t{rows[i]} * width;
    out[i] = static_cast<uint32_t>(
               absl::little_endian::Load64(base + (bit >> 3)) >> (bit & 7)) &
             mask;
  }
}

template<bool Wide>
IRS_FORCE_INLINE __m256i GatherBits(const uint8_t* base, __m256i rel,
                                    __m256i width, __m256i mask) noexcept {
  const __m256i bit = _mm256_mullo_epi32(rel, width);
  const __m256i at = _mm256_srli_epi32(bit, 3);
  const __m256i shift = _mm256_and_si256(bit, _mm256_set1_epi32(7));
  const auto* p = reinterpret_cast<const int*>(base);
  __m256i v = _mm256_srlv_epi32(_mm256_i32gather_epi32(p, at, 1), shift);
  if constexpr (Wide) {
    const __m256i hi = _mm256_i32gather_epi32(p + 1, at, 1);
    v = _mm256_or_si256(
      v, _mm256_sllv_epi32(hi, _mm256_sub_epi32(_mm256_set1_epi32(32), shift)));
  }
  return _mm256_and_si256(v, mask);
}

template<bool Wide>
IRS_FORCE_INLINE void GatherBlock(const uint8_t* IRS_RESTRICT base,
                                  const uint32_t* IRS_RESTRICT rows,
                                  uint32_t* IRS_RESTRICT out,
                                  uint32_t width) noexcept {
  const uint32_t first = rows[0] & ~uint32_t{7};
  const uint8_t* origin = base + size_t{first / 8} * width;
  const __m256i offset = _mm256_set1_epi32(static_cast<int>(first));
  const __m256i w = _mm256_set1_epi32(static_cast<int>(width));
  const __m256i mask = _mm256_set1_epi32(static_cast<int>(Mask(width)));
  for (size_t i = 0; i != kBlock; i += 8) {
    const __m256i rel = _mm256_sub_epi32(
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(rows + i)), offset);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + i),
                        GatherBits<Wide>(origin, rel, w, mask));
  }
}

template<uint32_t W>
[[gnu::noinline]] void FetchGather(const uint8_t* IRS_RESTRICT base,
                                   const uint32_t* IRS_RESTRICT rows,
                                   uint32_t* IRS_RESTRICT out) noexcept {
  GatherBlock<(W > 25 && W != 32)>(base, rows, out, W);
}

[[gnu::noinline]] void FetchGatherAny(const uint8_t* IRS_RESTRICT base,
                                      const uint32_t* IRS_RESTRICT rows,
                                      uint32_t* IRS_RESTRICT out,
                                      uint32_t width) noexcept {
  if (width <= 25 || width == 32) {
    GatherBlock<false>(base, rows, out, width);
  } else {
    GatherBlock<true>(base, rows, out, width);
  }
}

template<size_t... I>
constexpr std::array<Fetch, 33> ScalarTable(std::index_sequence<I...>) {
  return {nullptr, &FetchScalar<I + 1>...};
}

template<size_t... I>
constexpr std::array<Fetch, 33> GatherTable(std::index_sequence<I...>) {
  return {nullptr, &FetchGather<I + 1>...};
}

constexpr auto kScalar = ScalarTable(std::make_index_sequence<32>{});
constexpr auto kGather = GatherTable(std::make_index_sequence<32>{});

const Column& GetColumn(uint32_t rows, uint32_t width) {
  static Column column;
  if (column.Rows() != rows || column.Width() != width) {
    column.Build(rows, width);
  }
  return column;
}

const std::vector<uint32_t>& GetRows(uint32_t rows, uint32_t span) {
  static std::map<std::pair<uint32_t, uint32_t>, std::vector<uint32_t>> cache;
  auto& out = cache[{rows, span}];
  if (!out.empty()) {
    return out;
  }
  std::mt19937_64 rng{uint64_t{span} * 104729 + rows};
  std::uniform_int_distribution<uint32_t> start{0, rows - span};
  std::vector<uint32_t> starts(kBlocks);
  for (auto& s : starts) {
    s = start(rng);
  }
  std::sort(starts.begin(), starts.end());
  out.resize(kBlocks * kBlock);
  std::uniform_int_distribution<uint32_t> pick{0, span - 1};
  for (size_t b = 0; b != kBlocks; ++b) {
    auto* block = out.data() + b * kBlock;
    if (span == kBlock) {
      for (uint32_t i = 0; i != kBlock; ++i) {
        block[i] = starts[b] + i;
      }
      continue;
    }
    size_t n = 0;
    while (n != kBlock) {
      block[n++] = starts[b] + pick(rng);
      if (n == kBlock) {
        std::sort(block, block + kBlock);
        n = static_cast<size_t>(std::unique(block, block + kBlock) - block);
      }
    }
  }
  return out;
}

template<typename F>
bool Check(const Column& column, const std::vector<uint32_t>& rows, F&& fetch) {
  alignas(64) uint32_t out[kBlock];
  for (size_t b = 0; b < kBlocks; b += 97) {
    const auto* block = rows.data() + b * kBlock;
    fetch(block, out);
    for (size_t i = 0; i != kBlock; ++i) {
      if (out[i] != Value(block[i], column.Width())) {
        return false;
      }
    }
  }
  return true;
}

template<typename F>
void Run(benchmark::State& state, const std::vector<uint32_t>& rows,
         F&& fetch) {
  alignas(64) uint32_t out[kBlock];
  size_t b = 0;
  for (auto _ : state) {
    fetch(rows.data() + b * kBlock, out);
    benchmark::DoNotOptimize(out);
    benchmark::ClobberMemory();
    if (++b == kBlocks) {
      b = 0;
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kBlock);
}

void Bench(benchmark::State& state) {
  const auto rows = static_cast<uint32_t>(state.range(0));
  const auto width = static_cast<uint32_t>(state.range(1));
  const auto kernel = static_cast<Kernel>(state.range(2));
  const auto span = static_cast<uint32_t>(state.range(3));
  const auto& column = GetColumn(rows, width);
  const auto& ids = GetRows(rows, span);
  const uint8_t* base = column.Data();
  const auto measure = [&](auto&& fetch) {
    if (!Check(column, ids, fetch)) {
      state.SkipWithError("mismatch");
      return;
    }
    Run(state, ids, fetch);
  };
  switch (kernel) {
    case Kernel::Scalar:
      return measure([f = kScalar[width], base](
                       const uint32_t* r, uint32_t* o) { f(base, r, o); });
    case Kernel::ScalarAny:
      return measure([base, width](const uint32_t* r, uint32_t* o) {
        FetchScalarAny(base, r, o, width);
      });
    case Kernel::Gather:
      return measure([f = kGather[width], base](
                       const uint32_t* r, uint32_t* o) { f(base, r, o); });
    case Kernel::GatherAny:
      return measure([base, width](const uint32_t* r, uint32_t* o) {
        FetchGatherAny(base, r, o, width);
      });
  }
}

void Args(benchmark::internal::Benchmark* b) {
  for (int64_t rows : {int64_t{1} << 16, int64_t{1} << 20, int64_t{1} << 24,
                       int64_t{1} << 28}) {
    for (int64_t width = 1; width <= 32; ++width) {
      for (int kernel : {0, 1, 2, 3}) {
        for (int64_t span : {256, 4096, 32768, 262144}) {
          if (span <= rows) {
            b->Args({rows, width, kernel, span});
          }
        }
      }
    }
  }
  b->ArgNames({"rows", "width", "kernel", "span"});
}

BENCHMARK(Bench)->Apply(Args);

constexpr uint32_t kEscape = 0xFF;

class Slots {
 public:
  Slots() = default;
  Slots(const Slots&) = delete;
  Slots& operator=(const Slots&) = delete;
  ~Slots() { Reset(); }

  uint8_t* Map(size_t size) {
    Reset();
    void* p = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED) {
      std::abort();
    }
    madvise(p, size, MADV_NOHUGEPAGE);
    _data = static_cast<uint8_t*>(p);
    _size = size;
    return _data;
  }

  const uint8_t* Data() const noexcept { return _data; }

 private:
  void Reset() noexcept {
    if (_data != nullptr) {
      munmap(_data, _size);
      _data = nullptr;
    }
  }

  uint8_t* _data = nullptr;
  size_t _size = 0;
};

uint32_t Expected(uint32_t row) noexcept { return 1000 + (row & 1023); }

class Picked {
 public:
  void Build(uint32_t rows, uint32_t every) {
    _rows.clear();
    _marks.assign((rows + 63) / 64, 0);
    if (every == 0) {
      return;
    }
    std::mt19937_64 rng{uint64_t{every} * 31 + rows};
    std::geometric_distribution<uint32_t> gap{1.0 / every};
    for (uint64_t r = gap(rng); r < rows; r += 1 + gap(rng)) {
      _rows.push_back(static_cast<uint32_t>(r));
      _marks[r / 64] |= uint64_t{1} << (r % 64);
    }
  }

  const std::vector<uint32_t>& Rows() const noexcept { return _rows; }

  uint32_t Want(uint32_t row) const noexcept {
    return (_marks[row / 64] >> (row % 64)) & 1 ? Expected(row) : Value(row, 7);
  }

 private:
  std::vector<uint32_t> _rows;
  std::vector<uint64_t> _marks;
};

template<typename Offset>
class Escaped {
 public:
  void Build(uint32_t rows, uint32_t every, uint32_t shift) {
    uint8_t* slots = _slots.Map(rows + kSlack);
    _shift = shift;
    _picked.Build(rows, every);
    const auto& picked = _picked.Rows();
    for (uint32_t r = 0; r != rows; ++r) {
      slots[r] = static_cast<uint8_t>(Value(r, 7));
    }
    const uint32_t buckets = (rows >> shift) + 1;
    _starts.assign(buckets + 1, 0);
    _offsets.clear();
    _values.clear();
    size_t i = 0;
    for (uint32_t b = 0; b != buckets; ++b) {
      _starts[b] = static_cast<uint32_t>(i);
      while (i != picked.size() && (picked[i] >> shift) == b) {
        const auto r = picked[i++];
        slots[r] = kEscape;
        _offsets.push_back(static_cast<Offset>(r & ((1u << shift) - 1)));
        _values.push_back(static_cast<uint16_t>(Expected(r)));
      }
    }
    _starts[buckets] = static_cast<uint32_t>(i);
    _offsets.resize(_offsets.size() + 32);
    _any = !picked.empty();
    _exceptions = picked.size();
  }

  uint32_t Want(uint32_t row) const noexcept { return _picked.Want(row); }

  size_t Exceptions() const noexcept { return _exceptions; }
  size_t TableBytes() const noexcept {
    return _starts.size() * sizeof(uint32_t) + _exceptions * sizeof(Offset) +
           _exceptions * sizeof(uint16_t);
  }

  IRS_FORCE_INLINE void Fetch(const uint32_t* IRS_RESTRICT rows,
                              uint32_t* IRS_RESTRICT out) const noexcept {
    const uint8_t* slots = _slots.Data();
    for (size_t i = 0; i != kBlock; ++i) {
      out[i] = slots[rows[i]];
    }
    if (!_any) {
      return;
    }
    bool escaped = false;
    for (size_t i = 0; i != kBlock; ++i) {
      escaped |= out[i] == kEscape;
    }
    if (escaped) [[unlikely]] {
      Patch(rows, out);
    }
  }

 private:
  [[gnu::noinline]] void Patch(const uint32_t* IRS_RESTRICT rows,
                               uint32_t* IRS_RESTRICT out) const noexcept {
    const __m256i needle = _mm256_set1_epi32(static_cast<int>(kEscape));
    for (size_t i = 0; i != kBlock; i += 8) {
      auto m = static_cast<uint32_t>(
        _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(out + i)),
          needle))));
      for (; m != 0; m &= m - 1) {
        const auto j = i + static_cast<size_t>(std::countr_zero(m));
        out[j] = Lookup(rows[j]);
      }
    }
  }

  uint32_t Lookup(uint32_t row) const noexcept {
    const uint32_t b = row >> _shift;
    const uint32_t lo = _starts[b];
    const uint32_t n = _starts[b + 1] - lo;
    const auto key = static_cast<Offset>(row & ((1u << _shift) - 1));
    const Offset* offsets = _offsets.data() + lo;
    uint32_t k = 0;
    if constexpr (sizeof(Offset) == 1) {
      const __m256i needle = _mm256_set1_epi8(static_cast<char>(key));
      for (; k < n; k += 32) {
        auto m = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(offsets + k)),
          needle)));
        if (n - k < 32) {
          m &= (uint32_t{1} << (n - k)) - 1;
        }
        if (m != 0) {
          k += static_cast<uint32_t>(std::countr_zero(m));
          break;
        }
      }
    } else {
      const __m256i needle = _mm256_set1_epi16(static_cast<short>(key));
      for (; k < n; k += 16) {
        auto m = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi16(
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(offsets + k)),
          needle)));
        if (n - k < 16) {
          m &= (uint32_t{1} << (2 * (n - k))) - 1;
        }
        if (m != 0) {
          k += static_cast<uint32_t>(std::countr_zero(m)) / 2;
          break;
        }
      }
    }
    return _values[lo + k];
  }

  Slots _slots;
  Picked _picked;
  uint32_t _shift = 8;
  bool _any = false;
  size_t _exceptions = 0;
  std::vector<uint32_t> _starts;
  std::vector<Offset> _offsets;
  std::vector<uint16_t> _values;
};

template<bool Fused>
class Indexed {
 public:
  static constexpr uint32_t kCodes = 16;
  static constexpr uint32_t kFirst = 256 - kCodes;
  static constexpr uint32_t kOverflow = 255;
  static constexpr size_t kGroup = 64;

  void Build(uint32_t rows, uint32_t every) {
    uint8_t* slots = _slots.Map(rows + kSlack);
    _shift = every == 0 ? 20
                        : static_cast<uint32_t>(std::clamp<int>(
                            std::bit_width(uint64_t{every} * 4) - 1, 6, 20));
    _picked.Build(rows, every);
    const auto& picked = _picked.Rows();
    for (uint32_t r = 0; r != rows; ++r) {
      slots[r] = static_cast<uint8_t>(Value(r, 7));
    }
    const uint32_t buckets = (rows >> _shift) + 1;
    _bases.assign(buckets, 0);
    _values.clear();
    size_t i = 0;
    for (uint32_t b = 0; b != buckets; ++b) {
      _bases[b] = static_cast<uint32_t>(i);
      for (uint32_t local = 0; i != picked.size() && (picked[i] >> _shift) == b;
           ++local) {
        const auto r = picked[i++];
        slots[r] =
          static_cast<uint8_t>(local < kCodes - 1 ? kFirst + local : kOverflow);
        _values.push_back(static_cast<uint16_t>(Expected(r)));
      }
    }
    _values.push_back(0);
    _any = !picked.empty();
    _exceptions = picked.size();
  }

  uint32_t Want(uint32_t row) const noexcept { return _picked.Want(row); }

  size_t Exceptions() const noexcept { return _exceptions; }
  size_t TableBytes() const noexcept {
    return _bases.size() * sizeof(uint32_t) + _exceptions * sizeof(uint16_t);
  }

  IRS_FORCE_INLINE void Fetch(const uint32_t* IRS_RESTRICT rows,
                              uint32_t* IRS_RESTRICT out) const noexcept {
    const uint8_t* slots = _slots.Data();
    for (size_t i = 0; i != kBlock; ++i) {
      out[i] = slots[rows[i]];
    }
    if (!_any) {
      return;
    }
    if constexpr (Fused) {
      Gather(rows, out);
      return;
    }
    bool escaped = false;
    for (size_t i = 0; i != kBlock; ++i) {
      escaped |= out[i] >= kFirst;
    }
    if (escaped) [[unlikely]] {
      Patch(rows, out);
    }
  }

 private:
  IRS_FORCE_INLINE void Gather(const uint32_t* IRS_RESTRICT rows,
                               uint32_t* IRS_RESTRICT out) const noexcept {
    const __m256i limit = _mm256_set1_epi32(static_cast<int>(kFirst - 1));
    const __m256i first = _mm256_set1_epi32(static_cast<int>(kFirst));
    const __m256i overflow = _mm256_set1_epi32(static_cast<int>(kOverflow));
    const __m256i low = _mm256_set1_epi32(0xFFFF);
    const __m128i shift = _mm_cvtsi32_si128(static_cast<int>(_shift));
    const auto* bases = reinterpret_cast<const int*>(_bases.data());
    const auto* values = reinterpret_cast<const int*>(_values.data());
    for (size_t v = 0; v != kBlock; v += 8) {
      const __m256i codes =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(out + v));
      const __m256i m = _mm256_cmpgt_epi32(codes, limit);
      if (_mm256_testz_si256(m, m)) [[likely]] {
        continue;
      }
      if (!_mm256_testz_si256(m, _mm256_cmpeq_epi32(codes, overflow)))
        [[unlikely]] {
        for (size_t j = v; j != v + 8; ++j) {
          if (out[j] >= kFirst) {
            out[j] = Lookup(rows[j], out[j]);
          }
        }
        continue;
      }
      const __m256i buckets = _mm256_srl_epi32(
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(rows + v)), shift);
      const __m256i base = _mm256_mask_i32gather_epi32(_mm256_setzero_si256(),
                                                       bases, buckets, m, 4);
      const __m256i at = _mm256_add_epi32(base, _mm256_sub_epi32(codes, first));
      const __m256i got = _mm256_and_si256(
        _mm256_mask_i32gather_epi32(_mm256_setzero_si256(), values, at, m, 2),
        low);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + v),
                          _mm256_blendv_epi8(codes, got, m));
    }
  }

  [[gnu::noinline]] void Patch(const uint32_t* IRS_RESTRICT rows,
                               uint32_t* IRS_RESTRICT out) const noexcept {
    const __m256i limit = _mm256_set1_epi32(static_cast<int>(kFirst - 1));
    for (size_t g = 0; g != kBlock; g += kGroup) {
      __m256i any = _mm256_setzero_si256();
      for (size_t v = 0; v != kGroup; v += 8) {
        any = _mm256_or_si256(
          any,
          _mm256_cmpgt_epi32(
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(out + g + v)),
            limit));
      }
      if (_mm256_testz_si256(any, any)) {
        continue;
      }
      for (size_t v = 0; v != kGroup; v += 8) {
        auto m = static_cast<uint32_t>(
          _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(out + g + v)),
            limit))));
        for (; m != 0; m &= m - 1) {
          const auto j = g + v + static_cast<size_t>(std::countr_zero(m));
          out[j] = Lookup(rows[j], out[j]);
        }
      }
    }
  }

  IRS_FORCE_INLINE uint32_t Lookup(uint32_t row, uint32_t code) const noexcept {
    const uint32_t base = _bases[row >> _shift];
    if (code != kOverflow) [[likely]] {
      return _values[base + code - kFirst];
    }
    return Overflow(row, base);
  }

  [[gnu::noinline]] uint32_t Overflow(uint32_t row,
                                      uint32_t base) const noexcept {
    const uint8_t* slots = _slots.Data();
    uint32_t rank = 0;
    for (uint32_t r = (row >> _shift) << _shift; r != row; ++r) {
      rank += slots[r] >= kFirst;
    }
    return _values[base + rank];
  }

  Slots _slots;
  Picked _picked;
  uint32_t _shift = 20;
  bool _any = false;
  size_t _exceptions = 0;
  std::vector<uint32_t> _bases;
  std::vector<uint16_t> _values;
};

class Plain16 {
 public:
  void Build(uint32_t rows) {
    uint8_t* slots = _slots.Map(size_t{rows} * 2 + kSlack);
    for (uint32_t r = 0; r != rows; ++r) {
      absl::little_endian::Store16(slots + size_t{r} * 2,
                                   static_cast<uint16_t>(Value(r, 16)));
    }
  }

  uint32_t Want(uint32_t row) const noexcept { return Value(row, 16); }
  size_t Exceptions() const noexcept { return 0; }
  size_t TableBytes() const noexcept { return 0; }

  IRS_FORCE_INLINE void Fetch(const uint32_t* IRS_RESTRICT rows,
                              uint32_t* IRS_RESTRICT out) const noexcept {
    const uint8_t* slots = _slots.Data();
    for (size_t i = 0; i != kBlock; ++i) {
      out[i] = absl::little_endian::Load16(slots + size_t{rows[i]} * 2);
    }
  }

 private:
  Slots _slots;
};

template<typename Column>
void Measure(benchmark::State& state, const Column& column,
             const std::vector<uint32_t>& ids, uint32_t rows) {
  alignas(64) uint32_t out[kBlock];
  for (size_t b = 0; b < kBlocks; b += 7) {
    const auto* block = ids.data() + b * kBlock;
    column.Fetch(block, out);
    for (size_t i = 0; i != kBlock; ++i) {
      if (out[i] != column.Want(block[i])) {
        state.SkipWithError("mismatch");
        return;
      }
    }
  }
  Run(state, ids, [&](const uint32_t* r, uint32_t* o) { column.Fetch(r, o); });
  state.counters["table_bits_per_row"] =
    static_cast<double>(column.TableBytes()) * 8 / static_cast<double>(rows);
  state.counters["exceptions"] = static_cast<double>(column.Exceptions());
}

template<typename Column, typename... Key>
const Column& Cached(Key... key) {
  static Column column;
  static std::array<uint32_t, sizeof...(Key)> built{};
  if (built != std::array<uint32_t, sizeof...(Key)>{key...}) {
    column.Build(key...);
    built = {key...};
  }
  return column;
}

void BenchEscapes(benchmark::State& state) {
  const auto rows = static_cast<uint32_t>(state.range(0));
  const auto every = static_cast<uint32_t>(state.range(1));
  const auto shift = static_cast<uint32_t>(state.range(2));
  const auto& ids = GetRows(rows, static_cast<uint32_t>(state.range(3)));
  if (shift <= 8) {
    Measure(state, Cached<Escaped<uint8_t>>(rows, every, shift), ids, rows);
  } else {
    Measure(state, Cached<Escaped<uint16_t>>(rows, every, shift), ids, rows);
  }
}

void BenchIndexed(benchmark::State& state) {
  const auto rows = static_cast<uint32_t>(state.range(0));
  const auto every = static_cast<uint32_t>(state.range(1));
  const auto& ids = GetRows(rows, static_cast<uint32_t>(state.range(2)));
  Measure(state, Cached<Indexed<false>>(rows, every), ids, rows);
}

void BenchFused(benchmark::State& state) {
  const auto rows = static_cast<uint32_t>(state.range(0));
  const auto every = static_cast<uint32_t>(state.range(1));
  const auto& ids = GetRows(rows, static_cast<uint32_t>(state.range(2)));
  Measure(state, Cached<Indexed<true>>(rows, every), ids, rows);
}

void BenchPlain16(benchmark::State& state) {
  const auto rows = static_cast<uint32_t>(state.range(0));
  const auto& ids = GetRows(rows, static_cast<uint32_t>(state.range(1)));
  Measure(state, Cached<Plain16>(rows), ids, rows);
}

void EscapeArgs(benchmark::internal::Benchmark* b) {
  for (int64_t rows : {int64_t{1} << 20, int64_t{1} << 28}) {
    for (int64_t every : {0, 16384, 4096, 1024, 256, 64, 16}) {
      std::vector<int64_t> shifts{8};
      if (every != 0) {
        const int64_t fit =
          std::clamp<int64_t>(std::bit_width(uint64_t(every)) - 1, 4, 16);
        if (fit != 8) {
          shifts.push_back(fit);
        }
      }
      for (const auto shift : shifts) {
        for (int64_t span : {256, 4096, 262144}) {
          b->Args({rows, every, shift, span});
        }
      }
    }
  }
  b->ArgNames({"rows", "every", "shift", "span"});
}

void IndexedArgs(benchmark::internal::Benchmark* b) {
  for (int64_t rows : {int64_t{1} << 20, int64_t{1} << 28}) {
    for (int64_t every : {0, 16384, 4096, 1024, 256, 64, 16}) {
      for (int64_t span : {256, 4096, 262144}) {
        b->Args({rows, every, span});
      }
    }
  }
  b->ArgNames({"rows", "every", "span"});
}

void Plain16Args(benchmark::internal::Benchmark* b) {
  for (int64_t rows : {int64_t{1} << 20, int64_t{1} << 28}) {
    for (int64_t span : {256, 4096, 262144}) {
      b->Args({rows, span});
    }
  }
  b->ArgNames({"rows", "span"});
}

BENCHMARK(BenchEscapes)->Apply(EscapeArgs);
BENCHMARK(BenchIndexed)->Apply(IndexedArgs);
BENCHMARK(BenchFused)->Apply(IndexedArgs);
BENCHMARK(BenchPlain16)->Apply(Plain16Args);

}  // namespace

BENCHMARK_MAIN();
