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

#pragma once

#include <immintrin.h>
#include <roaring/bitset_util.h>
#include <roaring/containers/containers.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "iresearch/index/docs_mask/base.hpp"
#include "iresearch/index/docs_mask/kernels.hpp"
#include "iresearch/index/document_mask.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs_mask {

inline constexpr uint32_t kChunkShift = DocumentMask::kChunkShift;
inline constexpr uint64_t kChunkDocs = DocumentMask::kChunkDocs;
inline constexpr uint32_t kChunkLow = static_cast<uint32_t>(kChunkDocs - 1);
inline constexpr uint32_t kChunkWords =
  roaring::internal::BITSET_CONTAINER_SIZE_IN_WORDS;

struct Range {
  uint32_t first;
  uint32_t last;
};

struct ChunkState {
  int32_t pos = 0;
};

struct BitsetChunk {
  using Container = roaring::internal::bitset_container_t;
  static constexpr uint8_t kType = BITSET_CONTAINER_TYPE;

  using State = ChunkState;

  IRS_FORCE_INLINE static bool Test(const Container* c, uint32_t low) noexcept {
    return ((c->words[low / 64] >> (low % 64)) & 1) != 0;
  }

  IRS_FORCE_INLINE static bool Next(const Container* c, State&, uint32_t low,
                                    Range& out) noexcept {
    const auto* words = c->words;
    uint32_t w = low / 64;
    auto rest = words[w] & (~uint64_t{0} << (low % 64));
    while (rest == 0) {
      if (++w == kChunkWords) {
        return false;
      }
      rest = words[w];
    }
    const uint32_t first =
      w * 64 + static_cast<uint32_t>(std::countr_zero(rest));
    auto zeros = ~words[w] & (~uint64_t{0} << (first % 64));
    while (zeros == 0) {
      if (++w == kChunkWords) {
        out = {first, static_cast<uint32_t>(kChunkDocs)};
        return true;
      }
      zeros = ~words[w];
    }
    out = {first, w * 64 + static_cast<uint32_t>(std::countr_zero(zeros))};
    return true;
  }

  template<bool kAndNot>
  IRS_FORCE_INLINE static void Apply(const Container* c, State&, uint64_t base,
                                     uint64_t lo, uint64_t hi, doc_id_t min,
                                     doc_id_t max,
                                     uint64_t* IRS_RESTRICT dst) noexcept {
    ApplyWords<kAndNot>(c->words, kChunkWords, base, lo, hi, min, max, dst);
  }

  IRS_FORCE_INLINE static uint64_t Count(const Container* c, uint32_t first,
                                         uint32_t last) noexcept {
    if (first >= last) {
      return 0;
    }
    return static_cast<uint64_t>(roaring::internal::bitset_lenrange_cardinality(
      c->words, first, last - first - 1));
  }
};

struct ArrayChunk {
  using Container = roaring::internal::array_container_t;
  static constexpr uint8_t kType = ARRAY_CONTAINER_TYPE;
  static constexpr uint32_t kScanBlocks = 2;

  using State = ChunkState;

  IRS_NO_INLINE static int32_t Forward(const uint16_t* values, int32_t pos,
                                       int32_t size, uint32_t low) noexcept {
#ifdef __AVX2__
    const auto bound = _mm256_set1_epi16(static_cast<int16_t>(low));
    for (uint32_t block = 0; block != kScanBlocks && pos + 16 <= size;
         ++block, pos += 16) {
      const auto ids =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(values + pos));
      const auto above = static_cast<uint32_t>(_mm256_movemask_epi8(
        _mm256_cmpeq_epi16(_mm256_max_epu16(ids, bound), ids)));
      if (above != 0) {
        return pos + static_cast<int32_t>(std::countr_zero(above) / 2);
      }
    }
#endif
    return static_cast<int32_t>(
      Gallop(values, static_cast<uint32_t>(pos), static_cast<uint32_t>(size),
             [low](uint16_t v) noexcept { return v < low; }));
  }

  IRS_FORCE_INLINE static int32_t Seek(const Container* c, State& state,
                                       uint32_t low) noexcept {
    const auto* values = c->array;
    const auto size = c->cardinality;
    auto pos = state.pos;
    if (pos > 0 && values[pos - 1] >= low) [[unlikely]] {
      pos = static_cast<int32_t>(
        LowerBound(values, static_cast<uint32_t>(size),
                   [low](uint16_t v) noexcept { return v < low; }));
    } else if (pos < size && values[pos] < low) {
      ++pos;
      if (pos < size && values[pos] < low) {
        pos = Forward(values, pos + 1, size, low);
      }
    }
    return state.pos = pos;
  }

  IRS_FORCE_INLINE static bool Next(const Container* c, State& state,
                                    uint32_t low, Range& out) noexcept {
    auto pos = Seek(c, state, low);
    const auto size = c->cardinality;
    if (pos == size) {
      return false;
    }
    const auto* values = c->array;
    const uint32_t first = values[pos];
    uint32_t last = first + 1;
    while (pos + 1 < size && values[pos + 1] == last) {
      ++pos;
      ++last;
    }
    state.pos = pos;
    out = {first, last};
    return true;
  }

  template<bool kAndNot>
  IRS_FORCE_INLINE static void Apply(const Container* c, State& state,
                                     uint64_t base, uint64_t lo, uint64_t hi,
                                     doc_id_t min, doc_id_t,
                                     uint64_t* IRS_RESTRICT dst) noexcept {
    const auto* values = c->array;
    const auto size = c->cardinality;
    auto i = Seek(c, state, static_cast<uint32_t>(lo - base));
    const auto stop = hi - base;
    const auto shift = base - min;
    while (i < size && values[i] < stop) {
      const auto offset = shift + values[i];
      const auto word = offset / 64;
      auto bits = uint64_t{1} << (offset % 64);
      for (++i; i < size && values[i] < stop; ++i) {
        const auto next = shift + values[i];
        if (next / 64 != word) {
          break;
        }
        bits |= uint64_t{1} << (next % 64);
      }
      docs_mask::Apply<kAndNot>(dst[word], bits);
    }
    state.pos = i;
  }

  IRS_FORCE_INLINE static uint64_t Count(const Container* c, uint32_t first,
                                         uint32_t last) noexcept {
    const auto* values = c->array;
    const auto size = static_cast<uint32_t>(c->cardinality);
    return LowerBound(values, size,
                      [last](uint16_t v) noexcept { return v < last; }) -
           LowerBound(values, size,
                      [first](uint16_t v) noexcept { return v < first; });
  }
};

struct RunChunk {
  using Container = roaring::internal::run_container_t;
  using Run = roaring::internal::rle16_t;
  static constexpr uint8_t kType = RUN_CONTAINER_TYPE;

  using State = ChunkState;

  IRS_FORCE_INLINE static uint32_t End(const Run& run) noexcept {
    return uint32_t{run.value} + run.length + 1;
  }

  IRS_FORCE_INLINE static int32_t Seek(const Container* c, State& state,
                                       uint32_t low) noexcept {
    const auto* runs = c->runs;
    const auto size = c->n_runs;
    auto pos = state.pos;
    const auto before = [low](const Run& run) noexcept {
      return End(run) <= low;
    };
    if (pos > 0 && End(runs[pos - 1]) > low) [[unlikely]] {
      pos = static_cast<int32_t>(
        LowerBound(runs, static_cast<uint32_t>(size), before));
    } else if (pos < size && before(runs[pos])) {
      pos = static_cast<int32_t>(Gallop(runs, static_cast<uint32_t>(pos),
                                        static_cast<uint32_t>(size), before));
    }
    return state.pos = pos;
  }

  IRS_FORCE_INLINE static bool Next(const Container* c, State& state,
                                    uint32_t low, Range& out) noexcept {
    const auto pos = Seek(c, state, low);
    if (pos == c->n_runs) {
      return false;
    }
    const auto& run = c->runs[pos];
    out = {std::max<uint32_t>(low, run.value), End(run)};
    return true;
  }

  template<bool kAndNot>
  IRS_FORCE_INLINE static void Apply(const Container* c, State& state,
                                     uint64_t base, uint64_t lo, uint64_t hi,
                                     doc_id_t min, doc_id_t,
                                     uint64_t* IRS_RESTRICT dst) noexcept {
    const auto* runs = c->runs;
    const auto size = c->n_runs;
    auto pos = Seek(c, state, static_cast<uint32_t>(lo - base));
    for (; pos < size; ++pos) {
      const auto start = base + runs[pos].value;
      if (start >= hi) {
        break;
      }
      const auto stop = base + End(runs[pos]);
      const auto first = static_cast<uint32_t>(std::max(start, lo) - min);
      const auto last = static_cast<uint32_t>(std::min(stop, hi) - min);
      if (first < last) {
        ApplyRange<kAndNot>(dst, first, last);
      }
      if (stop > hi) {
        break;
      }
    }
    state.pos = pos;
  }

  IRS_FORCE_INLINE static uint64_t Count(const Container* c, uint32_t first,
                                         uint32_t last) noexcept {
    const auto* runs = c->runs;
    const auto size = static_cast<uint32_t>(c->n_runs);
    uint64_t total = 0;
    for (auto pos = LowerBound(
           runs, size,
           [first](const Run& run) noexcept { return End(run) <= first; });
         pos < size && runs[pos].value < last; ++pos) {
      total += std::min(End(runs[pos]), last) -
               std::max<uint32_t>(runs[pos].value, first);
    }
    return total;
  }
};

struct TypedContainer {
  const void* data;
  uint8_t type;
};

struct MixedChunk {
  using Container = TypedContainer;
  using State = ChunkState;

  IRS_FORCE_INLINE static bool Next(TypedContainer c, State& state,
                                    uint32_t low, Range& out) noexcept {
    switch (c.type) {
      case BITSET_CONTAINER_TYPE:
        return BitsetChunk::Next(
          static_cast<const BitsetChunk::Container*>(c.data), state, low, out);
      case ARRAY_CONTAINER_TYPE:
        return ArrayChunk::Next(
          static_cast<const ArrayChunk::Container*>(c.data), state, low, out);
      default:
        return RunChunk::Next(static_cast<const RunChunk::Container*>(c.data),
                              state, low, out);
    }
  }

  template<bool kAndNot>
  IRS_FORCE_INLINE static void Apply(TypedContainer c, State& state,
                                     uint64_t base, uint64_t lo, uint64_t hi,
                                     doc_id_t min, doc_id_t max,
                                     uint64_t* IRS_RESTRICT dst) noexcept {
    switch (c.type) {
      case BITSET_CONTAINER_TYPE:
        return BitsetChunk::Apply<kAndNot>(
          static_cast<const BitsetChunk::Container*>(c.data), state, base, lo,
          hi, min, max, dst);
      case ARRAY_CONTAINER_TYPE:
        return ArrayChunk::Apply<kAndNot>(
          static_cast<const ArrayChunk::Container*>(c.data), state, base, lo,
          hi, min, max, dst);
      default:
        return RunChunk::Apply<kAndNot>(
          static_cast<const RunChunk::Container*>(c.data), state, base, lo, hi,
          min, max, dst);
    }
  }

  IRS_FORCE_INLINE static uint64_t Count(TypedContainer c, uint32_t first,
                                         uint32_t last) noexcept {
    switch (c.type) {
      case BITSET_CONTAINER_TYPE:
        return BitsetChunk::Count(
          static_cast<const BitsetChunk::Container*>(c.data), first, last);
      case ARRAY_CONTAINER_TYPE:
        return ArrayChunk::Count(
          static_cast<const ArrayChunk::Container*>(c.data), first, last);
      default:
        return RunChunk::Count(static_cast<const RunChunk::Container*>(c.data),
                               first, last);
    }
  }
};

template<typename Chunk>
class DenseLayout {
 public:
  using Container = typename Chunk::Container;

  explicit DenseLayout(const DocumentMask* mask) noexcept
    : _containers{mask->Containers()},
      _first{mask->KeyAt(0)},
      _count{mask->ContainerCount()} {
    SDB_ASSERT(_count != 0);
    SDB_ASSERT(uint32_t{mask->KeyAt(_count - 1)} - _first == _count - 1);
  }

  IRS_FORCE_INLINE uint32_t Count() const noexcept { return _count; }
  IRS_FORCE_INLINE uint32_t KeyAt(uint32_t i) const noexcept {
    return _first + i;
  }
  IRS_FORCE_INLINE const Container* At(uint32_t i) const noexcept {
    return static_cast<const Container*>(_containers[i]);
  }
  IRS_FORCE_INLINE uint32_t Find(uint32_t key, uint32_t, bool) const noexcept {
    return LowerBound(key);
  }
  IRS_FORCE_INLINE uint32_t LowerBound(uint32_t key) const noexcept {
    return key <= _first ? 0 : std::min(key - _first, _count);
  }

 private:
  const void* const* _containers;
  uint32_t _first;
  uint32_t _count;
};

template<typename Chunk>
class GappedLayout {
 public:
  using Container = typename Chunk::Container;

  explicit GappedLayout(const DocumentMask* mask) noexcept
    : _containers{mask != nullptr ? mask->Containers() : nullptr},
      _keys{mask != nullptr ? mask->Keys() : nullptr},
      _types{mask != nullptr ? mask->Types() : nullptr},
      _count{mask != nullptr ? mask->ContainerCount() : 0} {}

  IRS_FORCE_INLINE uint32_t Count() const noexcept { return _count; }
  IRS_FORCE_INLINE uint32_t KeyAt(uint32_t i) const noexcept {
    return _keys[i];
  }
  IRS_FORCE_INLINE auto At(uint32_t i) const noexcept {
    if constexpr (std::is_same_v<Container, TypedContainer>) {
      return TypedContainer{_containers[i], _types[i]};
    } else {
      return static_cast<const Container*>(_containers[i]);
    }
  }
  IRS_FORCE_INLINE uint32_t Find(uint32_t key, uint32_t hint,
                                 bool backward) const noexcept {
    if (backward || hint >= _count || (hint != 0 && _keys[hint - 1] >= key)) {
      return LowerBound(key);
    }
    for (uint32_t step = 0; step != 4; ++step, ++hint) {
      if (hint == _count || _keys[hint] >= key) {
        return hint;
      }
    }
    return hint + docs_mask::LowerBound(
                    _keys + hint, _count - hint,
                    [key](uint16_t k) noexcept { return k < key; });
  }
  IRS_FORCE_INLINE uint32_t LowerBound(uint32_t key) const noexcept {
    return docs_mask::LowerBound(
      _keys, _count, [key](uint16_t k) noexcept { return k < key; });
  }

 private:
  const void* const* _containers;
  const uint16_t* _keys;
  const uint8_t* _types;
  uint32_t _count;
};

template<typename Derived, typename Chunk, typename Layout>
class Chunked : public DocsMaskBase<Derived> {
 public:
  using Container = typename Chunk::Container;

  Chunked(const DocumentMask* mask, doc_id_t visible_end) noexcept
    : _layout{mask}, _end{visible_end} {}

  MaskedSpan NextMasked(doc_id_t doc) noexcept {
    if (doc >= _end) {
      return {doc, doc_limits::eof()};
    }
    const auto count = _layout.Count();
    auto i = _layout.Find(doc >> kChunkShift, _probe.index, doc < _probe.last);
    _probe.last = doc;
    for (; i < count; ++i) {
      if (i != _probe.index) {
        _probe.index = i;
        _probe.state = {};
      }
      const auto base = uint64_t{_layout.KeyAt(i)} << kChunkShift;
      const auto low = doc > base ? static_cast<uint32_t>(doc - base) : 0;
      Range range;
      if (Chunk::Next(_layout.At(i), _probe.state, low, range)) {
        const auto first = base + range.first;
        if (first >= _end) {
          break;
        }
        return {
          static_cast<doc_id_t>(first),
          static_cast<doc_id_t>(std::min<uint64_t>(base + range.last, _end))};
      }
    }
    return {_end, doc_limits::eof()};
  }

  template<bool kAndNot>
  void Apply(doc_id_t min, doc_id_t max,
             uint64_t* IRS_RESTRICT words) noexcept {
    const auto stop = std::min<uint64_t>(max, _end);
    if (min < stop) {
      const auto count = _layout.Count();
      auto i =
        _layout.Find(min >> kChunkShift, _window.index, min < _window.last);
      _window.last = min;
      for (; i < count; ++i) {
        const auto base = uint64_t{_layout.KeyAt(i)} << kChunkShift;
        if (base >= stop) {
          break;
        }
        if (i != _window.index) {
          _window.index = i;
          _window.state = {};
        }
        Chunk::template Apply<kAndNot>(
          _layout.At(i), _window.state, base, std::max<uint64_t>(min, base),
          std::min<uint64_t>(stop, base + kChunkDocs), min, max, words);
        if (base + kChunkDocs >= stop) {
          break;
        }
      }
    }
    ApplyTail<kAndNot>(_end, min, max, words);
  }

  uint64_t CountIn(doc_id_t min, doc_id_t max) const noexcept {
    auto total = TailCount(_end, min, max);
    const auto stop = std::min<uint64_t>(max, _end);
    if (min >= stop) {
      return total;
    }
    const auto count = _layout.Count();
    for (auto i = _layout.LowerBound(min >> kChunkShift); i < count; ++i) {
      const auto base = uint64_t{_layout.KeyAt(i)} << kChunkShift;
      if (base >= stop) {
        break;
      }
      const auto lo = std::max<uint64_t>(min, base);
      const auto hi = std::min<uint64_t>(stop, base + kChunkDocs);
      total += Chunk::Count(_layout.At(i), static_cast<uint32_t>(lo - base),
                            static_cast<uint32_t>(hi - base));
    }
    return total;
  }

 protected:
  struct Cursor {
    uint32_t index = std::numeric_limits<uint32_t>::max();
    typename Chunk::State state;
    doc_id_t last = 0;
  };

  Layout _layout;
  doc_id_t _end;
  Cursor _probe;
  Cursor _window;
};

template<typename Layout>
class DirectWords {
 public:
  using Chunk = BitsetChunk;

  DirectWords(const DocumentMask* mask, doc_id_t visible_end) noexcept
    : _layout{mask},
      _begin{static_cast<doc_id_t>(_layout.KeyAt(0) << kChunkShift)},
      _count{_layout.Count()},
      _end{visible_end} {
    Rebase(doc_limits::min());
  }

  IRS_FORCE_INLINE bool Test(doc_id_t doc) noexcept {
    if (doc >= _end) [[unlikely]] {
      return true;
    }
    doc_id_t offset = doc - _base;
    if (offset >= kChunkDocs) [[unlikely]] {
      Rebase(doc);
      offset = doc - _base;
    }
    return ((_words[offset / 64] >> (offset % 64)) & 1) != 0;
  }

  template<typename Fn>
  IRS_FORCE_INLINE auto WithBlockTest(doc_id_t first, doc_id_t last, Fn&& fn) {
    if (last < _end && ((first ^ last) >> kChunkShift) == 0) {
      if (first - _base >= kChunkDocs) {
        Rebase(first);
      }
      return fn([words = _words, base = _base](doc_id_t doc) noexcept {
        const doc_id_t offset = doc - base;
        return ((words[offset / 64] >> (offset % 64)) & 1) != 0;
      });
    }
    return fn([this](doc_id_t doc) noexcept { return Test(doc); });
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t doc) const noexcept {
    if (doc >= _end) [[unlikely]] {
      return doc;
    }
    const doc_id_t offset = doc - _begin;
    const auto chunk = offset >> kChunkShift;
    if (chunk >= _count) [[unlikely]] {
      return doc < _begin ? std::min(_begin, _end) : _end;
    }
    const auto low = offset & kChunkLow;
    const auto rest =
      _layout.At(chunk)->words[low / 64] & (~uint64_t{0} << (low % 64));
    const auto found =
      uint64_t{doc} - low % 64 + static_cast<uint64_t>(std::countr_zero(rest));
    return static_cast<doc_id_t>(std::min<uint64_t>(found, _end));
  }

  doc_id_t NextLive(doc_id_t doc) const noexcept {
    uint64_t at = doc;
    while (at < _end) {
      const auto offset = at - _begin;
      if (at < _begin || (offset >> kChunkShift) >= _count) {
        return static_cast<doc_id_t>(at);
      }
      const auto* words =
        _layout.At(static_cast<uint32_t>(offset >> kChunkShift))->words;
      const auto base = at - (offset & kChunkLow);
      auto w = static_cast<uint32_t>((offset & kChunkLow) / 64);
      auto zeros = ~words[w] & (~uint64_t{0} << (offset % 64));
      while (zeros == 0 && ++w != kChunkWords) {
        zeros = ~words[w];
      }
      if (zeros != 0) {
        const auto found = base + uint64_t{w} * 64 +
                           static_cast<uint64_t>(std::countr_zero(zeros));
        return found < _end ? static_cast<doc_id_t>(found) : doc_limits::eof();
      }
      at = base + kChunkDocs;
    }
    return doc_limits::eof();
  }

 private:
  IRS_NO_INLINE void Rebase(doc_id_t doc) noexcept {
    _base = doc & ~kChunkLow;
    const doc_id_t offset = doc - _begin;
    const auto chunk = offset >> kChunkShift;
    _words =
      doc >= _begin && chunk < _count ? _layout.At(chunk)->words : kNoWords;
  }

  alignas(64) static constexpr uint64_t kNoWords[kChunkWords] = {};

  Layout _layout;
  doc_id_t _begin;
  uint32_t _count;
  doc_id_t _end;
  doc_id_t _base = 0;
  const uint64_t* _words = kNoWords;
};

}  // namespace irs::docs_mask
